/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.confignode.procedure.scheduler.SimpleProcedureScheduler;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.confignode.procedure.Procedure.NO_PROC_ID;

public class ProcedureExecutor<Env> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureExecutor.class);
  private static final ThreadLocal<Boolean> PROCEDURE_EXECUTION_CONTEXT =
      ThreadLocal.withInitial(() -> false);

  private final ConcurrentHashMap<Long, CompletedProcedureContainer<Env>> completed =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Long, RootProcedureStack<Env>> rollbackStack =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Long, Procedure<Env>> procedures = new ConcurrentHashMap<>();

  private ThreadGroup threadGroup;

  private CopyOnWriteArrayList<WorkerThread> workerThreads;

  private TimeoutExecutorThread<Env> timeoutExecutor;

  private TimeoutExecutorThread<Env> workerMonitorExecutor;

  private int corePoolSize;
  private int maxPoolSize;

  /**
   * The internal cleaner that recycles completed procedures. Kept as a reference so that its clean
   * interval / evict TTL can be reloaded at runtime (see {@link #restartCompletedCleaner}). All
   * accesses ({@link #startCompletedCleaner} on leader transition, {@link #restartCompletedCleaner}
   * on hot reload, and the test getter) are performed while holding this instance's monitor, so the
   * field is guarded by {@code synchronized} for both mutual exclusion and cross-thread visibility.
   */
  private CompletedProcedureRecycler<Env> completedProcedureRecycler;

  private final ProcedureScheduler scheduler;

  private final AtomicLong workId = new AtomicLong(0);
  private final AtomicInteger activeExecutorCount = new AtomicInteger(0);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Env environment;
  private final IProcedureStore<Env> store;

  private static final class LockStateResult<Env> {
    private final ProcedureLockState lockState;
    private final Procedure<Env> procedure;

    private LockStateResult(ProcedureLockState lockState, Procedure<Env> procedure) {
      this.lockState = lockState;
      this.procedure = procedure;
    }
  }

  public ProcedureExecutor(
      final Env environment, final IProcedureStore<Env> store, final ProcedureScheduler scheduler) {
    this.environment = environment;
    this.scheduler = scheduler;
    this.store = store;
  }

  @TestOnly
  public ProcedureExecutor(final Env environment, final IProcedureStore<Env> store) {
    this(environment, store, new SimpleProcedureScheduler());
  }

  public static boolean isProcedureExecutionThread() {
    return PROCEDURE_EXECUTION_CONTEXT.get();
  }

  public void init(int numThreads) {
    this.corePoolSize = numThreads;
    this.maxPoolSize = 10 * numThreads;
    this.threadGroup = new ThreadGroup(ThreadName.CONFIG_NODE_PROCEDURE_WORKER.getName());
    this.timeoutExecutor =
        new TimeoutExecutorThread<>(
            this, threadGroup, ThreadName.CONFIG_NODE_TIMEOUT_EXECUTOR.getName());
    this.workerMonitorExecutor =
        new TimeoutExecutorThread<>(
            this, threadGroup, ThreadName.CONFIG_NODE_WORKER_THREAD_MONITOR.getName());
    workId.set(0);
    workerThreads = new CopyOnWriteArrayList<>();
    for (int i = 0; i < corePoolSize; i++) {
      workerThreads.add(new WorkerThread(threadGroup));
    }
    // Add worker monitor
    workerMonitorExecutor.add(new WorkerMonitor());

    scheduler.start();
    recover();
  }

  private void recover() {
    // 1.Build rollback stack
    List<Procedure<Env>> procedureList = getProcedureListFromDifferentVersion();
    // Load procedure wal file
    for (Procedure<Env> proc : procedureList) {
      if (proc.isFinished()) {
        completed.putIfAbsent(proc.getProcId(), new CompletedProcedureContainer<>(proc));
      } else {
        if (!proc.hasParent()) {
          rollbackStack.put(proc.getProcId(), new RootProcedureStack<>());
        }
        procedures.putIfAbsent(proc.getProcId(), proc);
      }
    }
    List<Procedure<Env>> runnableList = new ArrayList<>();
    List<Procedure<Env>> failedList = new ArrayList<>();
    List<Procedure<Env>> waitingList = new ArrayList<>();
    List<Procedure<Env>> waitingTimeoutList = new ArrayList<>();
    for (Procedure<Env> proc : procedureList) {
      if (proc.isFinished() && !proc.hasParent()) {
        continue;
      }
      long rootProcedureId = getRootProcedureId(proc);
      if (proc.hasParent()) {
        Procedure<Env> parent = procedures.get(proc.getParentProcId());
        if (parent != null && !proc.isFinished()) {
          parent.incChildrenLatch();
        }
      }
      RootProcedureStack<Env> rootStack = rollbackStack.get(rootProcedureId);
      if (rootStack != null) {
        rootStack.loadStack(proc);
      }
      proc.setRootProcedureId(rootProcedureId);
      switch (proc.getState()) {
        case RUNNABLE:
          runnableList.add(proc);
          break;
        case FAILED:
          failedList.add(proc);
          break;
        case WAITING:
          waitingList.add(proc);
          break;
        case WAITING_TIMEOUT:
          waitingTimeoutList.add(proc);
          break;
        case ROLLEDBACK:
        case INITIALIZING:
          LOG.error(ProcedureMessages.UNEXPECTED_STATE_FOR, proc.getState(), proc);
          throw new UnsupportedOperationException(ProcedureMessages.UNEXPECTED_STATE);
        default:
          break;
      }
    }

    waitingList.forEach(
        procedure -> {
          if (!procedure.hasChildren()) {
            // Normally, WAITING procedures should be wakened by its children.
            // But, there is a case that, all the children are successful, and before
            // they can wake up their parent procedure, the master was killed.
            // So, during recovering the procedures from ProcedureWal, its children
            // are not loaded because of their SUCCESS state.
            // So we need to continue to run this WAITING procedure. Before
            // executing, we need to set its state to RUNNABLE.
            procedure.setState(ProcedureState.RUNNABLE);
            runnableList.add(procedure);
          }
        });
    // A submission-time deserialization failure may be persisted before a rollback stack index.
    failedList.forEach(
        procedure -> {
          RootProcedureStack<Env> rootStack = rollbackStack.get(getRootProcedureId(procedure));
          if (rootStack != null) {
            initializeRollbackStackForFailedProcedure(rootStack, procedure);
          }
        });
    restoreLocks();

    waitingTimeoutList.forEach(timeoutExecutor::add);

    failedList.forEach(scheduler::addBack);
    runnableList.forEach(scheduler::addBack);
    scheduler.signalAll();
  }

  private List<Procedure<Env>> getProcedureListFromDifferentVersion() {
    if (store.isOldVersionProcedureStore()) {
      LOG.info(ProcedureMessages.OLD_PROCEDURE_DIRECTORY_DETECTED_UPGRADE_BEGINNING);
      return store.load();
    } else {
      return store.getProcedures();
    }
  }

  /**
   * Helper to look up the root Procedure ID.
   *
   * @param proc given a specified procedure.
   */
  Long getRootProcedureId(Procedure<Env> proc) {
    while (proc.hasParent()) {
      proc = procedures.get(proc.getParentProcId());
      if (proc == null) {
        return NO_PROC_ID;
      }
    }
    return proc.getProcId();
  }

  private void releaseLock(Procedure<Env> procedure, boolean force) {
    if (force || !procedure.holdLock(this.environment) || procedure.isFinished()) {
      RetryUtils.executeWithEndlessBackoffRetry(
          () -> procedure.doReleaseLock(this.environment, store), "procedure release lock");
    }
  }

  private void restoreLock(Procedure procedure, Set<Long> restored) {
    procedure.restoreLock(environment);
    restored.add(procedure.getProcId());
  }

  private void restoreLocks(Deque<Procedure<Env>> stack, Set<Long> restored) {
    while (!stack.isEmpty()) {
      restoreLock(stack.pop(), restored);
    }
  }

  private void restoreLocks() {
    Set<Long> restored = new HashSet<>();
    Deque<Procedure<Env>> stack = new ArrayDeque<>();
    procedures
        .values()
        .forEach(
            procedure -> {
              while (procedure != null) {
                if (restored.contains(procedure.getProcId())) {
                  restoreLocks(stack, restored);
                  return;
                }
                if (!procedure.hasParent()) {
                  restoreLock(procedure, restored);
                  restoreLocks(stack, restored);
                  return;
                }
                stack.push(procedure);
                procedure = procedures.get(procedure.getParentProcId());
              }
            });
  }

  public void startWorkers() {
    if (!running.compareAndSet(false, true)) {
      LOG.warn(ProcedureMessages.ALREADY_RUNNING);
      return;
    }
    timeoutExecutor.start();
    workerMonitorExecutor.start();
    for (WorkerThread workerThread : workerThreads) {
      workerThread.start();
    }
    LOG.info(ProcedureMessages.PROCEDURE_WORKERS_ARE_STARTED, workerThreads.size());
  }

  public synchronized void startCompletedCleaner(long cleanTimeInterval, long cleanEvictTTL) {
    completedProcedureRecycler =
        new CompletedProcedureRecycler<>(store, completed, cleanTimeInterval, cleanEvictTTL);
    addInternalProcedure(completedProcedureRecycler);
  }

  /**
   * Reload the completed-procedure cleaner with a new clean interval / evict TTL at runtime. The
   * clean interval and evict TTL are captured by {@link CompletedProcedureRecycler} at
   * construction, so applying the new values requires removing the current recycler and scheduling
   * a fresh one.
   */
  public synchronized void restartCompletedCleaner(long cleanTimeInterval, long cleanEvictTTL) {
    if (completedProcedureRecycler != null) {
      removeInternalProcedure(completedProcedureRecycler);
    }
    startCompletedCleaner(cleanTimeInterval, cleanEvictTTL);
  }

  @TestOnly
  synchronized CompletedProcedureRecycler<Env> getCompletedProcedureRecycler() {
    return completedProcedureRecycler;
  }

  public void addInternalProcedure(InternalProcedure interalProcedure) {
    if (interalProcedure == null) {
      return;
    }
    timeoutExecutor.add(interalProcedure);
  }

  public boolean removeInternalProcedure(InternalProcedure internalProcedure) {
    if (internalProcedure == null) {
      return true;
    }
    internalProcedure.setState(ProcedureState.SUCCESS);
    return timeoutExecutor.remove(internalProcedure);
  }

  /**
   * Executes procedure
   *
   * <p>Calls doExecute() if success and return subprocedures submit sub procs set the state to
   * WAITING, wait for all sub procs completed. else if no sub procs procedure completed
   * successfully set procedure's parent to RUNNABLE in case of failure start rollback of the
   * procedure.
   *
   * @param proc procedure
   */
  private void executeProcedure(Procedure<Env> proc) {
    if (proc.isFinished()) {
      LOG.debug(ProcedureMessages.IS_ALREADY_FINISHED, proc);
      return;
    }
    final Long rootProcId = getRootProcedureId(proc);
    if (rootProcId == null) {
      LOG.warn(ProcedureMessages.ROLLBACK_BECAUSE_PARENT_IS_DONE_ROLLEDBACK_PROC_IS, proc);
      executeRollback(proc);
      return;
    }
    RootProcedureStack<Env> rootProcStack = rollbackStack.get(rootProcId);
    if (rootProcStack == null) {
      LOG.warn(ProcedureMessages.ROLLBACK_STACK_IS_NULL_FOR, proc.getProcId());
      return;
    }
    ProcedureLockState lockState = null;
    Procedure<Env> lockEventWaitProcedure = null;
    try {
      do {
        if (!rootProcStack.acquire()) {
          if (rootProcStack.setRollback()) {
            LockStateResult<Env> lockStateResult =
                executeRootStackRollback(rootProcId, rootProcStack);
            lockState = lockStateResult.lockState;
            switch (lockState) {
              case LOCK_ACQUIRED:
                break;
              case LOCK_EVENT_WAIT:
                LOG.info(ProcedureMessages.LOCK_EVENT_WAIT_ROLLBACK, lockStateResult.procedure);
                rootProcStack.unsetRollback();
                lockEventWaitProcedure = lockStateResult.procedure;
                break;
              case LOCK_YIELD_WAIT:
                rootProcStack.unsetRollback();
                scheduler.yield(lockStateResult.procedure);
                break;
              default:
                throw new UnsupportedOperationException();
            }
          } else {
            if (!proc.wasExecuted()) {
              lockState = executeRollback(proc);
              switch (lockState) {
                case LOCK_ACQUIRED:
                  break;
                case LOCK_EVENT_WAIT:
                  LOG.info(
                      ProcedureMessages.LOCK_EVENT_WAIT_CAN_T_ROLLBACK_CHILD_RUNNING_FOR, proc);
                  lockEventWaitProcedure = proc;
                  break;
                case LOCK_YIELD_WAIT:
                  scheduler.yield(proc);
                  break;
                default:
                  throw new UnsupportedOperationException();
              }
            }
          }
          break;
        }
        try {
          lockState = acquireLock(proc);
          switch (lockState) {
            case LOCK_ACQUIRED:
              executeProcedure(rootProcStack, proc);
              break;
            case LOCK_YIELD_WAIT:
            case LOCK_EVENT_WAIT:
              LOG.info(ProcedureMessages.LOCKSTATE_IS, proc, lockState);
              if (lockState == ProcedureLockState.LOCK_EVENT_WAIT) {
                lockEventWaitProcedure = proc;
              }
              break;
            default:
              throw new UnsupportedOperationException();
          }
        } finally {
          rootProcStack.release();
        }

        if (proc.isSuccess()) {
          // update metrics on finishing the procedure
          proc.updateMetricsOnFinish(getEnvironment(), proc.elapsedTime(), true);
          LOG.debug(ProcedureMessages.FINISHED_IN_MS_SUCCESSFULLY, proc, proc.elapsedTime());
          if (proc.getProcId() == rootProcId) {
            rootProcedureCleanup(proc);
          } else {
            executeCompletionCleanup(proc);
          }
          return;
        }

      } while (rootProcStack.isFailed());
    } finally {
      // Only after procedure has completed execution can it be allowed to be rescheduled to prevent
      // data races
      if (lockEventWaitProcedure != null) {
        LOG.info(ProcedureMessages.PROCEDUREID_WAIT_FOR_LOCK, lockEventWaitProcedure.getProcId());
        lockEventWaitProcedure.waitForLock(this.environment);
      }
    }
  }

  /**
   * execute procedure and submit its children
   *
   * @param rootProcStack procedure's root proc stack
   * @param proc procedure
   */
  private void executeProcedure(RootProcedureStack rootProcStack, Procedure<Env> proc) {
    if (proc.getState() != ProcedureState.RUNNABLE) {
      LOG.error(
          ProcedureMessages
              .LOG_EXECUTING_PROCEDURE_SHOULD_RUNNABLE_STATE_BUT_IT_S_NOT_PROCEDURE_7CF42CE8,
          proc);
      releaseLock(proc, false);
      return;
    }
    boolean reExecute;

    Procedure<Env>[] subprocs = null;
    do {
      reExecute = false;
      try {
        subprocs = proc.doExecute(this.environment);
        if (subprocs != null && subprocs.length == 0) {
          subprocs = null;
        }
      } catch (InterruptedException e) {
        LOG.warn(ProcedureMessages.INTERRUPT_DURING_EXECUTION_SUSPEND_OR_RETRY_IT_LATER, e);
        yieldProcedure(proc);
      } catch (Throwable e) {
        LOG.error(ProcedureMessages.CODE_BUG, proc, e);
        proc.setFailure(new ProcedureException(e.getMessage(), e));
      }

      if (!proc.isFailed()) {
        if (subprocs != null) {
          if (subprocs.length == 1 && subprocs[0] == proc) {
            subprocs = null;
            reExecute = true;
          } else {
            subprocs = initializeChildren(rootProcStack, proc, subprocs);
            LOG.info(ProcedureMessages.INITIALIZED_SUB_PROCS, Arrays.toString(subprocs));
          }
        } else if (proc.getState() == ProcedureState.WAITING_TIMEOUT) {
          LOG.info(ProcedureMessages.ADDED_INTO_TIMEOUTEXECUTOR, proc);
        } else {
          proc.setState(ProcedureState.SUCCESS);
        }
      }
      // add procedure into rollback stack.
      rootProcStack.addRollbackStep(proc);

      updateStoreOnExecution(rootProcStack, proc, subprocs);

      // Stop the in-place re-execution loop once this executor is shutting down (e.g. ConfigNode
      // leader switch / restart). Checking store.isRunning() alone is not enough: stopExecutor()
      // calls executor.stop() and executor.join() before store.stop(), so the store is still
      // running while join() waits for this very worker to finish. Without also checking the
      // executor's own running flag, a procedure that keeps returning HAS_MORE_STATE for the same
      // state (e.g. AddRegionPeerProcedure parking at DO_ADD_REGION_PEER after waitTaskFinish() is
      // interrupted) would re-execute forever here and join() would hang. The persisted state lets
      // the next leader resume from where it stopped.
      if (!isRunning() || !store.isRunning()) {
        return;
      }

      if (proc.isRunnable() && proc.isYieldAfterExecution(this.environment)) {
        yieldProcedure(proc);
        return;
      }
    } while (reExecute);

    if (subprocs != null && !proc.isFailed()) {
      submitChildrenProcedures(subprocs);
    }

    releaseLock(proc, false);
    if (proc.isFinished() && proc.hasParent()) {
      countDownChildren(rootProcStack, proc);
    }
  }

  /**
   * Serve as a countdown latch to check whether all children have already completed.
   *
   * @param rootProcStack root procedure stack
   * @param proc proc
   */
  private void countDownChildren(RootProcedureStack rootProcStack, Procedure<Env> proc) {
    Procedure<Env> parent = procedures.get(proc.getParentProcId());
    if (parent == null && rootProcStack.isRollingback()) {
      return;
    }
    if (parent != null && parent.tryRunnable()) {
      // If success, means all its children have completed, move parent to front of the queue.
      // Must endless retry here, since this step is not idempotent and can not be re-execute
      // correctly in new CN leader.
      RetryUtils.executeWithEndlessBackoffRetry(
          () -> store.update(parent), "count down children procedure");
      // do not add this procedure when exception occurred
      scheduler.addFront(parent);
      LOG.info(
          ProcedureMessages.LOG_FINISHED_SUBPROCEDURE_PID_ARG_RESUME_PROCESSING_PPID_ARG_93ED990B,
          proc.getProcId(),
          parent.getProcId());
    }
  }

  /**
   * Submit children procedures.
   *
   * @param subprocs children procedures
   */
  private void submitChildrenProcedures(Procedure<Env>[] subprocs) {
    for (Procedure<Env> subproc : subprocs) {
      subproc.updateMetricsOnSubmit(getEnvironment());
      procedures.put(subproc.getProcId(), subproc);
      scheduler.addFront(subproc);
      LOG.info(ProcedureMessages.SUB_PROCEDURE_PID_HAS_BEEN_SUBMITTED, subproc.getProcId());
    }
  }

  private void updateStoreOnExecution(
      RootProcedureStack rootProcStack, Procedure<Env> proc, Procedure<Env>[] subprocs) {
    if (subprocs != null && !proc.isFailed()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(ProcedureMessages.STORED_CHILDREN, proc, Arrays.toString(subprocs));
      }
      try {
        store.update(subprocs);
      } catch (Exception e) {
        // Do nothing since this step is idempotent. New CN leader can converge to the correct
        // state when restore this procedure.
        LOG.warn(ProcedureMessages.FAILED_TO_UPDATE_SUBPROCS_ON_EXECUTION, e);
      }
    } else {
      LOG.debug(ProcedureMessages.STORE_UPDATE, proc);
      if (proc.isFinished() && !proc.hasParent()) {
        final long[] childProcIds = rootProcStack.getSubprocedureIds();
        if (childProcIds != null) {
          try {
            store.delete(childProcIds);
            // do not remove these procedures when exception occurred
            for (long childProcId : childProcIds) {
              procedures.remove(childProcId);
            }
          } catch (Exception e) {
            // Do nothing since this step is idempotent. New CN leader can converge to the correct
            // state when restore this procedure.
            LOG.warn(ProcedureMessages.FAILED_TO_DELETE_SUBPROCEDURES_ON_EXECUTION, e);
          }
        } else {
          try {
            store.update(proc);
          } catch (Exception e) {
            LOG.warn(ProcedureMessages.FAILED_TO_UPDATE_PROCEDURE_ON_EXECUTION, e);
          }
        }
      } else {
        try {
          store.update(proc);
        } catch (Exception e) {
          // Do nothing since this step is idempotent. New CN leader can converge to the correct
          // state when restore this procedure.
          LOG.warn(ProcedureMessages.FAILED_TO_UPDATE_PROCEDURE_ON_EXECUTION, e);
        }
      }
    }
  }

  private Procedure<Env>[] initializeChildren(
      RootProcedureStack rootProcStack, Procedure<Env> proc, Procedure<Env>[] subprocs) {
    final long rootProcedureId = getRootProcedureId(proc);
    for (int i = 0; i < subprocs.length; i++) {
      Procedure<Env> subproc = subprocs[i];
      if (subproc == null) {
        String errMsg = "subproc[" + i + "] is null, aborting procedure";
        proc.setFailure(new ProcedureException((errMsg), new IllegalArgumentException(errMsg)));
        return null;
      }
      subproc.setParentProcId(proc.getProcId());
      subproc.setRootProcId(rootProcedureId);
      subproc.setProcId(store.getNextProcId());
      subproc.setProcRunnable();
      rootProcStack.addSubProcedure(subproc);
    }

    if (!proc.isFailed()) {
      proc.setChildrenLatch(subprocs.length);
      switch (proc.getState()) {
        case RUNNABLE:
          proc.setState(ProcedureState.WAITING);
          break;
        case WAITING_TIMEOUT:
          timeoutExecutor.add(proc);
          break;
        default:
          break;
      }
    }
    return subprocs;
  }

  private void yieldProcedure(Procedure<Env> proc) {
    releaseLock(proc, false);
    scheduler.yield(proc);
  }

  /**
   * Rollback full root procedure stack.
   *
   * @param rootProcId root procedure id
   * @param procedureStack root procedure stack
   * @return lock state
   */
  private LockStateResult<Env> executeRootStackRollback(
      Long rootProcId, RootProcedureStack<Env> procedureStack) {
    Procedure<Env> rootProcedure = procedures.get(rootProcId);
    ProcedureException exception = rootProcedure.getException();
    if (exception == null) {
      exception = procedureStack.getException();
      rootProcedure.setFailure(exception);
      // Endless retry since this step is not idempotent.
      RetryUtils.executeWithEndlessBackoffRetry(
          () -> store.update(rootProcedure), "root procedure rollback");
    }
    List<Procedure<Env>> subprocStack = procedureStack.getSubproceduresStack();
    int stackTail = subprocStack.size();
    while (stackTail-- > 0) {
      Procedure<Env> procedure = subprocStack.get(stackTail);
      if (procedure.isSuccess()) {
        subprocStack.remove(stackTail);
        cleanupAfterRollback(procedure);
        continue;
      }
      ProcedureLockState lockState = acquireLock(procedure);
      if (lockState != ProcedureLockState.LOCK_ACQUIRED) {
        return new LockStateResult<>(lockState, procedure);
      }
      lockState = executeRollback(procedure);
      releaseLock(procedure, false);

      boolean abortRollback = lockState != ProcedureLockState.LOCK_ACQUIRED;
      abortRollback |= !isRunning() || !store.isRunning();
      if (abortRollback) {
        return new LockStateResult<>(lockState, procedure);
      }

      if (!procedure.isFinished() && procedure.isYieldAfterExecution(this.environment)) {
        return new LockStateResult<>(ProcedureLockState.LOCK_YIELD_WAIT, procedure);
      }

      if (procedure != rootProcedure) {
        executeCompletionCleanup(procedure);
      }
    }

    LOG.info(
        ProcedureMessages.ROLLED_BACK_TIME_DURATION_IS, rootProcedure, rootProcedure.elapsedTime());
    rootProcedureCleanup(rootProcedure);
    return new LockStateResult<>(ProcedureLockState.LOCK_ACQUIRED, rootProcedure);
  }

  private ProcedureLockState acquireLock(Procedure<Env> proc) {
    if (proc.hasLock()) {
      return ProcedureLockState.LOCK_ACQUIRED;
    }
    return proc.doAcquireLock(this.environment, store);
  }

  /**
   * do execute defined in procedure and then update store or remove completely in case it is a
   * child.
   *
   * @param procedure procedure
   * @return procedure lock state
   */
  private ProcedureLockState executeRollback(Procedure<Env> procedure) {
    try {
      procedure.doRollback(this.environment);
    } catch (IOException e) {
      LOG.error(ProcedureMessages.ROLL_BACK_FAILED_FOR, procedure, e);
    } catch (InterruptedException e) {
      LOG.warn(ProcedureMessages.INTERRUPTED_EXCEPTION_OCCURRED_FOR, procedure, e);
    } catch (Throwable t) {
      LOG.error(ProcedureMessages.CODE_BUG_RUNTIME_EXCEPTION_FOR, procedure, t);
    }
    cleanupAfterRollback(procedure);
    return ProcedureLockState.LOCK_ACQUIRED;
  }

  private void cleanupAfterRollback(Procedure<Env> procedure) {
    if (procedure.removeStackIndex()) {
      if (!procedure.isSuccess()) {
        procedure.setState(ProcedureState.ROLLEDBACK);
      }

      // update metrics on finishing the procedure (fail)
      procedure.updateMetricsOnFinish(getEnvironment(), procedure.elapsedTime(), false);

      if (procedure.hasParent()) {
        try {
          store.delete(procedure.getProcId());
          // do not remove this procedure when exception occurred
          procedures.remove(procedure.getProcId());
        } catch (Exception e) {
          // Do nothing since this step is idempotent. New CN leader can converge to the correct
          // state when restore this procedure.
          LOG.warn(ProcedureMessages.FAILED_TO_DELETE_PROCEDURE_ON_ROLLBACK, e);
        }
      } else {
        final long[] childProcIds = rollbackStack.get(procedure.getProcId()).getSubprocedureIds();
        try {
          if (childProcIds != null) {
            store.delete(childProcIds);
          } else {
            store.update(procedure);
          }
        } catch (Exception e) {
          // Do nothing since this step is idempotent. New CN leader can converge to the correct
          // state when restore this procedure.
          LOG.warn(ProcedureMessages.FAILED_TO_DELETE_PROCEDURE_ON_ROLLBACK, e);
        }
      }
    } else {
      try {
        store.update(procedure);
      } catch (Exception e) {
        // Do nothing since this step is idempotent. New CN leader can converge to the correct
        // state when restore this procedure.
        LOG.warn(ProcedureMessages.FAILED_TO_UPDATE_PROCEDURE_ON_ROLLBACK, e);
      }
    }
  }

  private void executeCompletionCleanup(Procedure<Env> proc) {
    if (proc.hasLock()) {
      releaseLock(proc, true);
    }
  }

  private void rootProcedureCleanup(Procedure<Env> proc) {
    executeCompletionCleanup(proc);
    CompletedProcedureContainer<Env> retainer = new CompletedProcedureContainer<>(proc);
    completed.put(proc.getProcId(), retainer);
    rollbackStack.remove(proc.getProcId());
    procedures.remove(proc.getProcId());
  }

  /**
   * Add a Procedure to executor.
   *
   * @param procedure procedure
   * @return procedure id
   */
  private long pushProcedure(Procedure<Env> procedure) {
    final long currentProcId = procedure.getProcId();
    // Update metrics on start of a procedure
    procedure.updateMetricsOnSubmit(getEnvironment());
    RootProcedureStack<Env> stack = new RootProcedureStack<>();
    // Persisting a newly submitted procedure may serialize and deserialize it through the
    // consensus layer. If that process marks the procedure as failed before it is scheduled, the
    // rollback stack still needs an entry so the executor can finish the failed procedure instead
    // of leaving it in the active procedure map forever.
    if (initializeRollbackStackForFailedProcedure(stack, procedure)) {
      try {
        store.update(procedure);
      } catch (Exception e) {
        LOG.error(ProcedureMessages.FAILED_TO_UPDATE_STORE_PROCEDURE, procedure, e);
      }
    }
    rollbackStack.put(currentProcId, stack);
    procedures.put(currentProcId, procedure);
    scheduler.addBack(procedure);
    return procedure.getProcId();
  }

  private boolean initializeRollbackStackForFailedProcedure(
      RootProcedureStack<Env> stack, Procedure<Env> procedure) {
    if (procedure.isFailed() && !procedure.wasExecuted()) {
      stack.addRollbackStep(procedure);
      return true;
    }
    return false;
  }

  private class WorkerThread extends StoppableThread {
    private final AtomicLong startTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicReference<Procedure<Env>> activeProcedure = new AtomicReference<>();
    protected long keepAliveTime = -1;

    public WorkerThread(ThreadGroup threadGroup) {
      this(threadGroup, "ProcedureCoreWorker-");
    }

    public WorkerThread(ThreadGroup threadGroup, String prefix) {
      super(threadGroup, prefix + workId.incrementAndGet());
      setDaemon(true);
    }

    @Override
    public void sendStopSignal() {
      scheduler.signalAll();
    }

    @Override
    public void run() {
      long lastUpdated = System.currentTimeMillis();
      try {
        while (isRunning() && keepAlive(lastUpdated)) {
          Procedure<Env> procedure = scheduler.poll(keepAliveTime, TimeUnit.MILLISECONDS);
          if (procedure == null) {
            Thread.sleep(1000);
            continue;
          }
          boolean executionAcquired = false;
          while (isRunning() && !(executionAcquired = procedure.tryAcquireExecution())) {
            Thread.sleep(10);
          }
          if (!executionAcquired) {
            continue;
          }
          try {
            this.activeProcedure.set(procedure);
            activeExecutorCount.incrementAndGet();
            startTime.set(System.currentTimeMillis());
            try {
              PROCEDURE_EXECUTION_CONTEXT.set(true);
              try {
                executeProcedure(procedure);
              } finally {
                PROCEDURE_EXECUTION_CONTEXT.remove();
              }
            } finally {
              procedure.releaseExecution();
              activeExecutorCount.decrementAndGet();
              LOG.trace(
                  ProcedureMessages.MESSAGE_HALT_PID_ARG_ACTIVECOUNT_ARG_411F3EBF,
                  procedure.getProcId(),
                  activeExecutorCount.get());
              this.activeProcedure.set(null);
              lastUpdated = System.currentTimeMillis();
              startTime.set(lastUpdated);
            }
          } catch (Exception e) {
            LOG.warn(
                ProcedureMessages
                    .MESSAGE_EXCEPTION_HAPPENED_WHEN_WORKER_ARG_EXECUTE_PROCEDURE_ARG_6E3AD27D,
                getName(),
                procedure,
                e);
            throw e;
          }
        }

      } catch (Exception e) {
        if (this.activeProcedure.get() != null) {
          LOG.warn(
              ProcedureMessages.LOG_EXCEPTION_HAPPENED_WORKER_ARG_EXECUTE_PROCEDURE_ARG_6E3AD27D,
              getName(),
              this.activeProcedure.get(),
              e);
        }
        this.activeProcedure.set(null);
      } finally {
        LOG.info(ProcedureMessages.PROCEDURE_WORKER_TERMINATED, getName());
      }
      workerThreads.remove(this);
    }

    protected boolean keepAlive(long lastUpdated) {
      return true;
    }

    @Override
    public String toString() {
      Procedure<?> p = this.activeProcedure.get();
      return getName() + "(pid=" + (p == null ? NO_PROC_ID : p.getProcId() + ")");
    }

    /**
     * @return the time since the current procedure is running
     */
    public long getCurrentRunTime() {
      return System.currentTimeMillis() - startTime.get();
    }
  }

  // A temporary worker thread will be launched when too many core workers are stuck.
  // They will timeout after keepAliveTime if there is no procedure to run.
  private final class TemporaryWorkerThread extends WorkerThread {

    public TemporaryWorkerThread(ThreadGroup group) {
      super(group, "ProcedureTemporaryWorker-");
      this.keepAliveTime = TimeUnit.SECONDS.toMillis(10);
    }

    @Override
    protected boolean keepAlive(long lastUpdate) {
      return System.currentTimeMillis() - lastUpdate < keepAliveTime;
    }
  }

  private final class WorkerMonitor extends InternalProcedure<Env> {
    private static final int DEFAULT_WORKER_MONITOR_INTERVAL = 30000; // 30sec

    private static final int DEFAULT_WORKER_STUCK_THRESHOLD = 60000; // 60sec

    private static final float DEFAULT_WORKER_ADD_STUCK_PERCENTAGE = 0.5f; // 50% stuck

    public WorkerMonitor() {
      super(DEFAULT_WORKER_MONITOR_INTERVAL);
      updateTimestamp();
    }

    private int calculateRunningAndStuckWorkers() {
      // Check if any of the worker is stuck
      int runningCount = 0, stuckCount = 0;
      for (WorkerThread worker : workerThreads) {
        final Procedure<?> proc = worker.activeProcedure.get();
        if (proc == null) {
          continue;
        }
        runningCount++;
        // WARN the worker is stuck
        if (worker.getCurrentRunTime() > DEFAULT_WORKER_STUCK_THRESHOLD) {
          stuckCount++;
          LOG.warn(
              ProcedureMessages.LOG_WORKER_STUCK_ARG_ARG_RUN_TIME_ARG_MS_FB612354,
              worker,
              proc.getProcType(),
              worker.getCurrentRunTime());
        }
        LOG.info(
            ProcedureMessages.LOG_PROCEDURE_WORKERS_ARG_RUNNING_ARG_RUNNING_STUCK_1565936D,
            runningCount,
            stuckCount);
      }
      return stuckCount;
    }

    private void checkThreadCount(final int stuckCount) {
      // Nothing to do if there are no runnable tasks
      if (stuckCount < 1 || !scheduler.hasRunnables()) {
        return;
      }
      // Add a new thread if the worker stuck percentage exceed the threshold limit
      // and every handler is active.
      final float stuckPerc = ((float) stuckCount) / workerThreads.size();
      // Let's add new worker thread more aggressively, as they will timeout finally if there is no
      // work to do.
      if (stuckPerc >= DEFAULT_WORKER_ADD_STUCK_PERCENTAGE && workerThreads.size() < maxPoolSize) {
        final TemporaryWorkerThread worker = new TemporaryWorkerThread(threadGroup);
        workerThreads.add(worker);
        worker.start();
        LOG.debug(ProcedureMessages.ADDED_NEW_WORKER_THREAD, worker);
      }
    }

    @Override
    protected void periodicExecute(Env env) {
      final int stuckCount = calculateRunningAndStuckWorkers();
      checkThreadCount(stuckCount);
      updateTimestamp();
    }
  }

  public int getWorkerThreadCount() {
    return workerThreads.size();
  }

  public long getActiveWorkerThreadCount() {
    return workerThreads.stream().filter(worker -> worker.activeProcedure.get() != null).count();
  }

  public boolean isRunning() {
    return running.get();
  }

  public void stop() {
    if (!running.getAndSet(false)) {
      return;
    }
    LOG.info(ProcedureMessages.STOPPING);
    scheduler.stop();
    timeoutExecutor.sendStopSignal();
  }

  public void join() {
    timeoutExecutor.awaitTermination();
    workerMonitorExecutor.awaitTermination();
    for (WorkerThread workerThread : workerThreads) {
      workerThread.awaitTermination();
    }
    try {
      threadGroup.destroy();
    } catch (IllegalThreadStateException e) {
      LOG.warn(
          ProcedureMessages
              .LOG_PROCEDUREEXECUTOR_THREADGROUP_ARG_CONTAINS_RUNNING_THREADS_WHICH_USED_NON_PROCEDURE_BD865211,
          this.threadGroup);
      this.threadGroup.list();
    }
  }

  public boolean isStarted(long procId) {
    Procedure<Env> procedure = procedures.get(procId);
    if (procedure == null) {
      return completed.get(procId) != null;
    }
    return procedure.wasExecuted();
  }

  public boolean isFinished(final long procId) {
    return !procedures.containsKey(procId);
  }

  public ConcurrentHashMap<Long, Procedure<Env>> getProcedures() {
    return procedures;
  }

  // -----------------------------CLIENT IMPLEMENTATION-----------------------------------
  /**
   * Submit a new root-procedure to the executor, called by client.
   *
   * @param procedure root procedure
   * @return procedure id
   */
  public long submitProcedure(Procedure<Env> procedure) {
    Preconditions.checkArgument(procedure.getState() == ProcedureState.INITIALIZING);
    Preconditions.checkArgument(
        !procedure.hasParent(), ProcedureMessages.EXCEPTION_UNEXPECTED_PARENT_444B4289, procedure);
    // Initialize the procedure
    procedure.setProcId(store.getNextProcId());
    procedure.setProcRunnable();
    // Commit the transaction
    try {
      store.update(procedure);
    } catch (Exception e) {
      LOG.error(ProcedureMessages.FAILED_TO_UPDATE_STORE_PROCEDURE, procedure, e);
    }
    LOG.debug(ProcedureMessages.IS_STORED, procedure);
    // Add the procedure to the executor
    return pushProcedure(procedure);
  }

  public ProcedureScheduler getScheduler() {
    return scheduler;
  }

  public Env getEnvironment() {
    return environment;
  }

  public IProcedureStore getStore() {
    return store;
  }

  public RootProcedureStack<Env> getRollbackStack(long rootProcId) {
    return rollbackStack.get(rootProcId);
  }
}
