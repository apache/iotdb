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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
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
import java.util.Objects;
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

  private final ProcedureScheduler scheduler;

  private final AtomicLong workId = new AtomicLong(0);
  private final AtomicInteger activeExecutorCount = new AtomicInteger(0);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Env environment;
  private final IProcedureStore<Env> store;

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
          LOG.error("Unexpected state:{} for {}", proc.getState(), proc);
          throw new UnsupportedOperationException("Unexpected state");
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
          } else {
            procedure.afterRecover(environment);
          }
        });
    restoreLocks();

    waitingTimeoutList.forEach(
        procedure -> {
          procedure.afterRecover(environment);
          timeoutExecutor.add(procedure);
        });

    failedList.forEach(scheduler::addBack);
    runnableList.forEach(
        procedure -> {
          procedure.afterRecover(environment);
          scheduler.addBack(procedure);
        });
    scheduler.signalAll();
  }

  private List<Procedure<Env>> getProcedureListFromDifferentVersion() {
    if (store.isOldVersionProcedureStore()) {
      LOG.info("Old procedure directory detected, upgrade beginning...");
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
      procedure.doReleaseLock(this.environment, store);
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
      LOG.warn("Already running");
      return;
    }
    timeoutExecutor.start();
    workerMonitorExecutor.start();
    for (WorkerThread workerThread : workerThreads) {
      workerThread.start();
    }
    LOG.info("{} procedure workers are started.", workerThreads.size());
  }

  public void startCompletedCleaner(long cleanTimeInterval, long cleanEvictTTL) {
    addInternalProcedure(
        new CompletedProcedureRecycler(store, completed, cleanTimeInterval, cleanEvictTTL));
  }

  private void addInternalProcedure(InternalProcedure interalProcedure) {
    if (interalProcedure == null) {
      return;
    }
    interalProcedure.setState(ProcedureState.WAITING_TIMEOUT);
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
      LOG.debug("{} is already finished.", proc);
      return;
    }
    final Long rootProcId = getRootProcedureId(proc);
    if (rootProcId == null) {
      LOG.warn("Rollback because parent is done/rolledback, proc is {}", proc);
      executeRollback(proc);
      return;
    }
    RootProcedureStack<Env> rootProcStack = rollbackStack.get(rootProcId);
    if (rootProcStack == null) {
      LOG.warn("Rollback stack is null for {}", proc.getProcId());
      return;
    }
    ProcedureLockState lockState = null;
    try {
      do {
        if (!rootProcStack.acquire()) {
          if (rootProcStack.setRollback()) {
            lockState = executeRootStackRollback(rootProcId, rootProcStack);
            switch (lockState) {
              case LOCK_ACQUIRED:
                break;
              case LOCK_EVENT_WAIT:
                LOG.info("LOCK_EVENT_WAIT rollback {}", proc);
                rootProcStack.unsetRollback();
                break;
              case LOCK_YIELD_WAIT:
                rootProcStack.unsetRollback();
                scheduler.yield(proc);
                break;
              default:
                throw new UnsupportedOperationException();
            }
          } else {
            if (!proc.wasExecuted()) {
              switch (executeRollback(proc)) {
                case LOCK_ACQUIRED:
                  break;
                case LOCK_EVENT_WAIT:
                  LOG.info("LOCK_EVENT_WAIT can't rollback child running for {}", proc);
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
        lockState = acquireLock(proc);
        switch (lockState) {
          case LOCK_ACQUIRED:
            executeProcedure(rootProcStack, proc);
            break;
          case LOCK_YIELD_WAIT:
          case LOCK_EVENT_WAIT:
            LOG.info("{} lockstate is {}", proc, lockState);
            break;
          default:
            throw new UnsupportedOperationException();
        }
        rootProcStack.release();

        if (proc.isSuccess()) {
          // update metrics on finishing the procedure
          proc.updateMetricsOnFinish(getEnvironment(), proc.elapsedTime(), true);
          LOG.debug("{} finished in {}ms successfully.", proc, proc.elapsedTime());
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
      if (Objects.equals(lockState, ProcedureLockState.LOCK_EVENT_WAIT)) {
        LOG.info("procedureId {} wait for lock.", proc.getProcId());
        ((ConfigNodeProcedureEnv) this.environment).getNodeLock().waitProcedure(proc);
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
          "The executing procedure should in RUNNABLE state, but it's not. Procedure is {}", proc);
      return;
    }
    boolean suspended = false;
    boolean reExecute;

    Procedure<Env>[] subprocs = null;
    do {
      reExecute = false;
      proc.resetPersistance();
      try {
        subprocs = proc.doExecute(this.environment);
        if (subprocs != null && subprocs.length == 0) {
          subprocs = null;
        }
      } catch (ProcedureSuspendedException e) {
        LOG.debug("Suspend {}", proc);
        suspended = true;
      } catch (ProcedureYieldException e) {
        LOG.debug("Yield {}", proc);
        yieldProcedure(proc);
      } catch (InterruptedException e) {
        LOG.warn("Interrupt during execution, suspend or retry it later.", e);
        yieldProcedure(proc);
      } catch (Throwable e) {
        LOG.error("CODE-BUG:{}", proc, e);
        proc.setFailure(new ProcedureException(e.getMessage(), e));
      }

      if (!proc.isFailed()) {
        if (subprocs != null) {
          if (subprocs.length == 1 && subprocs[0] == proc) {
            subprocs = null;
            reExecute = true;
          } else {
            subprocs = initializeChildren(rootProcStack, proc, subprocs);
            LOG.info("Initialized sub procs:{}", Arrays.toString(subprocs));
          }
        } else if (proc.getState() == ProcedureState.WAITING_TIMEOUT) {
          LOG.info("Added into timeoutExecutor {}", proc);
        } else if (!suspended) {
          proc.setState(ProcedureState.SUCCESS);
        }
      }
      // add procedure into rollback stack.
      rootProcStack.addRollbackStep(proc);

      if (proc.needPersistance()) {
        updateStoreOnExecution(rootProcStack, proc, subprocs);
      }

      if (!store.isRunning()) {
        return;
      }

      if (proc.isRunnable() && !suspended && proc.isYieldAfterExecution(this.environment)) {
        yieldProcedure(proc);
        return;
      }
    } while (reExecute);

    if (subprocs != null && !proc.isFailed()) {
      submitChildrenProcedures(subprocs);
    }

    releaseLock(proc, false);
    if (!suspended && proc.isFinished() && proc.hasParent()) {
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
      store.update(parent);
      scheduler.addFront(parent);
      LOG.info(
          "Finished subprocedure pid={}, resume processing ppid={}",
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
    }
  }

  private void updateStoreOnExecution(
      RootProcedureStack rootProcStack, Procedure<Env> proc, Procedure<Env>[] subprocs) {
    if (subprocs != null && !proc.isFailed()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stored {}, children {}", proc, Arrays.toString(subprocs));
      }
      store.update(subprocs);
    } else {
      LOG.debug("Store update {}", proc);
      if (proc.isFinished() && !proc.hasParent()) {
        final long[] childProcIds = rootProcStack.getSubprocedureIds();
        if (childProcIds != null) {
          store.delete(childProcIds);
          for (long childProcId : childProcIds) {
            procedures.remove(childProcId);
          }
        } else {
          store.update(proc);
        }
      } else {
        store.update(proc);
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
  private ProcedureLockState executeRootStackRollback(
      Long rootProcId, RootProcedureStack procedureStack) {
    Procedure<Env> rootProcedure = procedures.get(rootProcId);
    ProcedureException exception = rootProcedure.getException();
    if (exception == null) {
      exception = procedureStack.getException();
      rootProcedure.setFailure(exception);
      store.update(rootProcedure);
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
        return lockState;
      }
      lockState = executeRollback(procedure);
      releaseLock(procedure, false);

      boolean abortRollback = lockState != ProcedureLockState.LOCK_ACQUIRED;
      abortRollback |= !isRunning() || !store.isRunning();
      if (abortRollback) {
        return lockState;
      }

      if (!procedure.isFinished() && procedure.isYieldAfterExecution(this.environment)) {
        return ProcedureLockState.LOCK_YIELD_WAIT;
      }

      if (procedure != rootProcedure) {
        executeCompletionCleanup(procedure);
      }
    }

    LOG.info("Rolled back {}, time duration is {}", rootProcedure, rootProcedure.elapsedTime());
    rootProcedureCleanup(rootProcedure);
    return ProcedureLockState.LOCK_ACQUIRED;
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
      LOG.error("Roll back failed for {}", procedure, e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted exception occured for {}", procedure, e);
    } catch (Throwable t) {
      LOG.error("CODE-BUG: runtime exception for {}", procedure, t);
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
        store.delete(procedure.getProcId());
        procedures.remove(procedure.getProcId());
      } else {
        final long[] childProcIds = rollbackStack.get(procedure.getProcId()).getSubprocedureIds();
        if (childProcIds != null) {
          store.delete(childProcIds);
        } else {
          store.update(procedure);
        }
      }
    } else {
      store.update(procedure);
    }
  }

  private void executeCompletionCleanup(Procedure<Env> proc) {
    if (proc.hasLock()) {
      releaseLock(proc, true);
    }
    try {
      proc.completionCleanup(this.environment);
    } catch (Throwable e) {
      LOG.error("CODE-BUG:Uncaught runtime exception for procedure {}", proc, e);
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
    rollbackStack.put(currentProcId, stack);
    procedures.put(currentProcId, procedure);
    scheduler.addBack(procedure);
    return procedure.getProcId();
  }

  private class WorkerThread extends StoppableThread {
    private final AtomicLong startTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicReference<Procedure<Env>> activeProcedure = new AtomicReference<>();
    protected long keepAliveTime = -1;

    public WorkerThread(ThreadGroup threadGroup) {
      this(threadGroup, "ProcExecWorker-");
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
            continue;
          }
          this.activeProcedure.set(procedure);
          int activeCount = activeExecutorCount.incrementAndGet();
          startTime.set(System.currentTimeMillis());
          executeProcedure(procedure);
          activeCount = activeExecutorCount.decrementAndGet();
          LOG.trace("Halt pid={}, activeCount={}", procedure.getProcId(), activeCount);
          this.activeProcedure.set(null);
          lastUpdated = System.currentTimeMillis();
          startTime.set(lastUpdated);
        }

      } catch (Throwable throwable) {
        if (this.activeProcedure.get() != null) {
          LOG.warn(
              "Procedure Worker {} terminated {}",
              getName(),
              this.activeProcedure.get(),
              throwable);
        }
      } finally {
        LOG.info("Procedure worker {} terminated.", getName());
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

  // A worker thread which can be added when core workers are stuck. Will timeout after
  // keepAliveTime if there is no procedure to run.
  private final class KeepAliveWorkerThread extends WorkerThread {

    public KeepAliveWorkerThread(ThreadGroup group) {
      super(group, "KAProcExecWorker-");
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

    private int checkForStuckWorkers() {
      // Check if any of the worker is stuck
      int stuckCount = 0;
      for (WorkerThread worker : workerThreads) {
        if (worker.activeProcedure.get() == null
            || worker.getCurrentRunTime() < DEFAULT_WORKER_STUCK_THRESHOLD) {
          continue;
        }

        // WARN the worker is stuck
        stuckCount++;
        LOG.warn(
            "Worker stuck {}({}), run time {} ms",
            worker,
            worker.activeProcedure.get().getProcType(),
            worker.getCurrentRunTime());
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
        final KeepAliveWorkerThread worker = new KeepAliveWorkerThread(threadGroup);
        workerThreads.add(worker);
        worker.start();
        LOG.debug("Added new worker thread {}", worker);
      }
    }

    @Override
    protected void periodicExecute(Env env) {
      final int stuckCount = checkForStuckWorkers();
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
    LOG.info("Stopping");
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
          "ProcedureExecutor threadGroup {} contains running threads which are used by non-procedure module.",
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
    Preconditions.checkArgument(!procedure.hasParent(), "Unexpected parent", procedure);
    // Initialize the procedure
    procedure.setProcId(store.getNextProcId());
    procedure.setProcRunnable();
    // Commit the transaction
    store.update(procedure);
    LOG.debug("{} is stored.", procedure);
    // Add the procedure to the executor
    return pushProcedure(procedure);
  }

  /**
   * Abort a specified procedure.
   *
   * @param procId procedure id
   * @param force whether abort the running procdure.
   * @return true if the procedure exists and has received the abort.
   */
  public boolean abort(long procId, boolean force) {
    Procedure<Env> procedure = procedures.get(procId);
    if (procedure != null) {
      if (!force && procedure.wasExecuted()) {
        return false;
      }
      return procedure.abort(this.environment);
    }
    return false;
  }

  public boolean abort(long procId) {
    return abort(procId, true);
  }

  public Procedure<Env> getResult(long procId) {
    CompletedProcedureContainer retainer = completed.get(procId);
    if (retainer == null) {
      return null;
    } else {
      return retainer.getProcedure();
    }
  }

  /**
   * Query a procedure result.
   *
   * @param procId procedure id
   * @return procedure or retainer
   */
  public Procedure<Env> getResultOrProcedure(long procId) {
    CompletedProcedureContainer retainer = completed.get(procId);
    if (retainer == null) {
      return procedures.get(procId);
    } else {
      return retainer.getProcedure();
    }
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
