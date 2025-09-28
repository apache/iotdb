1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure;
1
1import org.apache.iotdb.commons.concurrent.ThreadName;
1import org.apache.iotdb.commons.utils.TestOnly;
1import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
1import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
1import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
1import org.apache.iotdb.confignode.procedure.scheduler.SimpleProcedureScheduler;
1import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
1import org.apache.iotdb.confignode.procedure.state.ProcedureState;
1import org.apache.iotdb.confignode.procedure.store.IProcedureStore;
1
1import com.google.common.base.Preconditions;
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.io.IOException;
1import java.util.ArrayDeque;
1import java.util.ArrayList;
1import java.util.Arrays;
1import java.util.Deque;
1import java.util.HashSet;
1import java.util.List;
1import java.util.Objects;
1import java.util.Set;
1import java.util.concurrent.ConcurrentHashMap;
1import java.util.concurrent.CopyOnWriteArrayList;
1import java.util.concurrent.TimeUnit;
1import java.util.concurrent.atomic.AtomicBoolean;
1import java.util.concurrent.atomic.AtomicInteger;
1import java.util.concurrent.atomic.AtomicLong;
1import java.util.concurrent.atomic.AtomicReference;
1
1import static org.apache.iotdb.confignode.procedure.Procedure.NO_PROC_ID;
1
1public class ProcedureExecutor<Env> {
1  private static final Logger LOG = LoggerFactory.getLogger(ProcedureExecutor.class);
1
1  private final ConcurrentHashMap<Long, CompletedProcedureContainer<Env>> completed =
1      new ConcurrentHashMap<>();
1
1  private final ConcurrentHashMap<Long, RootProcedureStack<Env>> rollbackStack =
1      new ConcurrentHashMap<>();
1
1  private final ConcurrentHashMap<Long, Procedure<Env>> procedures = new ConcurrentHashMap<>();
1
1  private ThreadGroup threadGroup;
1
1  private CopyOnWriteArrayList<WorkerThread> workerThreads;
1
1  private TimeoutExecutorThread<Env> timeoutExecutor;
1
1  private TimeoutExecutorThread<Env> workerMonitorExecutor;
1
1  private int corePoolSize;
1  private int maxPoolSize;
1
1  private final ProcedureScheduler scheduler;
1
1  private final AtomicLong workId = new AtomicLong(0);
1  private final AtomicInteger activeExecutorCount = new AtomicInteger(0);
1  private final AtomicBoolean running = new AtomicBoolean(false);
1  private final Env environment;
1  private final IProcedureStore<Env> store;
1
1  public ProcedureExecutor(
1      final Env environment, final IProcedureStore<Env> store, final ProcedureScheduler scheduler) {
1    this.environment = environment;
1    this.scheduler = scheduler;
1    this.store = store;
1  }
1
1  @TestOnly
1  public ProcedureExecutor(final Env environment, final IProcedureStore<Env> store) {
1    this(environment, store, new SimpleProcedureScheduler());
1  }
1
1  public void init(int numThreads) {
1    this.corePoolSize = numThreads;
1    this.maxPoolSize = 10 * numThreads;
1    this.threadGroup = new ThreadGroup(ThreadName.CONFIG_NODE_PROCEDURE_WORKER.getName());
1    this.timeoutExecutor =
1        new TimeoutExecutorThread<>(
1            this, threadGroup, ThreadName.CONFIG_NODE_TIMEOUT_EXECUTOR.getName());
1    this.workerMonitorExecutor =
1        new TimeoutExecutorThread<>(
1            this, threadGroup, ThreadName.CONFIG_NODE_WORKER_THREAD_MONITOR.getName());
1    workId.set(0);
1    workerThreads = new CopyOnWriteArrayList<>();
1    for (int i = 0; i < corePoolSize; i++) {
1      workerThreads.add(new WorkerThread(threadGroup));
1    }
1    // Add worker monitor
1    workerMonitorExecutor.add(new WorkerMonitor());
1
1    scheduler.start();
1    recover();
1  }
1
1  private void recover() {
1    // 1.Build rollback stack
1    List<Procedure<Env>> procedureList = getProcedureListFromDifferentVersion();
1    // Load procedure wal file
1    for (Procedure<Env> proc : procedureList) {
1      if (proc.isFinished()) {
1        completed.putIfAbsent(proc.getProcId(), new CompletedProcedureContainer<>(proc));
1      } else {
1        if (!proc.hasParent()) {
1          rollbackStack.put(proc.getProcId(), new RootProcedureStack<>());
1        }
1        procedures.putIfAbsent(proc.getProcId(), proc);
1      }
1    }
1    List<Procedure<Env>> runnableList = new ArrayList<>();
1    List<Procedure<Env>> failedList = new ArrayList<>();
1    List<Procedure<Env>> waitingList = new ArrayList<>();
1    List<Procedure<Env>> waitingTimeoutList = new ArrayList<>();
1    for (Procedure<Env> proc : procedureList) {
1      if (proc.isFinished() && !proc.hasParent()) {
1        continue;
1      }
1      long rootProcedureId = getRootProcedureId(proc);
1      if (proc.hasParent()) {
1        Procedure<Env> parent = procedures.get(proc.getParentProcId());
1        if (parent != null && !proc.isFinished()) {
1          parent.incChildrenLatch();
1        }
1      }
1      RootProcedureStack<Env> rootStack = rollbackStack.get(rootProcedureId);
1      if (rootStack != null) {
1        rootStack.loadStack(proc);
1      }
1      proc.setRootProcedureId(rootProcedureId);
1      switch (proc.getState()) {
1        case RUNNABLE:
1          runnableList.add(proc);
1          break;
1        case FAILED:
1          failedList.add(proc);
1          break;
1        case WAITING:
1          waitingList.add(proc);
1          break;
1        case WAITING_TIMEOUT:
1          waitingTimeoutList.add(proc);
1          break;
1        case ROLLEDBACK:
1        case INITIALIZING:
1          LOG.error("Unexpected state:{} for {}", proc.getState(), proc);
1          throw new UnsupportedOperationException("Unexpected state");
1        default:
1          break;
1      }
1    }
1
1    waitingList.forEach(
1        procedure -> {
1          if (!procedure.hasChildren()) {
1            // Normally, WAITING procedures should be wakened by its children.
1            // But, there is a case that, all the children are successful, and before
1            // they can wake up their parent procedure, the master was killed.
1            // So, during recovering the procedures from ProcedureWal, its children
1            // are not loaded because of their SUCCESS state.
1            // So we need to continue to run this WAITING procedure. Before
1            // executing, we need to set its state to RUNNABLE.
1            procedure.setState(ProcedureState.RUNNABLE);
1            runnableList.add(procedure);
1          }
1        });
1    restoreLocks();
1
1    waitingTimeoutList.forEach(timeoutExecutor::add);
1
1    failedList.forEach(scheduler::addBack);
1    runnableList.forEach(scheduler::addBack);
1    scheduler.signalAll();
1  }
1
1  private List<Procedure<Env>> getProcedureListFromDifferentVersion() {
1    if (store.isOldVersionProcedureStore()) {
1      LOG.info("Old procedure directory detected, upgrade beginning...");
1      return store.load();
1    } else {
1      return store.getProcedures();
1    }
1  }
1
1  /**
1   * Helper to look up the root Procedure ID.
1   *
1   * @param proc given a specified procedure.
1   */
1  Long getRootProcedureId(Procedure<Env> proc) {
1    while (proc.hasParent()) {
1      proc = procedures.get(proc.getParentProcId());
1      if (proc == null) {
1        return NO_PROC_ID;
1      }
1    }
1    return proc.getProcId();
1  }
1
1  private void releaseLock(Procedure<Env> procedure, boolean force) {
1    if (force || !procedure.holdLock(this.environment) || procedure.isFinished()) {
1      procedure.doReleaseLock(this.environment, store);
1    }
1  }
1
1  private void restoreLock(Procedure procedure, Set<Long> restored) {
1    procedure.restoreLock(environment);
1    restored.add(procedure.getProcId());
1  }
1
1  private void restoreLocks(Deque<Procedure<Env>> stack, Set<Long> restored) {
1    while (!stack.isEmpty()) {
1      restoreLock(stack.pop(), restored);
1    }
1  }
1
1  private void restoreLocks() {
1    Set<Long> restored = new HashSet<>();
1    Deque<Procedure<Env>> stack = new ArrayDeque<>();
1    procedures
1        .values()
1        .forEach(
1            procedure -> {
1              while (procedure != null) {
1                if (restored.contains(procedure.getProcId())) {
1                  restoreLocks(stack, restored);
1                  return;
1                }
1                if (!procedure.hasParent()) {
1                  restoreLock(procedure, restored);
1                  restoreLocks(stack, restored);
1                  return;
1                }
1                stack.push(procedure);
1                procedure = procedures.get(procedure.getParentProcId());
1              }
1            });
1  }
1
1  public void startWorkers() {
1    if (!running.compareAndSet(false, true)) {
1      LOG.warn("Already running");
1      return;
1    }
1    timeoutExecutor.start();
1    workerMonitorExecutor.start();
1    for (WorkerThread workerThread : workerThreads) {
1      workerThread.start();
1    }
1    LOG.info("{} procedure workers are started.", workerThreads.size());
1  }
1
1  public void startCompletedCleaner(long cleanTimeInterval, long cleanEvictTTL) {
1    addInternalProcedure(
1        new CompletedProcedureRecycler(store, completed, cleanTimeInterval, cleanEvictTTL));
1  }
1
1  public void addInternalProcedure(InternalProcedure interalProcedure) {
1    if (interalProcedure == null) {
1      return;
1    }
1    interalProcedure.setState(ProcedureState.WAITING_TIMEOUT);
1    timeoutExecutor.add(interalProcedure);
1  }
1
1  public boolean removeInternalProcedure(InternalProcedure internalProcedure) {
1    if (internalProcedure == null) {
1      return true;
1    }
1    internalProcedure.setState(ProcedureState.SUCCESS);
1    return timeoutExecutor.remove(internalProcedure);
1  }
1
1  /**
1   * Executes procedure
1   *
1   * <p>Calls doExecute() if success and return subprocedures submit sub procs set the state to
1   * WAITING, wait for all sub procs completed. else if no sub procs procedure completed
1   * successfully set procedure's parent to RUNNABLE in case of failure start rollback of the
1   * procedure.
1   *
1   * @param proc procedure
1   */
1  private void executeProcedure(Procedure<Env> proc) {
1    if (proc.isFinished()) {
1      LOG.debug("{} is already finished.", proc);
1      return;
1    }
1    final Long rootProcId = getRootProcedureId(proc);
1    if (rootProcId == null) {
1      LOG.warn("Rollback because parent is done/rolledback, proc is {}", proc);
1      executeRollback(proc);
1      return;
1    }
1    RootProcedureStack<Env> rootProcStack = rollbackStack.get(rootProcId);
1    if (rootProcStack == null) {
1      LOG.warn("Rollback stack is null for {}", proc.getProcId());
1      return;
1    }
1    ProcedureLockState lockState = null;
1    try {
1      do {
1        if (!rootProcStack.acquire()) {
1          if (rootProcStack.setRollback()) {
1            lockState = executeRootStackRollback(rootProcId, rootProcStack);
1            switch (lockState) {
1              case LOCK_ACQUIRED:
1                break;
1              case LOCK_EVENT_WAIT:
1                LOG.info("LOCK_EVENT_WAIT rollback {}", proc);
1                rootProcStack.unsetRollback();
1                break;
1              case LOCK_YIELD_WAIT:
1                rootProcStack.unsetRollback();
1                scheduler.yield(proc);
1                break;
1              default:
1                throw new UnsupportedOperationException();
1            }
1          } else {
1            if (!proc.wasExecuted()) {
1              switch (executeRollback(proc)) {
1                case LOCK_ACQUIRED:
1                  break;
1                case LOCK_EVENT_WAIT:
1                  LOG.info("LOCK_EVENT_WAIT can't rollback child running for {}", proc);
1                  break;
1                case LOCK_YIELD_WAIT:
1                  scheduler.yield(proc);
1                  break;
1                default:
1                  throw new UnsupportedOperationException();
1              }
1            }
1          }
1          break;
1        }
1        lockState = acquireLock(proc);
1        switch (lockState) {
1          case LOCK_ACQUIRED:
1            executeProcedure(rootProcStack, proc);
1            break;
1          case LOCK_YIELD_WAIT:
1          case LOCK_EVENT_WAIT:
1            LOG.info("{} lockstate is {}", proc, lockState);
1            break;
1          default:
1            throw new UnsupportedOperationException();
1        }
1        rootProcStack.release();
1
1        if (proc.isSuccess()) {
1          // update metrics on finishing the procedure
1          proc.updateMetricsOnFinish(getEnvironment(), proc.elapsedTime(), true);
1          LOG.debug("{} finished in {}ms successfully.", proc, proc.elapsedTime());
1          if (proc.getProcId() == rootProcId) {
1            rootProcedureCleanup(proc);
1          } else {
1            executeCompletionCleanup(proc);
1          }
1          return;
1        }
1
1      } while (rootProcStack.isFailed());
1    } finally {
1      // Only after procedure has completed execution can it be allowed to be rescheduled to prevent
1      // data races
1      if (Objects.equals(lockState, ProcedureLockState.LOCK_EVENT_WAIT)) {
1        LOG.info("procedureId {} wait for lock.", proc.getProcId());
1        ((ConfigNodeProcedureEnv) this.environment).getNodeLock().waitProcedure(proc);
1      }
1    }
1  }
1
1  /**
1   * execute procedure and submit its children
1   *
1   * @param rootProcStack procedure's root proc stack
1   * @param proc procedure
1   */
1  private void executeProcedure(RootProcedureStack rootProcStack, Procedure<Env> proc) {
1    if (proc.getState() != ProcedureState.RUNNABLE) {
1      LOG.error(
1          "The executing procedure should in RUNNABLE state, but it's not. Procedure is {}", proc);
1      return;
1    }
1    boolean reExecute;
1
1    Procedure<Env>[] subprocs = null;
1    do {
1      reExecute = false;
1      try {
1        subprocs = proc.doExecute(this.environment);
1        if (subprocs != null && subprocs.length == 0) {
1          subprocs = null;
1        }
1      } catch (InterruptedException e) {
1        LOG.warn("Interrupt during execution, suspend or retry it later.", e);
1        yieldProcedure(proc);
1      } catch (Throwable e) {
1        LOG.error("CODE-BUG:{}", proc, e);
1        proc.setFailure(new ProcedureException(e.getMessage(), e));
1      }
1
1      if (!proc.isFailed()) {
1        if (subprocs != null) {
1          if (subprocs.length == 1 && subprocs[0] == proc) {
1            subprocs = null;
1            reExecute = true;
1          } else {
1            subprocs = initializeChildren(rootProcStack, proc, subprocs);
1            LOG.info("Initialized sub procs:{}", Arrays.toString(subprocs));
1          }
1        } else if (proc.getState() == ProcedureState.WAITING_TIMEOUT) {
1          LOG.info("Added into timeoutExecutor {}", proc);
1        } else {
1          proc.setState(ProcedureState.SUCCESS);
1        }
1      }
1      // add procedure into rollback stack.
1      rootProcStack.addRollbackStep(proc);
1
1      updateStoreOnExecution(rootProcStack, proc, subprocs);
1
1      if (!store.isRunning()) {
1        return;
1      }
1
1      if (proc.isRunnable() && proc.isYieldAfterExecution(this.environment)) {
1        yieldProcedure(proc);
1        return;
1      }
1    } while (reExecute);
1
1    if (subprocs != null && !proc.isFailed()) {
1      submitChildrenProcedures(subprocs);
1    }
1
1    releaseLock(proc, false);
1    if (proc.isFinished() && proc.hasParent()) {
1      countDownChildren(rootProcStack, proc);
1    }
1  }
1
1  /**
1   * Serve as a countdown latch to check whether all children have already completed.
1   *
1   * @param rootProcStack root procedure stack
1   * @param proc proc
1   */
1  private void countDownChildren(RootProcedureStack rootProcStack, Procedure<Env> proc) {
1    Procedure<Env> parent = procedures.get(proc.getParentProcId());
1    if (parent == null && rootProcStack.isRollingback()) {
1      return;
1    }
1    if (parent != null && parent.tryRunnable()) {
1      // If success, means all its children have completed, move parent to front of the queue.
1      store.update(parent);
1      scheduler.addFront(parent);
1      LOG.info(
1          "Finished subprocedure pid={}, resume processing ppid={}",
1          proc.getProcId(),
1          parent.getProcId());
1    }
1  }
1
1  /**
1   * Submit children procedures.
1   *
1   * @param subprocs children procedures
1   */
1  private void submitChildrenProcedures(Procedure<Env>[] subprocs) {
1    for (Procedure<Env> subproc : subprocs) {
1      subproc.updateMetricsOnSubmit(getEnvironment());
1      procedures.put(subproc.getProcId(), subproc);
1      scheduler.addFront(subproc);
1      LOG.info("Sub-Procedure pid={} has been submitted", subproc.getProcId());
1    }
1  }
1
1  private void updateStoreOnExecution(
1      RootProcedureStack rootProcStack, Procedure<Env> proc, Procedure<Env>[] subprocs) {
1    if (subprocs != null && !proc.isFailed()) {
1      if (LOG.isDebugEnabled()) {
1        LOG.debug("Stored {}, children {}", proc, Arrays.toString(subprocs));
1      }
1      store.update(subprocs);
1    } else {
1      LOG.debug("Store update {}", proc);
1      if (proc.isFinished() && !proc.hasParent()) {
1        final long[] childProcIds = rootProcStack.getSubprocedureIds();
1        if (childProcIds != null) {
1          store.delete(childProcIds);
1          for (long childProcId : childProcIds) {
1            procedures.remove(childProcId);
1          }
1        } else {
1          store.update(proc);
1        }
1      } else {
1        store.update(proc);
1      }
1    }
1  }
1
1  private Procedure<Env>[] initializeChildren(
1      RootProcedureStack rootProcStack, Procedure<Env> proc, Procedure<Env>[] subprocs) {
1    final long rootProcedureId = getRootProcedureId(proc);
1    for (int i = 0; i < subprocs.length; i++) {
1      Procedure<Env> subproc = subprocs[i];
1      if (subproc == null) {
1        String errMsg = "subproc[" + i + "] is null, aborting procedure";
1        proc.setFailure(new ProcedureException((errMsg), new IllegalArgumentException(errMsg)));
1        return null;
1      }
1      subproc.setParentProcId(proc.getProcId());
1      subproc.setRootProcId(rootProcedureId);
1      subproc.setProcId(store.getNextProcId());
1      subproc.setProcRunnable();
1      rootProcStack.addSubProcedure(subproc);
1    }
1
1    if (!proc.isFailed()) {
1      proc.setChildrenLatch(subprocs.length);
1      switch (proc.getState()) {
1        case RUNNABLE:
1          proc.setState(ProcedureState.WAITING);
1          break;
1        case WAITING_TIMEOUT:
1          timeoutExecutor.add(proc);
1          break;
1        default:
1          break;
1      }
1    }
1    return subprocs;
1  }
1
1  private void yieldProcedure(Procedure<Env> proc) {
1    releaseLock(proc, false);
1    scheduler.yield(proc);
1  }
1
1  /**
1   * Rollback full root procedure stack.
1   *
1   * @param rootProcId root procedure id
1   * @param procedureStack root procedure stack
1   * @return lock state
1   */
1  private ProcedureLockState executeRootStackRollback(
1      Long rootProcId, RootProcedureStack procedureStack) {
1    Procedure<Env> rootProcedure = procedures.get(rootProcId);
1    ProcedureException exception = rootProcedure.getException();
1    if (exception == null) {
1      exception = procedureStack.getException();
1      rootProcedure.setFailure(exception);
1      store.update(rootProcedure);
1    }
1    List<Procedure<Env>> subprocStack = procedureStack.getSubproceduresStack();
1    int stackTail = subprocStack.size();
1    while (stackTail-- > 0) {
1      Procedure<Env> procedure = subprocStack.get(stackTail);
1      if (procedure.isSuccess()) {
1        subprocStack.remove(stackTail);
1        cleanupAfterRollback(procedure);
1        continue;
1      }
1      ProcedureLockState lockState = acquireLock(procedure);
1      if (lockState != ProcedureLockState.LOCK_ACQUIRED) {
1        return lockState;
1      }
1      lockState = executeRollback(procedure);
1      releaseLock(procedure, false);
1
1      boolean abortRollback = lockState != ProcedureLockState.LOCK_ACQUIRED;
1      abortRollback |= !isRunning() || !store.isRunning();
1      if (abortRollback) {
1        return lockState;
1      }
1
1      if (!procedure.isFinished() && procedure.isYieldAfterExecution(this.environment)) {
1        return ProcedureLockState.LOCK_YIELD_WAIT;
1      }
1
1      if (procedure != rootProcedure) {
1        executeCompletionCleanup(procedure);
1      }
1    }
1
1    LOG.info("Rolled back {}, time duration is {}", rootProcedure, rootProcedure.elapsedTime());
1    rootProcedureCleanup(rootProcedure);
1    return ProcedureLockState.LOCK_ACQUIRED;
1  }
1
1  private ProcedureLockState acquireLock(Procedure<Env> proc) {
1    if (proc.hasLock()) {
1      return ProcedureLockState.LOCK_ACQUIRED;
1    }
1    return proc.doAcquireLock(this.environment, store);
1  }
1
1  /**
1   * do execute defined in procedure and then update store or remove completely in case it is a
1   * child.
1   *
1   * @param procedure procedure
1   * @return procedure lock state
1   */
1  private ProcedureLockState executeRollback(Procedure<Env> procedure) {
1    try {
1      procedure.doRollback(this.environment);
1    } catch (IOException e) {
1      LOG.error("Roll back failed for {}", procedure, e);
1    } catch (InterruptedException e) {
1      LOG.warn("Interrupted exception occured for {}", procedure, e);
1    } catch (Throwable t) {
1      LOG.error("CODE-BUG: runtime exception for {}", procedure, t);
1    }
1    cleanupAfterRollback(procedure);
1    return ProcedureLockState.LOCK_ACQUIRED;
1  }
1
1  private void cleanupAfterRollback(Procedure<Env> procedure) {
1    if (procedure.removeStackIndex()) {
1      if (!procedure.isSuccess()) {
1        procedure.setState(ProcedureState.ROLLEDBACK);
1      }
1
1      // update metrics on finishing the procedure (fail)
1      procedure.updateMetricsOnFinish(getEnvironment(), procedure.elapsedTime(), false);
1
1      if (procedure.hasParent()) {
1        store.delete(procedure.getProcId());
1        procedures.remove(procedure.getProcId());
1      } else {
1        final long[] childProcIds = rollbackStack.get(procedure.getProcId()).getSubprocedureIds();
1        if (childProcIds != null) {
1          store.delete(childProcIds);
1        } else {
1          store.update(procedure);
1        }
1      }
1    } else {
1      store.update(procedure);
1    }
1  }
1
1  private void executeCompletionCleanup(Procedure<Env> proc) {
1    if (proc.hasLock()) {
1      releaseLock(proc, true);
1    }
1  }
1
1  private void rootProcedureCleanup(Procedure<Env> proc) {
1    executeCompletionCleanup(proc);
1    CompletedProcedureContainer<Env> retainer = new CompletedProcedureContainer<>(proc);
1    completed.put(proc.getProcId(), retainer);
1    rollbackStack.remove(proc.getProcId());
1    procedures.remove(proc.getProcId());
1  }
1
1  /**
1   * Add a Procedure to executor.
1   *
1   * @param procedure procedure
1   * @return procedure id
1   */
1  private long pushProcedure(Procedure<Env> procedure) {
1    final long currentProcId = procedure.getProcId();
1    // Update metrics on start of a procedure
1    procedure.updateMetricsOnSubmit(getEnvironment());
1    RootProcedureStack<Env> stack = new RootProcedureStack<>();
1    rollbackStack.put(currentProcId, stack);
1    procedures.put(currentProcId, procedure);
1    scheduler.addBack(procedure);
1    return procedure.getProcId();
1  }
1
1  private class WorkerThread extends StoppableThread {
1    private final AtomicLong startTime = new AtomicLong(Long.MAX_VALUE);
1    private final AtomicReference<Procedure<Env>> activeProcedure = new AtomicReference<>();
1    protected long keepAliveTime = -1;
1
1    public WorkerThread(ThreadGroup threadGroup) {
1      this(threadGroup, "ProcedureCoreWorker-");
1    }
1
1    public WorkerThread(ThreadGroup threadGroup, String prefix) {
1      super(threadGroup, prefix + workId.incrementAndGet());
1      setDaemon(true);
1    }
1
1    @Override
1    public void sendStopSignal() {
1      scheduler.signalAll();
1    }
1
1    @Override
1    public void run() {
1      long lastUpdated = System.currentTimeMillis();
1      try {
1        while (isRunning() && keepAlive(lastUpdated)) {
1          Procedure<Env> procedure = scheduler.poll(keepAliveTime, TimeUnit.MILLISECONDS);
1          if (procedure == null) {
1            Thread.sleep(1000);
1            continue;
1          }
1          this.activeProcedure.set(procedure);
1          activeExecutorCount.incrementAndGet();
1          startTime.set(System.currentTimeMillis());
1          executeProcedure(procedure);
1          activeExecutorCount.decrementAndGet();
1          LOG.trace(
1              "Halt pid={}, activeCount={}", procedure.getProcId(), activeExecutorCount.get());
1          this.activeProcedure.set(null);
1          lastUpdated = System.currentTimeMillis();
1          startTime.set(lastUpdated);
1        }
1
1      } catch (Exception e) {
1        if (this.activeProcedure.get() != null) {
1          LOG.warn(
1              "Exception happened when worker {} execute procedure {}",
1              getName(),
1              this.activeProcedure.get(),
1              e);
1        }
1      } finally {
1        LOG.info("Procedure worker {} terminated.", getName());
1      }
1      workerThreads.remove(this);
1    }
1
1    protected boolean keepAlive(long lastUpdated) {
1      return true;
1    }
1
1    @Override
1    public String toString() {
1      Procedure<?> p = this.activeProcedure.get();
1      return getName() + "(pid=" + (p == null ? NO_PROC_ID : p.getProcId() + ")");
1    }
1
1    /**
1     * @return the time since the current procedure is running
1     */
1    public long getCurrentRunTime() {
1      return System.currentTimeMillis() - startTime.get();
1    }
1  }
1
1  // A temporary worker thread will be launched when too many core workers are stuck.
1  // They will timeout after keepAliveTime if there is no procedure to run.
1  private final class TemporaryWorkerThread extends WorkerThread {
1
1    public TemporaryWorkerThread(ThreadGroup group) {
1      super(group, "ProcedureTemporaryWorker-");
1      this.keepAliveTime = TimeUnit.SECONDS.toMillis(10);
1    }
1
1    @Override
1    protected boolean keepAlive(long lastUpdate) {
1      return System.currentTimeMillis() - lastUpdate < keepAliveTime;
1    }
1  }
1
1  private final class WorkerMonitor extends InternalProcedure<Env> {
1    private static final int DEFAULT_WORKER_MONITOR_INTERVAL = 30000; // 30sec
1
1    private static final int DEFAULT_WORKER_STUCK_THRESHOLD = 60000; // 60sec
1
1    private static final float DEFAULT_WORKER_ADD_STUCK_PERCENTAGE = 0.5f; // 50% stuck
1
1    public WorkerMonitor() {
1      super(DEFAULT_WORKER_MONITOR_INTERVAL);
1      updateTimestamp();
1    }
1
1    private int calculateRunningAndStuckWorkers() {
1      // Check if any of the worker is stuck
1      int runningCount = 0, stuckCount = 0;
1      for (WorkerThread worker : workerThreads) {
1        final Procedure<?> proc = worker.activeProcedure.get();
1        if (proc == null) {
1          continue;
1        }
1        runningCount++;
1        // WARN the worker is stuck
1        if (worker.getCurrentRunTime() > DEFAULT_WORKER_STUCK_THRESHOLD) {
1          stuckCount++;
1          LOG.warn(
1              "Worker stuck {}({}), run time {} ms",
1              worker,
1              proc.getProcType(),
1              worker.getCurrentRunTime());
1        }
1        LOG.info(
1            "Procedure workers: {} is running, {} is running and stuck", runningCount, stuckCount);
1      }
1      return stuckCount;
1    }
1
1    private void checkThreadCount(final int stuckCount) {
1      // Nothing to do if there are no runnable tasks
1      if (stuckCount < 1 || !scheduler.hasRunnables()) {
1        return;
1      }
1      // Add a new thread if the worker stuck percentage exceed the threshold limit
1      // and every handler is active.
1      final float stuckPerc = ((float) stuckCount) / workerThreads.size();
1      // Let's add new worker thread more aggressively, as they will timeout finally if there is no
1      // work to do.
1      if (stuckPerc >= DEFAULT_WORKER_ADD_STUCK_PERCENTAGE && workerThreads.size() < maxPoolSize) {
1        final TemporaryWorkerThread worker = new TemporaryWorkerThread(threadGroup);
1        workerThreads.add(worker);
1        worker.start();
1        LOG.debug("Added new worker thread {}", worker);
1      }
1    }
1
1    @Override
1    protected void periodicExecute(Env env) {
1      final int stuckCount = calculateRunningAndStuckWorkers();
1      checkThreadCount(stuckCount);
1      updateTimestamp();
1    }
1  }
1
1  public int getWorkerThreadCount() {
1    return workerThreads.size();
1  }
1
1  public long getActiveWorkerThreadCount() {
1    return workerThreads.stream().filter(worker -> worker.activeProcedure.get() != null).count();
1  }
1
1  public boolean isRunning() {
1    return running.get();
1  }
1
1  public void stop() {
1    if (!running.getAndSet(false)) {
1      return;
1    }
1    LOG.info("Stopping");
1    scheduler.stop();
1    timeoutExecutor.sendStopSignal();
1  }
1
1  public void join() {
1    timeoutExecutor.awaitTermination();
1    workerMonitorExecutor.awaitTermination();
1    for (WorkerThread workerThread : workerThreads) {
1      workerThread.awaitTermination();
1    }
1    try {
1      threadGroup.destroy();
1    } catch (IllegalThreadStateException e) {
1      LOG.warn(
1          "ProcedureExecutor threadGroup {} contains running threads which are used by non-procedure module.",
1          this.threadGroup);
1      this.threadGroup.list();
1    }
1  }
1
1  public boolean isStarted(long procId) {
1    Procedure<Env> procedure = procedures.get(procId);
1    if (procedure == null) {
1      return completed.get(procId) != null;
1    }
1    return procedure.wasExecuted();
1  }
1
1  public boolean isFinished(final long procId) {
1    return !procedures.containsKey(procId);
1  }
1
1  public ConcurrentHashMap<Long, Procedure<Env>> getProcedures() {
1    return procedures;
1  }
1
1  // -----------------------------CLIENT IMPLEMENTATION-----------------------------------
1  /**
1   * Submit a new root-procedure to the executor, called by client.
1   *
1   * @param procedure root procedure
1   * @return procedure id
1   */
1  public long submitProcedure(Procedure<Env> procedure) {
1    Preconditions.checkArgument(procedure.getState() == ProcedureState.INITIALIZING);
1    Preconditions.checkArgument(!procedure.hasParent(), "Unexpected parent", procedure);
1    // Initialize the procedure
1    procedure.setProcId(store.getNextProcId());
1    procedure.setProcRunnable();
1    // Commit the transaction
1    store.update(procedure);
1    LOG.debug("{} is stored.", procedure);
1    // Add the procedure to the executor
1    return pushProcedure(procedure);
1  }
1
1  public ProcedureScheduler getScheduler() {
1    return scheduler;
1  }
1
1  public Env getEnvironment() {
1    return environment;
1  }
1
1  public IProcedureStore getStore() {
1    return store;
1  }
1
1  public RootProcedureStack<Env> getRollbackStack(long rootProcId) {
1    return rollbackStack.get(rootProcId);
1  }
1}
1