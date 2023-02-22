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
package org.apache.iotdb.db.mpp.execution.driver;

import org.apache.iotdb.db.mpp.execution.exchange.sink.ISink;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.Boolean.TRUE;
import static org.apache.iotdb.db.mpp.execution.operator.Operator.NOT_BLOCKED;
import static org.apache.iotdb.db.mpp.metric.QueryExecutionMetricSet.DRIVER_INTERNAL_PROCESS;

public abstract class Driver implements IDriver {

  protected static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);

  protected final DriverContext driverContext;
  protected final Operator root;
  protected final ISink sink;
  protected final AtomicReference<SettableFuture<?>> driverBlockedFuture = new AtomicReference<>();
  protected final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);

  protected final DriverLock exclusiveLock = new DriverLock();

  protected final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  protected enum State {
    ALIVE,
    NEED_DESTRUCTION,
    DESTROYED
  }

  protected Driver(Operator root, DriverContext driverContext) {
    checkNotNull(root, "root Operator should not be null");
    checkNotNull(driverContext.getSink(), "Sink should not be null");
    this.driverContext = driverContext;
    this.root = root;
    this.sink = driverContext.getSink();

    // initially the driverBlockedFuture is not blocked (it is completed)
    SettableFuture<Void> future = SettableFuture.create();
    future.set(null);
    driverBlockedFuture.set(future);
  }

  @Override
  public boolean isFinished() {
    checkLockNotHeld("Cannot check finished status while holding the driver lock");

    // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
    Optional<Boolean> result = tryWithLockUnInterruptibly(this::isFinishedInternal);
    return result.orElseGet(() -> state.get() != State.ALIVE || driverContext.isDone());
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  /**
   * do initialization
   *
   * @return true if init succeed, false otherwise
   */
  protected abstract boolean init(SettableFuture<?> blockedFuture);

  /** release resource this driver used */
  protected abstract void releaseResource();

  public int getDependencyDriverIndex() {
    return driverContext.getDependencyDriverIndex();
  }

  @Override
  public ListenableFuture<?> processFor(Duration duration) {

    SettableFuture<?> blockedFuture = driverBlockedFuture.get();

    // if the driver is blocked we don't need to continue
    if (!blockedFuture.isDone()) {
      return blockedFuture;
    }

    long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

    Optional<ListenableFuture<?>> result =
        tryWithLock(
            100,
            TimeUnit.MILLISECONDS,
            true,
            () -> {
              // only keep doing query processing if driver state is still alive
              if (state.get() == State.ALIVE) {
                long start = System.nanoTime();
                // initialization may be time-consuming, so we keep it in the processFor method
                // in normal case, it won't cause deadlock and should finish soon, otherwise it will
                // be a
                // critical bug
                // We should do initialization after holding the lock to avoid parallelism problems
                // with close
                if (!init(blockedFuture)) {
                  return blockedFuture;
                }

                do {
                  ListenableFuture<?> future = processInternal();
                  if (!future.isDone()) {
                    return updateDriverBlockedFuture(future);
                  }
                } while (System.nanoTime() - start < maxRuntime && !isFinishedInternal());
              }
              return NOT_BLOCKED;
            });

    return result.orElse(NOT_BLOCKED);
  }

  @Override
  public DriverTaskId getDriverTaskId() {
    return driverContext.getDriverTaskID();
  }

  @Override
  public void setDriverTaskId(DriverTaskId driverTaskId) {
    this.driverContext.setDriverTaskID(driverTaskId);
  }

  @Override
  public void close() {
    // mark the service for destruction
    if (!state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION)) {
      return;
    }

    exclusiveLock.interruptCurrentOwner();

    // if we can get the lock, attempt a clean shutdown; otherwise someone else will shut down
    tryWithLockUnInterruptibly(() -> TRUE);
  }

  @Override
  public void failed(Throwable t) {
    driverContext.failed(t);
  }

  @Override
  public ISink getSink() {
    return sink;
  }

  @GuardedBy("exclusiveLock")
  private boolean isFinishedInternal() {
    checkLockHeld("Lock must be held to call isFinishedInternal");

    boolean finished = state.get() != State.ALIVE || driverContext.isDone() || root.isFinished();
    if (finished) {
      state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION);
    }
    return finished;
  }

  private ListenableFuture<?> processInternal() {
    long startTimeNanos = System.nanoTime();
    try {
      ListenableFuture<?> blocked = root.isBlocked();
      if (!blocked.isDone()) {
        return blocked;
      }
      blocked = sink.isFull();
      if (!blocked.isDone()) {
        return blocked;
      }
      if (root.hasNextWithTimer()) {
        TsBlock tsBlock = root.nextWithTimer();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          sink.send(tsBlock);
        }
      }
      return NOT_BLOCKED;
    } catch (Throwable t) {
      List<StackTraceElement> interrupterStack = exclusiveLock.getInterrupterStack();
      if (interrupterStack == null) {
        driverContext.failed(t);
        throw t;
      }

      // Driver thread was interrupted which should only happen if the task is already finished.
      // If this becomes the actual cause of a failed query there is a bug in the task state
      // machine.
      Exception exception = new Exception("Interrupted By");
      exception.setStackTrace(interrupterStack.toArray(new StackTraceElement[0]));
      RuntimeException newException = new RuntimeException("Driver was interrupted", exception);
      newException.addSuppressed(t);
      driverContext.failed(newException);
      throw newException;
    } finally {
      QUERY_METRICS.recordExecutionCost(
          DRIVER_INTERNAL_PROCESS, System.nanoTime() - startTimeNanos);
    }
  }

  private ListenableFuture<?> updateDriverBlockedFuture(ListenableFuture<?> sourceBlockedFuture) {
    // driverBlockedFuture will be completed as soon as the sourceBlockedFuture is completed
    // or any of the operators gets a memory revocation request
    SettableFuture<?> newDriverBlockedFuture = SettableFuture.create();
    driverBlockedFuture.set(newDriverBlockedFuture);
    sourceBlockedFuture.addListener(() -> newDriverBlockedFuture.set(null), directExecutor());

    // TODO Although we don't have memory management for operator now, we should consider it for
    // future
    // it's possible that memory revoking is requested for some operator
    // before we update driverBlockedFuture above and we don't want to miss that
    // notification, so we check to see whether that's the case before returning.

    return newDriverBlockedFuture;
  }

  private synchronized void checkLockNotHeld(String message) {
    checkState(!exclusiveLock.isHeldByCurrentThread(), message);
  }

  @GuardedBy("exclusiveLock")
  private synchronized void checkLockHeld(String message) {
    checkState(exclusiveLock.isHeldByCurrentThread(), message);
  }

  /**
   * Try to acquire the {@code exclusiveLock} immediately and run a {@code task} The task will not
   * be interrupted if the {@code Driver} is closed.
   *
   * <p>Note: task cannot return null
   */
  private <T> Optional<T> tryWithLockUnInterruptibly(Supplier<T> task) {
    return tryWithLock(0, TimeUnit.MILLISECONDS, false, task);
  }

  /**
   * Try to acquire the {@code exclusiveLock} with {@code timeout} and run a {@code task}. If the
   * {@code interruptOnClose} flag is set to {@code true} the {@code task} will be interrupted if
   * the {@code Driver} is closed.
   *
   * <p>Note: task cannot return null
   */
  private <T> Optional<T> tryWithLock(
      long timeout, TimeUnit unit, boolean interruptOnClose, Supplier<T> task) {
    checkLockNotHeld("Lock cannot be reacquired");

    boolean acquired = false;
    try {
      acquired = exclusiveLock.tryLock(timeout, unit, interruptOnClose);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (!acquired) {
      return Optional.empty();
    }

    Optional<T> result;
    try {
      result = Optional.of(task.get());
    } finally {
      try {
        destroyIfNecessary();
      } finally {
        exclusiveLock.unlock();
      }
    }

    // We need to recheck whether the state is NEED_DESTRUCTION, if so, destroy the driver.
    // We assume that there is another concurrent Thread-A calling close method, it successfully CAS
    // state from ALIVE to NEED_DESTRUCTION just after current Thread-B do destroyIfNecessary() and
    // before exclusiveLock.unlock().
    // Then Thread-A call this method, trying to acquire lock and do destroy things, but it won't
    // succeed because the lock is still held by Thread-B. So Thread-A exit.
    // If we don't do this recheck here, Thread-B will exit too. Nobody will do destroy things.
    if (state.get() == State.NEED_DESTRUCTION && exclusiveLock.tryLock(interruptOnClose)) {
      try {
        destroyIfNecessary();
      } finally {
        exclusiveLock.unlock();
      }
    }

    return result;
  }

  @GuardedBy("exclusiveLock")
  private void destroyIfNecessary() {
    checkLockHeld("Lock must be held to call destroyIfNecessary");

    if (!state.compareAndSet(State.NEED_DESTRUCTION, State.DESTROYED)) {
      return;
    }

    // if we get an error while closing a driver, record it and we will throw it at the end
    Throwable inFlightException = null;
    try {
      inFlightException = closeAndDestroyOperators();
      driverContext.finished();
    } catch (Throwable t) {
      // this shouldn't happen but be safe
      inFlightException =
          addSuppressedException(
              inFlightException,
              t,
              "Error destroying driver for task %s",
              driverContext.getDriverTaskID());
    } finally {
      releaseResource();
    }

    if (inFlightException != null) {
      // this will always be an Error or Runtime
      throwIfUnchecked(inFlightException);
      throw new RuntimeException(inFlightException);
    }
  }

  private Throwable closeAndDestroyOperators() {
    // record the current interrupted status (and clear the flag); we'll reset it later
    boolean wasInterrupted = Thread.interrupted();

    Throwable inFlightException = null;

    try {
      root.close();
      sink.setNoMoreTsBlocks();

      // record operator execution statistics to metrics
      List<OperatorContext> operatorContexts = driverContext.getOperatorContexts();
      for (OperatorContext operatorContext : operatorContexts) {
        String operatorType = operatorContext.getOperatorType();
        QUERY_METRICS.recordOperatorExecutionCost(
            operatorType, operatorContext.getTotalExecutionTimeInNanos());
        QUERY_METRICS.recordOperatorExecutionCount(
            operatorType, operatorContext.getNextCalledCount());
      }
    } catch (InterruptedException t) {
      // don't record the stack
      wasInterrupted = true;
    } catch (Throwable t) {
      // TODO currently, we won't know exact operator which is failed in closing
      inFlightException =
          addSuppressedException(
              inFlightException,
              t,
              "Error closing operator {} for driver task {}",
              root.getOperatorContext().getOperatorId(),
              driverContext.getDriverTaskID());
    } finally {
      // reset the interrupted flag
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return inFlightException;
  }

  private static Throwable addSuppressedException(
      Throwable inFlightException, Throwable newException, String message, Object... args) {
    if (newException instanceof Error) {
      if (inFlightException == null) {
        inFlightException = newException;
      } else {
        // Self-suppression not permitted
        if (inFlightException != newException) {
          inFlightException.addSuppressed(newException);
        }
      }
    } else {
      // log normal exceptions instead of rethrowing them
      LOGGER.error(message, args, newException);
    }
    return inFlightException;
  }

  private static class DriverLock {

    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("this")
    private Thread currentOwner;

    @GuardedBy("this")
    private boolean currentOwnerInterruptionAllowed;

    @GuardedBy("this")
    private List<StackTraceElement> interrupterStack;

    public boolean isHeldByCurrentThread() {
      return lock.isHeldByCurrentThread();
    }

    public boolean tryLock(boolean currentThreadInterruptionAllowed) {
      checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
      boolean acquired = lock.tryLock();
      if (acquired) {
        setOwner(currentThreadInterruptionAllowed);
      }
      return acquired;
    }

    public boolean tryLock(long timeout, TimeUnit unit, boolean currentThreadInterruptionAllowed)
        throws InterruptedException {
      checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
      boolean acquired = lock.tryLock(timeout, unit);
      if (acquired) {
        setOwner(currentThreadInterruptionAllowed);
      }
      return acquired;
    }

    private synchronized void setOwner(boolean interruptionAllowed) {
      checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
      currentOwner = Thread.currentThread();
      currentOwnerInterruptionAllowed = interruptionAllowed;
      // NOTE: We do not use interrupted stack information to know that another
      // thread has attempted to interrupt the driver, and interrupt this new lock
      // owner.  The interrupted stack information is for debugging purposes only.
      // In the case of interruption, the caller should (and does) have a separate
      // state to prevent further processing in the Driver.
    }

    public synchronized void unlock() {
      checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
      currentOwner = null;
      currentOwnerInterruptionAllowed = false;
      lock.unlock();
    }

    public synchronized List<StackTraceElement> getInterrupterStack() {
      return interrupterStack;
    }

    public synchronized void interruptCurrentOwner() {
      if (!currentOwnerInterruptionAllowed) {
        return;
      }
      // there is a benign race condition here were the lock holder
      // can be change between attempting to get lock and grabbing
      // the synchronized lock here, but in either case we want to
      // interrupt the lock holder thread
      if (interrupterStack == null) {
        interrupterStack = ImmutableList.copyOf(Thread.currentThread().getStackTrace());
      }

      if (currentOwner != null) {
        currentOwner.interrupt();
      }
    }
  }
}
