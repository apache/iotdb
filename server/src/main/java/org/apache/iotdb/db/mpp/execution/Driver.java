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
package org.apache.iotdb.db.mpp.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.iotdb.db.mpp.buffer.ISinkHandle;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.Boolean.TRUE;
import static org.apache.iotdb.db.mpp.operator.Operator.NOT_BLOCKED;

/**
 * Driver encapsulates some methods which are necessary for execution scheduler to run a fragment
 * instance
 */
public abstract class Driver {

  protected static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);


  protected final Operator root;
  protected final ISinkHandle sinkHandle;
  protected final DriverContext driverContext;
  protected final AtomicReference<SettableFuture<Void>> driverBlockedFuture = new AtomicReference<>();
  protected final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);

  protected final DriverLock exclusiveLock = new DriverLock();

  protected enum State {
    ALIVE, NEED_DESTRUCTION, DESTROYED
  }

  public Driver(Operator root, ISinkHandle sinkHandle, DriverContext driverContext) {
    this.root = root;
    this.sinkHandle = sinkHandle;
    this.driverContext = driverContext;

    // initially the driverBlockedFuture is not blocked (it is completed)
    SettableFuture<Void> future = SettableFuture.create();
    future.set(null);
    driverBlockedFuture.set(future);
  }

  /**
   * Used to judge whether this fragment instance should be scheduled for execution anymore
   *
   * @return true if the FragmentInstance is done or terminated due to failure, otherwise false.
   */
  public boolean isFinished() {
    checkLockNotHeld("Cannot check finished status while holding the driver lock");

    // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
    Optional<Boolean> result = tryWithLockUnInterruptibly(this::isFinishedInternal);
    return result.orElseGet(() -> state.get() != State.ALIVE || driverContext.isDone());
  }

  /**
   * do initialization
   *
   * @return true if init succeed, false otherwise
   */
  protected abstract boolean init(SettableFuture<Void> blockedFuture);

  /**
   * release resource this driver used
   */
  protected abstract void releaseResource();

  /**
   * run the fragment instance for {@param duration} time slice, the time of this run is likely not
   * to be equal to {@param duration}, the actual run time should be calculated by the caller
   *
   * @param duration how long should this fragment instance run
   * @return the returned ListenableFuture<Void> is used to represent status of this processing if
   * isDone() return true, meaning that this fragment instance is not blocked and is ready for
   * next processing otherwise, meaning that this fragment instance is blocked and not ready for
   * next processing.
   */
  public ListenableFuture<Void> processFor(Duration duration) {

    SettableFuture<Void> blockedFuture = driverBlockedFuture.get();
    // initialization may be time-consuming, so we keep it in the processFor method
    // in normal case, it won't cause deadlock and should finish soon, otherwise it will be a
    // critical bug
    if (!init(blockedFuture)) {
      return blockedFuture;
    }

    // if the driver is blocked we don't need to continue
    if (!blockedFuture.isDone()) {
      return blockedFuture;
    }

    long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

    Optional<ListenableFuture<Void>> result = tryWithLock(100, TimeUnit.MILLISECONDS, true, () -> {
      long start = System.nanoTime();
      do {
        ListenableFuture<Void> future = processInternal();
        if (!future.isDone()) {
          return updateDriverBlockedFuture(future);
        }
      }
      while (System.nanoTime() - start < maxRuntime && !isFinishedInternal());
      return NOT_BLOCKED;
    });

    return result.orElse(NOT_BLOCKED);
  }

  /**
   * the id information about this Fragment Instance.
   *
   * @return a {@link FragmentInstanceId} instance.
   */
  public FragmentInstanceId getInfo() {
    return driverContext.getId();
  }

  /**
   * clear resource used by this fragment instance
   */
  public void close() {
    // mark the service for destruction
    if (!state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION)) {
      return;
    }

    exclusiveLock.interruptCurrentOwner();

    // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
    tryWithLockUnInterruptibly(() -> TRUE);
  }

  /**
   * fail current driver
   *
   * @param t reason cause this failure
   */
  public void failed(Throwable t) {
    driverContext.failed(t);
  }

  public ISinkHandle getSinkHandle() {
    return sinkHandle;
  }

  @GuardedBy("exclusiveLock")
  private boolean isFinishedInternal() {
    checkLockHeld("Lock must be held to call isFinishedInternal");

    boolean finished = state.get() != State.ALIVE || driverContext.isDone() || root == null || root.isFinished();
    if (finished) {
      state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION);
    }
    return finished;
  }


  private ListenableFuture<Void> processInternal() {
    try {
      ListenableFuture<Void> blocked = root.isBlocked();
      if (!blocked.isDone()) {
        return blocked;
      }
      blocked = sinkHandle.isFull();
      if (!blocked.isDone()) {
        return blocked;
      }
      if (root.hasNext()) {
        TsBlock tsBlock = root.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          sinkHandle.send(Collections.singletonList(tsBlock));
        }
      }
      return NOT_BLOCKED;
    } catch (Throwable t) {
      LOGGER.error("Failed to execute fragment instance {}", driverContext.getId(), t);
      List<StackTraceElement> interrupterStack = exclusiveLock.getInterrupterStack();
      if (interrupterStack == null) {
        driverContext.failed(t);
        throw t;
      }

      // Driver thread was interrupted which should only happen if the task is already finished.
      // If this becomes the actual cause of a failed query there is a bug in the task state machine.
      Exception exception = new Exception("Interrupted By");
      exception.setStackTrace(interrupterStack.toArray(new StackTraceElement[0]));
      RuntimeException newException = new RuntimeException("Driver was interrupted", exception);
      newException.addSuppressed(t);
      driverContext.failed(newException);
      throw newException;
    }
  }

  private ListenableFuture<Void> updateDriverBlockedFuture(
      ListenableFuture<Void> sourceBlockedFuture) {
    // driverBlockedFuture will be completed as soon as the sourceBlockedFuture is completed
    // or any of the operators gets a memory revocation request
    SettableFuture<Void> newDriverBlockedFuture = SettableFuture.create();
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
   * Try to acquire the {@code exclusiveLock} immediately and run a {@code task}
   * The task will not be interrupted if the {@code Driver} is closed.
   * <p>
   * Note: task cannot return null
   */
  private <T> Optional<T> tryWithLockUnInterruptibly(Supplier<T> task) {
    return tryWithLock(0, TimeUnit.MILLISECONDS, false, task);
  }

  /**
   * Try to acquire the {@code exclusiveLock} with {@code timeout} and run a {@code task}.
   * If the {@code interruptOnClose} flag is set to {@code true} the {@code task} will be
   * interrupted if the {@code Driver} is closed.
   * <p>
   * Note: task cannot return null
   */
  private <T> Optional<T> tryWithLock(long timeout, TimeUnit unit, boolean interruptOnClose, Supplier<T> task) {
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
      inFlightException = addSuppressedException(
          inFlightException,
          t,
          "Error destroying driver for task %s",
          driverContext.getId());
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
      if (root != null) {
        root.close();
      }
      if (sinkHandle != null) {
        sinkHandle.close();
      }
    } catch (InterruptedException t) {
      // don't record the stack
      wasInterrupted = true;
    } catch (Throwable t) {
      // TODO currently, we won't know exact operator which is failed in closing
      inFlightException = addSuppressedException(
          inFlightException,
          t,
          "Error closing operator {} for fragment instance {}",
          root.getOperatorContext().getOperatorId(),
          driverContext.getId());
    } finally {
      // reset the interrupted flag
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return inFlightException;
  }

  private static Throwable addSuppressedException(Throwable inFlightException, Throwable newException, String message, Object... args) {
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
