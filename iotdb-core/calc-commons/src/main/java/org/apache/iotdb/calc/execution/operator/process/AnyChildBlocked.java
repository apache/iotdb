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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.Operator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.Futures.getDone;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * Observes direct children without calling operators from completion callbacks. The driver owns
 * selection/invalidation; the lock protects listener records shared with asynchronous completions.
 */
final class AnyChildBlocked implements Accountable {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AnyChildBlocked.class);
  private static final long LISTENER_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ChildFutureListener.class);
  private static final long ROUND_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(WaitRound.class)
          + RamUsageEstimator.shallowSizeOfInstance(SettableFuture.class);

  private final List<Operator> children;
  private final Object lock = new Object();
  private final ChildFutureListener[] listeners;
  private ChildFutureListener readyHead;
  private ChildFutureListener readyTail;
  private WaitRound currentRound;
  private Throwable firstFailure;
  private boolean closed;
  private boolean initialized;
  private int childToObserve = -1;

  AnyChildBlocked(List<Operator> children) {
    this.children = children;
    this.listeners = new ChildFutureListener[children.size()];
  }

  ListenableFuture<Integer> getAnyChild() {
    WaitRound round;
    WaitRound selection;
    boolean initialize;
    int invalidatedChild;
    synchronized (lock) {
      if (currentRound != null) {
        return currentRound.selected;
      }
      round = new WaitRound();
      currentRound = round;
      initialize = !initialized;
      initialized = true;
      invalidatedChild = childToObserve;
      childToObserve = -1;
      selection = selectReadyChild();
    }
    completeSelection(selection);

    if (initialize) {
      // Activate every direct input once, even if an earlier input is already ready. Pending
      // listeners enqueue their completion without being retargeted or polled on every block.
      for (int childId = 0; childId < children.size(); childId++) {
        observeChild(childId);
      }
    } else if (invalidatedChild >= 0) {
      // Only advancing an input invalidates its readiness. Requeue it behind inputs that are
      // already ready so that a continuously ready input cannot starve its siblings.
      observeChild(invalidatedChild);
    }
    return round.selected;
  }

  private void observeChild(int childId) {
    Operator child;
    synchronized (lock) {
      if (closed) {
        return;
      }
      child = children.get(childId);
    }
    if (child == null) {
      return;
    }
    ListenableFuture<?> observedFuture = child.isBlocked();
    ChildFutureListener listener = new ChildFutureListener(childId, observedFuture);
    synchronized (lock) {
      if (closed) {
        return;
      }
      listeners[childId] = listener;
    }
    // Publish the record before registration, including when completion runs inline. Callbacks
    // only update readiness; operator methods remain confined to the driver thread.
    observedFuture.addListener(listener, directExecutor());
  }

  /** Called under lock; claiming a selection and removing its ready record are atomic. */
  private WaitRound selectReadyChild() {
    if (currentRound == null || currentRound.claimed) {
      return null;
    }
    if (closed || children.isEmpty()) {
      currentRound.childId = -1;
    } else if (firstFailure != null) {
      currentRound.failure = firstFailure;
    } else {
      ChildFutureListener listener;
      do {
        listener = readyHead;
        if (listener == null) {
          return null;
        }
        readyHead = listener.nextReady;
        listener.nextReady = null;
        if (readyHead == null) {
          readyTail = null;
        }
        // Retired records are discarded once, rather than scanning closed input slots per block.
      } while (listeners[listener.childId] != listener);
      currentRound.childId = listener.childId;
    }
    currentRound.claimed = true;
    return currentRound;
  }

  private static void completeSelection(WaitRound selection) {
    if (selection == null) {
      return;
    }
    // Completing the future can invoke downstream listeners synchronously. Do not hold lock.
    if (selection.failure == null) {
      selection.selected.set(selection.childId);
    } else {
      selection.selected.setException(selection.failure);
    }
  }

  void checkFailure() throws Exception {
    Throwable failure;
    synchronized (lock) {
      failure = firstFailure;
    }
    if (failure instanceof Exception) {
      throw (Exception) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    if (failure != null) {
      throw new RuntimeException(failure);
    }
  }

  void invalidateChild(int childId) {
    synchronized (lock) {
      listeners[childId] = null;
      currentRound = null;
      childToObserve = childId;
    }
  }

  void close() {
    WaitRound round;
    synchronized (lock) {
      closed = true;
      round = currentRound;
      currentRound = null;
      childToObserve = -1;
      while (readyHead != null) {
        ChildFutureListener next = readyHead.nextReady;
        readyHead.nextReady = null;
        readyHead = next;
      }
      readyTail = null;
      Arrays.fill(listeners, null);
    }
    if (round != null) {
      round.selected.set(-1);
    }
  }

  @Override
  public long ramBytesUsed() {
    synchronized (lock) {
      long size =
          INSTANCE_SIZE
              + RamUsageEstimator.shallowSizeOfInstance(Object.class)
              + RamUsageEstimator.shallowSizeOf(listeners);
      for (ChildFutureListener listener : listeners) {
        if (listener != null) {
          size += LISTENER_SIZE;
        }
      }
      return size + (currentRound == null ? 0 : ROUND_SIZE);
    }
  }

  private static final class WaitRound {
    private final SettableFuture<Integer> selected = SettableFuture.create();
    private boolean claimed;
    private int childId;
    private Throwable failure;
  }

  private final class ChildFutureListener implements Runnable {
    private final int childId;
    private final ListenableFuture<?> observedFuture;
    private ChildFutureListener nextReady;
    private boolean completed;

    private ChildFutureListener(int childId, ListenableFuture<?> observedFuture) {
      this.childId = childId;
      this.observedFuture = observedFuture;
    }

    @Override
    public void run() {
      Throwable failure = null;
      try {
        getDone(observedFuture);
      } catch (ExecutionException e) {
        failure = e.getCause();
      } catch (CancellationException e) {
        failure = e;
      }
      WaitRound selection;
      synchronized (lock) {
        if (closed || listeners[childId] != this || completed) {
          return;
        }
        completed = true;
        if (failure != null && firstFailure == null) {
          firstFailure = failure;
        }
        if (failure == null) {
          if (readyTail == null) {
            readyHead = this;
          } else {
            readyTail.nextReady = this;
          }
          readyTail = this;
        }
        selection = selectReadyChild();
      }
      completeSelection(selection);
    }
  }
}
