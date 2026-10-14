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
  private WaitRound currentRound;
  private Throwable firstFailure;
  private boolean closed;
  private int nextChild;

  AnyChildBlocked(List<Operator> children) {
    this.children = children;
    this.listeners = new ChildFutureListener[children.size()];
  }

  ListenableFuture<Integer> getAnyChild() {
    WaitRound round;
    synchronized (lock) {
      if (currentRound != null) {
        return currentRound.selected;
      }
      round = new WaitRound();
      currentRound = round;
      for (ChildFutureListener listener : listeners) {
        if (listener != null) {
          listener.target = round;
        }
      }
    }

    // Activate every live direct child, even if an earlier one has already become ready. Only
    // invalidated records call child.isBlocked(); a long-pending future keeps its one listener.
    for (int offset = 0; offset < children.size(); offset++) {
      int childId = (nextChild + offset) % children.size();
      Operator child;
      ChildFutureListener listener;
      synchronized (lock) {
        if (closed) {
          break;
        }
        child = children.get(childId);
        listener = listeners[childId];
      }
      if (child == null) {
        continue;
      }
      if (listener == null) {
        ListenableFuture<?> observedFuture = child.isBlocked();
        synchronized (lock) {
          if (closed) {
            break;
          }
          listener = new ChildFutureListener(childId, observedFuture, round);
          listeners[childId] = listener;
        }
        // The target is published before registration. addListener also handles a future that
        // finishes between isBlocked and registration, including inline completion.
        observedFuture.addListener(listener, directExecutor());
      } else if (listener.observedFuture.isDone()) {
        // A completed record may have notified a previous round. Recheck it for this round.
        listener.run();
      }
    }

    Throwable failure;
    boolean isClosed;
    synchronized (lock) {
      failure = firstFailure;
      isClosed = closed;
    }
    if (isClosed || children.isEmpty()) {
      round.selected.set(-1);
    } else if (failure != null) {
      round.selected.setException(failure);
    }
    return round.selected;
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
      if (currentRound != null) {
        currentRound.valid = false;
        currentRound = null;
      }
      nextChild = (childId + 1) % children.size();
    }
  }

  void finishChild(int childId) {
    invalidateChild(childId);
  }

  void close() {
    WaitRound round;
    synchronized (lock) {
      closed = true;
      round = currentRound;
      currentRound = null;
      if (round != null) {
        round.valid = false;
      }
      for (ChildFutureListener listener : listeners) {
        if (listener != null) {
          listener.target = null;
        }
      }
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
    private boolean valid = true;
  }

  private final class ChildFutureListener implements Runnable {
    private final int childId;
    private final ListenableFuture<?> observedFuture;
    private WaitRound target;

    private ChildFutureListener(int childId, ListenableFuture<?> observedFuture, WaitRound target) {
      this.childId = childId;
      this.observedFuture = observedFuture;
      this.target = target;
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
      WaitRound round;
      synchronized (lock) {
        if (closed || listeners[childId] != this) {
          return;
        }
        if (failure != null && firstFailure == null) {
          firstFailure = failure;
        }
        round = target;
        if (round == null || !round.valid || round != currentRound) {
          return;
        }
        failure = firstFailure;
      }
      // Completing a selection can invoke downstream listeners synchronously. Do not hold lock.
      if (failure == null) {
        round.selected.set(childId);
      } else {
        round.selected.setException(failure);
      }
    }
  }
}
