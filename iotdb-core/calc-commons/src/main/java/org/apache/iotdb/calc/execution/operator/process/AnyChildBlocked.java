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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * Tracks ready children while registering at most one listener per child's pending future.
 *
 * <p>Methods are called by the driver thread. Only ReadyListener.run() executes on completion
 * threads. The supplier returns null for a closed child. A child's future is refreshed after its
 * index is taken, so the caller must perform at most one hasNext/next step before checking
 * readiness again.
 */
final class AnyChildBlocked implements AutoCloseable {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AnyChildBlocked.class);
  private static final long CHILD_STATE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ChildState.class);
  private static final long LISTENER_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ReadyListener.class);

  private final ChildState[] states;
  private final IntFunction<ListenableFuture<?>> blockedSupplier;

  private int readyIndex = -1;
  private SettableFuture<Integer> waiting;
  private boolean closed;

  AnyChildBlocked(int childCount, IntFunction<ListenableFuture<?>> blockedSupplier) {
    this.blockedSupplier = blockedSupplier;
    states = new ChildState[childCount];
    for (int i = 0; i < childCount; i++) {
      states[i] = new ChildState(i);
    }
  }

  ListenableFuture<?> isBlocked() {
    if (closed || readyIndex >= 0) {
      return Futures.immediateVoidFuture();
    }
    if (waiting != null && !waiting.isCancelled()) {
      return waiting;
    }
    waiting = null;

    boolean hasChild = false;
    for (ChildState state : states) {
      if (state.future == null) {
        state.future = blockedSupplier.apply(state.index);
      }
      if (state.future != null) {
        hasChild = true;
        if (readyIndex < 0 && state.future.isDone()) {
          readyIndex = state.index;
        }
      }
    }

    // Initialize all children's futures, even when an earlier child is already ready.
    if (readyIndex >= 0 || !hasChild) {
      return Futures.immediateVoidFuture();
    }

    SettableFuture<Integer> round = SettableFuture.create();
    waiting = round;
    for (ChildState state : states) {
      if (state.future != null) {
        state.await(round);
      }
    }
    return round;
  }

  /** Returns -1 when no child can be consumed, without blocking the driver thread. */
  int takeReadyChildIndex() throws ExecutionException {
    if (!isBlocked().isDone() || closed) {
      return -1;
    }
    int index = readyIndex;
    if (index < 0 && waiting != null) {
      index = Futures.getDone(waiting);
    }
    if (index < 0) {
      return -1;
    }

    // Completion may also mean failure/cancellation; propagate it before accessing the child.
    Futures.getDone(states[index].future);
    states[index].reset();
    readyIndex = -1;
    waiting = null;
    return index;
  }

  @Override
  public void close() {
    closed = true;
    readyIndex = -1;
    for (ChildState state : states) {
      state.reset();
    }
    if (waiting != null) {
      waiting.set(-1);
      waiting = null;
    }
  }

  long ramBytesUsed() {
    long size = INSTANCE_SIZE + RamUsageEstimator.shallowSizeOf(states);
    for (ChildState state : states) {
      size += CHILD_STATE_SIZE;
      if (state.listener != null) {
        size += LISTENER_SIZE;
      }
    }
    return size;
  }

  private static final class ChildState {
    private final int index;
    private ListenableFuture<?> future;
    private ReadyListener listener;

    private ChildState(int index) {
      this.index = index;
    }

    private void await(SettableFuture<Integer> round) {
      if (listener == null) {
        listener = new ReadyListener(index, round);
        future.addListener(listener, directExecutor());
      } else {
        listener.target = round;
      }
      // Covers completion before/during retargeting, even if the old callback has already run.
      if (future.isDone()) {
        round.set(index);
      }
    }

    private void reset() {
      future = null;
      if (listener != null) {
        listener.target = null;
        listener = null;
      }
    }
  }

  private static final class ReadyListener implements Runnable {
    private final int index;
    private volatile SettableFuture<Integer> target;

    private ReadyListener(int index, SettableFuture<Integer> target) {
      this.index = index;
      this.target = target;
    }

    @Override
    public void run() {
      SettableFuture<Integer> round = target;
      if (round != null) {
        round.set(index);
      }
    }
  }
}
