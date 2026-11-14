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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Per-group timers on a shared scheduler; arming a kind replaces its previous deadline. */
public final class RaftTimer {

  private final ScheduledExecutorService scheduler;
  private final Consumer<Event> sink;
  private final Map<Action.TimerKind, ScheduledFuture<?>> armed =
      new EnumMap<>(Action.TimerKind.class);
  private volatile boolean closed;

  public RaftTimer(ScheduledExecutorService scheduler, Consumer<Event> sink) {
    this.scheduler = scheduler;
    this.sink = sink;
  }

  public synchronized void arm(Action.TimerKind kind, long delayMs) {
    if (closed) {
      return;
    }
    cancel(kind);
    armed.put(kind, scheduler.schedule(() -> fire(kind), delayMs, TimeUnit.MILLISECONDS));
  }

  public synchronized void cancel(Action.TimerKind kind) {
    ScheduledFuture<?> f = armed.remove(kind);
    if (f != null) {
      f.cancel(false);
    }
  }

  private void fire(Action.TimerKind kind) {
    if (closed) {
      return;
    }
    switch (kind) {
      case ELECTION:
        sink.accept(new Event.ElectionTimeout());
        break;
      case HEARTBEAT:
        sink.accept(new Event.HeartbeatTick());
        break;
      case LEASE:
        sink.accept(new Event.LeaseCheck(System.nanoTime()));
        break;
      default:
        break;
    }
  }

  /** One-shot: feed {@code ev} after {@code delayMs} (independent of the kinds). */
  public synchronized void after(long delayMs, Event ev) {
    if (closed) {
      return;
    }
    scheduler.schedule(
        () -> {
          if (!closed) {
            sink.accept(ev);
          }
        },
        Math.max(1, delayMs),
        TimeUnit.MILLISECONDS);
  }

  public synchronized void close() {
    closed = true;
    for (ScheduledFuture<?> f : armed.values()) {
      f.cancel(false);
    }
    armed.clear();
  }
}
