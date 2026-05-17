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

package org.apache.iotdb.db.subscription.task.subtask;

import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.consensus.PrefetchRoundResult;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsensusPrefetchSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusPrefetchSubtask.class);

  private final String taskId;
  private final ConsensusPrefetchingQueue queue;
  private final Object monitor = new Object();

  private ConsensusSubscriptionPrefetchExecutor executor;

  private boolean scheduledOrRunning = false;
  private boolean running = false;
  private boolean wakeupPending = false;
  private boolean closed = false;
  private long delayedWakeToken = 0L;

  public ConsensusPrefetchSubtask(final ConsensusPrefetchingQueue queue) {
    this.queue = queue;
    this.taskId = queue.getPrefetchingQueueId() + "_" + queue.getConsensusGroupId();
  }

  public String getTaskId() {
    return taskId;
  }

  public void bindExecutor(final ConsensusSubscriptionPrefetchExecutor executor) {
    this.executor = executor;
  }

  public void requestWakeupNow() {
    final ConsensusSubscriptionPrefetchExecutor currentExecutor = executor;
    if (currentExecutor == null) {
      return;
    }

    boolean shouldEnqueue = false;
    synchronized (monitor) {
      if (closed) {
        return;
      }
      delayedWakeToken++;
      if (scheduledOrRunning) {
        wakeupPending = true;
        return;
      }
      scheduledOrRunning = true;
      shouldEnqueue = true;
    }

    if (shouldEnqueue) {
      currentExecutor.enqueue(this);
    }
  }

  public void scheduleWakeupAfter(final long delayMs) {
    final ConsensusSubscriptionPrefetchExecutor currentExecutor = executor;
    if (currentExecutor == null) {
      return;
    }

    long delayedToken;
    synchronized (monitor) {
      if (closed || scheduledOrRunning || wakeupPending) {
        return;
      }
      delayedToken = ++delayedWakeToken;
    }
    currentExecutor.schedule(this, delayMs, delayedToken);
  }

  public void runOneRound() {
    PrefetchRoundResult result = PrefetchRoundResult.dormant();

    synchronized (monitor) {
      if (closed) {
        scheduledOrRunning = false;
        monitor.notifyAll();
        return;
      }
      running = true;
    }

    try {
      result = queue.drivePrefetchOnce();
    } catch (final Throwable t) {
      LOGGER.error(
          "ConsensusPrefetchSubtask {}: unexpected error while driving queue {}", taskId, queue, t);
      result = PrefetchRoundResult.rescheduleAfter(100L);
    }

    boolean shouldEnqueue = false;
    Long delayedWakeMs = null;
    long delayedToken = 0L;
    synchronized (monitor) {
      running = false;
      if (closed) {
        scheduledOrRunning = false;
        monitor.notifyAll();
        return;
      }

      if (wakeupPending) {
        wakeupPending = false;
        shouldEnqueue = true;
      } else {
        switch (result.getType()) {
          case RESCHEDULE_NOW:
            shouldEnqueue = true;
            break;
          case RESCHEDULE_LATER:
            delayedToken = ++delayedWakeToken;
            delayedWakeMs = result.getDelayMs();
            scheduledOrRunning = false;
            break;
          case DORMANT:
          default:
            scheduledOrRunning = false;
            break;
        }
      }

      if (shouldEnqueue) {
        scheduledOrRunning = true;
      }
      monitor.notifyAll();
    }

    final ConsensusSubscriptionPrefetchExecutor currentExecutor = executor;
    if (currentExecutor == null) {
      return;
    }
    if (shouldEnqueue) {
      currentExecutor.enqueue(this);
    } else if (delayedWakeMs != null) {
      currentExecutor.schedule(this, delayedWakeMs, delayedToken);
    }
  }

  public void fireScheduledWakeup(final long delayedToken) {
    final ConsensusSubscriptionPrefetchExecutor currentExecutor = executor;
    if (currentExecutor == null) {
      return;
    }

    boolean shouldEnqueue = false;
    synchronized (monitor) {
      if (closed || delayedWakeToken != delayedToken || scheduledOrRunning) {
        return;
      }
      scheduledOrRunning = true;
      shouldEnqueue = true;
    }

    if (shouldEnqueue) {
      currentExecutor.enqueue(this);
    }
  }

  public void cancelPendingExecution() {
    synchronized (monitor) {
      delayedWakeToken++;
      wakeupPending = false;
      if (scheduledOrRunning && !running) {
        scheduledOrRunning = false;
      }
      monitor.notifyAll();
    }
  }

  public void awaitIdle() {
    synchronized (monitor) {
      while (running || scheduledOrRunning) {
        try {
          monitor.wait(50L);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  public void close() {
    synchronized (monitor) {
      closed = true;
      delayedWakeToken++;
      wakeupPending = false;
      if (!running) {
        scheduledOrRunning = false;
        monitor.notifyAll();
        return;
      }
      while (scheduledOrRunning) {
        try {
          monitor.wait(50L);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  public boolean isClosed() {
    synchronized (monitor) {
      return closed;
    }
  }

  public boolean isScheduledOrRunning() {
    synchronized (monitor) {
      return scheduledOrRunning;
    }
  }
}
