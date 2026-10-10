/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.utils.TestOnly;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Monotonic timing that remains readable while a queue's prefetch worker is blocked. */
class SubscriptionQueueTimeTracker {

  private final LongSupplier nanoTime;

  private volatile boolean started;
  private boolean delivered;
  private volatile long lastDeliveryTimeNs;
  private volatile long lastDeliveryIntervalMs;
  private volatile long maxDeliveryIntervalMs;
  private volatile long lastPrefetchCompletionTimeNs;
  private volatile long prefetchStartTimeNs;
  private volatile boolean prefetchRunning;
  private volatile long maxPrefetchDurationMs;

  SubscriptionQueueTimeTracker() {
    this(System::nanoTime);
  }

  @TestOnly
  SubscriptionQueueTimeTracker(final LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
  }

  synchronized void start() {
    if (!started) {
      final long nowNs = nanoTime.getAsLong();
      lastDeliveryTimeNs = nowNs;
      lastPrefetchCompletionTimeNs = nowNs;
      started = true;
    }
  }

  synchronized void recordDelivery() {
    start();
    final long nowNs = nanoTime.getAsLong();
    if (delivered) {
      lastDeliveryIntervalMs = elapsedMs(nowNs, lastDeliveryTimeNs);
      maxDeliveryIntervalMs = Math.max(maxDeliveryIntervalMs, lastDeliveryIntervalMs);
    }
    lastDeliveryTimeNs = nowNs;
    delivered = true;
  }

  synchronized void beginPrefetch() {
    start();
    prefetchStartTimeNs = nanoTime.getAsLong();
    prefetchRunning = true;
  }

  synchronized long endPrefetch() {
    final long nowNs = nanoTime.getAsLong();
    final long durationMs = elapsedMs(nowNs, prefetchStartTimeNs);
    maxPrefetchDurationMs = Math.max(maxPrefetchDurationMs, durationMs);
    lastPrefetchCompletionTimeNs = nowNs;
    prefetchRunning = false;
    return durationMs;
  }

  long getLastDeliveryIntervalMs() {
    return lastDeliveryIntervalMs;
  }

  long getMaxDeliveryIntervalMs() {
    return maxDeliveryIntervalMs;
  }

  long getDeliveryIdleTimeMs() {
    return started ? elapsedMs(nanoTime.getAsLong(), lastDeliveryTimeNs) : 0L;
  }

  /** Elapsed time of an ongoing round, including waiting to acquire the queue lock. */
  long getPrefetchDurationMs() {
    return prefetchRunning ? elapsedMs(nanoTime.getAsLong(), prefetchStartTimeNs) : 0L;
  }

  long getMaxPrefetchDurationMs() {
    return Math.max(maxPrefetchDurationMs, getPrefetchDurationMs());
  }

  /** Time since the last completed round, or activation before the first completed round. */
  long getPrefetchIdleTimeMs() {
    return started ? elapsedMs(nanoTime.getAsLong(), lastPrefetchCompletionTimeNs) : 0L;
  }

  private static long elapsedMs(final long nowNs, final long thenNs) {
    return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(nowNs - thenNs));
  }
}
