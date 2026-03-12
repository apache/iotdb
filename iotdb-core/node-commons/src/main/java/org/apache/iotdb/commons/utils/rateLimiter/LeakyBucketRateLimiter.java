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

package org.apache.iotdb.commons.utils.rateLimiter;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A global leaky-bucket rate limiter for bytes throughput. Features: - Strict throughput limiting
 * (no burst) - Smooth bandwidth shaping - Thread-safe - Fair for multi-thread - Low contention
 */
public class LeakyBucketRateLimiter {
  /** bytes per second */
  private volatile long bytesPerSecond;

  /** start time */
  private final long startTimeNs;

  /** total consumed bytes */
  private final AtomicLong totalBytes = new AtomicLong(0);

  public LeakyBucketRateLimiter(long bytesPerSecond) {
    if (bytesPerSecond <= 0) {
      throw new IllegalArgumentException("bytesPerSecond must be > 0");
    }
    this.bytesPerSecond = bytesPerSecond;
    this.startTimeNs = System.nanoTime();
  }

  /**
   * Acquire permission for reading bytes.
   *
   * <p>This method will block if reading too fast.
   */
  public void acquire(long bytes) {
    if (bytes <= 0) {
      return;
    }

    long currentTotal = totalBytes.addAndGet(bytes);

    long expectedTimeNs = expectedTimeNs(currentTotal);
    long now = System.nanoTime();

    long sleepNs = expectedTimeNs - now;

    if (sleepNs > 0) {
      LockSupport.parkNanos(sleepNs);
    }
  }

  /**
   * Try acquire without blocking.
   *
   * @return true if allowed immediately
   */
  public boolean tryAcquire(long bytes) {
    if (bytes <= 0) {
      return true;
    }

    long currentTotal = totalBytes.addAndGet(bytes);

    long expectedTimeNs = expectedTimeNs(currentTotal);
    long now = System.nanoTime();

    if (expectedTimeNs <= now) {
      return true;
    }

    // rollback
    totalBytes.addAndGet(-bytes);
    return false;
  }

  /** Update rate dynamically. */
  public void setRate(long newBytesPerSecond) {
    if (newBytesPerSecond <= 0) {
      throw new IllegalArgumentException("bytesPerSecond must be > 0");
    }
    this.bytesPerSecond = newBytesPerSecond;
  }

  /** Current rate. */
  public long getRate() {
    return bytesPerSecond;
  }

  /** Total bytes processed. */
  public long getTotalBytes() {
    return totalBytes.get();
  }

  /** Expected time based on bytes processed. */
  private long expectedTimeNs(long totalBytes) {
    return startTimeNs + (totalBytes * 1_000_000_000L) / bytesPerSecond;
  }
}
