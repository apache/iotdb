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

package org.apache.iotdb.commons.pipe.sink.limiter;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class RateLimitUtilsTest {

  /** The rate limit and the request size reported from the field reproduction. */
  private static final double ONE_MB_PER_SECOND = 1024 * 1024;

  private static final long FIVE_MB = 5 * 1024 * 1024;

  private static final long CHECK_INTERVAL_MS = 1000;

  private static boolean acquire(final RateLimiter rateLimiter, final long bytes) {
    return RateLimitUtils.acquireWithRateLimit(rateLimiter, bytes, () -> true, CHECK_INTERVAL_MS);
  }

  /**
   * A single request larger than the limiter's burst capacity (which is one second's worth of
   * permits) must still be acquired. Before the fix, such a request made every attempt time out
   * immediately, so the caller spun at 100% CPU without ever making progress.
   */
  @Test(timeout = 60_000)
  public void testAcquireRequestLargerThanBurstCapacity() {
    final RateLimiter rateLimiter = RateLimiter.create(ONE_MB_PER_SECOND);

    final long startTime = System.nanoTime();
    final boolean acquired = acquire(rateLimiter, FIVE_MB);
    final double elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;

    Assert.assertTrue(acquired);
    // 5 MB at 1 MB/s, minus the one-second initial burst, must take about 4 seconds.
    Assert.assertTrue(
        "expected to be paced by the rate limiter, but took " + elapsedSeconds + "s",
        elapsedSeconds >= 3.0);
  }

  /** The average throughput must converge to the configured rate limit. */
  @Test(timeout = 60_000)
  public void testAcquireIsPacedAtTheConfiguredRate() {
    final RateLimiter rateLimiter = RateLimiter.create(FIVE_MB / 10.0);

    final long startTime = System.nanoTime();
    acquire(rateLimiter, FIVE_MB);
    final double elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;

    // 5 MB at 0.5 MB/s is about 10 seconds, minus the one-second initial burst.
    Assert.assertTrue(
        "expected to be paced at the configured rate, but took " + elapsedSeconds + "s",
        elapsedSeconds >= 7.0);
  }

  /** A request of more than Integer.MAX_VALUE bytes must not overflow nor spin. */
  @Test(timeout = 120_000)
  public void testAcquireRequestLargerThanIntegerMaxValue() {
    final long bytes = 3L * Integer.MAX_VALUE;

    final RateLimiter rateLimiter = RateLimiter.create(256 * ONE_MB_PER_SECOND);
    final long startTime = System.nanoTime();

    Assert.assertTrue(acquire(rateLimiter, bytes));
    Assert.assertTrue(
        "3 * Integer.MAX_VALUE bytes should be acquired in a few seconds",
        (System.nanoTime() - startTime) / 1_000_000_000.0 < 60.0);
  }

  /**
   * The same must hold when rate limiting is disabled via {@code Double.MAX_VALUE}, even though
   * such a rate makes the piece size calculation take its dedicated branch.
   */
  @Test(timeout = 60_000)
  public void testAcquireRequestLargerThanIntegerMaxValueWithRateLimitingDisabled() {
    final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);
    final long startTime = System.nanoTime();

    // More than two pieces are needed, and every piece but the first is charged to the unlimited
    // burst capacity, so this must complete without waiting.
    Assert.assertTrue(acquire(rateLimiter, 3L * Integer.MAX_VALUE));
    Assert.assertTrue(
        "an unlimited limiter should not make the caller wait",
        (System.nanoTime() - startTime) / 1_000_000_000.0 < 10.0);
  }

  /**
   * When the checker asks to stop (e.g. the pipe has been dropped), a multi-piece request must
   * abort instead of waiting for the remaining pieces.
   */
  @Test(timeout = 60_000)
  public void testAcquireAbortsWhenCheckerSaysStop() {
    // The first piece is served by the initial burst, so the checker is consulted when the second
    // one is about to be acquired and the request is dropped there.
    final RateLimiter rateLimiter = RateLimiter.create(ONE_MB_PER_SECOND);
    final AtomicBoolean checked = new AtomicBoolean(false);

    final long startTime = System.nanoTime();
    final boolean acquired =
        RateLimitUtils.acquireWithRateLimit(
            rateLimiter,
            4 * 1024 * 1024,
            () -> {
              checked.set(true);
              return false;
            },
            CHECK_INTERVAL_MS);
    final double elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;

    Assert.assertTrue(checked.get());
    Assert.assertFalse(acquired);
    // 4 MB at 1 MB/s would need about 3 seconds if the whole request were awaited.
    Assert.assertTrue(
        "expected to abort before waiting for the remaining pieces, but took "
            + elapsedSeconds
            + "s",
        elapsedSeconds < 1.0);
  }

  /** Non-positive requests and unlimited limiters are passed through. */
  @Test
  public void testAcquireEdgeCases() {
    final RateLimiter rateLimiter = RateLimiter.create(ONE_MB_PER_SECOND);

    Assert.assertTrue(acquire(rateLimiter, 0));
    Assert.assertTrue(acquire(rateLimiter, -1));
    // Double.MAX_VALUE is used to disable rate limiting, so nothing needs to be chunked or waited.
    Assert.assertTrue(acquire(RateLimiter.create(Double.MAX_VALUE), FIVE_MB));
  }
}
