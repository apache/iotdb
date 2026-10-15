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

import org.apache.iotdb.commons.i18n.PipeMessages;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Bridges a byte-based request to {@link RateLimiter}, which is a permit-based limiter.
 *
 * <p>{@link RateLimiter#create(double)} builds a {@code SmoothBursty} limiter whose burst capacity
 * is at most one second's worth of permits, and {@link RateLimiter#tryAcquire(int, long, TimeUnit)}
 * only grants a request when its whole cost fits inside the given timeout. When a single request
 * costs more than the timeout, every attempt fails instantly and the surrounding retry loop
 * degenerates into a 100% CPU busy-wait that never logs anything. Such a request must therefore
 * never be passed to {@code tryAcquire} in one piece, which is what this class takes care of.
 */
public class RateLimitUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitUtils.class);

  /** The default value of {@code RateLimiterHotReloadCheckIntervalMs}. */
  public static final long DEFAULT_CHECK_INTERVAL_MS = 1000;

  // The loop below is a busy-wait guard as well: a healthy chunk needs at most one timed attempt
  // before it falls back to a blocking acquisition, so exceeding this budget means something is
  // wrong (e.g. the limiter is stuck) rather than that we merely have to wait longer.
  private static final int MAX_ATTEMPTS_PER_CHUNK = 20;

  private RateLimitUtils() {
    // Utility class
  }

  /** Decides whether to keep waiting for the rate limiter. */
  @FunctionalInterface
  public interface ContinueChecker {
    /**
     * @return {@code true} to keep acquiring, {@code false} to abort the whole acquisition
     */
    boolean shouldContinue();
  }

  /**
   * Acquires {@code bytes} permits from {@code rateLimiter}, checking the limiter every {@link
   * #DEFAULT_CHECK_INTERVAL_MS}.
   *
   * @param rateLimiter the permit-based limiter to acquire from
   * @param bytes the number of bytes to acquire, non-positive values return immediately
   * @param continueChecker consulted between pieces to stop waiting for a dead pipe
   * @return {@code true} if the whole request was acquired, {@code false} if {@code
   *     continueChecker} asked to abort
   */
  public static boolean acquireWithRateLimit(
      final RateLimiter rateLimiter, final long bytes, final ContinueChecker continueChecker) {
    return acquireWithRateLimit(rateLimiter, bytes, continueChecker, DEFAULT_CHECK_INTERVAL_MS);
  }

  /**
   * Acquires {@code bytes} permits from {@code rateLimiter}.
   *
   * <p>The request is split into pieces that the burst capacity can actually grant, each piece is
   * acquired with a timeout that covers its own cost, and a piece that keeps timing out (which can
   * only happen when the limiter's rate is reloaded downwards) falls back to a blocking acquisition
   * so that the caller stops burning CPU. The average throughput always converges to the configured
   * rate.
   *
   * @param rateLimiter the permit-based limiter to acquire from
   * @param bytes the number of bytes to acquire, non-positive values return immediately
   * @param continueChecker consulted between pieces and on a timeout, to stop waiting for a dead
   *     pipe
   * @param checkIntervalMs how often {@code continueChecker} is consulted while waiting
   * @return {@code true} if the whole request was acquired, {@code false} if {@code
   *     continueChecker} asked to abort
   */
  public static boolean acquireWithRateLimit(
      final RateLimiter rateLimiter,
      final long bytes,
      final ContinueChecker continueChecker,
      final long checkIntervalMs) {
    final long boundedCheckIntervalMs = Math.max(1L, checkIntervalMs);

    long remaining = bytes;
    boolean logPipeSizeMismatch = true;

    while (remaining > 0) {
      // A request that fits into a single piece is served by the limiter's initial burst without
      // waiting, so the checker only matters for requests that need several pieces, i.e. the ones
      // that actually take time to transfer.
      if (remaining < bytes && !continueChecker.shouldContinue()) {
        return false;
      }

      final long piece = calculatePieceSize(rateLimiter, remaining);
      if (logPipeSizeMismatch && piece < remaining) {
        logPipeSizeMismatch = false;
        LOGGER.warn(
            PipeMessages
                .LOG_PIPE_RATE_LIMIT_IS_SMALLER_THAN_THE_SINGLE_REQUEST_SIZE_SO_ARG_BYTES_WILL_BE_TRANSFERRED_IN_ARG_SIZED_PIECES_AT_ARG_BYTES_PER_SECOND_CONSIDER_SETTING_A_LARGER_RATE_LIMIT_FOR_BETTER_PERFORMANCE_4450D4DC,
            bytes,
            piece,
            rateLimiter.getRate());
      }

      final long timeoutMs = calculateTimeoutMs(rateLimiter, piece, boundedCheckIntervalMs);
      int attempts = 0;
      boolean acquired = false;
      while (!acquired) {
        acquired = rateLimiter.tryAcquire((int) piece, timeoutMs, TimeUnit.MILLISECONDS);
        if (acquired) {
          break;
        }

        if (!continueChecker.shouldContinue()) {
          return false;
        }

        if (++attempts >= MAX_ATTEMPTS_PER_CHUNK) {
          // The rate has most likely been reloaded downwards while we were waiting, so our
          // estimate is far too optimistic. Wait for the permits without a deadline instead of
          // retrying with a deadline that we now know we cannot meet, which would only spin.
          rateLimiter.acquire((int) piece);
          acquired = true;
        }
      }

      remaining -= piece;
    }

    return true;
  }

  /**
   * @return a piece size that is small enough for the limiter's burst capacity to grant
   */
  private static long calculatePieceSize(final RateLimiter rateLimiter, final long bytes) {
    // The burst capacity of RateLimiter.create(rate) is one second's worth of permits, so anything
    // larger than that can never be granted by a single timeout-bounded attempt.
    final double bytesPerSecond = rateLimiter.getRate();
    if (!(bytesPerSecond > 0) || bytesPerSecond >= Integer.MAX_VALUE) {
      // Rate limiting is effectively disabled (e.g. the rate is set to Double.MAX_VALUE), but the
      // piece still has to stay within the int range of the limiter's API.
      return Math.min(bytes, Integer.MAX_VALUE);
    }
    return Math.min(bytes, (long) bytesPerSecond);
  }

  /**
   * @return a timeout that covers the expected wait for {@code piece}, so that a legitimate wait is
   *     not mistaken for a timeout
   */
  private static long calculateTimeoutMs(
      final RateLimiter rateLimiter, final long piece, final long checkIntervalMs) {
    final double rate = rateLimiter.getRate();
    if (!(rate > 0)) {
      return checkIntervalMs;
    }

    final double expectedWaitMs = piece / rate * 1000.0;
    if (!(expectedWaitMs > 0) || expectedWaitMs >= Long.MAX_VALUE - checkIntervalMs) {
      return checkIntervalMs;
    }

    return Math.max(checkIntervalMs, (long) expectedWaitMs + checkIntervalMs);
  }
}
