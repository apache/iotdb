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
package org.apache.iotdb.consensus.ratis.utils;

import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

public class Retriable {
  public static final Logger logger = LoggerFactory.getLogger(Retriable.class);

  public static class RetryPolicy<RESP> {
    private final Function<RESP, Boolean> retryHandler;
    private final int maxAttempts;
    private final TimeDuration waitTime;

    public RetryPolicy(
        Function<RESP, Boolean> retryHandler, int maxAttempts, TimeDuration waitTime) {
      this.retryHandler = retryHandler;
      this.maxAttempts = maxAttempts;
      this.waitTime = waitTime;
    }

    boolean shouldRetry(RESP resp) {
      return retryHandler.apply(resp);
    }

    public int getMaxAttempts() {
      return maxAttempts;
    }

    public TimeDuration getWaitTime() {
      return waitTime;
    }
  }

  public static class RetryPolicyBuilder<RESP> {
    private Function<RESP, Boolean> retryHandler = (r) -> false;
    private int maxAttempts = 0;
    private TimeDuration waitTime = TimeDuration.ZERO;

    public static <R> RetryPolicyBuilder<R> newBuilder() {
      return new RetryPolicyBuilder<>();
    }

    public RetryPolicyBuilder<RESP> setRetryHandler(Function<RESP, Boolean> retryHandler) {
      this.retryHandler = retryHandler;
      return this;
    }

    public RetryPolicyBuilder<RESP> setMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public RetryPolicyBuilder<RESP> setWaitTime(TimeDuration waitTime) {
      this.waitTime = waitTime;
      return this;
    }

    public RetryPolicy<RESP> build() {
      return new RetryPolicy<>(retryHandler, maxAttempts, waitTime);
    }
  }

  static <RETURN, THROWABLE extends Throwable> RETURN attempt(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      BiPredicate<RETURN, Integer> shouldRetry,
      TimeDuration sleepTime,
      Supplier<?> name,
      Logger log)
      throws THROWABLE, InterruptedException {
    Objects.requireNonNull(supplier, "supplier == null");
    Objects.requireNonNull(shouldRetry, "shouldRetry == null");
    Preconditions.assertTrue(!sleepTime.isNegative(), () -> "sleepTime = " + sleepTime + " < 0");

    for (int i = 1; /* Forever Loop */ ; i++) {
      try {
        final RETURN ret = supplier.get();
        if (!shouldRetry.test(ret, i)) {
          return ret;
        }

        if (log != null && log.isDebugEnabled()) {
          log.debug("Failed {}, attempt #{}, sleep {} and then retry", name.get(), i, sleepTime);
        }
        sleepTime.sleep();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (log != null && log.isDebugEnabled()) {
          log.debug("{}: interrupted when waiting for retry", name.get());
        }
        throw e;
      }
    }
  }

  /** Attempt to get a return value from the given supplier multiple times. */
  static <RETURN, THROWABLE extends Throwable> void attemptRepeatedly(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      BiPredicate<RETURN, Integer> shouldRetry,
      TimeDuration sleepTime,
      String name,
      Logger log)
      throws THROWABLE, InterruptedException {
    attempt(supplier, shouldRetry, sleepTime, () -> name, log);
  }

  /** Attempt to wait the given condition to return true multiple times. */
  public static void attemptUntilTrue(
      BooleanSupplier condition, TimeDuration sleepTime, String name, Logger log)
      throws InterruptedException {
    Objects.requireNonNull(condition, "condition == null");
    attemptRepeatedly(
        () -> null, (nul, attempts) -> condition.getAsBoolean(), sleepTime, name, log);
  }

  public static <RETURN, THROWABLE extends Throwable> RETURN attempt(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      RetryPolicy<RETURN> policy,
      String name,
      Logger logger)
      throws THROWABLE, InterruptedException {
    return attempt(
        supplier,
        (ret, attemptedCount) ->
            policy.shouldRetry(ret) && attemptedCount <= policy.getMaxAttempts(),
        policy.getWaitTime(),
        () -> name,
        logger);
  }
}
