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

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Retriable {
  /**
   * Attempt the given operation {@param supplier}. If the result is not expected (as indicated via
   * {@param shouldRetry}), then retry this operation.
   *
   * @param maxAttempts max retry attempts. *-1 indicates for retrying indefinitely.*
   * @param sleepTime sleep time during each retry.
   * @param name the operation's name.
   * @param log the logger to print messages.
   * @throws InterruptedException if the sleep is interrupted.
   * @throws THROWABLE if the operation throws a pre-defined error.
   * @return the result of given operation if it executes successfully
   */
  public static <RETURN, THROWABLE extends Throwable> RETURN attempt(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      Predicate<RETURN> shouldRetry,
      int maxAttempts,
      TimeDuration sleepTime,
      Supplier<?> name,
      Logger log)
      throws THROWABLE, InterruptedException {
    Objects.requireNonNull(supplier, "supplier == null");
    Objects.requireNonNull(shouldRetry, "shouldRetry == null");
    Preconditions.assertTrue(maxAttempts == -1 || maxAttempts > 0);
    Preconditions.assertTrue(!sleepTime.isNegative(), () -> "sleepTime = " + sleepTime + " < 0");

    for (int i = 1; /* Forever Loop */ ; i++) {
      try {
        final RETURN ret = supplier.get();
        // if we should retry and the total attempt doesn't reach max allowed attempts
        if (shouldRetry.test(ret) && (maxAttempts == -1 || i <= maxAttempts)) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Failed {}, attempt #{}, sleep {} and then retry", name.get(), i, sleepTime);
          }
          sleepTime.sleep();
          continue;
        }
        return ret;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (log != null && log.isDebugEnabled()) {
          log.debug("{}: interrupted when waiting for retry", name.get());
        }
        if (maxAttempts != -1 && i == maxAttempts) {
          // throws out the InterruptedException if it's the last chance to retry
          throw e;
        }
      }
    }
  }

  /** Attempt indefinitely until the given {@param condition} holds */
  public static void attemptUntilTrue(
      BooleanSupplier condition, TimeDuration sleepTime, String name, Logger log)
      throws InterruptedException {
    Objects.requireNonNull(condition, "condition == null");
    attempt(() -> null, ret -> !condition.getAsBoolean(), -1, sleepTime, () -> name, log);
  }

  /**
   * * Attempt the given operation {@param supplier}. May retry several times according to the given
   * retry policy {@param policy}
   *
   * @param name the operation's name.
   * @param logger the logger to print messages.
   * @throws InterruptedException if the sleep is interrupted.
   * @throws THROWABLE if the operation throws a pre-defined error.
   * @return the result of given operation if it executes successfully
   */
  public static <RETURN, THROWABLE extends Throwable> RETURN attempt(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      RetryPolicy<RETURN> policy,
      Supplier<?> name,
      Logger logger)
      throws THROWABLE, InterruptedException {
    return attempt(
        supplier, policy::shouldRetry, policy.getMaxAttempts(), policy.getWaitTime(), name, logger);
  }
}
