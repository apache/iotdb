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

import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class Retriable {

  /**
   * * Attempt the given operation {@param supplier}. May retry several times according to the given
   * retry policy {@param policy}
   *
   * @param name the operation's name.
   * @param log the logger to print messages.
   * @throws InterruptedException if the sleep is interrupted.
   * @throws THROWABLE if the operation throws a pre-defined error.
   * @return the result of given operation if it executes successfully
   */
  public static <RETURN, THROWABLE extends Throwable> RETURN attempt(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      RetryPolicy<RETURN> policy,
      Supplier<?> name,
      Logger log)
      throws THROWABLE, InterruptedException {
    Objects.requireNonNull(supplier, "supplier == null");
    for (int attempt = 0; ; attempt++) {
      try {
        final RETURN ret = supplier.get();
        // if we should retry and the total attempt doesn't reach max allowed attempts
        if (policy.shouldRetry(ret) && policy.shoudAttempt(attempt)) {
          TimeDuration waitTime = policy.getWaitTime(attempt);
          if (log != null && log.isDebugEnabled()) {
            log.debug(
                "Failed {}, attempt #{}, sleep {} and then retry", name.get(), attempt, waitTime);
          }
          waitTime.sleep();
          continue;
        }
        return ret;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (log != null && log.isWarnEnabled()) {
          log.warn("{}: interrupted when waiting for retry", name.get());
        }
        throw e;
      }
    }
  }

  /** Attempt indefinitely until the given {@param condition} holds */
  public static void attemptUntilTrue(
      BooleanSupplier condition, TimeDuration sleepTime, String name, Logger log)
      throws InterruptedException {
    attemptUntilTrue(condition, -1, sleepTime, name, log);
  }

  /**
   * Attempt indefinitely until the given {@param condition} holds or reaches {@param maxAttempts}
   */
  public static void attemptUntilTrue(
      BooleanSupplier condition, int maxAttempts, TimeDuration sleepTime, String name, Logger log)
      throws InterruptedException {
    Objects.requireNonNull(condition, "condition == null");
    attempt(
        () -> null,
        RetryPolicy.newBuilder()
            .setRetryHandler(ret -> !condition.getAsBoolean())
            .setWaitTime(sleepTime)
            .setMaxAttempts(maxAttempts)
            .build(),
        () -> name,
        log);
  }
}
