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

package org.apache.iotdb.commons.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ConditionAwaiter {

  private static final long DEFAULT_POLL_INTERVAL_MS = 100;
  private static final long DEFAULT_TIMEOUT_MS = 10_000;

  private long timeoutMs = DEFAULT_TIMEOUT_MS;
  private long pollIntervalMs = DEFAULT_POLL_INTERVAL_MS;
  private long pollDelayMs = 0;
  private boolean ignoreAllExceptions = false;
  private boolean forever = false;
  private final List<Class<? extends Exception>> ignoredExceptions = new ArrayList<>();

  ConditionAwaiter() {}

  public ConditionAwaiter atMost(long time, TimeUnit unit) {
    this.timeoutMs = unit.toMillis(time);
    return this;
  }

  public ConditionAwaiter pollInterval(long time, TimeUnit unit) {
    this.pollIntervalMs = unit.toMillis(time);
    return this;
  }

  public ConditionAwaiter pollDelay(long time, TimeUnit unit) {
    this.pollDelayMs = unit.toMillis(time);
    return this;
  }

  public ConditionAwaiter ignoreExceptions() {
    this.ignoreAllExceptions = true;
    return this;
  }

  public ConditionAwaiter ignoreException(Class<? extends Exception> exceptionType) {
    this.ignoredExceptions.add(exceptionType);
    return this;
  }

  public ConditionAwaiter forever() {
    this.forever = true;
    return this;
  }

  public void until(Callable<Boolean> conditionEvaluator) {
    long startTime = System.currentTimeMillis();

    if (pollDelayMs > 0) {
      sleep(pollDelayMs);
    }

    Exception lastException = null;
    while (true) {
      try {
        Boolean result = conditionEvaluator.call();
        if (Boolean.TRUE.equals(result)) {
          return;
        }
        lastException = null;
      } catch (Exception e) {
        if (shouldIgnore(e)) {
          lastException = e;
        } else if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          throw new AwaitTimeoutException("Interrupted while awaiting condition", e);
        } else {
          throw new AwaitTimeoutException("Exception while evaluating condition", e);
        }
      }

      if (!forever && System.currentTimeMillis() - startTime >= timeoutMs) {
        String message = String.format("Condition was not met within %d ms", timeoutMs);
        if (lastException != null) {
          throw new AwaitTimeoutException(message, lastException);
        }
        throw new AwaitTimeoutException(message);
      }

      sleep(pollIntervalMs);
    }
  }

  public void untilAsserted(Runnable assertion) {
    final AssertionErrorHolder holder = new AssertionErrorHolder();
    try {
      until(
          () -> {
            try {
              assertion.run();
              return true;
            } catch (AssertionError e) {
              holder.error = e;
              return false;
            }
          });
    } catch (AwaitTimeoutException e) {
      if (holder.error != null) {
        throw new AwaitTimeoutException(e.getMessage(), holder.error);
      }
      throw e;
    }
  }

  private static final class AssertionErrorHolder {
    AssertionError error;
  }

  private boolean shouldIgnore(Exception e) {
    if (ignoreAllExceptions) {
      return true;
    }
    for (Class<? extends Exception> ignoredType : ignoredExceptions) {
      if (ignoredType.isInstance(e)) {
        return true;
      }
    }
    return false;
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AwaitTimeoutException("Interrupted while awaiting condition", e);
    }
  }
}
