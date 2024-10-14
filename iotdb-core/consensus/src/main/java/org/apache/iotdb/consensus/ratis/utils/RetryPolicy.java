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

import java.util.function.Function;

public class RetryPolicy<RESP> {
  private final Function<RESP, Boolean> retryHandler;

  /** -1 means retry indefinitely */
  private final int maxAttempts;

  private final boolean exponentialBackoff;
  private final TimeDuration waitTime;
  private final TimeDuration maxWaitTime;

  public RetryPolicy(
      Function<RESP, Boolean> retryHandler,
      int maxAttempts,
      TimeDuration waitTime,
      TimeDuration maxWaitTime,
      boolean exponentialBackoff) {
    this.retryHandler = retryHandler;
    this.maxAttempts = maxAttempts;
    this.waitTime = waitTime;
    this.maxWaitTime = maxWaitTime;
    this.exponentialBackoff = exponentialBackoff;
  }

  boolean shouldRetry(RESP resp) {
    return retryHandler.apply(resp);
  }

  boolean shoudAttempt(int attempt) {
    return maxAttempts == -1 || attempt < maxAttempts;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public TimeDuration getWaitTime(int attempt) {
    if (exponentialBackoff) {
      TimeDuration sleepTime = waitTime.multiply(Math.pow(2, attempt));
      return maxWaitTime.getDuration() != 0 && sleepTime.compareTo(maxWaitTime) > 0
          ? maxWaitTime
          : sleepTime;
    }
    return waitTime;
  }

  public static <RESP> RetryPolicyBuilder<RESP> newBuilder() {
    return new RetryPolicyBuilder<>();
  }

  public static class RetryPolicyBuilder<RESP> {
    private Function<RESP, Boolean> retryHandler = (r) -> false;
    private int maxAttempts = 0;
    public boolean exponentialBackoff = false;
    private TimeDuration waitTime = TimeDuration.ZERO;
    private TimeDuration maxWaitTime = TimeDuration.ZERO;

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

    public RetryPolicyBuilder<RESP> setMaxWaitTime(TimeDuration maxWaitTime) {
      this.maxWaitTime = maxWaitTime;
      return this;
    }

    public RetryPolicyBuilder<RESP> setExponentialBackoff(boolean exponentialBackoff) {
      this.exponentialBackoff = exponentialBackoff;
      return this;
    }

    public RetryPolicy<RESP> build() {
      return new RetryPolicy<>(
          retryHandler, maxAttempts, waitTime, maxWaitTime, exponentialBackoff);
    }
  }
}
