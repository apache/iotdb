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

package org.apache.iotdb.commons.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class LogThrottler {

  public static final long DEFAULT_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

  private static final String DEFAULT_KEY = "";

  private final long logIntervalMs;
  private final ConcurrentMap<String, FailureLogState> failureLogStateMap =
      new ConcurrentHashMap<>();

  public LogThrottler() {
    this(DEFAULT_LOG_INTERVAL_MS);
  }

  public LogThrottler(long logIntervalMs) {
    if (logIntervalMs < 0) {
      throw new IllegalArgumentException("Log interval should not be negative");
    }
    this.logIntervalMs = logIntervalMs;
  }

  public boolean shouldLog(String failureSignature) {
    return shouldLog(DEFAULT_KEY, failureSignature);
  }

  public boolean shouldLog(String key, String failureSignature) {
    return shouldLog(key, failureSignature, System.currentTimeMillis());
  }

  @TestOnly
  boolean shouldLog(String failureSignature, long currentTime) {
    return shouldLog(DEFAULT_KEY, failureSignature, currentTime);
  }

  @TestOnly
  boolean shouldLog(String key, String failureSignature, long currentTime) {
    String safeKey = key == null ? DEFAULT_KEY : key;
    String safeFailureSignature = String.valueOf(failureSignature);
    return failureLogStateMap
        .computeIfAbsent(safeKey, ignored -> new FailureLogState())
        .shouldLog(safeFailureSignature, currentTime, logIntervalMs);
  }

  public void reset() {
    failureLogStateMap.clear();
  }

  public void reset(String key) {
    failureLogStateMap.remove(key == null ? DEFAULT_KEY : key);
  }

  public static String getFailureSignature(Throwable failure) {
    StringBuilder builder = new StringBuilder(failure.getClass().getName());
    if (failure.getMessage() != null) {
      builder.append(':').append(failure.getMessage());
    }
    Throwable cause = failure.getCause();
    if (cause != null) {
      builder.append(";cause=").append(cause.getClass().getName());
      if (cause.getMessage() != null) {
        builder.append(':').append(cause.getMessage());
      }
    }
    return builder.toString();
  }

  private static class FailureLogState {

    private String lastFailureSignature = "";
    private long nextLogTime = 0L;

    private synchronized boolean shouldLog(
        String failureSignature, long currentTime, long logIntervalMs) {
      if (!failureSignature.equals(lastFailureSignature) || currentTime >= nextLogTime) {
        lastFailureSignature = failureSignature;
        nextLogTime = currentTime + logIntervalMs;
        return true;
      }
      return false;
    }
  }
}
