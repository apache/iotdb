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
package org.apache.iotdb.db.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RetryCounter {

  private static final Logger logger = LoggerFactory.getLogger(RetryCounter.class);

  private RetryConfig retryConfig;
  private int attempts;

  public RetryCounter(RetryConfig retryConfig) {
    this.attempts = 1;
    this.retryConfig = retryConfig;
  }

  public void sleepToNextRetry() throws InterruptedException {
    int attempts = getAttempts();
    long sleepTime = getBackoffTime();
    logger.trace("Sleeping {} ms before retry {}", sleepTime, attempts);
    retryConfig.getTimeUnit().sleep(sleepTime);
    useRetry();
  }

  public boolean shouldRetry() {
    return attempts < retryConfig.getMaxAttempts();
  }

  public RetryConfig getRetryConfig() {
    return retryConfig;
  }

  public int getAttempts() {
    return attempts;
  }

  public long getBackoffTime() {
    return this.retryConfig.backoffPolicy.getBackoffTime(this.retryConfig, getAttempts());
  }

  private void useRetry() {
    attempts++;
  }

  public static class RetryConfig {
    private int maxAttempts;
    private long sleepInterval;
    private TimeUnit timeUnit;
    private BackoffPolicy backoffPolicy;
    private float jitter;

    private static final BackoffPolicy DEFAULT_BACKOFF_POLICY = new ExponentialBackoffPolicy();

    public RetryConfig() {
      maxAttempts = 1;
      sleepInterval = 100;
      timeUnit = TimeUnit.MILLISECONDS;
      backoffPolicy = DEFAULT_BACKOFF_POLICY;
      jitter = 0.0f;
    }

    public RetryConfig setMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public RetryConfig setSleepInterval(long sleepInterval) {
      this.sleepInterval = sleepInterval;
      return this;
    }

    public int getMaxAttempts() {
      return maxAttempts;
    }

    public long getSleepInterval() {
      return sleepInterval;
    }

    public TimeUnit getTimeUnit() {
      return timeUnit;
    }

    public float getJitter() {
      return jitter;
    }
  }

  private static long addJitter(long interval, float jitter) {
    long jitterInterval = (long) (interval * ThreadLocalRandom.current().nextFloat() * jitter);
    return interval + jitterInterval;
  }

  public static class BackoffPolicy {
    public long getBackoffTime(RetryConfig config, int attempts) {
      return addJitter(config.getSleepInterval(), config.getJitter());
    }
  }

  public static class ExponentialBackoffPolicy extends BackoffPolicy {
    @Override
    public long getBackoffTime(RetryConfig config, int attempts) {
      long backoffTime = (long) (config.getSleepInterval() * Math.pow(2, attempts));
      return addJitter(config.getSleepInterval(), config.getJitter());
    }
  }
}
