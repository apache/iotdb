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

package org.apache.iotdb.commons.pipe.connector.rateLimiter;

import org.apache.iotdb.commons.pipe.config.PipeConfig;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;

/**
 * PipeAllConnectorsRateLimiter is a singleton class that controls all connectors' rate limit in one
 * node.
 */
public class PipeAllConnectorsRateLimiter {

  private static final PipeConfig CONFIG = PipeConfig.getInstance();

  private final AtomicDouble throughputBytesPerSecond =
      new AtomicDouble(CONFIG.getPipeAllConnectorsRateLimitBytesPerSecond());
  private final RateLimiter rateLimiter;

  public void acquire(long bytes) {
    if (throughputBytesPerSecond.get() != CONFIG.getPipeAllConnectorsRateLimitBytesPerSecond()) {
      final double newThroughputBytesPerSecond =
          CONFIG.getPipeAllConnectorsRateLimitBytesPerSecond();
      throughputBytesPerSecond.set(newThroughputBytesPerSecond);
      rateLimiter.setRate(
          // if throughput <= 0, disable rate limiting
          newThroughputBytesPerSecond <= 0 ? Double.MAX_VALUE : newThroughputBytesPerSecond);
    }

    while (bytes > 0) {
      if (bytes > Integer.MAX_VALUE) {
        rateLimiter.acquire(Integer.MAX_VALUE);
        bytes -= Integer.MAX_VALUE;
      } else {
        rateLimiter.acquire((int) bytes);
        return;
      }
    }
  }

  ///////////////////////////  SINGLETON  ///////////////////////////

  private PipeAllConnectorsRateLimiter() {
    final double throughputBytesPerSecondLimit = throughputBytesPerSecond.get();
    rateLimiter =
        throughputBytesPerSecondLimit <= 0
            ? RateLimiter.create(Double.MAX_VALUE)
            : RateLimiter.create(throughputBytesPerSecondLimit);
  }

  private static class PipeAllConnectorsRateLimiterHolder {
    private static final PipeAllConnectorsRateLimiter INSTANCE = new PipeAllConnectorsRateLimiter();
  }

  public static PipeAllConnectorsRateLimiter getInstance() {
    return PipeAllConnectorsRateLimiterHolder.INSTANCE;
  }
}
