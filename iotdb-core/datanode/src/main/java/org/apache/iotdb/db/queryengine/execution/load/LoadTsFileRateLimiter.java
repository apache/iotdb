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

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;

public class LoadTsFileRateLimiter {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final AtomicDouble throughputBytesPerSecond =
      new AtomicDouble(CONFIG.getLoadWriteThroughputBytesPerSecond());
  private final RateLimiter loadWriteRateLimiter;

  public void acquire(long bytes) {
    final double throughputBytesPerSecondLimit = CONFIG.getLoadWriteThroughputBytesPerSecond();

    if (throughputBytesPerSecond.get() != throughputBytesPerSecondLimit) {
      throughputBytesPerSecond.set(throughputBytesPerSecondLimit);
      loadWriteRateLimiter.setRate(
          // if throughput <= 0, disable rate limiting
          throughputBytesPerSecondLimit <= 0 ? Double.MAX_VALUE : throughputBytesPerSecondLimit);
    }

    // For performance, we don't need to acquire rate limiter if throughput <= 0
    if (throughputBytesPerSecondLimit <= 0) {
      return;
    }

    while (bytes > 0) {
      if (bytes > Integer.MAX_VALUE) {
        loadWriteRateLimiter.acquire(Integer.MAX_VALUE);
        bytes -= Integer.MAX_VALUE;
      } else {
        loadWriteRateLimiter.acquire((int) bytes);
        return;
      }
    }
  }

  //////////////////////////// Singleton ////////////////////////////

  private LoadTsFileRateLimiter() {
    final double throughputBytesPerSecondLimit = throughputBytesPerSecond.get();
    loadWriteRateLimiter =
        // if throughput <= 0, disable rate limiting
        throughputBytesPerSecondLimit <= 0
            ? RateLimiter.create(Double.MAX_VALUE)
            : RateLimiter.create(throughputBytesPerSecondLimit);
  }

  private static class LoadTsFileRateLimiterHolder {

    private static final LoadTsFileRateLimiter INSTANCE = new LoadTsFileRateLimiter();

    private LoadTsFileRateLimiterHolder() {
      // Prevent instantiation
    }
  }

  public static LoadTsFileRateLimiter getInstance() {
    return LoadTsFileRateLimiterHolder.INSTANCE;
  }
}
