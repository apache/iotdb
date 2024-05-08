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

  private RateLimiter loadWriteRateLimiter;

  private AtomicDouble throughputBytesPerSec =
      new AtomicDouble(CONFIG.getLoadWriteThroughputBytesPerSecond());

  public LoadTsFileRateLimiter() {
    loadWriteRateLimiter =
        throughputBytesPerSec.get() <= 0
            ? RateLimiter.create(Double.MAX_VALUE) // if throughput <= 0, disable rate limiting
            : RateLimiter.create(throughputBytesPerSec.get());
  }

  private void setWritePointRate(final double throughputBytesPerSec) {
    // if throughput <= 0, disable rate limiting
    loadWriteRateLimiter.setRate(
        throughputBytesPerSec <= 0 ? Double.MAX_VALUE : throughputBytesPerSec);
  }

  public void acquireWrittenBytesWithLoadWriteRateLimiter(long writtenDataSizeInBytes) {
    if (throughputBytesPerSec.get() != CONFIG.getLoadWriteThroughputBytesPerSecond()) {
      throughputBytesPerSec.set(CONFIG.getLoadWriteThroughputBytesPerSecond());
      setWritePointRate(throughputBytesPerSec.get());
    }

    while (writtenDataSizeInBytes > 0) {
      if (writtenDataSizeInBytes > Integer.MAX_VALUE) {
        loadWriteRateLimiter.acquire(Integer.MAX_VALUE);
        writtenDataSizeInBytes -= Integer.MAX_VALUE;
      } else {
        loadWriteRateLimiter.acquire((int) writtenDataSizeInBytes);
        return;
      }
    }
  }

  //////////////////////////// Singleton ///////////////////////////////////////
  private static class LoadTsFileRateLimiterHolder {
    private static final LoadTsFileRateLimiter INSTANCE = new LoadTsFileRateLimiter();
  }

  public static LoadTsFileRateLimiter getInstance() {
    return LoadTsFileRateLimiterHolder.INSTANCE;
  }
}
