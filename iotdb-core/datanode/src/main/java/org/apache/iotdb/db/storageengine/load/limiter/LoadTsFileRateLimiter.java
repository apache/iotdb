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

package org.apache.iotdb.db.storageengine.load.limiter;

import org.apache.iotdb.commons.pipe.sink.limiter.GlobalRateLimiter;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;

public class LoadTsFileRateLimiter extends GlobalRateLimiter {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public void acquire(long bytes) {
    LoadTsFileCostMetricsSet.getInstance().recordDiskIO(bytes);
    super.acquire(bytes);
  }

  @Override
  protected double getThroughputBytesPerSecond() {
    return CONFIG.getLoadWriteThroughputBytesPerSecond();
  }

  //////////////////////////// Singleton ////////////////////////////

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
