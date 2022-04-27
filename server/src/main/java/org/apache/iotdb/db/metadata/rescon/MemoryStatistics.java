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

package org.apache.iotdb.db.metadata.rescon;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryStatistics {

  private static final Logger logger = LoggerFactory.getLogger(MemoryStatistics.class);

  /** threshold total size of MTree */
  private long memoryCapacity;

  private final AtomicLong memoryUsage = new AtomicLong(0);

  private volatile boolean allowToCreateNewSeries;

  private static class MemoryStatisticsHolder {

    private MemoryStatisticsHolder() {
      // allowed to do nothing
    }

    private static final MemoryStatistics INSTANCE = new MemoryStatistics();
  }

  public static MemoryStatistics getInstance() {
    return MemoryStatisticsHolder.INSTANCE;
  }

  private MemoryStatistics() {}

  public void init() {
    memoryCapacity = IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchema();
    memoryUsage.getAndSet(0);
    allowToCreateNewSeries = true;
  }

  public boolean isAllowToCreateNewSeries() {
    return allowToCreateNewSeries;
  }

  public boolean isExceedCapacity() {
    return memoryUsage.get() > memoryCapacity;
  }

  public long getMemoryCapacity() {
    return memoryCapacity;
  }

  public long getMemoryUsage() {
    return memoryUsage.get();
  }

  public void requestMemory(long size) {
    memoryUsage.getAndUpdate(v -> v += size);
    if (memoryUsage.get() >= memoryCapacity) {
      logger.warn("Current series number {} is too large...", memoryUsage);
      allowToCreateNewSeries = false;
    }
  }

  public void releaseMemory(long size) {
    memoryUsage.getAndUpdate(v -> v -= size);
    if (!allowToCreateNewSeries && memoryUsage.get() < memoryCapacity) {
      logger.info("Current series number {} come back to normal level", memoryUsage);
      allowToCreateNewSeries = true;
    }
  }

  public void clear() {
    memoryUsage.getAndSet(0);
    allowToCreateNewSeries = true;
  }
}
