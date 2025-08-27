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

package org.apache.iotdb.db.storageengine.load.memory;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileMemMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class LoadTsFileMemoryBlock extends LoadTsFileAbstractMemoryBlock {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileMemoryBlock.class);

  private long totalMemorySizeInBytes;
  private final AtomicLong memoryUsageInBytes;

  LoadTsFileMemoryBlock(long totalMemorySizeInBytes) {
    super();

    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.memoryUsageInBytes = new AtomicLong(0);
  }

  @Override
  public synchronized boolean hasEnoughMemory(long memoryTobeAddedInBytes) {
    return memoryUsageInBytes.get() + memoryTobeAddedInBytes <= totalMemorySizeInBytes;
  }

  @Override
  public synchronized void addMemoryUsage(long memoryInBytes) {
    // May temporarily exceed the max size
    if (memoryUsageInBytes.addAndGet(memoryInBytes) > totalMemorySizeInBytes) {
      LOGGER.debug("{} has exceed total memory size", this);
    }

    MetricService.getInstance()
        .getOrCreateGauge(
            Metric.LOAD_MEM.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            LoadTsFileMemMetricSet.LOAD_TSFILE_OTHER_MEMORY)
        .incr(memoryInBytes);
  }

  @Override
  public synchronized void reduceMemoryUsage(long memoryInBytes) {
    if (memoryUsageInBytes.addAndGet(-memoryInBytes) < 0) {
      LOGGER.warn("{} has reduce memory usage to negative", this);
    }

    MetricService.getInstance()
        .getOrCreateGauge(
            Metric.LOAD_MEM.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            LoadTsFileMemMetricSet.LOAD_TSFILE_OTHER_MEMORY)
        .decr(memoryInBytes);
  }

  @Override
  synchronized long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  synchronized long getTotalMemorySizeInBytes() {
    return totalMemorySizeInBytes;
  }

  synchronized void setTotalMemorySizeInBytes(long totalMemorySizeInBytes) {
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
  }

  @Override
  public synchronized void forceResize(long newSizeInBytes) {
    MEMORY_MANAGER.forceResize(this, newSizeInBytes);
  }

  @Override
  protected synchronized void releaseAllMemory() {
    if (memoryUsageInBytes.get() != 0) {
      LOGGER.warn(
          "Try to release memory from a memory block {} which has not released all memory", this);
    }
    MEMORY_MANAGER.releaseToQuery(totalMemorySizeInBytes);
  }

  @Override
  public String toString() {
    return "LoadTsFileMemoryBlock{"
        + "totalMemorySizeInBytes="
        + totalMemorySizeInBytes
        + ", usedMemoryInBytes="
        + memoryUsageInBytes.get()
        + '}';
  }
}
