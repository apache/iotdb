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

package org.apache.iotdb.db.pipe.processor.downsampling;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.utils.MemUtils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Map-like component to look up for the last chosen time of a timeSeries. It has max size and
 * timeSeries may fail to find its last time and must design the logic to handle this.
 */
public class PartialPathLastTimeCache implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartialPathLastTimeCache.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final PipeMemoryBlock allocatedMemoryBlock;
  // Used to adjust the memory usage of the cache
  private final AtomicDouble memoryUsageCheatFactor = new AtomicDouble(1);
  private final Cache<String, Long> partialPath2TimeCache;

  public PartialPathLastTimeCache() {
    final long requestedAllocateSize = (long) (CONFIG.getAllocateMemoryForPipe() * 0.8 / 5);

    allocatedMemoryBlock =
        PipeResourceManager.memory()
            .tryAllocate(requestedAllocateSize)
            .setShrinkMethod(oldMemory -> Math.max(oldMemory / 2, 1))
            .setShrinkCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.set(
                      memoryUsageCheatFactor.get() * ((double) oldMemory / newMemory));
                  LOGGER.info(
                      "PartialPathLastTimeCache.allocatedMemoryBlock has shrunk from {} to {}.",
                      oldMemory,
                      newMemory);
                })
            .setExpandMethod(
                oldMemory -> Math.min(Math.max(oldMemory, 1) * 2, requestedAllocateSize))
            .setExpandCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.set(
                      memoryUsageCheatFactor.get() / ((double) newMemory / oldMemory));
                  LOGGER.info(
                      "PartialPathLastTimeCache.allocatedMemoryBlock has expanded from {} to {}.",
                      oldMemory,
                      newMemory);
                });

    // Currently disable the metric here because it's not a constant cache and the number may
    // fluctuate. In the future all the "processorCache"s may be recorded in single metric entry
    partialPath2TimeCache =
        Caffeine.newBuilder()
            .maximumWeight(allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                // Here partial path is a part of full path adequate to inspect the last time
                (Weigher<String, Long>)
                    (partialPath, timeStamp) -> {
                      final long weightInLong =
                          (long)
                              ((MemUtils.getStringMem(partialPath) + Long.BYTES)
                                  * memoryUsageCheatFactor.get());
                      if (weightInLong <= 0) {
                        return Integer.MAX_VALUE;
                      }
                      final int weightInInt = (int) weightInLong;
                      return weightInInt != weightInLong ? Integer.MAX_VALUE : weightInInt;
                    })
            .build();
  }

  /////////////////////////// Getter & Setter ///////////////////////////

  public Long getPartialPathLastTime(String partialPath) {
    return partialPath2TimeCache.getIfPresent(partialPath);
  }

  public void setPartialPathLastTime(String partialPath, long timeStamp) {
    partialPath2TimeCache.put(partialPath, timeStamp);
  }

  /////////////////////////// Close ///////////////////////////

  @Override
  public void close() throws Exception {
    partialPath2TimeCache.invalidateAll();
    allocatedMemoryBlock.close();
  }
}
