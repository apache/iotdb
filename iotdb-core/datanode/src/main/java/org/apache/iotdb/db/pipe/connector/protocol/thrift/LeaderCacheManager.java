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

package org.apache.iotdb.db.pipe.connector.protocol.thrift;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class LeaderCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCacheManager.class);
  private static final PipeConfig CONFIG = PipeConfig.getInstance();

  private final AtomicDouble memoryUsageCheatFactor = new AtomicDouble(1);

  // leader cache built by LRU
  private final Cache<String, TEndPoint> device2endpoint;
  // a hashmap to reuse the created endpoint
  private final ConcurrentHashMap<TEndPoint, TEndPoint> endPoints = new ConcurrentHashMap<>();

  public LeaderCacheManager() {
    long initMemorySizeInBytes = PipeResourceManager.memory().getTotalMemorySizeInBytes() / 10;
    long maxMemorySizeInBytes =
        (long)
            (PipeResourceManager.memory().getTotalMemorySizeInBytes()
                * CONFIG.getPipeLeaderCacheMemoryUsagePercentage());

    // properties required by pipe memory control framework
    PipeMemoryBlock allocatedMemoryBlock =
        PipeResourceManager.memory()
            .tryAllocate(initMemorySizeInBytes)
            .setShrinkMethod(oldMemory -> Math.max(oldMemory / 2, 1))
            .setShrinkCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.set(
                      memoryUsageCheatFactor.get() * ((double) oldMemory / newMemory));
                  LOGGER.info(
                      "LeaderCacheManager.allocatedMemoryBlock has shrunk from {} to {}.",
                      oldMemory,
                      newMemory);
                })
            .setExpandMethod(
                oldMemory -> Math.min(Math.max(oldMemory, 1) * 2, maxMemorySizeInBytes))
            .setExpandCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.set(
                      memoryUsageCheatFactor.get() / ((double) newMemory / oldMemory));
                  LOGGER.info(
                      "LeaderCacheManager.allocatedMemoryBlock has expanded from {} to {}.",
                      oldMemory,
                      newMemory);
                });

    device2endpoint =
        Caffeine.newBuilder()
            .maximumWeight(allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                (Weigher<String, TEndPoint>)
                    (device, endPoint) -> {
                      final long weightInLong =
                          (long) (device.getBytes().length * memoryUsageCheatFactor.get());
                      if (weightInLong <= 0) {
                        return Integer.MAX_VALUE;
                      }
                      final int weightInInt = (int) weightInLong;
                      return weightInInt != weightInLong ? Integer.MAX_VALUE : weightInInt;
                    })
            .recordStats()
            .build();
  }

  public TEndPoint getLeaderEndPoint(String deviceId) {
    return deviceId == null ? null : device2endpoint.getIfPresent(deviceId);
  }

  public void updateLeaderEndPoint(String deviceId, TEndPoint endPoint) {
    TEndPoint endPointFromMap = endPoints.putIfAbsent(endPoint, endPoint);
    if (endPointFromMap != null) {
      device2endpoint.put(deviceId, endPointFromMap);
    } else {
      device2endpoint.put(deviceId, endPoint);
    }
  }
}
