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

package org.apache.iotdb.db.pipe.sink.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public interface IoTDBDataNodeCacheLeaderClientManager {

  LeaderCacheManager LEADER_CACHE_MANAGER = new LeaderCacheManager();

  class LeaderCacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCacheManager.class);
    private static final PipeConfig CONFIG = PipeConfig.getInstance();

    // leader cache built by LRU
    private final Cache<String, TEndPoint> device2endpoint;
    // a hashmap to reuse the created endpoint
    private final ConcurrentHashMap<TEndPoint, TEndPoint> endPoints = new ConcurrentHashMap<>();

    public LeaderCacheManager() {
      final long initMemorySizeInBytes =
          PipeDataNodeResourceManager.memory().getTotalNonFloatingMemorySizeInBytes() / 10;

      // properties required by pipe memory control framework
      final PipeMemoryBlock allocatedMemoryBlock =
          PipeDataNodeResourceManager.memory().tryAllocate(initMemorySizeInBytes);

      device2endpoint =
          Caffeine.newBuilder()
              .maximumWeight(allocatedMemoryBlock.getMemoryUsageInBytes())
              .weigher((Weigher<String, TEndPoint>) (device, endPoint) -> device.getBytes().length)
              .recordStats()
              .build();

      allocatedMemoryBlock
          .setShrinkMethod(oldMemory -> Math.max(oldMemory / 2, 1))
          .setShrinkCallback(
              (oldMemory, newMemory) -> {
                device2endpoint
                    .policy()
                    .eviction()
                    .ifPresent(eviction -> eviction.setMaximum(newMemory));
                LOGGER.info(
                    "LeaderCacheManager.allocatedMemoryBlock has shrunk from {} to {}.",
                    oldMemory,
                    newMemory);
              })
          .setExpandMethod(
              oldMemory ->
                  Math.min(
                      Math.max(oldMemory, 1) * 2,
                      (long)
                          (PipeDataNodeResourceManager.memory()
                                  .getTotalNonFloatingMemorySizeInBytes()
                              * CONFIG.getPipeLeaderCacheMemoryUsagePercentage())))
          .setExpandCallback(
              (oldMemory, newMemory) -> {
                device2endpoint
                    .policy()
                    .eviction()
                    .ifPresent(eviction -> eviction.setMaximum(newMemory));
                LOGGER.info(
                    "LeaderCacheManager.allocatedMemoryBlock has expanded from {} to {}.",
                    oldMemory,
                    newMemory);
              });
    }

    public TEndPoint getLeaderEndPoint(final String deviceId) {
      return deviceId == null ? null : device2endpoint.getIfPresent(deviceId);
    }

    public void updateLeaderEndPoint(final String deviceId, final TEndPoint endPoint) {
      if (deviceId == null || endPoint == null) {
        return;
      }

      final TEndPoint endPointFromMap = endPoints.putIfAbsent(endPoint, endPoint);
      if (endPointFromMap != null) {
        device2endpoint.put(deviceId, endPointFromMap);
      } else {
        device2endpoint.put(deviceId, endPoint);
      }
    }
  }
}
