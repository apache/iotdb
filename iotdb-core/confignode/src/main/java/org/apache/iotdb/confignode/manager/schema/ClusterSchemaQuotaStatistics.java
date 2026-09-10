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
package org.apache.iotdb.confignode.manager.schema;

import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterSchemaQuotaStatistics {

  // TODO: it can be merged with statistics in ClusterQuotaManager
  private long seriesThreshold;
  private long deviceThreshold;

  private final Map<Integer, Long> seriesCountMap = new ConcurrentHashMap<>();
  private final Map<Integer, Long> deviceCountMap = new ConcurrentHashMap<>();

  public ClusterSchemaQuotaStatistics(long seriesThreshold, long deviceThreshold) {
    this.seriesThreshold = seriesThreshold;
    this.deviceThreshold = deviceThreshold;
  }

  public void updateTimeSeriesUsage(@NotNull Map<Integer, Long> seriesUsage) {
    seriesCountMap.putAll(seriesUsage);
  }

  public void updateDeviceUsage(@NotNull Map<Integer, Long> deviceUsage) {
    deviceCountMap.putAll(deviceUsage);
  }

  /**
   * Get the remain quota of series and device for the given consensus group.
   *
   * @param consensusGroupIdSet the consensus group id set
   * @return the remain quota, >=0
   */
  public long getSeriesQuotaRemain(Set<Integer> consensusGroupIdSet) {
    long res =
        seriesThreshold
            - seriesCountMap.entrySet().stream()
                .filter(i -> consensusGroupIdSet.contains(i.getKey()))
                .mapToLong(Map.Entry::getValue)
                .sum();
    return res > 0 ? res : 0;
  }

  /**
   * Get the remain quota of device for the given consensus group.
   *
   * @param consensusGroupIdSet the consensus group id set
   * @return the remain quota, >=0
   */
  public long getDeviceQuotaRemain(Set<Integer> consensusGroupIdSet) {
    long res =
        deviceThreshold
            - deviceCountMap.entrySet().stream()
                .filter(i -> consensusGroupIdSet.contains(i.getKey()))
                .mapToLong(Map.Entry::getValue)
                .sum();
    return res > 0 ? res : 0;
  }

  public long getSeriesThreshold() {
    return seriesThreshold;
  }

  public void setSeriesThreshold(long seriesThreshold) {
    this.seriesThreshold = seriesThreshold;
  }

  public long getDeviceThreshold() {
    return deviceThreshold;
  }

  public void setDeviceThreshold(long deviceThreshold) {
    this.deviceThreshold = deviceThreshold;
  }

  public void clear() {
    seriesCountMap.clear();
    deviceCountMap.clear();
  }
}
