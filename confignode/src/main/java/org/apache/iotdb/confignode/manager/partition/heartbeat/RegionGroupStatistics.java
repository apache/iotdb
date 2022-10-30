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
package org.apache.iotdb.confignode.manager.partition.heartbeat;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class RegionGroupStatistics {

  private RegionGroupStatus regionGroupStatus;

  private final Map<Integer, RegionStatistics> regionStatisticsMap;

  public RegionGroupStatistics() {
    this.regionStatisticsMap = new ConcurrentHashMap<>();
  }

  public RegionGroupStatistics(
      RegionGroupStatus regionGroupStatus, Map<Integer, RegionStatistics> regionStatisticsMap) {
    this.regionGroupStatus = regionGroupStatus;
    this.regionStatisticsMap = regionStatisticsMap;
  }

  public RegionGroupStatus getRegionGroupStatus() {
    return regionGroupStatus;
  }

  /**
   * Get the specified Region's status
   *
   * @param dataNodeId Where the Region resides
   * @return Region's latest status if received heartbeat recently, Unknown otherwise
   */
  public RegionStatus getRegionStatus(int dataNodeId) {
    return regionStatisticsMap.containsKey(dataNodeId)
        ? regionStatisticsMap.get(dataNodeId).getRegionStatus()
        : RegionStatus.Unknown;
  }

  public Map<Integer, RegionStatistics> getRegionStatisticsMap() {
    return regionStatisticsMap;
  }

  public static RegionGroupStatistics generateDefaultRegionGroupStatistics() {
    return new RegionGroupStatistics(RegionGroupStatus.Disabled, new ConcurrentHashMap<>());
  }

  public RegionGroupStatistics deepCopy() {
    Map<Integer, RegionStatistics> deepCopyMap = new ConcurrentHashMap<>();
    regionStatisticsMap.forEach(
        (dataNodeId, regionStatistics) -> deepCopyMap.put(dataNodeId, regionStatistics.deepCopy()));
    return new RegionGroupStatistics(regionGroupStatus, deepCopyMap);
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(regionGroupStatus.getStatus(), stream);

    ReadWriteIOUtils.write(regionStatisticsMap.size(), stream);
    for (Map.Entry<Integer, RegionStatistics> regionStatisticsEntry :
        regionStatisticsMap.entrySet()) {
      ReadWriteIOUtils.write(regionStatisticsEntry.getKey(), stream);
      regionStatisticsEntry.getValue().serialize(stream);
    }
  }

  // Deserializer for snapshot
  public void deserialize(InputStream inputStream) throws IOException {
    this.regionGroupStatus = RegionGroupStatus.parse(ReadWriteIOUtils.readString(inputStream));

    int regionNum = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < regionNum; i++) {
      int belongedDataNodeId = ReadWriteIOUtils.readInt(inputStream);
      RegionStatistics regionStatistics = new RegionStatistics();
      regionStatistics.deserialize(inputStream);
      regionStatisticsMap.put(belongedDataNodeId, regionStatistics);
    }
  }

  // Deserializer for consensus-write
  public void deserialize(ByteBuffer buffer) {
    this.regionGroupStatus = RegionGroupStatus.parse(ReadWriteIOUtils.readString(buffer));

    int regionNum = buffer.getInt();
    for (int i = 0; i < regionNum; i++) {
      int belongedDataNodeId = buffer.getInt();
      RegionStatistics regionStatistics = new RegionStatistics();
      regionStatistics.deserialize(buffer);
      regionStatisticsMap.put(belongedDataNodeId, regionStatistics);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RegionGroupStatistics that = (RegionGroupStatistics) o;
    return regionGroupStatus == that.regionGroupStatus
        && regionStatisticsMap.equals(that.regionStatisticsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionGroupStatus, regionStatisticsMap);
  }

  @Override
  public String toString() {
    return "RegionGroupStatistics{" + "regionGroupStatus=" + regionGroupStatus + '}';
  }
}
