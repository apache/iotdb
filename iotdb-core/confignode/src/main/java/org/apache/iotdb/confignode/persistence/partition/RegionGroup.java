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
package org.apache.iotdb.confignode.persistence.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RegionGroup {

  private long createTime;
  private final TRegionReplicaSet replicaSet;

  // Map<TSeriesPartitionSlot, TTimePartitionSlot Count>
  // For SchemaRegion, each SeriesSlot constitute a SchemaPartition.
  // For DataRegion, a SeriesSlot and a TimeSlot constitute a DataPartition.
  // Eg: A DataRegion contains SeriesSlot-1 which has TimeSlot-1, TimeSlot-2 and Timeslot-3,
  // then (SeriesSlot-1 -> TimeSlot-1) constitute a DataPartition.
  private final Map<TSeriesPartitionSlot, AtomicLong> slotCountMap;

  private final AtomicLong totalTimeSlotCount;

  public RegionGroup() {
    this.createTime = 0;
    this.replicaSet = new TRegionReplicaSet();
    this.slotCountMap = new ConcurrentHashMap<>();
    this.totalTimeSlotCount = new AtomicLong();
  }

  public RegionGroup(long createTime, TRegionReplicaSet replicaSet) {
    this.createTime = createTime;
    this.replicaSet = replicaSet;
    this.slotCountMap = new ConcurrentHashMap<>();
    this.totalTimeSlotCount = new AtomicLong(0);
  }

  public synchronized long getCreateTime() {
    return createTime;
  }

  public synchronized TConsensusGroupId getId() {
    return replicaSet.getRegionId();
  }

  public synchronized TRegionReplicaSet getReplicaSet() {
    return replicaSet.deepCopy();
  }

  /**
   * Update the DataNodeLocation in TRegionReplicaSet if necessary.
   *
   * @param newDataNodeLocation The new DataNodeLocation.
   */
  public synchronized void updateDataNode(TDataNodeLocation newDataNodeLocation) {
    for (int i = 0; i < replicaSet.getDataNodeLocationsSize(); i++) {
      if (replicaSet.getDataNodeLocations().get(i).getDataNodeId()
          == newDataNodeLocation.getDataNodeId()) {
        replicaSet.getDataNodeLocations().set(i, newDataNodeLocation);
        return;
      }
    }
  }

  public synchronized void addRegionLocation(TDataNodeLocation node) {
    replicaSet.addToDataNodeLocations(node);
    replicaSet.getDataNodeLocations().sort(TDataNodeLocation::compareTo);
  }

  public synchronized void removeRegionLocation(TDataNodeLocation node) {
    replicaSet.getDataNodeLocations().remove(node);
    replicaSet.getDataNodeLocations().sort(TDataNodeLocation::compareTo);
  }

  /**
   * @param deltaMap Map<TSeriesPartitionSlot, Delta TTimePartitionSlot Count>
   */
  public synchronized void updateSlotCountMap(Map<TSeriesPartitionSlot, AtomicLong> deltaMap) {
    deltaMap.forEach(
        ((seriesPartitionSlot, delta) -> {
          slotCountMap
              .computeIfAbsent(seriesPartitionSlot, empty -> new AtomicLong(0))
              .getAndAdd(delta.get());
          totalTimeSlotCount.getAndAdd(delta.get());
        }));
  }

  public synchronized int getSeriesSlotCount() {
    return slotCountMap.size();
  }

  public synchronized long getTimeSlotCount() {
    return totalTimeSlotCount.get();
  }

  /**
   * Check if the RegionGroup belongs to the specified DataNode.
   *
   * @param dataNodeId The specified DataNodeId.
   * @return True if the RegionGroup belongs to the specified DataNode.
   */
  public synchronized boolean belongsToDataNode(int dataNodeId) {
    return replicaSet.getDataNodeLocations().stream()
        .anyMatch(dataNodeLocation -> dataNodeLocation.getDataNodeId() == dataNodeId);
  }

  public synchronized void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(createTime, outputStream);
    replicaSet.write(protocol);

    ReadWriteIOUtils.write(slotCountMap.size(), outputStream);
    for (Map.Entry<TSeriesPartitionSlot, AtomicLong> slotCountEntry : slotCountMap.entrySet()) {
      slotCountEntry.getKey().write(protocol);
      ReadWriteIOUtils.write(slotCountEntry.getValue().get(), outputStream);
    }

    ReadWriteIOUtils.write(totalTimeSlotCount.get(), outputStream);
  }

  public synchronized void deserialize(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    this.createTime = ReadWriteIOUtils.readLong(inputStream);
    replicaSet.read(protocol);

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot();
      seriesPartitionSlot.read(protocol);
      AtomicLong slotCount = new AtomicLong(ReadWriteIOUtils.readLong(inputStream));
      slotCountMap.put(seriesPartitionSlot, slotCount);
    }

    totalTimeSlotCount.set(ReadWriteIOUtils.readLong(inputStream));
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RegionGroup that = (RegionGroup) o;
    for (Map.Entry<TSeriesPartitionSlot, AtomicLong> slotCountEntry : slotCountMap.entrySet()) {
      if (!that.slotCountMap.containsKey(slotCountEntry.getKey())) {
        return false;
      }
      if (slotCountEntry.getValue().get() != that.slotCountMap.get(slotCountEntry.getKey()).get()) {
        return false;
      }
    }
    return createTime == that.createTime
        && replicaSet.equals(that.replicaSet)
        && totalTimeSlotCount.get() == that.totalTimeSlotCount.get();
  }

  @Override
  public synchronized int hashCode() {
    return Objects.hash(createTime, replicaSet, slotCountMap, totalTimeSlotCount);
  }
}
