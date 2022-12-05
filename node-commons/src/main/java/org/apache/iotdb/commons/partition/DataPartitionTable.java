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
package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DataPartitionTable {

  private final Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap;

  public DataPartitionTable() {
    this.dataPartitionMap = new ConcurrentHashMap<>();
  }

  public DataPartitionTable(Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap) {
    this.dataPartitionMap = dataPartitionMap;
  }

  public Map<TSeriesPartitionSlot, SeriesPartitionTable> getDataPartitionMap() {
    return dataPartitionMap;
  }

  /**
   * Thread-safely get DataPartition within the specific StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @param dataPartitionTable Store the matched Partitions
   * @return True if all the PartitionSlots are matched, false otherwise
   */
  public boolean getDataPartition(
      Map<TSeriesPartitionSlot, TTimeSlotList> partitionSlots,
      DataPartitionTable dataPartitionTable) {
    AtomicBoolean result = new AtomicBoolean(true);
    if (partitionSlots.isEmpty()) {
      // Return all DataPartitions in one StorageGroup when the queried PartitionSlots are empty
      dataPartitionTable.getDataPartitionMap().putAll(dataPartitionMap);
    } else {
      // Return the DataPartition for each SeriesPartitionSlot
      partitionSlots.forEach(
          (seriesPartitionSlot, timePartitionSlotList) -> {
            if (dataPartitionMap.containsKey(seriesPartitionSlot)) {
              SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();
              if (!dataPartitionMap
                  .get(seriesPartitionSlot)
                  .getDataPartition(timePartitionSlotList, seriesPartitionTable)) {
                result.set(false);
              }

              if (!seriesPartitionTable.getSeriesPartitionMap().isEmpty()) {
                // Only return those non-empty DataPartitions
                dataPartitionTable
                    .getDataPartitionMap()
                    .put(seriesPartitionSlot, seriesPartitionTable);
              }
            } else {
              result.set(false);
            }
          });
    }
    return result.get();
  }

  /**
   * Checks whether the specified DataPartition has a predecessor and returns if it does
   *
   * @param seriesPartitionSlot Corresponding SeriesPartitionSlot
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @param timePartitionInterval Time partition interval
   * @return The specific DataPartition's predecessor if exists, null otherwise
   */
  public TConsensusGroupId getPrecededDataPartition(
      TSeriesPartitionSlot seriesPartitionSlot,
      TTimePartitionSlot timePartitionSlot,
      long timePartitionInterval) {
    if (dataPartitionMap.containsKey(seriesPartitionSlot)) {
      return dataPartitionMap
          .get(seriesPartitionSlot)
          .getPrecededDataPartition(timePartitionSlot, timePartitionInterval);
    } else {
      return null;
    }
  }

  /**
   * Create DataPartition within the specific StorageGroup
   *
   * @param assignedDataPartition Assigned result
   * @return Map<TConsensusGroupId, Map<TSeriesPartitionSlot, Delta TTimePartitionSlot Count>>
   */
  public Map<TConsensusGroupId, Map<TSeriesPartitionSlot, AtomicLong>> createDataPartition(
      DataPartitionTable assignedDataPartition) {
    Map<TConsensusGroupId, Map<TSeriesPartitionSlot, AtomicLong>> groupDeltaMap =
        new ConcurrentHashMap<>();

    assignedDataPartition
        .getDataPartitionMap()
        .forEach(
            ((seriesPartitionSlot, seriesPartitionTable) ->
                dataPartitionMap
                    .computeIfAbsent(seriesPartitionSlot, empty -> new SeriesPartitionTable())
                    .createDataPartition(
                        seriesPartitionTable, seriesPartitionSlot, groupDeltaMap)));

    return groupDeltaMap;
  }

  /**
   * Only Leader use this interface. Filter unassigned DataPartitionSlots within the specific
   * StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @return Unassigned PartitionSlots
   */
  public Map<TSeriesPartitionSlot, TTimeSlotList> filterUnassignedDataPartitionSlots(
      Map<TSeriesPartitionSlot, TTimeSlotList> partitionSlots) {
    Map<TSeriesPartitionSlot, TTimeSlotList> result = new ConcurrentHashMap<>();

    partitionSlots.forEach(
        (seriesPartitionSlot, timePartitionSlots) ->
            result.put(
                seriesPartitionSlot,
                new TTimeSlotList(
                    dataPartitionMap
                        .computeIfAbsent(seriesPartitionSlot, empty -> new SeriesPartitionTable())
                        .filterUnassignedDataPartitionSlots(
                            timePartitionSlots.getTimePartitionSlots()),
                    false,
                    false)));

    return result;
  }

  /**
   * Query a timePartition's corresponding dataRegionIds
   *
   * @param seriesSlotId SeriesPartitionSlot
   * @param timeSlotId TimePartitionSlot
   * @return the timePartition's corresponding dataRegionIds, if timeSlotId == -1, then return all
   *     the seriesSlot's dataRegionIds
   */
  public List<TConsensusGroupId> getRegionId(
      TSeriesPartitionSlot seriesSlotId, TTimePartitionSlot timeSlotId) {
    if (!dataPartitionMap.containsKey(seriesSlotId)) {
      return new ArrayList<>();
    }
    SeriesPartitionTable seriesPartitionTable = dataPartitionMap.get(seriesSlotId);
    return seriesPartitionTable.getRegionId(timeSlotId);
  }

  public List<TTimePartitionSlot> getTimeSlotList(
      TSeriesPartitionSlot seriesSlotId, long startTime, long endTime) {
    if (!dataPartitionMap.containsKey(seriesSlotId)) {
      return new ArrayList<>();
    }
    SeriesPartitionTable seriesPartitionTable = dataPartitionMap.get(seriesSlotId);
    return seriesPartitionTable.getTimeSlotList(startTime, endTime);
  }

  public List<TSeriesPartitionSlot> getSeriesSlotList() {
    return dataPartitionMap.keySet().stream()
        .sorted(Comparator.comparing(TSeriesPartitionSlot::getSlotId))
        .collect(Collectors.toList());
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(dataPartitionMap.size(), outputStream);
    for (Map.Entry<TSeriesPartitionSlot, SeriesPartitionTable> seriesPartitionTableEntry :
        dataPartitionMap.entrySet()) {
      seriesPartitionTableEntry.getKey().write(protocol);
      seriesPartitionTableEntry.getValue().serialize(outputStream, protocol);
    }
  }

  /** Only for ConsensusRequest */
  public void deserialize(ByteBuffer buffer) {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      TSeriesPartitionSlot seriesPartitionSlot =
          ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer);
      SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();
      seriesPartitionTable.deserialize(buffer);
      dataPartitionMap.put(seriesPartitionSlot, seriesPartitionTable);
    }
  }

  /** Only for Snapshot */
  public void deserialize(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int length = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < length; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot();
      seriesPartitionSlot.read(protocol);
      SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();
      seriesPartitionTable.deserialize(inputStream, protocol);
      dataPartitionMap.put(seriesPartitionSlot, seriesPartitionTable);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataPartitionTable that = (DataPartitionTable) o;
    return dataPartitionMap.equals(that.dataPartitionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataPartitionMap);
  }
}
