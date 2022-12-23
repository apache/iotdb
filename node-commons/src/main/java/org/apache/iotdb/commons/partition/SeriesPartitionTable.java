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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SeriesPartitionTable {

  private final Map<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionMap;

  public SeriesPartitionTable() {
    this.seriesPartitionMap = new ConcurrentHashMap<>();
  }

  public SeriesPartitionTable(Map<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionMap) {
    this.seriesPartitionMap = seriesPartitionMap;
  }

  public Map<TTimePartitionSlot, List<TConsensusGroupId>> getSeriesPartitionMap() {
    return seriesPartitionMap;
  }

  /**
   * Thread-safely get DataPartition within the specific StorageGroup
   *
   * @param partitionSlotList TimePartitionSlotList
   * @param seriesPartitionTable Store the matched SeriesPartitions
   * @return True if all the SeriesPartitionSlots are matched, false otherwise
   */
  public boolean getDataPartition(
      TTimeSlotList partitionSlotList, SeriesPartitionTable seriesPartitionTable) {
    AtomicBoolean result = new AtomicBoolean(true);
    List<TTimePartitionSlot> partitionSlots = partitionSlotList.getTimePartitionSlots();

    if (partitionSlots.isEmpty()) {
      // Return all DataPartitions in one SeriesPartitionSlot
      // when the queried TimePartitionSlots are empty
      seriesPartitionTable.getSeriesPartitionMap().putAll(seriesPartitionMap);
    } else {
      boolean isNeedLeftAll = partitionSlotList.isNeedLeftAll(),
          isNeedRightAll = partitionSlotList.isNeedRightAll();
      if (isNeedLeftAll || isNeedRightAll) {
        // we need to calculate the leftMargin which contains all the time partition on the unclosed
        // left side: (-oo, leftMargin)
        // and the rightMargin which contains all the time partition on the unclosed right side:
        // (rightMargin, +oo)
        // all the remaining closed time range which locates in [leftMargin, rightMargin] will be
        // calculated outside if block
        long leftMargin = isNeedLeftAll ? partitionSlots.get(0).getStartTime() : Long.MIN_VALUE,
            rightMargin =
                isNeedRightAll
                    ? partitionSlots.get(partitionSlots.size() - 1).getStartTime()
                    : Long.MAX_VALUE;
        seriesPartitionTable
            .getSeriesPartitionMap()
            .putAll(
                seriesPartitionMap.entrySet().stream()
                    .filter(
                        entry -> {
                          long startTime = entry.getKey().getStartTime();
                          return startTime < leftMargin || startTime > rightMargin;
                        })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
      }

      // Return the DataPartition for each match TimePartitionSlot
      partitionSlots.forEach(
          timePartitionSlot -> {
            if (seriesPartitionMap.containsKey(timePartitionSlot)) {
              seriesPartitionTable
                  .getSeriesPartitionMap()
                  .put(timePartitionSlot, seriesPartitionMap.get(timePartitionSlot));
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
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @param timePartitionInterval Time partition interval
   * @return The specific DataPartition's predecessor if exists, null otherwise
   */
  public TConsensusGroupId getPrecededDataPartition(
      TTimePartitionSlot timePartitionSlot, long timePartitionInterval) {
    if (timePartitionSlot.getStartTime() < timePartitionInterval) {
      // The first DataPartition doesn't have predecessor
      return null;
    } else {
      TTimePartitionSlot predecessorSlot =
          new TTimePartitionSlot(timePartitionSlot.getStartTime() - timePartitionInterval);
      return seriesPartitionMap
          .getOrDefault(predecessorSlot, Collections.singletonList(null))
          .get(0);
    }
  }

  /**
   * Query a timePartition's corresponding dataRegionIds
   *
   * @param timeSlotId Time partition's timeSlotId
   * @return the timePartition's corresponding dataRegionIds
   */
  List<TConsensusGroupId> getRegionId(TTimePartitionSlot timeSlotId) {
    if (timeSlotId != null) {
      if (!seriesPartitionMap.containsKey(timeSlotId)) {
        return new ArrayList<>();
      }
      return seriesPartitionMap.get(timeSlotId).stream()
          .sorted(Comparator.comparing(TConsensusGroupId::getId))
          .collect(Collectors.toList());
    } else {
      Set<TConsensusGroupId> result = new HashSet<>();
      seriesPartitionMap.values().forEach(result::addAll);
      return result.stream()
          .sorted(Comparator.comparing(TConsensusGroupId::getId))
          .collect(Collectors.toList());
    }
  }

  List<TTimePartitionSlot> getTimeSlotList(long startTime, long endTime) {
    return seriesPartitionMap.keySet().stream()
        .filter(e -> e.getStartTime() >= startTime && e.getStartTime() < endTime)
        .sorted(Comparator.comparing(TTimePartitionSlot::getStartTime))
        .collect(Collectors.toList());
  }

  /**
   * Create DataPartition within the specific SeriesPartitionSlot
   *
   * @param assignedSeriesPartitionTable Assigned result
   * @param seriesPartitionSlot Corresponding TSeriesPartitionSlot
   * @param groupDeltaMap Map<TConsensusGroupId, Map<TSeriesPartitionSlot, Delta TTimePartitionSlot
   *     Count>>
   */
  public void createDataPartition(
      SeriesPartitionTable assignedSeriesPartitionTable,
      TSeriesPartitionSlot seriesPartitionSlot,
      Map<TConsensusGroupId, Map<TSeriesPartitionSlot, AtomicLong>> groupDeltaMap) {
    assignedSeriesPartitionTable
        .getSeriesPartitionMap()
        .forEach(
            ((timePartitionSlot, consensusGroupIds) -> {
              seriesPartitionMap.put(timePartitionSlot, new Vector<>(consensusGroupIds));
              consensusGroupIds.forEach(
                  consensusGroupId ->
                      groupDeltaMap
                          .computeIfAbsent(consensusGroupId, empty -> new ConcurrentHashMap<>())
                          .computeIfAbsent(seriesPartitionSlot, empty -> new AtomicLong(0))
                          .getAndIncrement());
            }));
  }

  /**
   * Only Leader use this interface. And this interface is synchronized. Thread-safely filter no
   * assigned DataPartitionSlots within the specific SeriesPartitionSlot
   *
   * @param partitionSlots TimePartitionSlots
   * @return Unassigned PartitionSlots
   */
  public synchronized List<TTimePartitionSlot> filterUnassignedDataPartitionSlots(
      List<TTimePartitionSlot> partitionSlots) {
    List<TTimePartitionSlot> result = new Vector<>();

    partitionSlots.forEach(
        timePartitionSlot -> {
          if (!seriesPartitionMap.containsKey(timePartitionSlot)) {
            result.add(timePartitionSlot);
          }
        });

    return result;
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(seriesPartitionMap.size(), outputStream);
    for (Map.Entry<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionEntry :
        seriesPartitionMap.entrySet()) {
      seriesPartitionEntry.getKey().write(protocol);
      ReadWriteIOUtils.write(seriesPartitionEntry.getValue().size(), outputStream);
      for (TConsensusGroupId consensusGroupId : seriesPartitionEntry.getValue()) {
        consensusGroupId.write(protocol);
      }
    }
  }

  /** Only for ConsensusRequest */
  public void deserialize(ByteBuffer buffer) {
    int timePartitionSlotNum = buffer.getInt();
    for (int i = 0; i < timePartitionSlotNum; i++) {
      TTimePartitionSlot timePartitionSlot =
          ThriftCommonsSerDeUtils.deserializeTTimePartitionSlot(buffer);

      int consensusGroupIdNum = buffer.getInt();
      List<TConsensusGroupId> consensusGroupIds = new Vector<>();
      for (int j = 0; j < consensusGroupIdNum; j++) {
        consensusGroupIds.add(ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer));
      }

      seriesPartitionMap.put(timePartitionSlot, consensusGroupIds);
    }
  }

  /** Only for Snapshot */
  public void deserialize(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int timePartitionSlotNum = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < timePartitionSlotNum; i++) {
      TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot();
      timePartitionSlot.read(protocol);

      int consensusGroupIdNum = ReadWriteIOUtils.readInt(inputStream);
      List<TConsensusGroupId> consensusGroupIds = new Vector<>();
      for (int j = 0; j < consensusGroupIdNum; j++) {
        TConsensusGroupId consensusGroupId = new TConsensusGroupId();
        consensusGroupId.read(protocol);
        consensusGroupIds.add(consensusGroupId);
      }

      seriesPartitionMap.put(timePartitionSlot, consensusGroupIds);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SeriesPartitionTable that = (SeriesPartitionTable) o;
    return seriesPartitionMap.equals(that.seriesPartitionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(seriesPartitionMap);
  }
}
