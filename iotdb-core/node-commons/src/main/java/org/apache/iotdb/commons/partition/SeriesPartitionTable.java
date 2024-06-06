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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SeriesPartitionTable {

  private final ReentrantReadWriteLock seriesPartitionMapLock;
  private final TreeMap<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionMap;

  public SeriesPartitionTable() {
    this.seriesPartitionMapLock = new ReentrantReadWriteLock();
    this.seriesPartitionMap = new TreeMap<>();
  }

  public SeriesPartitionTable(Map<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionMap) {
    this.seriesPartitionMapLock = new ReentrantReadWriteLock();
    this.seriesPartitionMap = new TreeMap<>(seriesPartitionMap);
  }

  public Map<TTimePartitionSlot, List<TConsensusGroupId>> getSeriesPartitionMap() {
    seriesPartitionMapLock.readLock().lock();
    try {
      return seriesPartitionMap;
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }

  public void putDataPartition(TTimePartitionSlot timePartitionSlot, TConsensusGroupId groupId) {
    seriesPartitionMapLock.writeLock().lock();
    try {
      seriesPartitionMap
          .computeIfAbsent(timePartitionSlot, empty -> new ArrayList<>())
          .add(groupId);
    } finally {
      seriesPartitionMapLock.writeLock().unlock();
    }
  }

  /**
   * Thread-safely get DataPartition within the specific Database.
   *
   * @param partitionSlotList TimePartitionSlotList
   * @param seriesPartitionTable Store the matched SeriesPartitions
   * @return True if all the SeriesPartitionSlots are matched, false otherwise
   */
  public boolean getDataPartition(
      TTimeSlotList partitionSlotList, SeriesPartitionTable seriesPartitionTable) {
    AtomicBoolean result = new AtomicBoolean(true);
    seriesPartitionMapLock.readLock().lock();
    List<TTimePartitionSlot> partitionSlots = partitionSlotList.getTimePartitionSlots();
    try {
      if (partitionSlots.isEmpty()) {
        // Return all DataPartitions in one SeriesPartitionSlot
        // when the queried TimePartitionSlots are empty
        seriesPartitionTable.getSeriesPartitionMap().putAll(seriesPartitionMap);
      } else {
        boolean isNeedLeftAll = partitionSlotList.isNeedLeftAll(),
            isNeedRightAll = partitionSlotList.isNeedRightAll();
        if (isNeedLeftAll || isNeedRightAll) {
          // we need to calculate the leftMargin which contains all the time partition on the
          // unclosed
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
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
    return result.get();
  }

  /**
   * Check and return the specified DataPartition's successor.
   *
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @return The specified DataPartition's successor if exists, null otherwise
   */
  public TConsensusGroupId getSuccessorDataPartition(TTimePartitionSlot timePartitionSlot) {
    seriesPartitionMapLock.readLock().lock();
    try {
      TTimePartitionSlot successorSlot = seriesPartitionMap.higherKey(timePartitionSlot);
      return successorSlot == null ? null : seriesPartitionMap.get(successorSlot).get(0);
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }

  /**
   * Check and return the specified DataPartition's predecessor.
   *
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @return The specified DataPartition's predecessor if exists, null otherwise
   */
  public TConsensusGroupId getPredecessorDataPartition(TTimePartitionSlot timePartitionSlot) {
    seriesPartitionMapLock.readLock().lock();
    try {
      TTimePartitionSlot predecessorSlot = seriesPartitionMap.lowerKey(timePartitionSlot);
      return predecessorSlot == null ? null : seriesPartitionMap.get(predecessorSlot).get(0);
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }

  /**
   * Query a timePartition's corresponding dataRegionIds.
   *
   * @param startTimeSlotId start Time partition's timeSlotId
   * @param endTimeSlotId end Time partition's timeSlotId
   * @return the timePartition's corresponding dataRegionIds. return the dataRegions which
   *     timeslotIds are in the time range [startTimeSlotId, endTimeSlotId].
   */
  public List<TConsensusGroupId> getRegionId(
      TTimePartitionSlot startTimeSlotId, TTimePartitionSlot endTimeSlotId) {
    seriesPartitionMapLock.readLock().lock();
    try {
      return seriesPartitionMap.entrySet().stream()
          .filter(
              entry ->
                  entry.getKey().getStartTime() >= startTimeSlotId.getStartTime()
                      && entry.getKey().getStartTime() <= endTimeSlotId.getStartTime())
          .flatMap(entry -> entry.getValue().stream())
          .collect(Collectors.toList());
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }

  public List<TTimePartitionSlot> getTimeSlotList(
      TConsensusGroupId regionId, long startTime, long endTime) {
    seriesPartitionMapLock.readLock().lock();
    try {
      if (regionId.getId() == -1) {
        return seriesPartitionMap.keySet().stream()
            .filter(e -> e.getStartTime() >= startTime && e.getStartTime() < endTime)
            .collect(Collectors.toList());
      } else {
        return seriesPartitionMap.keySet().stream()
            .filter(e -> e.getStartTime() >= startTime && e.getStartTime() < endTime)
            .filter(e -> seriesPartitionMap.get(e).contains(regionId))
            .collect(Collectors.toList());
      }
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }

  /**
   * Create DataPartition within the specific SeriesPartitionSlot.
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
    seriesPartitionMapLock.writeLock().lock();
    try {
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
    } finally {
      seriesPartitionMapLock.writeLock().unlock();
    }
  }

  /**
   * Only Leader use this interface. And this interface is synchronized. Thread-safely filter no
   * assigned DataPartitionSlots within the specific SeriesPartitionSlot.
   *
   * @param partitionSlots TimePartitionSlots
   * @return Unassigned PartitionSlots
   */
  public synchronized List<TTimePartitionSlot> filterUnassignedDataPartitionSlots(
      List<TTimePartitionSlot> partitionSlots) {
    seriesPartitionMapLock.readLock().lock();
    List<TTimePartitionSlot> result = new Vector<>();
    try {
      partitionSlots.forEach(
          timePartitionSlot -> {
            if (!seriesPartitionMap.containsKey(timePartitionSlot)) {
              result.add(timePartitionSlot);
            }
          });
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Get the last DataPartition's ConsensusGroupId.
   *
   * @return The last DataPartition's ConsensusGroupId, null if there are no DataPartitions yet
   */
  public TConsensusGroupId getLastConsensusGroupId() {
    seriesPartitionMapLock.readLock().lock();
    try {
      Map.Entry<TTimePartitionSlot, List<TConsensusGroupId>> lastEntry =
          seriesPartitionMap.lastEntry();
      if (lastEntry == null) {
        return null;
      }
      return lastEntry.getValue().get(lastEntry.getValue().size() - 1);
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    seriesPartitionMapLock.readLock().lock();
    try {
      ReadWriteIOUtils.write(seriesPartitionMap.size(), outputStream);
      for (Map.Entry<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionEntry :
          seriesPartitionMap.entrySet()) {
        seriesPartitionEntry.getKey().write(protocol);
        ReadWriteIOUtils.write(seriesPartitionEntry.getValue().size(), outputStream);
        for (TConsensusGroupId consensusGroupId : seriesPartitionEntry.getValue()) {
          consensusGroupId.write(protocol);
        }
      }
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }

  /** Only for ConsensusRequest. */
  public void deserialize(ByteBuffer buffer) {
    seriesPartitionMapLock.writeLock().lock();
    try {
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
    } finally {
      seriesPartitionMapLock.writeLock().unlock();
    }
  }

  /** Only for Snapshot. */
  public void deserialize(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    seriesPartitionMapLock.writeLock().lock();
    try {
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
    } finally {
      seriesPartitionMapLock.writeLock().unlock();
    }
  }

  @Override
  public boolean equals(Object o) {
    seriesPartitionMapLock.readLock().lock();
    try {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SeriesPartitionTable that = (SeriesPartitionTable) o;
      return seriesPartitionMap.equals(that.seriesPartitionMap);
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }

  @Override
  public int hashCode() {
    seriesPartitionMapLock.readLock().lock();
    try {
      return Objects.hash(seriesPartitionMap);
    } finally {
      seriesPartitionMapLock.readLock().unlock();
    }
  }
}
