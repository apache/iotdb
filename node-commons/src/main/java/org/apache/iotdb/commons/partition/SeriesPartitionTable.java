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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
   * @param partitionSlots TimePartitionSlots
   * @return RegionIds of the PartitionSlots
   */
  public SeriesPartitionTable getDataPartition(List<TTimePartitionSlot> partitionSlots) {
    if (partitionSlots.isEmpty()) {
      // Return all DataPartitions in one SeriesPartitionSlot when the queried PartitionSlots are
      // empty
      return new SeriesPartitionTable(new ConcurrentHashMap<>(seriesPartitionMap));
    } else {
      // Return the DataPartition for each TimePartitionSlot
      Map<TTimePartitionSlot, List<TConsensusGroupId>> result = new ConcurrentHashMap<>();
      partitionSlots.forEach(
          timePartitionSlot -> {
            if (seriesPartitionMap.containsKey(timePartitionSlot)) {
              result.put(
                  timePartitionSlot, new ArrayList<>(seriesPartitionMap.get(timePartitionSlot)));
            }
          });
      return new SeriesPartitionTable(result);
    }
  }

  /**
   * Thread-safely create DataPartition within the specific SeriesPartitionSlot
   *
   * @param assignedSeriesPartitionTable Assigned result
   * @param deltaMap Number of DataPartitions added to each Region
   */
  public void createDataPartition(
      SeriesPartitionTable assignedSeriesPartitionTable,
      Map<TConsensusGroupId, AtomicInteger> deltaMap) {
    assignedSeriesPartitionTable
        .getSeriesPartitionMap()
        .forEach(
            ((timePartitionSlot, consensusGroupIds) -> {
              // In concurrent scenarios, some DataPartitions may have already been created by
              // another thread.
              // Therefore, we could just ignore them.
              if (!seriesPartitionMap.containsKey(timePartitionSlot)) {
                // Notice: For each TimePartitionSlot, we only allocate one DataPartition at a time
                seriesPartitionMap.put(timePartitionSlot, new Vector<>(consensusGroupIds));
                deltaMap
                    .computeIfAbsent(consensusGroupIds.get(0), empty -> new AtomicInteger(0))
                    .getAndIncrement();
              }
            }));
  }

  /**
   * Only Leader use this interface Thread-safely filter no assigned DataPartitionSlots within the
   * specific SeriesPartitionSlot
   *
   * @param partitionSlots TimePartitionSlots
   * @return Un-assigned PartitionSlots
   */
  public List<TTimePartitionSlot> filterNoAssignedSchemaPartitionSlots(
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

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(seriesPartitionMap.size());
    seriesPartitionMap.forEach(
        ((timePartitionSlot, consensusGroupIds) -> {
          ThriftCommonsSerDeUtils.serializeTTimePartitionSlot(timePartitionSlot, buffer);
          buffer.putInt(consensusGroupIds.size());
          consensusGroupIds.forEach(
              consensusGroupId ->
                  ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId, buffer));
        }));
  }

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
