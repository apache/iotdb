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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
   * @return RegionIds of the PartitionSlots
   */
  public DataPartitionTable getDataPartition(
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> partitionSlots) {
    if (partitionSlots.isEmpty()) {
      // Return all DataPartitions in one StorageGroup when the queried PartitionSlots are empty
      return new DataPartitionTable(new ConcurrentHashMap<>(dataPartitionMap));
    } else {
      // Return the DataPartition for each SeriesPartitionSlot
      Map<TSeriesPartitionSlot, SeriesPartitionTable> result = new ConcurrentHashMap<>();

      partitionSlots.forEach(
          (seriesPartitionSlot, timePartitionSlots) -> {
            if (dataPartitionMap.containsKey(seriesPartitionSlot)) {
              result.put(
                  seriesPartitionSlot,
                  dataPartitionMap.get(seriesPartitionSlot).getDataPartition(timePartitionSlots));
            }
          });

      return new DataPartitionTable(result);
    }
  }

  /**
   * Thread-safely create DataPartition within the specific StorageGroup
   *
   * @param assignedDataPartition Assigned result
   * @return Number of DataPartitions added to each Region
   */
  public Map<TConsensusGroupId, AtomicInteger> createDataPartition(
      DataPartitionTable assignedDataPartition) {
    Map<TConsensusGroupId, AtomicInteger> deltaMap = new ConcurrentHashMap<>();

    assignedDataPartition
        .getDataPartitionMap()
        .forEach(
            ((seriesPartitionSlot, seriesPartitionTable) ->
                dataPartitionMap
                    .computeIfAbsent(seriesPartitionSlot, empty -> new SeriesPartitionTable())
                    .createDataPartition(seriesPartitionTable, deltaMap)));

    return deltaMap;
  }

  /**
   * Only Leader use this interface Thread-safely filter unassigned DataPartitionSlots within the
   * specific StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @return Unassigned PartitionSlots
   */
  public Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> filterUnassignedDataPartitionSlots(
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> partitionSlots) {
    Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> result = new ConcurrentHashMap<>();

    partitionSlots.forEach(
        (seriesPartitionSlot, timePartitionSlots) -> {
          if (!dataPartitionMap.containsKey(seriesPartitionSlot)) {
            result.put(seriesPartitionSlot, timePartitionSlots);
          } else {
            result.put(
                seriesPartitionSlot,
                dataPartitionMap
                    .get(seriesPartitionSlot)
                    .filterNoAssignedSchemaPartitionSlots(timePartitionSlots));
          }
        });

    return result;
  }

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(dataPartitionMap.size());
    dataPartitionMap.forEach(
        ((seriesPartitionSlot, seriesPartitionTable) -> {
          ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesPartitionSlot, buffer);
          seriesPartitionTable.serialize(buffer);
        }));
  }

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

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(dataPartitionMap.size(), outputStream);
    for (Map.Entry<TSeriesPartitionSlot, SeriesPartitionTable> seriesPartitionTableEntry :
        dataPartitionMap.entrySet()) {
      seriesPartitionTableEntry.getKey().write(protocol);
      seriesPartitionTableEntry.getValue().serialize(outputStream, protocol);
    }
  }

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
