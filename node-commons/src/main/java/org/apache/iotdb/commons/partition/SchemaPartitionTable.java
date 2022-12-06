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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SchemaPartitionTable {

  private final Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap;

  public SchemaPartitionTable() {
    this.schemaPartitionMap = new ConcurrentHashMap<>();
  }

  public SchemaPartitionTable(Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap) {
    this.schemaPartitionMap = schemaPartitionMap;
  }

  public Map<TSeriesPartitionSlot, TConsensusGroupId> getSchemaPartitionMap() {
    return schemaPartitionMap;
  }

  /**
   * Thread-safely get SchemaPartition within the specific StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots
   * @param schemaPartitionTable Store the matched SchemaPartitions
   * @return True if all the SeriesPartitionSlots are matched, false otherwise
   */
  public boolean getSchemaPartition(
      List<TSeriesPartitionSlot> partitionSlots, SchemaPartitionTable schemaPartitionTable) {
    AtomicBoolean result = new AtomicBoolean(true);

    if (partitionSlots.isEmpty()) {
      // Return all SchemaPartitions in one StorageGroup when the queried PartitionSlots are empty
      schemaPartitionTable.getSchemaPartitionMap().putAll(schemaPartitionMap);
    } else {
      // Return the SchemaPartition for each SeriesPartitionSlot
      partitionSlots.forEach(
          seriesPartitionSlot -> {
            if (schemaPartitionMap.containsKey(seriesPartitionSlot)) {
              schemaPartitionTable
                  .getSchemaPartitionMap()
                  .put(seriesPartitionSlot, schemaPartitionMap.get(seriesPartitionSlot));
            } else {
              result.set(false);
            }
          });
    }

    return result.get();
  }

  /**
   * Create SchemaPartition within the specific StorageGroup
   *
   * @param assignedSchemaPartition assigned result
   * @return Map<TConsensusGroupId, Map<TSeriesPartitionSlot, Delta TTimePartitionSlot Count(always
   *     0)>>
   */
  public Map<TConsensusGroupId, Map<TSeriesPartitionSlot, AtomicLong>> createSchemaPartition(
      SchemaPartitionTable assignedSchemaPartition) {
    Map<TConsensusGroupId, Map<TSeriesPartitionSlot, AtomicLong>> groupDeltaMap =
        new ConcurrentHashMap<>();

    assignedSchemaPartition
        .getSchemaPartitionMap()
        .forEach(
            ((seriesPartitionSlot, consensusGroupId) -> {
              schemaPartitionMap.put(seriesPartitionSlot, consensusGroupId);
              groupDeltaMap
                  .computeIfAbsent(consensusGroupId, empty -> new ConcurrentHashMap<>())
                  .put(seriesPartitionSlot, new AtomicLong(0));
            }));

    return groupDeltaMap;
  }

  /**
   * Only Leader use this interface. Filter unassigned SchemaPartitionSlots within the specific
   * StorageGroup.
   *
   * @param partitionSlots List<TSeriesPartitionSlot>
   * @return Unassigned PartitionSlots
   */
  public List<TSeriesPartitionSlot> filterUnassignedSchemaPartitionSlots(
      List<TSeriesPartitionSlot> partitionSlots) {
    List<TSeriesPartitionSlot> result = new Vector<>();

    partitionSlots.forEach(
        seriesPartitionSlot -> {
          if (!schemaPartitionMap.containsKey(seriesPartitionSlot)) {
            result.add(seriesPartitionSlot);
          }
        });

    return result;
  }

  public List<TConsensusGroupId> getRegionId(TSeriesPartitionSlot seriesSlotId) {
    if (!schemaPartitionMap.containsKey(seriesSlotId)) {
      return new ArrayList<>();
    }
    return Collections.singletonList(schemaPartitionMap.get(seriesSlotId));
  }

  public List<TSeriesPartitionSlot> getSeriesSlotList() {
    return schemaPartitionMap.keySet().stream()
        .sorted(Comparator.comparing(TSeriesPartitionSlot::getSlotId))
        .collect(Collectors.toList());
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(schemaPartitionMap.size(), outputStream);
    for (Map.Entry<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionEntry :
        schemaPartitionMap.entrySet()) {
      schemaPartitionEntry.getKey().write(protocol);
      schemaPartitionEntry.getValue().write(protocol);
    }
  }

  /** Only for ConsensusRequest */
  public void deserialize(ByteBuffer buffer) {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      schemaPartitionMap.put(
          ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer),
          ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer));
    }
  }

  /** Only for Snapshot */
  public void deserialize(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int length = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < length; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot();
      seriesPartitionSlot.read(protocol);
      TConsensusGroupId consensusGroupId = new TConsensusGroupId();
      consensusGroupId.read(protocol);
      schemaPartitionMap.put(seriesPartitionSlot, consensusGroupId);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaPartitionTable that = (SchemaPartitionTable) o;
    return schemaPartitionMap.equals(that.schemaPartitionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaPartitionMap);
  }
}
