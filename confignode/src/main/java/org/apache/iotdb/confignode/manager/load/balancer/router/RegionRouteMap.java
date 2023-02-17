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
package org.apache.iotdb.confignode.manager.load.balancer.router;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class RegionRouteMap {

  // Map<RegionGroupId, LeaderDataNodeId>
  private Map<TConsensusGroupId, Integer> regionLeaderMap;

  // Map<RegionGroupId, TRegionReplicaSet>
  // Indicate the routing priority of read/write requests for each RegionGroup.
  // The replica with higher sorting result have higher priority.
  // TODO: Might be split into readRouteMap and writeRouteMap in the future
  private Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap;

  public RegionRouteMap() {
    this.regionLeaderMap = new ConcurrentHashMap<>();
    this.regionPriorityMap = new ConcurrentHashMap<>();
  }

  /**
   * @return DataNodeId where the specified RegionGroup's leader resides. And return -1 if the
   *     leader is not recorded yet
   */
  public int getLeader(TConsensusGroupId regionGroupId) {
    return regionLeaderMap.getOrDefault(regionGroupId, -1);
  }

  public void setLeader(TConsensusGroupId regionGroupId, int leaderId) {
    regionLeaderMap.put(regionGroupId, leaderId);
  }

  public Map<TConsensusGroupId, Integer> getRegionLeaderMap() {
    return regionLeaderMap;
  }

  public void setRegionLeaderMap(Map<TConsensusGroupId, Integer> regionLeaderMap) {
    this.regionLeaderMap = regionLeaderMap;
  }

  public Map<TConsensusGroupId, TRegionReplicaSet> getRegionPriorityMap() {
    return regionPriorityMap;
  }

  public boolean isEmpty() {
    return regionLeaderMap.isEmpty() && regionPriorityMap.isEmpty();
  }

  public void setRegionPriorityMap(Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap) {
    this.regionPriorityMap = regionPriorityMap;
  }

  public void removeRegionRouteCache(TConsensusGroupId regionGroupId) {
    this.regionLeaderMap.remove(regionGroupId);
    this.regionPriorityMap.remove(regionGroupId);
  }

  public void serialize(OutputStream stream, TProtocol protocol) throws IOException {
    try {
      ReadWriteIOUtils.write(regionLeaderMap.size(), stream);
      for (Map.Entry<TConsensusGroupId, Integer> leaderEntry : regionLeaderMap.entrySet()) {
        leaderEntry.getKey().write(protocol);
        ReadWriteIOUtils.write(leaderEntry.getValue(), stream);
      }

      ReadWriteIOUtils.write(regionPriorityMap.size(), stream);
      for (Map.Entry<TConsensusGroupId, TRegionReplicaSet> priorityEntry :
          regionPriorityMap.entrySet()) {
        priorityEntry.getKey().write(protocol);
        priorityEntry.getValue().write(protocol);
      }
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  // Deserializer for consensus-write
  public void deserialize(ByteBuffer buffer) {
    this.regionLeaderMap = new ConcurrentHashMap<>();
    int leaderEntryNum = buffer.getInt();
    for (int i = 0; i < leaderEntryNum; i++) {
      TConsensusGroupId regionGroupId =
          ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
      int leaderId = buffer.getInt();
      regionLeaderMap.put(regionGroupId, leaderId);
    }

    this.regionPriorityMap = new ConcurrentHashMap<>();
    int priorityEntryNum = buffer.getInt();
    for (int i = 0; i < priorityEntryNum; i++) {
      TConsensusGroupId regionGroupId =
          ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
      TRegionReplicaSet regionReplicaSet =
          ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(buffer);
      regionPriorityMap.put(regionGroupId, regionReplicaSet);
    }
  }

  // Deserializer for snapshot
  public void deserialize(InputStream stream, TProtocol protocol) throws IOException, TException {
    this.regionLeaderMap = new ConcurrentHashMap<>();
    int leaderEntryNum = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < leaderEntryNum; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId();
      regionGroupId.read(protocol);
      int leaderId = ReadWriteIOUtils.readInt(stream);
      regionLeaderMap.put(regionGroupId, leaderId);
    }

    this.regionPriorityMap = new ConcurrentHashMap<>();
    int priorityEntryNum = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < priorityEntryNum; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId();
      regionGroupId.read(protocol);
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.read(protocol);
      regionPriorityMap.put(regionGroupId, regionReplicaSet);
    }
  }

  public void clear() {
    regionLeaderMap.clear();
    regionPriorityMap.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RegionRouteMap that = (RegionRouteMap) o;
    return regionLeaderMap.equals(that.regionLeaderMap)
        && regionPriorityMap.equals(that.regionPriorityMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionLeaderMap, regionPriorityMap);
  }
}
