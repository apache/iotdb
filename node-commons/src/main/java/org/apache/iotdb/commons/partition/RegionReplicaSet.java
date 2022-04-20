/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RegionReplicaSet {
  private ConsensusGroupId consensusGroupId;
  private List<DataNodeLocation> dataNodeList;

  public RegionReplicaSet() {}

  public RegionReplicaSet(ConsensusGroupId consensusGroupId, List<DataNodeLocation> dataNodeList) {
    this.consensusGroupId = consensusGroupId;
    this.dataNodeList = dataNodeList;
  }

  public RegionReplicaSet(TRegionReplicaSet respRegionReplicaSet) {
    this.consensusGroupId = ConsensusGroupId.Factory.create(respRegionReplicaSet.regionId);
    this.dataNodeList =
        respRegionReplicaSet.getEndpoint().stream()
            .map(
                respEndpoint ->
                    new DataNodeLocation(
                        new Endpoint(respEndpoint.getIp(), respEndpoint.getPort())))
            .collect(Collectors.toList());
  }

  public List<DataNodeLocation> getDataNodeList() {
    return dataNodeList;
  }

  public void setDataNodeList(List<DataNodeLocation> dataNodeList) {
    this.dataNodeList = dataNodeList;
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public void setConsensusGroupId(ConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
  }

  /** Convert RegionReplicaSet to TRegionReplicaSet */
  public TRegionReplicaSet convertToRPCTRegionReplicaSet() {
    TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();

    // Convert ConsensusGroupId
    ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES);
    consensusGroupId.serializeImpl(buffer);
    buffer.flip();
    tRegionReplicaSet.setRegionId(buffer);

    // Convert EndPoints
    List<TEndPoint> endPointList = new ArrayList<>();
    dataNodeList.forEach(
        dataNodeLocation ->
            endPointList.add(
                new TEndPoint(
                    dataNodeLocation.getEndPoint().getIp(),
                    dataNodeLocation.getEndPoint().getPort())));
    tRegionReplicaSet.setEndpoint(endPointList);

    return tRegionReplicaSet;
  }

  @Override
  public String toString() {
    return String.format("RegionReplicaSet[%s]: %s", consensusGroupId, dataNodeList);
  }

  public void serializeImpl(ByteBuffer buffer) {
    consensusGroupId.serializeImpl(buffer);
    buffer.putInt(dataNodeList.size());
    dataNodeList.forEach(
        dataNode -> {
          dataNode.serializeImpl(buffer);
        });
  }

  public static RegionReplicaSet deserializeImpl(ByteBuffer buffer) {
    ConsensusGroupId consensusGroupId = ConsensusGroupId.Factory.create(buffer);

    int size = buffer.getInt();
    // We should always make dataNodeList as a new Object when deserialization
    List<DataNodeLocation> dataNodeList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      dataNodeList.add(DataNodeLocation.deserializeImpl(buffer));
    }
    return new RegionReplicaSet(consensusGroupId, dataNodeList);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegionReplicaSet that = (RegionReplicaSet) o;
    return Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(dataNodeList, that.dataNodeList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consensusGroupId, dataNodeList);
  }

  public boolean isEmpty() {
    return dataNodeList == null || dataNodeList.isEmpty();
  }
}
