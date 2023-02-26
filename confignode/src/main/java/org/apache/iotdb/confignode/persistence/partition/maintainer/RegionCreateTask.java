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
package org.apache.iotdb.confignode.persistence.partition.maintainer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RegionCreateTask extends RegionMaintainTask {

  private String storageGroup;
  private TRegionReplicaSet regionReplicaSet;
  private long TTL;

  public RegionCreateTask() {
    super(RegionMaintainType.CREATE);
  }

  public RegionCreateTask(
      TDataNodeLocation targetDataNode, String storageGroup, TRegionReplicaSet regionReplicaSet) {
    super(RegionMaintainType.CREATE);
    this.targetDataNode = targetDataNode;
    this.storageGroup = storageGroup;
    this.regionReplicaSet = regionReplicaSet;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public long getTTL() {
    return TTL;
  }

  public RegionCreateTask setTTL(long TTL) {
    this.TTL = TTL;
    return this;
  }

  @Override
  public TConsensusGroupId getRegionId() {
    return regionReplicaSet.getRegionId();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(RegionMaintainType.CREATE.ordinal());

    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(targetDataNode, stream);
    ReadWriteIOUtils.write(storageGroup, stream);
    ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet, stream);
    if (TConsensusGroupType.DataRegion.equals(regionReplicaSet.getRegionId().getType())) {
      ReadWriteIOUtils.write(TTL, stream);
    }
  }

  @Override
  protected void deserialize(ByteBuffer buffer) throws IOException {
    this.targetDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer);
    this.storageGroup = ReadWriteIOUtils.readString(buffer);
    this.regionReplicaSet = ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(buffer);
    if (TConsensusGroupType.DataRegion.equals(regionReplicaSet.getRegionId().getType())) {
      this.TTL = ReadWriteIOUtils.readLong(buffer);
    }
  }

  @Override
  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(RegionMaintainType.CREATE.ordinal(), outputStream);

    this.targetDataNode.write(protocol);
    ReadWriteIOUtils.write(storageGroup, outputStream);
    this.regionReplicaSet.write(protocol);
    if (TConsensusGroupType.DataRegion.equals(regionReplicaSet.getRegionId().getType())) {
      ReadWriteIOUtils.write(TTL, outputStream);
    }
  }

  @Override
  protected void deserialize(InputStream inputStream, TProtocol protocol)
      throws TException, IOException {
    this.targetDataNode = new TDataNodeLocation();
    this.targetDataNode.read(protocol);
    this.storageGroup = ReadWriteIOUtils.readString(inputStream);
    this.regionReplicaSet = new TRegionReplicaSet();
    this.regionReplicaSet.read(protocol);
    if (TConsensusGroupType.DataRegion.equals(regionReplicaSet.getRegionId().getType())) {
      this.TTL = ReadWriteIOUtils.readLong(inputStream);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RegionCreateTask)) return false;
    if (!super.equals(o)) return false;
    RegionCreateTask that = (RegionCreateTask) o;
    return TTL == that.TTL
        && storageGroup.equals(that.storageGroup)
        && regionReplicaSet.equals(that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), storageGroup, regionReplicaSet, TTL);
  }
}
