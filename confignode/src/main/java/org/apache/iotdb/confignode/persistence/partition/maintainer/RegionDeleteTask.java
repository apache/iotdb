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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
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

public class RegionDeleteTask extends RegionMaintainTask {

  private TConsensusGroupId regionId;

  public RegionDeleteTask() {
    super(RegionMaintainType.DELETE);
  }

  public RegionDeleteTask(TDataNodeLocation targetDataNode, TConsensusGroupId regionId) {
    super(RegionMaintainType.DELETE);
    this.targetDataNode = targetDataNode;
    this.regionId = regionId;
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(RegionMaintainType.DELETE.ordinal());

    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(targetDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
  }

  @Override
  protected void deserialize(ByteBuffer buffer) throws IOException {
    this.targetDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer);
    this.regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
  }

  @Override
  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(RegionMaintainType.DELETE.ordinal(), outputStream);

    this.targetDataNode.write(protocol);
    this.regionId.write(protocol);
  }

  @Override
  protected void deserialize(InputStream inputStream, TProtocol protocol) throws TException {
    this.targetDataNode = new TDataNodeLocation();
    this.targetDataNode.read(protocol);
    this.regionId = new TConsensusGroupId();
    this.regionId.read(protocol);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RegionDeleteTask)) return false;
    if (!super.equals(o)) return false;
    RegionDeleteTask that = (RegionDeleteTask) o;
    return regionId.equals(that.regionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), regionId);
  }
}
