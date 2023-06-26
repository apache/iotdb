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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class RegionMaintainTask {

  protected final RegionMaintainType type;
  protected TDataNodeLocation targetDataNode;

  protected RegionMaintainTask(RegionMaintainType type) {
    this.type = type;
  }

  public RegionMaintainType getType() {
    return type;
  }

  public TDataNodeLocation getTargetDataNode() {
    return targetDataNode;
  }

  public void setTargetDataNode(TDataNodeLocation targetDataNode) {
    this.targetDataNode = targetDataNode;
  }

  public abstract TConsensusGroupId getRegionId();

  // Serialize interface for consensus log
  public abstract void serialize(DataOutputStream stream) throws IOException;

  // Deserialize interface for consensus log
  protected abstract void deserialize(ByteBuffer buffer) throws IOException;

  // Serialize interface for snapshot
  public abstract void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException;

  // Deserialize interface for snapshot
  protected abstract void deserialize(InputStream inputStream, TProtocol protocol)
      throws TException, IOException;

  public static class Factory {

    public static RegionMaintainTask create(ByteBuffer buffer) throws IOException {
      int typeNum = buffer.getInt();
      if (typeNum >= RegionMaintainType.values().length) {
        throw new IOException("Unrecognized RegionMaintainType: " + typeNum);
      }
      RegionMaintainTask task;
      RegionMaintainType type = RegionMaintainType.values()[typeNum];
      switch (type) {
        case CREATE:
          task = new RegionCreateTask();
          break;
        case DELETE:
          task = new RegionDeleteTask();
          break;
        default:
          throw new IOException("Unrecognized RegionMaintainType: " + typeNum);
      }
      task.deserialize(buffer);
      return task;
    }

    public static RegionMaintainTask create(InputStream inputStream, TProtocol protocol)
        throws IOException, TException {
      int typeNum = ReadWriteIOUtils.readInt(inputStream);
      if (typeNum >= RegionMaintainType.values().length) {
        throw new IOException("Unrecognized RegionMaintainType: " + typeNum);
      }
      RegionMaintainTask task;
      RegionMaintainType type = RegionMaintainType.values()[typeNum];
      switch (type) {
        case CREATE:
          task = new RegionCreateTask();
          break;
        case DELETE:
          task = new RegionDeleteTask();
          break;
        default:
          throw new IOException("Unrecognized RegionMaintainType: " + typeNum);
      }
      task.deserialize(inputStream, protocol);
      return task;
    }

    private Factory() {
      // Empty constructor
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RegionMaintainTask)) return false;
    RegionMaintainTask task = (RegionMaintainTask) o;
    return type == task.type && targetDataNode.equals(task.targetDataNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, targetDataNode);
  }
}
