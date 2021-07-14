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

package org.apache.iotdb.cluster.log.logtypes;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.NodeSerializeUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RemoveNodeLog extends Log {

  private ByteBuffer partitionTable;

  private Node removedNode;

  private long metaLogIndex;

  public RemoveNodeLog(ByteBuffer partitionTable, Node removedNode) {
    this.partitionTable = partitionTable;
    this.removedNode = removedNode;
  }

  public RemoveNodeLog() {}

  public long getMetaLogIndex() {
    return metaLogIndex;
  }

  public void setMetaLogIndex(long metaLogIndex) {
    this.metaLogIndex = metaLogIndex;
  }

  public ByteBuffer getPartitionTable() {
    partitionTable.rewind();
    return partitionTable;
  }

  public void setPartitionTable(ByteBuffer partitionTable) {
    this.partitionTable = partitionTable;
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeByte(Types.REMOVE_NODE.ordinal());
      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getCurrLogTerm());
      dataOutputStream.writeLong(getMetaLogIndex());

      NodeSerializeUtils.serialize(removedNode, dataOutputStream);

      dataOutputStream.writeInt(partitionTable.array().length);
      dataOutputStream.write(partitionTable.array());
    } catch (IOException e) {
      // ignored
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());
    setMetaLogIndex(buffer.getLong());

    removedNode = new Node();
    NodeSerializeUtils.deserialize(removedNode, buffer);

    int len = buffer.getInt();
    byte[] data = new byte[len];
    System.arraycopy(buffer.array(), buffer.position(), data, 0, len);
    partitionTable = ByteBuffer.wrap(data);
  }

  public Node getRemovedNode() {
    return removedNode;
  }

  public void setRemovedNode(Node removedNode) {
    this.removedNode = removedNode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RemoveNodeLog that = (RemoveNodeLog) o;
    return Objects.equals(removedNode, that.removedNode)
        && Objects.equals(partitionTable, that.partitionTable);
  }

  @Override
  public String toString() {
    return "RemoveNodeLog{" + "removedNode=" + removedNode.toString() + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), removedNode, partitionTable);
  }
}
