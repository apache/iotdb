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

package org.apache.iotdb.cluster.partition;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.utils.NodeSerializeUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * PartitionGroup contains all the nodes of a data group. The first element of the list is called
 * header.
 */
public class PartitionGroup extends ArrayList<Node> {

  private int raftId;

  public PartitionGroup() {}

  public PartitionGroup(Collection<Node> nodes) {
    this.addAll(nodes);
  }

  public PartitionGroup(int raftId, Node... nodes) {
    this.addAll(Arrays.asList(nodes));
    this.raftId = raftId;
  }

  public PartitionGroup(PartitionGroup other) {
    super(other);
    this.raftId = other.getRaftId();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionGroup group = (PartitionGroup) o;
    return Objects.equals(raftId, group.getRaftId()) && super.equals(group);
  }

  public void serialize(DataOutputStream dataOutputStream) throws IOException {
    dataOutputStream.writeInt(getRaftId());
    dataOutputStream.writeInt(size());
    for (Node node : this) {
      NodeSerializeUtils.serialize(node, dataOutputStream);
    }
  }

  public void deserialize(ByteBuffer buffer) {
    raftId = buffer.getInt();
    int nodeNum = buffer.getInt();
    for (int i2 = 0; i2 < nodeNum; i2++) {
      Node node = new Node();
      NodeSerializeUtils.deserialize(node, buffer);
      add(node);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(raftId, super.hashCode());
  }

  public RaftNode getHeader() {
    return new RaftNode(get(0), getRaftId());
  }

  public int getRaftId() {
    return raftId;
  }

  public void setRaftId(int raftId) {
    this.raftId = raftId;
  }

  @Override
  public String toString() {
    return super.toString() + ", id = " + raftId;
  }
}
