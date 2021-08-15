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
 * PartitionGroup contains all the nodes that will form a data group with a certain node, which are
 * the REPLICATION_NUM - 1 different physical nodes after this node. The first element of the list
 * is called header, which is also the identifier of the data group.
 */
public class PartitionGroup extends ArrayList<Node> {

  private int id;

  public PartitionGroup() {}

  public PartitionGroup(Collection<Node> nodes) {
    this.addAll(nodes);
  }

  public PartitionGroup(int id, Node... nodes) {
    this.addAll(Arrays.asList(nodes));
    this.id = id;
  }

  public PartitionGroup(PartitionGroup other) {
    super(other);
    this.id = other.getId();
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
    return Objects.equals(id, group.getId()) && super.equals(group);
  }

  public void serialize(DataOutputStream dataOutputStream) throws IOException {
    dataOutputStream.writeInt(getId());
    dataOutputStream.writeInt(size());
    for (Node node : this) {
      NodeSerializeUtils.serialize(node, dataOutputStream);
    }
  }

  public void deserialize(ByteBuffer buffer) {
    id = buffer.getInt();
    int nodeNum = buffer.getInt();
    for (int i2 = 0; i2 < nodeNum; i2++) {
      Node node = new Node();
      NodeSerializeUtils.deserialize(node, buffer);
      add(node);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, super.hashCode());
  }

  public RaftNode getHeader() {
    return new RaftNode(get(0), getId());
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }
}
