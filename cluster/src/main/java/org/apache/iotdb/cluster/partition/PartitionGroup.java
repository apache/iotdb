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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.cluster.rpc.thrift.Node;

/**
 * PartitionGroup contains all the nodes that will form data group with a certain node, which are
 * the previous REPLICATION_NUM - 1 nodes, the node itself and the next REPLICATION_NUM - 1 nodes.
 * From the beginning of the list, every REPLICATION_NUM nodes form a data group, so a node will
 * join REPLICATION_NUM data groups.
 */
public class PartitionGroup extends ArrayList<VNode> {
  private Set<Node> physicalNodes = new HashSet<>();

  Set<Node> getPhysicalNodes() {
    return physicalNodes;
  }

  public void setPhysicalNodes(Set<Node> physicalNodes) {
    this.physicalNodes = physicalNodes;
  }

  @Override
  public boolean add(VNode vNode) {
    if (super.add(vNode)) {
      physicalNodes.add(vNode.getNode());
      return true;
    }
    return false;
  }

  public void addPhysicalNode(Node node) {
    physicalNodes.add(node);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
