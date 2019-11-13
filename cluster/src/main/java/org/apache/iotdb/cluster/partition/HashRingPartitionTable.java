/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.partition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;

public class HashRingPartitionTable implements PartitionTable {

  private static final int REPLICATION_NUM =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();

  private List<VNode> ring = new ArrayList<>();
  private VNode[] thisVNodes;
  private PartitionGroup[] partitionGroups;

  public HashRingPartitionTable(Set<Node> nodes, Node thisNode) {
    init(nodes, thisNode);
  }

  private void init(Set<Node> nodes, Node thisNode) {
    for (Node node : nodes) {
      for (int i = 0; i < ClusterConfig.HASH_SALTS.length; i++) {
        ring.add(new VNode(node, Objects.hash(node.getNodeIdentifier()), i));
      }
    }
    // because hash may conflict, add identifier to make sure the order is consistent
    ring.sort(Comparator.comparingInt(VNode::getHash)
        .thenComparingInt(o -> o.getNode().getNodeIdentifier()));

    thisVNodes = getVNode(thisNode);
    partitionGroups = getPartitionGroups();
  }

  public PartitionGroup[] getPartitionGroups() {
    PartitionGroup[] ret = new PartitionGroup[thisVNodes.length];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = getPartitionGroup(thisVNodes[i]);
    }
    return ret;
  }

  private PartitionGroup getPartitionGroup(VNode vNode) {
    PartitionGroup ret = new PartitionGroup();
    ret.addPhysicalNode(vNode.getNode());

    int vNodeIndex = ring.indexOf(vNode);
    // find the previous REPLICATION_NUM - 1 different physical nodes
    int remaining = REPLICATION_NUM - 1;
    int currIndex = vNodeIndex;
    while (remaining > 0) {
      // move to the previous node
      currIndex = currIndex == 0 ? ring.size() - 1 : currIndex - 1;
      VNode curr = ring.get(currIndex);
      // if the physical node is not currently included, add it
      if (!ret.getPhysicalNodes().contains(curr.getNode())) {
        ret.add(curr);
        remaining--;
      }
    }
    ret.add(vNode);

    // find the next REPLICATION_NUM - 1 different physical nodes
    remaining = REPLICATION_NUM - 1;
    currIndex = vNodeIndex;
    while (remaining > 0) {
      // move to the next node
      currIndex = currIndex == ring.size() - 1 ? 0 : currIndex + 1;
      VNode curr = ring.get(currIndex);
      // if the physical node is not currently included, add it
      if (!ret.getPhysicalNodes().contains(curr.getNode())) {
        ret.add(curr);
        remaining--;
      }
    }
    return ret;
  }

  private VNode[] getVNode(Node node) {
    VNode[] ret = new VNode[ClusterConfig.HASH_SALTS.length];
    for (int i = 0; i < ClusterConfig.HASH_SALTS.length; i++) {
      ret[i] = new VNode(node, Objects.hash(node.getNodeIdentifier(),
          ClusterConfig.HASH_SALTS[i]), i);
    }
    return ret;
  }

  @Override
  public List<Node> route(String storageGroupName, long timestamp) {
    return null;
  }

  @Override
  public void addNode(Node node) {

  }
}
