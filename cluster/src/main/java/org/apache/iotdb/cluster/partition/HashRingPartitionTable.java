/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.VNode;

public class HashRingPartitionTable implements PartitionTable {

  private static final int REPLICATION_NUM =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();

  private List<VNode> ring = new ArrayList<>();
  private VNode[] thisVNodes;
  // the data groups which the VNode of this node belongs to
  private List<PartitionGroup>[] headerGroups;

  public HashRingPartitionTable(Collection<Node> nodes, Node thisNode) {
    init(nodes, thisNode);
  }

  private void init(Collection<Node> nodes, Node thisNode) {
    for (Node node : nodes) {
      for (int i = 0; i < ClusterConfig.HASH_SALTS.length; i++) {
        ring.add(new VNode(node, Objects.hash(node.getNodeIdentifier()), i));
      }
    }
    // because hash may conflict, add identifier to make sure the order is consistent
    ring.sort(Comparator.comparingInt(VNode::getHash)
        .thenComparingInt(o -> o.getPNode().getNodeIdentifier()));

    thisVNodes = getVNode(thisNode);
    headerGroups = getPartitionGroups();
  }

  private List<PartitionGroup>[] getPartitionGroups() {
    List<PartitionGroup>[] ret = new List[thisVNodes.length];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = getPartitionGroups(thisVNodes[i]);
    }
    return ret;
  }

  private List<PartitionGroup> getPartitionGroups(VNode node) {
    List<PartitionGroup> ret = new ArrayList<>();
    PartitionGroup nextNodes = getHeaderGroup(node);
    nextNodes.setThisVNode(node);
    ret.add(nextNodes);

    List<VNode> previousNodes = getPreviousHeaders(node);

    // find the data groups formed by the previous nodes together with this node
    for (VNode prevNode : previousNodes) {
      PartitionGroup group = getHeaderGroup(prevNode);
      group.setThisVNode(node);
      ret.add(group);
    }
    return ret;
  }

  /**
   * Get the VNodes that is previous to node and may form a data group with node.
   * @param node
   */
  private List<VNode> getPreviousHeaders(VNode node) {
    Set<Node> previousPhysicalNodes = new HashSet<>();
    List<VNode> previousNodes = new ArrayList<>();

    // find the previous nodes that are in the same data group with node
    int currIndex = ring.indexOf(node);
    boolean moreNodes = true;
    while (true) {
      // move to the previous node
      currIndex = prevIndex(currIndex);
      VNode curr = ring.get(currIndex);
      if (curr.getPNode().equals(node.getPNode())) {
        // if a node of the same physical node is found, this node will not form data groups with
        // more previous nodes because they will choose that node
        moreNodes = false;
      } else {
        if (!previousPhysicalNodes.contains(curr.getPNode())) {
          if (previousPhysicalNodes.size() == REPLICATION_NUM - 1) {
            // this node and previous node will not form data groups with this node because there
            // are already REPLICATION_NUM - 1 physical nodes behind it
            moreNodes = false;
          } else {
            previousPhysicalNodes.add(curr.getPNode());
          }
        }
      }
      if (!moreNodes) {
        break;
      }
      // this node may form a data group as a header with this node
      previousNodes.add(curr);
    }
    return previousNodes;
  }

  @Override
  public PartitionGroup getHeaderGroup(VNode vNode) {
    PartitionGroup ret = new PartitionGroup();
    ret.add(vNode);

    int vNodeIndex = ring.indexOf(vNode);
    // find the next REPLICATION_NUM - 1 different physical nodes
    int remaining = REPLICATION_NUM - 1;
    int currIndex = vNodeIndex;
    while (remaining > 0) {
      // move to the next node
      currIndex = nextIndex(currIndex);
      VNode curr = ring.get(currIndex);
      // if the physical node is not currently included, add it
      if (!ret.getPhysicalNodes().contains(curr.getPNode())) {
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

  private int prevIndex(int currIndex) {
    return currIndex == 0 ? ring.size() - 1 : currIndex - 1;
  }

  private int nextIndex(int currIndex) {
    return currIndex == ring.size() - 1 ? 0 : currIndex + 1;
  }

  @Override
  public List<VNode> route(String storageGroupName, long timestamp) {
    return null;
  }

  @Override
  public void addNode(Node node) {

  }

  @Override
  public List<PartitionGroup>[] getHeaderGroups() {
    return headerGroups;
  }
}
