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

package org.apache.iotdb.cluster.partition.balancer;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** This balancer aims to avg slots to all raft groups. */
public class DefaultSlotBalancer implements SlotBalancer {

  private int multiRaftFactor = ClusterDescriptor.getInstance().getConfig().getMultiRaftFactor();
  private SlotPartitionTable table;

  public DefaultSlotBalancer(SlotPartitionTable partitionTable) {
    this.table = partitionTable;
  }

  /**
   * Move last slots from each group whose slot number is bigger than the new average to the new
   * node.
   */
  @Override
  public void moveSlotsToNew(Node newNode, List<Node> oldRing) {
    Map<RaftNode, List<Integer>> nodeSlotMap = table.getAllNodeSlots();
    Map<RaftNode, Map<Integer, PartitionGroup>> previousNodeMap = table.getPreviousNodeMap();
    RaftNode[] slotNodes = table.getSlotNodes();

    // as a node is added, the average slots for each node decrease
    // move the slots to the new node if any previous node have more slots than the new average
    int newAvg = table.getTotalSlotNumbers() / table.getAllNodes().size() / multiRaftFactor;
    Map<RaftNode, List<Integer>> newNodeSlotMap = new HashMap<>();
    int raftId = 0;
    for (int i = 0; i < multiRaftFactor; i++) {
      RaftNode raftNode = new RaftNode(newNode, i);
      newNodeSlotMap.putIfAbsent(raftNode, new ArrayList<>());
      previousNodeMap.putIfAbsent(raftNode, new HashMap<>());
    }
    for (Entry<RaftNode, List<Integer>> entry : nodeSlotMap.entrySet()) {
      List<Integer> slots = entry.getValue();
      int transferNum = slots.size() - newAvg;
      if (transferNum > 0) {
        RaftNode curNode = new RaftNode(newNode, raftId);
        int numToMove = transferNum;
        if (raftId != multiRaftFactor - 1) {
          numToMove = Math.min(numToMove, newAvg - newNodeSlotMap.get(curNode).size());
        }
        List<Integer> slotsToMove =
            slots.subList(slots.size() - transferNum, slots.size() - transferNum + numToMove);
        newNodeSlotMap.get(curNode).addAll(slotsToMove);
        for (Integer slot : slotsToMove) {
          // record what node previously hold the integer
          previousNodeMap.get(curNode).put(slot, table.getHeaderGroup(entry.getKey(), oldRing));
          slotNodes[slot] = curNode;
        }
        slotsToMove.clear();
        transferNum -= numToMove;
        if (transferNum > 0) {
          curNode = new RaftNode(newNode, ++raftId);
          slotsToMove = slots.subList(slots.size() - transferNum, slots.size());
          newNodeSlotMap.get(curNode).addAll(slotsToMove);
          for (Integer slot : slotsToMove) {
            // record what node previously hold the integer
            previousNodeMap.get(curNode).put(slot, table.getHeaderGroup(entry.getKey(), oldRing));
            slotNodes[slot] = curNode;
          }
          slotsToMove.clear();
        }
      }
    }
    nodeSlotMap.putAll(newNodeSlotMap);
  }

  @Override
  public Map<RaftNode, List<Integer>> retrieveSlots(Node target) {
    Map<RaftNode, List<Integer>> nodeSlotMap = table.getAllNodeSlots();
    RaftNode[] slotNodes = table.getSlotNodes();
    List<Node> nodeRing = table.getAllNodes();

    Map<RaftNode, List<Integer>> newHolderSlotMap = new HashMap<>();
    for (int raftId = 0; raftId < multiRaftFactor; raftId++) {
      RaftNode raftNode = new RaftNode(target, raftId);
      List<Integer> slots = nodeSlotMap.remove(raftNode);
      for (int i = 0; i < slots.size(); i++) {
        int slot = slots.get(i);
        RaftNode newHolder = new RaftNode(nodeRing.get(i % nodeRing.size()), raftId);
        slotNodes[slot] = newHolder;
        nodeSlotMap.computeIfAbsent(newHolder, n -> new ArrayList<>()).add(slot);
        newHolderSlotMap.computeIfAbsent(newHolder, n -> new ArrayList<>()).add(slot);
      }
    }
    return newHolderSlotMap;
  }
}
