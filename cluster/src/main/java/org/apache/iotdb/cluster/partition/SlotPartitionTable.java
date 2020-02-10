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


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SlotPartitionTable manages the slots (data partition) of each node using a look-up table.
 */
public class SlotPartitionTable implements PartitionTable {

  private static final Logger logger = LoggerFactory.getLogger(SlotPartitionTable.class);
  private int replicationNum =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();
  private List<Node> nodeRing = new ArrayList<>();
  // the slots held by each node
  private Map<Node, List<Integer>> nodeSlotMap = new ConcurrentHashMap<>();
  // each slot is managed by whom
  private Map<Integer, Node> slotNodeMap = new ConcurrentHashMap<>();
  // the nodes that each slot belongs to before a new node is added, used for the new node to
  // find the data source
  private Map<Node, Map<Integer, Node>> previousNodeMap = new ConcurrentHashMap<>();

  // the data groups which this node belongs to
  private List<PartitionGroup> localGroups;
  private Node thisNode;
  private int slotNum;

  public SlotPartitionTable(Node thisNode) {
    this.thisNode = thisNode;
  }

  public SlotPartitionTable(Collection<Node> nodes, Node thisNode) {
    this(nodes, thisNode, ClusterConstant.SLOT_NUM);
  }

  public SlotPartitionTable(Collection<Node> nodes, Node thisNode, int slotNum) {
    this.thisNode = thisNode;
    this.slotNum = slotNum;
    init(nodes);
  }

  private void init(Collection<Node> nodes) {
    logger.info("Initializing a new partition table");
    nodeRing.addAll(nodes);
    nodeRing.sort(Comparator.comparingInt(Node::getNodeIdentifier));
    localGroups = getPartitionGroups(thisNode);
    assignPartitions();
  }

  private void assignPartitions() {
    // evenly assign the slots to each node
    int nodeNum = nodeRing.size();
    int slotsPerNode = slotNum / nodeNum;
    for (Node node : nodeRing) {
      nodeSlotMap.put(node, new ArrayList<>());
    }

    for (int i = 0; i < slotNum; i++) {
      int nodeIdx = i / slotsPerNode;
      if (nodeIdx >= nodeNum) {
        // the last node may receive a little more if total slots cannot de divided by node number
        nodeIdx--;
      }
      nodeSlotMap.get(nodeRing.get(nodeIdx)).add(i);
    }

    // build the index to find a node by slot
    for (Entry<Node, List<Integer>> entry : nodeSlotMap.entrySet()) {
      for (Integer slot : entry.getValue()) {
        slotNodeMap.put(slot, entry.getKey());
      }
    }
  }


  // find replicationNum groups that a node is in
  private List<PartitionGroup> getPartitionGroups(Node node) {
    List<PartitionGroup> ret = new ArrayList<>();

    int nodeIndex = nodeRing.indexOf(node);
    for (int i = 0; i < replicationNum; i++) {
      // the previous replicationNum nodes (including the node itself) are the headers of the
      // groups the node is in
      int startIndex = nodeIndex - i;
      if (startIndex < 0) {
        startIndex = startIndex + nodeRing.size();
      }
      ret.add(getHeaderGroup(nodeRing.get(startIndex)));
    }

    logger.debug("The partition groups of {} are: {}", node, ret);
    return ret;
  }

  @Override
  public PartitionGroup getHeaderGroup(Node node) {
    PartitionGroup ret = new PartitionGroup();

    // assuming the nodes are [1,2,3,4,5]
    int nodeIndex = nodeRing.indexOf(node);
    int endIndex = nodeIndex + replicationNum;
    if (endIndex > nodeRing.size()) {
      // for startIndex = 4, we concat [4, 5] and [1] to generate the group
      ret.addAll(nodeRing.subList(nodeIndex, nodeRing.size()));
      ret.addAll(nodeRing.subList(0, endIndex - nodeRing.size()));
    } else {
      // for startIndex = 2, [2,3,4] is the group
      ret.addAll(nodeRing.subList(nodeIndex, endIndex));
    }
    return ret;
  }

  @Override
  public PartitionGroup route(String storageGroupName, long timestamp) {
    synchronized (nodeRing) {
      int slot = PartitionUtils.calculateStorageGroupSlot(storageGroupName, timestamp, getSlotNum());
      Node node = slotNodeMap.get(slot);
      logger.debug("The slot of {}@{} is {}, held by {}", storageGroupName, timestamp,
          slot, node);
      return getHeaderGroup(node);
    }
  }


  @Override
  public PartitionGroup addNode(Node node) {
    synchronized (nodeRing) {
      if (nodeRing.contains(node)) {
        return null;
      }

      nodeRing.add(node);
      nodeRing.sort(Comparator.comparingInt(Node::getNodeIdentifier));

      List<PartitionGroup> retiredGroups = new ArrayList<>();
      for(int i = 0; i < localGroups.size(); i++) {
        PartitionGroup oldGroup = localGroups.get(i);
        Node header = oldGroup.getHeader();
        PartitionGroup newGrp = getHeaderGroup(header);
        if (newGrp.contains(node) && newGrp.contains(thisNode)) {
          // this group changes but still contains the local node
          localGroups.set(i, newGrp);
        } else if (newGrp.contains(node) && !newGrp.contains(thisNode)) {
          // the local node retires from the group
          retiredGroups.add(newGrp);
        }
      }

      // remove retired groups
      Iterator<PartitionGroup> groupIterator = localGroups.iterator();
      while (groupIterator.hasNext()) {
        PartitionGroup partitionGroup = groupIterator.next();
        for (PartitionGroup retiredGroup : retiredGroups) {
          if (retiredGroup.getHeader().equals(partitionGroup.getHeader())) {
            groupIterator.remove();
            break;
          }
        }
      }

      PartitionGroup newGroup = getHeaderGroup(node);
      if (newGroup.contains(thisNode)) {
        localGroups.add(newGroup);
      }

      // the slots movement is only done logically, the new node itself will pull data from the
      // old node
      moveSlotsToNew(node);
      return newGroup;
    }
  }

  private void moveSlotsToNew(Node node) {
    // as a node is added, the average slots for each node decrease
    // move the slots to the new node if any previous node have more slots than the new average
    List<Integer> newSlots = new ArrayList<>();
    Map<Integer, Node> previousHolders = new HashMap<>();
    int newAvg = slotNum / nodeRing.size();
    for (Entry<Node, List<Integer>> entry : nodeSlotMap.entrySet()) {
      List<Integer> slots = entry.getValue();
      int transferNum = slots.size() - newAvg;
      if (transferNum > 0) {
        List<Integer> slotsToMove = slots.subList(slots.size() - transferNum, slots.size());
        newSlots.addAll(slotsToMove);
        for (Integer integer : slotsToMove) {
          // record what node previously hold the integer
          previousHolders.put(integer, entry.getKey());
        }
        slotsToMove.clear();
      }
    }
    nodeSlotMap.put(node, newSlots);
    previousNodeMap.put(node, previousHolders);
  }

  @Override
  public List<PartitionGroup> getLocalGroups() {
    return localGroups;
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096);
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

    try {
      dataOutputStream.writeInt(slotNum);
      dataOutputStream.writeInt(nodeSlotMap.size());
      for (Entry<Node, List<Integer>> entry : nodeSlotMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        SerializeUtils.serialize(entry.getValue(), dataOutputStream);
      }

      dataOutputStream.writeInt(previousNodeMap.size());
      for (Entry<Node, Map<Integer, Node>> nodeMapEntry : previousNodeMap.entrySet()) {
        dataOutputStream.writeInt(nodeMapEntry.getKey().getNodeIdentifier());

        Map<Integer, Node> prevHolders = nodeMapEntry.getValue();
        dataOutputStream.writeInt(prevHolders.size());
        for (Entry<Integer, Node> integerNodeEntry : prevHolders.entrySet()) {
          dataOutputStream.writeInt(integerNodeEntry.getKey());
          dataOutputStream.writeInt(integerNodeEntry.getValue().getNodeIdentifier());
        }
      }
    } catch (IOException ignored) {
      // not reachable
    }
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    logger.info("Initializing the partition table from buffer");
    slotNum = buffer.getInt();
    int size = buffer.getInt();
    Map<Integer, Node> idNodeMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      Node node = new Node();
      List<Integer> slots = new ArrayList<>();
      SerializeUtils.deserialize(node, buffer);
      SerializeUtils.deserialize(slots, buffer);
      nodeSlotMap.put(node, slots);
      idNodeMap.put(node.getNodeIdentifier(), node);
      for (Integer slot : slots) {
        slotNodeMap.put(slot, node);
      }
    }

    int prevNodeMapSize = buffer.getInt();
    previousNodeMap = new HashMap<>();
    for (int i = 0; i < prevNodeMapSize; i++) {
      int nodeId = buffer.getInt();
      Node node = idNodeMap.get(nodeId);

      Map<Integer, Node> prevHolders = new HashMap<>();
      int holderNum = buffer.getInt();
      for (int i1 = 0; i1 < holderNum; i1++) {
        int slot = buffer.getInt();
        Node holder = idNodeMap.get(buffer.getInt());
        prevHolders.put(slot, holder);
      }
      previousNodeMap.put(node, prevHolders);
    }

    nodeRing.addAll(nodeSlotMap.keySet());
    nodeRing.sort(Comparator.comparingInt(Node::getNodeIdentifier));

    localGroups = getPartitionGroups(thisNode);
  }

  @Override
  public List<Node> getAllNodes() {
    return nodeRing;
  }

  @Override
  public Map<Integer, Node> getPreviousNodeMap(Node node) {
    return previousNodeMap.get(node);
  }

  @Override
  public List<Integer> getNodeSlots(Node header) {
    return nodeSlotMap.get(header);
  }

  @Override
  public Map<Node, List<Integer>> getAllNodeSlots() {
    return nodeSlotMap;
  }

  @Override
  public int getSlotNum() {
    return slotNum;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SlotPartitionTable that = (SlotPartitionTable) o;
    return slotNum == that.slotNum &&
        Objects.equals(nodeRing, that.nodeRing) &&
        Objects.equals(nodeSlotMap, that.nodeSlotMap) &&
        Objects.equals(slotNodeMap, that.slotNodeMap) &&
        Objects.equals(previousNodeMap, that.previousNodeMap);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public NodeRemovalResult removeNode(Node target) {
    synchronized (nodeRing) {
      if (!nodeRing.contains(target)) {
        return null;
      }

      NodeRemovalResult result = new NodeRemovalResult();
      result.setRemovedGroup(getHeaderGroup(target));
      nodeRing.remove(target);

      // if the node belongs to a group that headed by target, this group should be removed
      // and other groups containing target should be updated
      int removedGroupIdx = -1;
      for(int i = 0; i < localGroups.size(); i++) {
        PartitionGroup oldGroup = localGroups.get(i);
        Node header = oldGroup.getHeader();
        if (header.equals(target)) {
          removedGroupIdx = i;
        } else {
          PartitionGroup newGrp = getHeaderGroup(header);
          localGroups.set(i, newGrp);
        }
      }
      if (removedGroupIdx != -1) {
        localGroups.remove(removedGroupIdx);
        // each node exactly joins replicationNum groups, so when a group is removed, the node
        // should join a new one
        int thisNodeIdx = nodeRing.indexOf(thisNode);
        // this node must be the last node of the new group
        int headerNodeIdx = thisNodeIdx - (replicationNum - 1);
        headerNodeIdx = headerNodeIdx < 0 ? headerNodeIdx + nodeRing.size() : headerNodeIdx;
        Node header = nodeRing.get(headerNodeIdx);
        PartitionGroup newGrp = getHeaderGroup(header);
        localGroups.add(newGrp);
        result.setNewGroup(newGrp);
      }

      // the slots movement is only done logically, the new node itself will pull data from the
      // old node
      Map<Node, List<Integer>> nodeListMap = retrieveSlots(target);
      result.setNewSlotOwners(nodeListMap);
      return result;
    }
  }

  private Map<Node, List<Integer>> retrieveSlots(Node target) {
    Map<Node, List<Integer>> newHolderSlotMap = new HashMap<>();
    List<Integer> slots = nodeSlotMap.remove(target);
    for (int i = 0; i < slots.size(); i++) {
      int slot = slots.get(i);
      Node newHolder = nodeRing.get(i % nodeRing.size());
      slotNodeMap.put(slot, newHolder);
      nodeSlotMap.get(newHolder).add(slot);
      newHolderSlotMap.computeIfAbsent(newHolder, n -> new ArrayList<>()).add(slot);
    }
    return newHolderSlotMap;
  }
}
