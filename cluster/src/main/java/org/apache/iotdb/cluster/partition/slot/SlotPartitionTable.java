/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.partition.slot;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.NodeAdditionResult;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotStrategy.DefaultStrategy;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SlotPartitionTable manages the slots (data partition) of each node using a look-up table. Slot:
 * 1,2,3...
 */
@SuppressWarnings("DuplicatedCode") // Using SerializeUtils causes unknown thread crush
public class SlotPartitionTable implements PartitionTable {

  private static final Logger logger = LoggerFactory.getLogger(SlotPartitionTable.class);
  private static SlotStrategy slotStrategy = new DefaultStrategy();

  private int replicationNum =
      ClusterDescriptor.getInstance().getConfig().getReplicationNum();

  private int multiRaftFactor =
      ClusterDescriptor.getInstance().getConfig().getMultiRaftFactor();

  //all nodes
  private List<Node> nodeRing = new ArrayList<>();
  //normally, it is equal to ClusterConstant.SLOT_NUM.
  private int totalSlotNumbers;

  //The following fields are used for determining which node a data item belongs to.
  // the slots held by each node
  private Map<RaftNode, List<Integer>> nodeSlotMap = new ConcurrentHashMap<>();
  // each slot is managed by whom
  private RaftNode[] slotNodes = new RaftNode[ClusterConstant.SLOT_NUM];
  // the nodes that each slot belongs to before a new node is added, used for the new node to
  // find the data source
  // find the data source
  private Map<RaftNode, Map<Integer, PartitionGroup>> previousNodeMap = new ConcurrentHashMap<>();

  private NodeRemovalResult nodeRemovalResult = new NodeRemovalResult();

  //the filed is used for determining which nodes need to be a group.
  // the data groups which this node belongs to.
  private List<PartitionGroup> localGroups;

  private Node thisNode;

  private List<PartitionGroup> globalGroups;

  // last log index that modifies the partition table
  private long lastLogIndex = -1;

  /**
   * only used for deserialize.
   *
   * @param thisNode
   */
  public SlotPartitionTable(Node thisNode) {
    this.thisNode = thisNode;
  }

  public SlotPartitionTable(Collection<Node> nodes, Node thisNode) {
    this(nodes, thisNode, ClusterConstant.SLOT_NUM);
  }

  private SlotPartitionTable(Collection<Node> nodes, Node thisNode, int totalSlotNumbers) {
    this.thisNode = thisNode;
    this.totalSlotNumbers = totalSlotNumbers;
    init(nodes);
  }

  public static SlotStrategy getSlotStrategy() {
    return slotStrategy;
  }

  public static void setSlotStrategy(SlotStrategy slotStrategy) {
    SlotPartitionTable.slotStrategy = slotStrategy;
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
    int slotsPerNode = totalSlotNumbers / nodeNum;
    int slotsPerRaftGroup = slotsPerNode / multiRaftFactor;
    for (Node node : nodeRing) {
      for (int i = 0; i < multiRaftFactor; i++) {
        nodeSlotMap.put(new RaftNode(node, i), new ArrayList<>());
      }
    }

    for (int i = 0; i < totalSlotNumbers; i++) {
      int nodeIdx = i / slotsPerNode;
      int raftId = i % slotsPerNode / slotsPerRaftGroup;
      if (nodeIdx >= nodeNum) {
        // the last node may receive a little more if total slots cannot de divided by node number
        nodeIdx--;
      }
      if (raftId >= multiRaftFactor) {
        raftId--;
      }
      nodeSlotMap.get(new RaftNode(nodeRing.get(nodeIdx), raftId)).add(i);
    }

    // build the index to find a node by slot
    for (Entry<RaftNode, List<Integer>> entry : nodeSlotMap.entrySet()) {
      for (Integer slot : entry.getValue()) {
        slotNodes[slot] = entry.getKey();
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
      for (int j = 0; j < multiRaftFactor; j++) {
        ret.add(getHeaderGroup(new RaftNode(nodeRing.get(startIndex), j)));
      }
    }

    logger.debug("The partition groups of {} are: {}", node, ret);
    return ret;
  }

  private PartitionGroup getHeaderGroup(RaftNode raftNode, List<Node> nodeRing) {
    PartitionGroup ret = new PartitionGroup(raftNode.getRaftId());

    // assuming the nodes are [1,2,3,4,5]
    int nodeIndex = nodeRing.indexOf(raftNode.getNode());
    if (nodeIndex == -1) {
      logger.error("Node {} is not in the cluster", raftNode.getNode());
      return null;
    }
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
  public PartitionGroup getHeaderGroup(RaftNode raftNode) {
    return getHeaderGroup(raftNode, this.nodeRing);
  }

  @Override
  public PartitionGroup getHeaderGroup(Node node) {
    return getHeaderGroup(new RaftNode(node, 0));
  }

  @Override
  public PartitionGroup route(String storageGroupName, long timestamp) {
    synchronized (nodeRing) {
      RaftNode raftNode = routeToHeaderByTime(storageGroupName, timestamp);
      return getHeaderGroup(raftNode);
    }
  }

  public PartitionGroup route(int slot) {
    if (slot >= slotNodes.length || slot < 0) {
      logger.warn("Invalid slot to route: {}, stack trace: {}", slot,
          Thread.currentThread().getStackTrace());
      return null;
    }
    RaftNode raftNode = slotNodes[slot];
    logger.debug("The slot of {} is held by {}", slot, raftNode);
    if (raftNode.getNode() == null) {
      logger.warn("The slot {} is incorrect", slot);
      return null;
    }
    return getHeaderGroup(raftNode);
  }

  @Override
  public RaftNode routeToHeaderByTime(String storageGroupName, long timestamp) {
    synchronized (nodeRing) {
      int slot = getSlotStrategy()
          .calculateSlotByTime(storageGroupName, timestamp, getTotalSlotNumbers());
      RaftNode raftNode = slotNodes[slot];
      logger.trace("The slot of {}@{} is {}, held by {}", storageGroupName, timestamp,
          slot, raftNode);
      return raftNode;
    }
  }

  @Override
  public void addNode(Node node) {
    List<Node> oldRing;
    synchronized (nodeRing) {
      if (nodeRing.contains(node)) {
        return;
      }

      oldRing = new ArrayList<>(nodeRing);
      nodeRing.add(node);
      nodeRing.sort(Comparator.comparingInt(Node::getNodeIdentifier));

      List<PartitionGroup> retiredGroups = new ArrayList<>();
      for (int i = 0; i < localGroups.size(); i++) {
        PartitionGroup oldGroup = localGroups.get(i);
        Node header = oldGroup.getHeader();
        PartitionGroup newGrp = getHeaderGroup(new RaftNode(header, oldGroup.getId()));
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
          if (retiredGroup.getHeader().equals(partitionGroup.getHeader())
              && retiredGroup.getId() == partitionGroup.getId()) {
            groupIterator.remove();
            break;
          }
        }
      }
    }

    for (int raftId = 0; raftId < multiRaftFactor; raftId++) {
      PartitionGroup newGroup = getHeaderGroup(new RaftNode(node, raftId));
      if (newGroup.contains(thisNode)) {
        localGroups.add(newGroup);
      }
    }

    calculateGlobalGroups(nodeRing);

    // the slots movement is only done logically, the new node itself will pull data from the
    // old node
    moveSlotsToNew(node, oldRing);

  }

  @Override
  public NodeAdditionResult getNodeAdditionResult(Node node) {
    SlotNodeAdditionResult result = new SlotNodeAdditionResult();
    Map<RaftNode, Set<Integer>> lostSlotsMap = new HashMap<>();
    for (int raftId = 0; raftId < multiRaftFactor; raftId++) {
      RaftNode raftNode = new RaftNode(node, raftId);
      result.addNewGroup(getHeaderGroup(raftNode));
      for (Entry<Integer, PartitionGroup> entry: previousNodeMap.get(raftNode).entrySet()) {
        RaftNode header = new RaftNode(entry.getValue().getHeader(), entry.getValue().getId());
        lostSlotsMap.computeIfAbsent(header, k -> new HashSet<>()).add(entry.getKey());
      }
    }
    result.setLostSlots(lostSlotsMap);
    return result;
  }


  /**
   * Move last slots from each group whose slot number is bigger than the new average to the new
   * node.
   *
   * @param newNode
   */
  private void moveSlotsToNew(Node newNode, List<Node> oldRing) {
    // as a node is added, the average slots for each node decrease
    // move the slots to the new node if any previous node have more slots than the new average
    int newAvg = totalSlotNumbers / nodeRing.size() / multiRaftFactor;
    int raftId = 0;
    for (int i = 0; i < multiRaftFactor; i++) {
      RaftNode raftNode = new RaftNode(newNode, i);
      nodeSlotMap.putIfAbsent(raftNode, new ArrayList<>());
      previousNodeMap.putIfAbsent(raftNode, new HashMap<>());
    }
    for (Entry<RaftNode, List<Integer>> entry : nodeSlotMap.entrySet()) {
      List<Integer> slots = entry.getValue();
      int transferNum = slots.size() - newAvg;
      if (transferNum > 0) {
        RaftNode curNode = new RaftNode(newNode, raftId);
        int numToMove = transferNum;
        if (raftId != multiRaftFactor - 1) {
          numToMove = Math.min(numToMove, newAvg - nodeSlotMap.get(curNode).size());
        }
        List<Integer> slotsToMove = slots
            .subList(slots.size() - transferNum, slots.size() - transferNum + numToMove);
        nodeSlotMap.get(curNode).addAll(slotsToMove);
        for (Integer slot : slotsToMove) {
          // record what node previously hold the integer
          previousNodeMap.get(curNode).put(slot, getHeaderGroup(entry.getKey(), oldRing));
          slotNodes[slot] = curNode;
        }
        transferNum -= numToMove;
        if (transferNum > 0) {
          curNode = new RaftNode(newNode, ++raftId);
          slotsToMove = slots.subList(slots.size() - transferNum, slots.size());
          nodeSlotMap.get(curNode).addAll(slotsToMove);
          for (Integer slot : slotsToMove) {
            // record what node previously hold the integer
            previousNodeMap.get(curNode).put(slot, getHeaderGroup(entry.getKey(), oldRing));
            slotNodes[slot] = curNode;
          }
        }
      }
    }
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
      dataOutputStream.writeLong(lastLogIndex);
      dataOutputStream.writeInt(totalSlotNumbers);
      dataOutputStream.writeInt(nodeSlotMap.size());
      for (Entry<RaftNode, List<Integer>> entry : nodeSlotMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey().getNode(), dataOutputStream);
        dataOutputStream.writeInt(entry.getKey().getRaftId());
        SerializeUtils.serializeIntList(entry.getValue(), dataOutputStream);
      }

      dataOutputStream.writeInt(previousNodeMap.size());
      for (Entry<RaftNode, Map<Integer, PartitionGroup>> nodeMapEntry : previousNodeMap.entrySet()) {
        dataOutputStream.writeInt(nodeMapEntry.getKey().getNode().getNodeIdentifier());
        dataOutputStream.writeInt(nodeMapEntry.getKey().getRaftId());
        Map<Integer, PartitionGroup> prevHolders = nodeMapEntry.getValue();
        dataOutputStream.writeInt(prevHolders.size());
        for (Entry<Integer, PartitionGroup> integerNodeEntry : prevHolders.entrySet()) {
          dataOutputStream.writeInt(integerNodeEntry.getKey());
          integerNodeEntry.getValue().serialize(dataOutputStream);
        }
      }

      nodeRemovalResult.serialize(dataOutputStream);
    } catch (IOException ignored) {
      // not reachable
    }
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  @Override
  public synchronized boolean deserialize(ByteBuffer buffer) {
    long newLastLogIndex = buffer.getLong();

    // judge whether the partition table of byte buffer is out of date
    if (lastLogIndex >= newLastLogIndex) {
      return lastLogIndex <= newLastLogIndex;
    }
    lastLogIndex = newLastLogIndex;
    logger.info("Initializing the partition table from buffer");
    totalSlotNumbers = buffer.getInt();
    int size = buffer.getInt();
    Map<Integer, Node> idNodeMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      Node node = new Node();
      SerializeUtils.deserialize(node, buffer);
      RaftNode raftNode = new RaftNode(node, buffer.getInt());
      List<Integer> slots = new ArrayList<>();
      SerializeUtils.deserializeIntList(slots, buffer);
      nodeSlotMap.put(raftNode, slots);
      idNodeMap.put(node.getNodeIdentifier(), node);
      for (Integer slot : slots) {
        slotNodes[slot] = raftNode;
      }
    }

    int prevNodeMapSize = buffer.getInt();
    previousNodeMap = new HashMap<>();
    for (int i = 0; i < prevNodeMapSize; i++) {
      int nodeId = buffer.getInt();
      RaftNode node = new RaftNode(idNodeMap.get(nodeId), buffer.getInt());

      Map<Integer, PartitionGroup> prevHolders = new HashMap<>();
      int holderNum = buffer.getInt();
      for (int i1 = 0; i1 < holderNum; i1++) {
        PartitionGroup group = new PartitionGroup();
        group.deserialize(buffer, idNodeMap);
        prevHolders.put(buffer.getInt(), group);
      }
      previousNodeMap.put(node, prevHolders);
    }

    nodeRemovalResult = new NodeRemovalResult();
    nodeRemovalResult.deserialize(buffer, idNodeMap);

    for (RaftNode raftNode : nodeSlotMap.keySet()) {
      if (!nodeRing.contains(raftNode.getNode())) {
        nodeRing.add(raftNode.getNode());
      }
    }
    nodeRing.sort(Comparator.comparingInt(Node::getNodeIdentifier));
    logger.info("All known nodes: {}", nodeRing);

    localGroups = getPartitionGroups(thisNode);
    return true;
  }

  @Override
  public List<Node> getAllNodes() {
    return nodeRing;
  }

  public Map<Integer, PartitionGroup> getPreviousNodeMap(RaftNode raftNode) {
    return previousNodeMap.get(raftNode);
  }

  public List<Integer> getNodeSlots(Node header, int raftId) {
    return getNodeSlots(new RaftNode(header, raftId));
  }

  public List<Integer> getNodeSlots(RaftNode header) {
    return nodeSlotMap.get(header);
  }

  public Map<RaftNode, List<Integer>> getAllNodeSlots() {
    return nodeSlotMap;
  }

  public int getTotalSlotNumbers() {
    return totalSlotNumbers;
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
    return totalSlotNumbers == that.totalSlotNumbers &&
        Objects.equals(nodeRing, that.nodeRing) &&
        Objects.equals(nodeSlotMap, that.nodeSlotMap) &&
        Arrays.equals(slotNodes, that.slotNodes) &&
        Objects.equals(previousNodeMap, that.previousNodeMap);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public void removeNode(Node target) {
    synchronized (nodeRing) {
      if (!nodeRing.contains(target)) {
        return;
      }

      SlotNodeRemovalResult result = new SlotNodeRemovalResult();
      for(int raftId = 0; raftId < multiRaftFactor; raftId++) {
        result.addRemovedGroup(getHeaderGroup(new RaftNode(target, raftId)));
      }
      nodeRing.remove(target);

      // if the node belongs to a group that headed by target, this group should be removed
      // and other groups containing target should be updated
      List<Integer> removedGroupIdxs = new ArrayList<>();
      for (int i = 0; i < localGroups.size(); i++) {
        PartitionGroup oldGroup = localGroups.get(i);
        Node header = oldGroup.getHeader();
        if (header.equals(target)) {
          removedGroupIdxs.add(i);
        } else {
          PartitionGroup newGrp = getHeaderGroup(new RaftNode(header, oldGroup.getId()));
          localGroups.set(i, newGrp);
        }
      }
      for(int i = removedGroupIdxs.size() - 1; i >= 0 ; i--) {
        int removedGroupIdx = removedGroupIdxs.get(i);
        int raftId = localGroups.get(removedGroupIdx).getId();
        localGroups.remove(removedGroupIdx);
        // each node exactly joins replicationNum groups, so when a group is removed, the node
        // should join a new one
        int thisNodeIdx = nodeRing.indexOf(thisNode);

        // check if this node is to be removed
        if (thisNodeIdx == -1) {
          continue;
        }

        // this node must be the last node of the new group
        int headerNodeIdx = thisNodeIdx - (replicationNum - 1);
        headerNodeIdx = headerNodeIdx < 0 ? headerNodeIdx + nodeRing.size() : headerNodeIdx;
        Node header = nodeRing.get(headerNodeIdx);
        PartitionGroup newGrp = getHeaderGroup(new RaftNode(header, raftId));
        localGroups.add(newGrp);
        result.addNewGroup(newGrp);
      }

      calculateGlobalGroups(nodeRing);

      // the slots movement is only done logically, the new node itself will pull data from the
      // old node
      Map<RaftNode, List<Integer>> raftNodeListMap = retrieveSlots(target);
      result.addNewSlotOwners(raftNodeListMap);
      this.nodeRemovalResult = result;
    }
  }

  @Override
  public NodeRemovalResult getNodeRemovalResult() {
    return nodeRemovalResult;
  }

  private Map<RaftNode, List<Integer>> retrieveSlots(Node target) {
    Map<RaftNode, List<Integer>> newHolderSlotMap = new HashMap<>();
    for(int raftId = 0 ; raftId < multiRaftFactor; raftId++) {
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

  @Override
  public List<PartitionGroup> getGlobalGroups() {
    // preventing a thread from getting incomplete globalGroups
    synchronized (nodeRing) {
      if (globalGroups == null) {
        globalGroups = calculateGlobalGroups(nodeRing);
      }
      return globalGroups;
    }
  }

  @Override
  public boolean judgeHoldSlot(Node node, int slot) {
    return getHeaderGroup(slotNodes[slot]).contains(node);
  }

  @Override
  public List<PartitionGroup> calculateGlobalGroups(List<Node> nodeRing) {
    List<PartitionGroup> result = new ArrayList<>();
    for (Node node : nodeRing) {
      for (int i = 0; i < multiRaftFactor; i++) {
        result.add(getHeaderGroup(new RaftNode(node, i)));
      }
    }
    return result;
  }

  public synchronized long getLastLogIndex() {
    return lastLogIndex;
  }

  public synchronized void setLastLogIndex(long lastLogIndex) {
    this.lastLogIndex = Math.max(this.lastLogIndex, lastLogIndex);
  }
}
