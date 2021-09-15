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

package org.apache.iotdb.cluster.partition.slot;

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.NodeAdditionResult;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.balancer.DefaultSlotBalancer;
import org.apache.iotdb.cluster.partition.balancer.SlotBalancer;
import org.apache.iotdb.cluster.partition.slot.SlotStrategy.DefaultStrategy;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.utils.NodeSerializeUtils;
import org.apache.iotdb.db.utils.SerializeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

/**
 * SlotPartitionTable manages the slots (data partition) of each node using a look-up table. Slot:
 * 1,2,3...
 */
@SuppressWarnings("DuplicatedCode") // Using SerializeUtils causes unknown thread crush
public class SlotPartitionTable implements PartitionTable {

  private static final Logger logger = LoggerFactory.getLogger(SlotPartitionTable.class);
  private static SlotStrategy slotStrategy = new DefaultStrategy();

  private int replicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();

  private int multiRaftFactor = ClusterDescriptor.getInstance().getConfig().getMultiRaftFactor();

  // all nodes
  private List<Node> nodeRing = new ArrayList<>();
  // normally, it is equal to ClusterConstant.SLOT_NUM.
  private int totalSlotNumbers;

  // The following fields are used for determining which node a data item belongs to.
  // the slots held by each node
  private Map<RaftNode, List<Integer>> nodeSlotMap = new ConcurrentHashMap<>();
  // each slot is managed by whom
  private RaftNode[] slotNodes = new RaftNode[ClusterConstant.SLOT_NUM];
  // the nodes that each slot belongs to before a new node is added, used for the new node to
  // find the data source
  private Map<RaftNode, Map<Integer, PartitionGroup>> previousNodeMap = new ConcurrentHashMap<>();

  private NodeRemovalResult nodeRemovalResult = new SlotNodeRemovalResult();

  // the filed is used for determining which nodes need to be a group.
  // the data groups which this node belongs to.
  private List<PartitionGroup> localGroups;

  private Node thisNode;

  private List<PartitionGroup> globalGroups;

  // the last meta log index that modifies the partition table
  private volatile long lastMetaLogIndex = -1;

  private SlotBalancer slotBalancer = new DefaultSlotBalancer(this);

  /**
   * only used for deserialize.
   *
   * @param thisNode
   */
  public SlotPartitionTable(Node thisNode) {
    this.thisNode = thisNode;
  }

  public SlotPartitionTable(SlotPartitionTable other) {
    this.thisNode = other.thisNode;
    this.totalSlotNumbers = other.totalSlotNumbers;
    this.lastMetaLogIndex = other.lastMetaLogIndex;
    this.nodeRing = new ArrayList<>(other.nodeRing);
    this.nodeSlotMap = new HashMap<>(other.nodeSlotMap);
    this.slotNodes = new RaftNode[totalSlotNumbers];
    System.arraycopy(other.slotNodes, 0, this.slotNodes, 0, totalSlotNumbers);
    this.previousNodeMap = new HashMap<>(previousNodeMap);

    localGroups = getPartitionGroups(thisNode);
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

  public SlotBalancer getLoadBalancer() {
    return slotBalancer;
  }

  public void setLoadBalancer(SlotBalancer slotBalancer) {
    this.slotBalancer = slotBalancer;
  }

  private void init(Collection<Node> nodes) {
    logger.info("Initializing a new partition table");
    nodeRing.addAll(nodes);
    Collections.sort(nodeRing);
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
    if (nodeIndex == -1) {
      logger.info("PartitionGroups is empty due to this node has been removed from the cluster!");
      return ret;
    }
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

  public PartitionGroup getHeaderGroup(RaftNode raftNode, List<Node> nodeRing) {
    PartitionGroup ret = new PartitionGroup(raftNode.getRaftId());

    // assuming the nodes are [1,2,3,4,5]
    int nodeIndex = nodeRing.indexOf(raftNode.getNode());
    if (nodeIndex == -1) {
      logger.warn("Node {} is not in the cluster", raftNode.getNode());
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
  public PartitionGroup route(String storageGroupName, long timestamp) {
    synchronized (nodeRing) {
      RaftNode raftNode = routeToHeaderByTime(storageGroupName, timestamp);
      return getHeaderGroup(raftNode);
    }
  }

  public PartitionGroup route(int slot) {
    if (slot >= slotNodes.length || slot < 0) {
      logger.warn(
          "Invalid slot to route: {}, stack trace: {}",
          slot,
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
      int slot =
          getSlotStrategy().calculateSlotByTime(storageGroupName, timestamp, getTotalSlotNumbers());
      RaftNode raftNode = slotNodes[slot];
      logger.trace(
          "The slot of {}@{} is {}, held by {}", storageGroupName, timestamp, slot, raftNode);
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
        RaftNode header = oldGroup.getHeader();
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

    globalGroups = calculateGlobalGroups(nodeRing);

    // the slots movement is only done logically, the new node itself will pull data from the
    // old node
    slotBalancer.moveSlotsToNew(node, oldRing);
    this.nodeRemovalResult = new SlotNodeRemovalResult();
  }

  @Override
  public NodeAdditionResult getNodeAdditionResult(Node node) {
    SlotNodeAdditionResult result = new SlotNodeAdditionResult();
    Map<RaftNode, Set<Integer>> lostSlotsMap = new HashMap<>();
    for (int raftId = 0; raftId < multiRaftFactor; raftId++) {
      RaftNode raftNode = new RaftNode(node, raftId);
      result.addNewGroup(getHeaderGroup(raftNode));
      for (Entry<Integer, PartitionGroup> entry : previousNodeMap.get(raftNode).entrySet()) {
        RaftNode header = entry.getValue().getHeader();
        lostSlotsMap.computeIfAbsent(header, k -> new HashSet<>()).add(entry.getKey());
      }
    }
    result.setLostSlots(lostSlotsMap);
    return result;
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
      dataOutputStream.writeLong(lastMetaLogIndex);
      dataOutputStream.writeInt(totalSlotNumbers);
      dataOutputStream.writeInt(nodeSlotMap.size());
      for (Entry<RaftNode, List<Integer>> entry : nodeSlotMap.entrySet()) {
        NodeSerializeUtils.serialize(entry.getKey().getNode(), dataOutputStream);
        dataOutputStream.writeInt(entry.getKey().getRaftId());
        SerializeUtils.serializeIntList(entry.getValue(), dataOutputStream);
      }

      dataOutputStream.writeInt(previousNodeMap.size());
      for (Entry<RaftNode, Map<Integer, PartitionGroup>> nodeMapEntry :
          previousNodeMap.entrySet()) {
        NodeSerializeUtils.serialize(nodeMapEntry.getKey().getNode(), dataOutputStream);
        dataOutputStream.writeInt(nodeMapEntry.getKey().getRaftId());
        Map<Integer, PartitionGroup> prevHolders = nodeMapEntry.getValue();
        dataOutputStream.writeInt(prevHolders.size());
        for (Entry<Integer, PartitionGroup> integerNodeEntry : prevHolders.entrySet()) {
          integerNodeEntry.getValue().serialize(dataOutputStream);
          dataOutputStream.writeInt(integerNodeEntry.getKey());
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

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Partition table: lastMetaLogIndex {}, newLastLogIndex {}",
          lastMetaLogIndex,
          newLastLogIndex);
    }
    // judge whether the partition table of byte buffer is out of date
    if (lastMetaLogIndex != -1 && lastMetaLogIndex >= newLastLogIndex) {
      return lastMetaLogIndex == newLastLogIndex;
    }
    lastMetaLogIndex = newLastLogIndex;
    logger.info("Initializing the partition table from buffer");
    totalSlotNumbers = buffer.getInt();
    int size = buffer.getInt();
    nodeSlotMap = new HashMap<>();
    Node node;
    for (int i = 0; i < size; i++) {
      node = new Node();
      NodeSerializeUtils.deserialize(node, buffer);
      RaftNode raftNode = new RaftNode(node, buffer.getInt());
      List<Integer> slots = new ArrayList<>();
      SerializeUtils.deserializeIntList(slots, buffer);
      nodeSlotMap.put(raftNode, slots);
      for (Integer slot : slots) {
        slotNodes[slot] = raftNode;
      }
    }

    int prevNodeMapSize = buffer.getInt();
    previousNodeMap = new HashMap<>();
    for (int i = 0; i < prevNodeMapSize; i++) {
      node = new Node();
      NodeSerializeUtils.deserialize(node, buffer);
      RaftNode raftNode = new RaftNode(node, buffer.getInt());

      Map<Integer, PartitionGroup> prevHolders = new HashMap<>();
      int holderNum = buffer.getInt();
      for (int i1 = 0; i1 < holderNum; i1++) {
        PartitionGroup group = new PartitionGroup();
        group.deserialize(buffer);
        prevHolders.put(buffer.getInt(), group);
      }
      previousNodeMap.put(raftNode, prevHolders);
    }

    nodeRemovalResult = new SlotNodeRemovalResult();
    nodeRemovalResult.deserialize(buffer);

    nodeRing.clear();
    for (RaftNode raftNode : nodeSlotMap.keySet()) {
      if (!nodeRing.contains(raftNode.getNode())) {
        nodeRing.add(raftNode.getNode());
      }
    }
    Collections.sort(nodeRing);
    logger.info("All known nodes: {}", nodeRing);

    localGroups = getPartitionGroups(thisNode);
    return true;
  }

  @Override
  public List<Node> getAllNodes() {
    return nodeRing;
  }

  public Map<RaftNode, Map<Integer, PartitionGroup>> getPreviousNodeMap() {
    return previousNodeMap;
  }

  public Map<Integer, PartitionGroup> getPreviousNodeMap(RaftNode raftNode) {
    return previousNodeMap.get(raftNode);
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
    return totalSlotNumbers == that.totalSlotNumbers
        && Objects.equals(nodeRing, that.nodeRing)
        && Objects.equals(nodeSlotMap, that.nodeSlotMap)
        && Arrays.equals(slotNodes, that.slotNodes)
        && Objects.equals(previousNodeMap, that.previousNodeMap)
        && lastMetaLogIndex == that.lastMetaLogIndex;
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
      for (int raftId = 0; raftId < multiRaftFactor; raftId++) {
        result.addRemovedGroup(getHeaderGroup(new RaftNode(target, raftId)));
      }
      nodeRing.remove(target);

      // if the node belongs to a group that headed by target, this group should be removed
      // and other groups containing target should be updated
      List<Integer> removedGroupIdxs = new ArrayList<>();
      for (int i = 0; i < localGroups.size(); i++) {
        PartitionGroup oldGroup = localGroups.get(i);
        RaftNode header = oldGroup.getHeader();
        if (header.getNode().equals(target)) {
          removedGroupIdxs.add(i);
        } else {
          PartitionGroup newGrp = getHeaderGroup(header);
          localGroups.set(i, newGrp);
        }
      }
      for (int i = removedGroupIdxs.size() - 1; i >= 0; i--) {
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
      }

      globalGroups = calculateGlobalGroups(nodeRing);

      // the slots movement is only done logically, the new node itself will pull data from the
      // old node
      Map<RaftNode, List<Integer>> raftNodeListMap = slotBalancer.retrieveSlots(target);
      result.addNewSlotOwners(raftNodeListMap);
      this.nodeRemovalResult = result;
    }
  }

  @Override
  public NodeRemovalResult getNodeRemovalResult() {
    return nodeRemovalResult;
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

  /**
   * Judge whether the data of slot is held by node
   *
   * @param node target node
   */
  public boolean judgeHoldSlot(Node node, int slot) {
    return getHeaderGroup(slotNodes[slot]).contains(node);
  }

  @Override
  public List<PartitionGroup> calculateGlobalGroups(List<Node> nodeRing) {
    List<PartitionGroup> result = new ArrayList<>();
    for (Node node : nodeRing) {
      for (int i = 0; i < multiRaftFactor; i++) {
        result.add(getHeaderGroup(new RaftNode(node, i), nodeRing));
      }
    }
    return result;
  }

  @Override
  public long getLastMetaLogIndex() {
    return lastMetaLogIndex;
  }

  @Override
  public void setLastMetaLogIndex(long lastMetaLogIndex) {
    if (logger.isDebugEnabled()) {
      logger.debug("Set last meta log index of partition table to {}", lastMetaLogIndex);
    }
    this.lastMetaLogIndex = Math.max(this.lastMetaLogIndex, lastMetaLogIndex);
  }

  public RaftNode[] getSlotNodes() {
    return slotNodes;
  }
}
