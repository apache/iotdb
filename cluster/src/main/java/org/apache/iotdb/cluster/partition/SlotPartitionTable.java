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
import org.apache.iotdb.db.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SlotPartitionTable manages the slots (data partition) of each node using a look-up table.
 * Slot: 1,2,3...
 */
public class SlotPartitionTable implements PartitionTable {

  private static final Logger logger = LoggerFactory.getLogger(SlotPartitionTable.class);
  private int replicationNum =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();

  //all nodes
  private List<Node> nodeRing = new ArrayList<>();
  //normally, it is equal to ClusterConstant.SLOT_NUM.
  private int totalSlotNumbers;

  //The following fields are used for determining which node a data item belongs to.
  // the slots held by each node
  private Map<Node, List<Integer>> nodeSlotMap = new ConcurrentHashMap<>();
  // each slot is managed by whom
  private Map<Integer, Node> slotNodeMap = new ConcurrentHashMap<>();//TODO a List is enough
  // the nodes that each slot belongs to before a new node is added, used for the new node to
  // find the data source
  private Map<Node, Map<Integer, Node>> previousNodeMap = new ConcurrentHashMap<>();

  //the filed is used for determining which nodes need to be a group.
  // the data groups which this node belongs to.
  private List<PartitionGroup> localGroups;

  private Node thisNode;
  MManager mManager = MManager.getInstance();

  /**
   * only used for deserialize.
   * @param thisNode
   */
  public SlotPartitionTable(Node thisNode) {
    this.thisNode = thisNode;
  }

  public SlotPartitionTable(Collection<Node> nodes, Node thisNode) {
    this(nodes, thisNode, ClusterConstant.SLOT_NUM);
  }

  public SlotPartitionTable(Collection<Node> nodes, Node thisNode, int totalSlotNumbers) {
    this.thisNode = thisNode;
    this.totalSlotNumbers = totalSlotNumbers;
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
    int slotsPerNode = totalSlotNumbers / nodeNum;
      for (Node node : nodeRing) {
        List<Integer> nodeSlots = new ArrayList<>();
        nodeSlotMap.put(node, nodeSlots);
      }

      for (int i = 0; i < totalSlotNumbers; i++) {
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

  private int nextIndex(int currIndex) {
    return currIndex == nodeRing.size() - 1 ? 0 : currIndex + 1;
  }

  @Override
  public PartitionGroup route(String storageGroupName, long timestamp) {
    synchronized (nodeRing) {
      Node node = routeToHeader(storageGroupName, timestamp);
      return getHeaderGroup(node);
    }
  }
  @Override
  public int getPartitionKey(String storageGroupName, long timestamp){
    return PartitionUtils.calculateStorageGroupSlot(storageGroupName, timestamp, getTotalSlotNumbers());
  }

  @Override
  public PartitionGroup route(int slot) {
    Node node = slotNodeMap.get(slot);
    logger.debug("The slot of {} is held by {}", slot, node);
    if (node == null) {
      logger.warn("The slot {} is incorrect", slot);
      return null;
    }
    return getHeaderGroup(node);
  }

  @Override
  public Node routeToHeader(String storageGroupName, long timestamp) {
    synchronized (nodeRing) {
      int slot = PartitionUtils.calculateStorageGroupSlot(storageGroupName, timestamp, getTotalSlotNumbers());
      Node node = slotNodeMap.get(slot);
      logger.debug("The slot of {}@{} is {}, held by {}", storageGroupName, timestamp,
          slot, node);
      return node;
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
    int newAvg = totalSlotNumbers / nodeRing.size();
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
    //thisNode is not need to serialized;

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096);
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

    try {
      dataOutputStream.writeInt(totalSlotNumbers);
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
    //thisNode is not need to deserialized;

    logger.info("Initializing the partition table from buffer");
    totalSlotNumbers = buffer.getInt();
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
  public int getTotalSlotNumbers() {
    return totalSlotNumbers;
  }

  @Override
  public MManager getMManager() {
    return mManager;
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
        Objects.equals(slotNodeMap, that.slotNodeMap) &&
        Objects.equals(previousNodeMap, that.previousNodeMap);
  }

  @Override
  public int hashCode() {
    return 0;
  }
}
