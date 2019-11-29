/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
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
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SocketPartitionTable manages the sockets (data partition) of each node using a look-up table.
 */
public class SocketPartitionTable implements PartitionTable {

  private static final Logger logger = LoggerFactory.getLogger(SocketPartitionTable.class);

  private static final int REPLICATION_NUM =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();
  private static final long PARTITION_INTERVAL =
      ClusterDescriptor.getINSTANCE().getConfig().getPartitionInterval();
  private static final int SOCKET_NUM = 10000;

  private List<Node> nodeRing = new ArrayList<>();
  // the sockets held by each node
  private Map<Node, List<Integer>> nodeSocketMap = new ConcurrentHashMap<>();
  // each socket is managed by whom
  private Map<Integer, Node> socketNodeMap = new ConcurrentHashMap<>();
  // the nodes that each socket belongs to before a new node is added, used for the new node to
  // find the data source
  private Map<Node, Map<Integer, Node>> previousNodeMap = new ConcurrentHashMap<>();

  // the data groups which this node belongs to
  private List<PartitionGroup> localGroups;
  private Node thisNode;

  public SocketPartitionTable(Node thisNode) {
    this.thisNode = thisNode;
  }

  public SocketPartitionTable(Collection<Node> nodes, Node thisNode) {
    this.thisNode = thisNode;
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
    // evenly assign the sockets to each node
    int nodeNum = nodeRing.size();
    int socketsPerNode = SOCKET_NUM / nodeNum;
    for (Node node : nodeRing) {
      List<Integer> nodeSockets = new ArrayList<>();
      nodeSocketMap.put(node, nodeSockets);
    }

    for (int i = 0; i < SOCKET_NUM; i++) {
      int nodeIdx = i / socketsPerNode;
      if (nodeIdx >= nodeNum) {
        // the last node may receive a little more if total sockets cannot de divided by node number
        nodeIdx--;
      }
      nodeSocketMap.get(nodeRing.get(nodeIdx)).add(i);
    }

    // build the index to find a node by socket
    for (Entry<Node, List<Integer>> entry : nodeSocketMap.entrySet()) {
      for (Integer socket : entry.getValue()) {
        socketNodeMap.put(socket, entry.getKey());
      }
    }
  }


  // find REPLICATION_NUM groups that a node is in
  private List<PartitionGroup> getPartitionGroups(Node node) {
    List<PartitionGroup> ret = new ArrayList<>();

    int nodeIndex = nodeRing.indexOf(node);
    for (int i = 0; i < REPLICATION_NUM; i++) {
      // the previous REPLICATION_NUM nodes (including the node itself) are the headers of the
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
    ret.add(node);

    // assuming the nodes are [1,2,3,4,5]
    int nodeIndex = nodeRing.indexOf(node);
    int endIndex = nodeIndex + REPLICATION_NUM;
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
  public List<Node> route(String storageGroupName, long timestamp) {
    synchronized (nodeRing) {
      long partitionInstance = timestamp / PARTITION_INTERVAL;
      int hash = Objects.hash(storageGroupName, partitionInstance);
      int socketNum = Math.abs(hash % SOCKET_NUM);
      Node node = socketNodeMap.get(socketNum);

      return getHeaderGroup(node);
    }
  }


  @Override
  public PartitionGroup addNode(Node node) {
    synchronized (nodeRing) {
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

      // the sockets movement is only done logically, the new node itself will pull data from the
      // old node
      moveSocketsToNew(node);
      return newGroup;
    }
  }

  private void moveSocketsToNew(Node node) {
    // as a node is added, the average sockets for each node decrease
    // move the sockets to the new node if any previous node have more sockets than the new average
    List<Integer> newSockets = new ArrayList<>();
    Map<Integer, Node> previousHolders = new HashMap<>();
    int newAvg = SOCKET_NUM / nodeRing.size();
    for (Entry<Node, List<Integer>> entry : nodeSocketMap.entrySet()) {
      List<Integer> sockets = entry.getValue();
      int transferNum = sockets.size() - newAvg;
      if (transferNum > 0) {
        List<Integer> socketsToMove = sockets.subList(sockets.size() - transferNum, sockets.size());
        newSockets.addAll(socketsToMove);
        for (Integer integer : socketsToMove) {
          // record what node previously hold the integer
          previousHolders.put(integer, entry.getKey());
        }
        socketsToMove.clear();
      }
    }
    nodeSocketMap.put(node, newSockets);
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
      dataOutputStream.writeInt(nodeSocketMap.size());
      for (Entry<Node, List<Integer>> entry : nodeSocketMap.entrySet()) {
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
    int size = buffer.getInt();
    Map<Integer, Node> idNodeMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      Node node = new Node();
      List<Integer> sockets = new ArrayList<>();
      SerializeUtils.deserialize(node, buffer);
      SerializeUtils.deserialize(sockets, buffer);
      nodeSocketMap.put(node, sockets);
      idNodeMap.put(node.getNodeIdentifier(), node);
      for (Integer socket : sockets) {
        socketNodeMap.put(socket, node);
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
        int socket = buffer.getInt();
        Node holder = idNodeMap.get(buffer.getInt());
        prevHolders.put(socket, holder);
      }
      previousNodeMap.put(node, prevHolders);
    }

    nodeRing.addAll(nodeSocketMap.keySet());
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
  public List<Integer> getNodeSockets(Node header) {
    return nodeSocketMap.get(header);
  }
}
