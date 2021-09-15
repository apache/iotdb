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
package org.apache.iotdb.cluster.utils.nodetool;

import org.apache.iotdb.cluster.ClusterMain;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.utils.nodetool.function.NodeToolCmd;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.cluster.utils.nodetool.function.NodeToolCmd.BUILDING_CLUSTER_INFO;
import static org.apache.iotdb.cluster.utils.nodetool.function.NodeToolCmd.META_LEADER_UNKNOWN_INFO;

public class ClusterMonitor implements ClusterMonitorMBean, IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMonitor.class);

  public static final ClusterMonitor INSTANCE = new ClusterMonitor();

  private final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private ClusterMonitor() {}

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(INSTANCE, mbeanName);
    } catch (Exception e) {
      String errorMessage =
          String.format("Failed to start %s because of %s", this.getID().getName(), e.getMessage());
      throw new StartupException(errorMessage);
    }
  }

  @Override
  public List<Pair<Node, NodeCharacter>> getMetaGroup() {
    MetaGroupMember metaMember = getMetaGroupMember();
    if (metaMember == null || metaMember.getPartitionTable() == null) {
      return null;
    }
    List<Pair<Node, NodeCharacter>> res = new ArrayList<>();
    Node leader = metaMember.getLeader();
    List<Node> nodes = metaMember.getPartitionTable().getAllNodes();
    for (Node node : nodes) {
      if (node.equals(leader)) {
        res.add(new Pair<>(node, NodeCharacter.LEADER));
      } else {
        res.add(new Pair<>(node, NodeCharacter.FOLLOWER));
      }
    }
    return res;
  }

  public List<Node> getRing() {
    MetaGroupMember metaMember = getMetaGroupMember();
    if (metaMember == null || metaMember.getPartitionTable() == null) {
      return null;
    }
    return metaMember.getPartitionTable().getAllNodes();
  }

  @Override
  public List<Pair<Node, NodeCharacter>> getDataGroup(int raftId) throws Exception {
    MetaGroupMember metaMember = getMetaGroupMember();
    if (metaMember == null || metaMember.getPartitionTable() == null) {
      return null;
    }
    RaftNode raftNode = new RaftNode(metaMember.getThisNode(), raftId);
    DataGroupMember dataMember =
        metaMember.getDataClusterServer().getHeaderGroupMap().getOrDefault(raftNode, null);
    if (dataMember == null) {
      throw new Exception(String.format("Partition whose header is %s doesn't exist.", raftNode));
    }
    List<Pair<Node, NodeCharacter>> res = new ArrayList<>();
    for (Node node : dataMember.getAllNodes()) {
      if (node.equals(metaMember.getThisNode())) {
        res.add(new Pair<>(node, NodeCharacter.LEADER));
      } else {
        res.add(new Pair<>(node, NodeCharacter.FOLLOWER));
      }
    }
    return res;
  }

  @Override
  public Map<PartitionGroup, Integer> getSlotNumInDataMigration() throws Exception {
    MetaGroupMember member = getMetaGroupMember();
    if (member == null || member.getPartitionTable() == null) {
      throw new Exception(BUILDING_CLUSTER_INFO);
    }
    if (member.getCharacter() != NodeCharacter.LEADER) {
      if (member.getLeader() == null || member.getLeader().equals(ClusterConstant.EMPTY_NODE)) {
        throw new Exception(META_LEADER_UNKNOWN_INFO);
      } else {
        throw new Exception(NodeToolCmd.redirectToQueryMetaLeader(member.getLeader()));
      }
    }
    return member.collectAllPartitionMigrationStatus();
  }

  @Override
  public MultiKeyMap<Long, PartitionGroup> getDataPartition(
      String path, long startTime, long endTime) {
    PartitionTable partitionTable = getPartitionTable();
    if (partitionTable == null) {
      return null;
    }
    try {
      return partitionTable.partitionByPathRangeTime(new PartialPath(path), startTime, endTime);
    } catch (MetadataException e) {
      return new MultiKeyMap<>();
    }
  }

  @Override
  public PartitionGroup getMetaPartition(String path) {
    PartitionTable partitionTable = getPartitionTable();
    if (partitionTable == null) {
      return null;
    }
    try {
      return partitionTable.partitionByPathTime(new PartialPath(path), 0);
    } catch (MetadataException e) {
      return new PartitionGroup();
    }
  }

  @Override
  public Map<PartitionGroup, Integer> getSlotNumOfAllNode() {
    PartitionTable partitionTable = getPartitionTable();
    if (partitionTable == null) {
      return null;
    }
    List<Node> allNodes = partitionTable.getAllNodes();
    Map<RaftNode, List<Integer>> nodeSlotMap =
        ((SlotPartitionTable) partitionTable).getAllNodeSlots();
    Map<PartitionGroup, Integer> raftGroupMapSlotNum = new HashMap<>();
    for (Node header : allNodes) {
      for (int raftId = 0;
          raftId < ClusterDescriptor.getInstance().getConfig().getMultiRaftFactor();
          raftId++) {
        RaftNode raftNode = new RaftNode(header, raftId);
        raftGroupMapSlotNum.put(
            partitionTable.getHeaderGroup(raftNode), nodeSlotMap.get(raftNode).size());
      }
    }
    return raftGroupMapSlotNum;
  }

  @Override
  public Map<Node, Integer> getAllNodeStatus() {
    MetaGroupMember metaGroupMember = getMetaGroupMember();
    if (metaGroupMember == null) {
      return null;
    }
    return metaGroupMember.getAllNodeStatus();
  }

  private MetaGroupMember getMetaGroupMember() {
    MetaClusterServer metaClusterServer = ClusterMain.getMetaServer();
    if (metaClusterServer == null) {
      return null;
    }
    return metaClusterServer.getMember();
  }

  private PartitionTable getPartitionTable() {
    MetaGroupMember metaGroupMember = getMetaGroupMember();
    if (metaGroupMember == null) {
      return null;
    }
    return metaGroupMember.getPartitionTable();
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CLUSTER_MONITOR_SERVICE;
  }

  public String getMbeanName() {
    return mbeanName;
  }

  @Override
  public String getInstrumentingInfo() {
    return Timer.getReport();
  }

  @Override
  public void resetInstrumenting() {
    Timer.Statistic.resetAll();
  }
}
