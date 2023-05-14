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

import org.apache.iotdb.cluster.ClusterIoTDB;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.nodetool.function.NodeToolCmd;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.service.metrics.Metric;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
      if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
        startCollectClusterStatus();
      }
    } catch (Exception e) {
      String errorMessage =
          String.format("Failed to start %s because of %s", this.getID().getName(), e.getMessage());
      throw new StartupException(errorMessage);
    }
  }

  private void startCollectClusterStatus() {
    // monitor all nodes' live status
    LOGGER.info("start metric node status and leader distribution");
    IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(ThreadName.Cluster_Monitor.getName())
        .scheduleAtFixedRate(
            () -> {
              MetaGroupMember metaGroupMember = ClusterIoTDB.getInstance().getMetaGroupMember();
              if (metaGroupMember != null
                  && metaGroupMember.getLeader().equals(metaGroupMember.getThisNode())) {
                metricNodeStatus(metaGroupMember);
                metricLeaderDistribution(metaGroupMember);
              }
            },
            10L,
            10L,
            TimeUnit.SECONDS);
  }

  private void metricLeaderDistribution(MetaGroupMember metaGroupMember) {
    Map<Node, Integer> leaderCountMap = new HashMap<>();
    ClusterIoTDB.getInstance()
        .getDataGroupEngine()
        .getHeaderGroupMap()
        .forEach(
            (header, dataGroupMember) -> {
              Node leader = dataGroupMember.getLeader();
              int delta = 1;
              Integer count = leaderCountMap.getOrDefault(leader, 0);
              leaderCountMap.put(leader, count + delta);
            });
    List<Node> ring = getRing();
    for (Node node : ring) {
      Integer count = leaderCountMap.getOrDefault(node, 0);
      MetricsService.getInstance()
          .getMetricManager()
          .gauge(
              count,
              Metric.CLUSTER_NODE_LEADER_COUNT.toString(),
              Tag.NAME.toString(),
              node.internalIp);
    }
  }

  private void metricNodeStatus(MetaGroupMember metaGroupMember) {
    List<Node> ring = getRing();
    for (Node node : ring) {
      boolean isAlive = false;
      if (node.equals(metaGroupMember.getThisNode())) {
        isAlive = true;
      }
      SyncMetaClient client = (SyncMetaClient) metaGroupMember.getSyncClient(node);
      if (client != null) {
        try {
          client.checkAlive();
          isAlive = true;
        } catch (TException e) {
          client.getInputProtocol().getTransport().close();
        } finally {
          ClientUtils.putBackSyncClient(client);
        }
      }
      MetricsService.getInstance()
          .getMetricManager()
          .gauge(
              isAlive ? 1 : 0,
              Metric.CLUSTER_NODE_STATUS.toString(),
              Tag.NAME.toString(),
              node.internalIp);
    }
  }

  @Override
  public List<Pair<Node, NodeCharacter>> getMetaGroup() {
    MetaGroupMember metaMember = ClusterIoTDB.getInstance().getMetaGroupMember();
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
    MetaGroupMember metaMember = ClusterIoTDB.getInstance().getMetaGroupMember();
    if (metaMember == null || metaMember.getPartitionTable() == null) {
      return null;
    }
    return metaMember.getPartitionTable().getAllNodes();
  }

  @Override
  public List<Pair<Node, NodeCharacter>> getDataGroup(int raftId) throws Exception {
    MetaGroupMember metaMember = ClusterIoTDB.getInstance().getMetaGroupMember();
    if (metaMember == null || metaMember.getPartitionTable() == null) {
      return null;
    }
    RaftNode raftNode = new RaftNode(metaMember.getThisNode(), raftId);
    DataGroupMember dataMember =
        ClusterIoTDB.getInstance()
            .getDataGroupEngine()
            .getHeaderGroupMap()
            .getOrDefault(raftNode, null);
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
    MetaGroupMember member = ClusterIoTDB.getInstance().getMetaGroupMember();
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
            partitionTable.getPartitionGroup(raftNode), nodeSlotMap.get(raftNode).size());
      }
    }
    return raftGroupMapSlotNum;
  }

  @Override
  public Map<Node, Integer> getAllNodeStatus() {
    MetaGroupMember metaGroupMember = ClusterIoTDB.getInstance().getMetaGroupMember();
    if (metaGroupMember == null) {
      return null;
    }
    return metaGroupMember.getAllNodeStatus();
  }

  private PartitionTable getPartitionTable() {
    MetaGroupMember metaGroupMember = ClusterIoTDB.getInstance().getMetaGroupMember();
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
