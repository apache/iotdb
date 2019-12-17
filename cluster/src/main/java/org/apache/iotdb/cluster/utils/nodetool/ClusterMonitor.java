/**
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.ClusterMain;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;

public class ClusterMonitor implements ClusterMonitorMBean, IService {

  /**
   * Original format = String.format("%s:%s=%s",
   * IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName()
   */
  public static final String MBEAN_NAME = "org.apache.iotdb.service:type=Cluster Monitor";

  public static final ClusterMonitor INSTANCE = new ClusterMonitor();

  public String getMbeanName() {
    return MBEAN_NAME;
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(INSTANCE, MBEAN_NAME);
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      throw new StartupException(errorMessage);
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(MBEAN_NAME);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CLUSTER_MONITOR_SERVICE;
  }

  @Override
  public List<Node> getRing() {
    PartitionTable partitionTable = getPartitionTable();
    return partitionTable != null ? partitionTable.getAllNodes() : null;
  }

  @Override
  public String getDataPartitionOfSG(String sg) {
//    PeerId[] nodes = RaftUtils.getDataPartitionOfSG(sg);
//    StringBuilder builder = new StringBuilder();
//    builder.append(nodes[0].getIp()).append(" (leader)");
//    for (int i = 1; i < nodes.length; i++) {
//      builder.append(", ").append(nodes[i].getIp());
//    }
//    return builder.toString();
    throw new RuntimeException("Unsupport.");
  }

  @Override
  public Set<String> getAllStorageGroupsLocally() {
//    return RaftUtils.getAllStorageGroupsLocally();
    throw new RuntimeException("Unsupport.");
  }

  @Override
  public Map<PartitionGroup, Integer> getSlotNumOfCurNode() {
    PartitionTable partitionTable = getPartitionTable();
    if (partitionTable == null || partitionTable.getLocalGroups() == null) {
      return null;
    }
    List<PartitionGroup> localGroups = partitionTable.getLocalGroups();
    Map<Node, List<Integer>> nodeSlotMap = partitionTable.getAllNodeSlots();
    Map<PartitionGroup, Integer> raftGroupMapSlotNum = new HashMap<>();
    for (PartitionGroup group : localGroups) {
      raftGroupMapSlotNum.put(group, nodeSlotMap.get(group.getHeader()).size());
    }
    return raftGroupMapSlotNum;
  }

  @Override
  public Map<PartitionGroup, Integer> getSlotNumOfAllNode() {
    PartitionTable partitionTable = getPartitionTable();
    if (partitionTable == null) {
      return null;
    }
    List<Node> allNodes = partitionTable.getAllNodes();
    Map<Node, List<Integer>> nodeSlotMap = partitionTable.getAllNodeSlots();
    Map<PartitionGroup, Integer> raftGroupMapSlotNum = new HashMap<>();
    for (Node header : allNodes) {
      raftGroupMapSlotNum
          .put(partitionTable.getHeaderGroup(header), nodeSlotMap.get(header).size());
    }
    return raftGroupMapSlotNum;
  }

  @Override
  public Map<String, Boolean> getStatusMap() {
//    return RaftUtils.getStatusMapForCluster();
    throw new RuntimeException("Unsupport.");
  }

  private PartitionTable getPartitionTable(){
    MetaClusterServer metaClusterServer = ClusterMain.metaServer;
    if (metaClusterServer == null || metaClusterServer.getMember() == null
        || metaClusterServer.getMember().getPartitionTable() == null) {
      return null;
    }
    return metaClusterServer.getMember().getPartitionTable();
  }
}
