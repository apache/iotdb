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
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public List<Node> getRing() {
    PartitionTable partitionTable = getPartitionTable();
    return partitionTable != null ? partitionTable.getAllNodes() : null;
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
      LOGGER.error("The storage group of path {} doesn't exist.", path, e);
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
      LOGGER.error("The storage group of path {} doesn't exist.", path, e);
      return new PartitionGroup();
    }
  }

  @Override
  public Map<PartitionGroup, Integer> getSlotNumOfCurNode() {
    PartitionTable partitionTable = getPartitionTable();
    if (partitionTable == null || partitionTable.getLocalGroups() == null) {
      return null;
    }
    List<PartitionGroup> localGroups = partitionTable.getLocalGroups();
    Map<Node, List<Integer>> nodeSlotMap = ((SlotPartitionTable) partitionTable).getAllNodeSlots();
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
    Map<Node, List<Integer>> nodeSlotMap = ((SlotPartitionTable) partitionTable).getAllNodeSlots();
    Map<PartitionGroup, Integer> raftGroupMapSlotNum = new HashMap<>();
    for (Node header : allNodes) {
      raftGroupMapSlotNum.put(
          partitionTable.getHeaderGroup(header), nodeSlotMap.get(header).size());
    }
    return raftGroupMapSlotNum;
  }

  @Override
  public Map<Node, Boolean> getAllNodeStatus() {
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
