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

package org.apache.iotdb.cluster.server.service;

import org.apache.iotdb.cluster.ClusterIoTDB;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.NoHeaderNodeException;
import org.apache.iotdb.cluster.exception.NotInSameGroupException;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.partition.NodeAdditionResult;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.StoppedMemberManager;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.monitor.NodeReport.DataMemberReport;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class DataGroupEngine implements IService, DataGroupEngineMBean {

  private static final Logger logger = LoggerFactory.getLogger(DataGroupEngine.class);
  // key: the header of a data group, value: the member representing this node in this group and
  // it is currently at service
  private static final Map<RaftNode, DataGroupMember> headerGroupMap = new ConcurrentHashMap<>();
  private static final Map<RaftNode, DataAsyncService> asyncServiceMap = new ConcurrentHashMap<>();
  private static final Map<RaftNode, DataSyncService> syncServiceMap = new ConcurrentHashMap<>();
  // key: the header of a data group, value: the member representing this node in this group but
  // it is out of service because another node has joined the group and expelled this node, or
  // the node itself is removed, but it is still stored to provide snapshot for other nodes
  private final StoppedMemberManager stoppedMemberManager;
  private PartitionTable partitionTable;
  private DataGroupMember.Factory dataMemberFactory;
  private static MetaGroupMember metaGroupMember;
  private final Node thisNode = ClusterIoTDB.getInstance().getThisNode();
  private static TProtocolFactory protocolFactory;

  private DataGroupEngine() {
    dataMemberFactory = new DataGroupMember.Factory(protocolFactory, metaGroupMember);
    stoppedMemberManager = new StoppedMemberManager(dataMemberFactory);
  }

  public static DataGroupEngine getInstance() {
    if (metaGroupMember == null || protocolFactory == null) {
      logger.error("MetaGroupMember or protocolFactory init failed.");
    }
    return InstanceHolder.Instance;
  }

  @TestOnly
  public void resetFactory() {
    dataMemberFactory = new DataGroupMember.Factory(protocolFactory, metaGroupMember);
  }

  @TestOnly
  public DataGroupEngine(
      DataGroupMember.Factory dataMemberFactory, MetaGroupMember metaGroupMember) {
    DataGroupEngine.metaGroupMember = metaGroupMember;
    this.dataMemberFactory = dataMemberFactory;
    this.stoppedMemberManager = new StoppedMemberManager(dataMemberFactory);
  }

  @Override
  public void start() throws StartupException {}

  @Override
  public void stop() {
    closeLogManagers();
    for (DataGroupMember member : headerGroupMap.values()) {
      member.stop();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CLUSTER_DATA_ENGINE;
  }

  public void closeLogManagers() {
    for (DataGroupMember member : headerGroupMap.values()) {
      member.closeLogManager();
    }
  }

  public <T> DataAsyncService getDataAsyncService(
      RaftNode header, AsyncMethodCallback<T> resultHandler, Object request) {
    return asyncServiceMap.computeIfAbsent(
        header,
        h -> {
          DataGroupMember dataMember = getDataMember(header, resultHandler, request);
          return dataMember != null ? new DataAsyncService(dataMember) : null;
        });
  }

  public DataSyncService getDataSyncService(RaftNode header) {
    return syncServiceMap.computeIfAbsent(
        header,
        h -> {
          DataGroupMember dataMember = getDataMember(header, null, null);
          return dataMember != null ? new DataSyncService(dataMember) : null;
        });
  }

  /**
   * Add a DataGroupMember into this server, if a member with the same header exists, the old member
   * will be stopped and replaced by the new one.
   */
  public DataGroupMember addDataGroupMember(DataGroupMember dataGroupMember, RaftNode header) {
    synchronized (headerGroupMap) {
      // TODO this method won't update headerMap if a new dataGroupMember comes with the same
      // header.
      if (headerGroupMap.containsKey(header)) {
        logger.debug("Group {} already exist.", dataGroupMember.getAllNodes());
        return headerGroupMap.get(header);
      }
      stoppedMemberManager.remove(header);
      headerGroupMap.put(header, dataGroupMember);

      dataGroupMember.start();
    }
    logger.info("Add group {} successfully.", dataGroupMember.getName());
    resetServiceCache(header); // avoid dead-lock

    return dataGroupMember;
  }

  private void resetServiceCache(RaftNode header) {
    asyncServiceMap.remove(header);
    syncServiceMap.remove(header);
  }

  /**
   * @param header the header of the group which the local node is in
   * @param resultHandler can be set to null if the request is an internal request
   * @param request the toString() of this parameter should explain what the request is and it is
   *     only used in logs for tracing
   * @return
   */
  public <T> DataGroupMember getDataMember(
      RaftNode header, AsyncMethodCallback<T> resultHandler, Object request) {
    // if the resultHandler is not null, then the request is a external one and must be with a
    // header
    if (header.getNode() == null) {
      if (resultHandler != null) {
        resultHandler.onError(new NoHeaderNodeException());
      }
      return null;
    }
    DataGroupMember member = stoppedMemberManager.get(header);
    if (member != null) {
      return member;
    }

    // avoid creating two members for a header
    Exception ex = null;
    member = headerGroupMap.get(header);
    if (member != null) {
      return member;
    }
    logger.info("Received a request \"{}\" from unregistered header {}", request, header);
    if (partitionTable != null) {
      try {
        member = createNewMember(header);
      } catch (NotInSameGroupException | CheckConsistencyException e) {
        ex = e;
      }
    } else {
      logger.info("Partition is not ready, cannot create member");
      ex = new PartitionTableUnavailableException(thisNode);
    }
    if (ex != null && resultHandler != null) {
      resultHandler.onError(ex);
    }
    return member;
  }

  /**
   * @return A DataGroupMember representing this node in the data group of the header.
   * @throws NotInSameGroupException If this node is not in the group of the header.
   */
  private DataGroupMember createNewMember(RaftNode header)
      throws NotInSameGroupException, CheckConsistencyException {
    PartitionGroup partitionGroup;
    partitionGroup = partitionTable.getPartitionGroup(header);
    if (partitionGroup == null || !partitionGroup.contains(thisNode)) {
      // if the partition table is old, this node may have not been moved to the new group
      metaGroupMember.syncLeaderWithConsistencyCheck(true);
      partitionGroup = partitionTable.getPartitionGroup(header);
    }
    DataGroupMember member;
    synchronized (headerGroupMap) {
      member = headerGroupMap.get(header);
      if (member != null) {
        return member;
      }
      if (partitionGroup != null && partitionGroup.contains(thisNode)) {
        // the two nodes are in the same group, create a new data member
        member = dataMemberFactory.create(partitionGroup);
        headerGroupMap.put(header, member);
        stoppedMemberManager.remove(header);
        logger.info("Created a member for header {}, group is {}", header, partitionGroup);
        member.start();
      } else {
        // the member may have been stopped after syncLeader
        member = stoppedMemberManager.get(header);
        if (member != null) {
          return member;
        }
        logger.info(
            "This node {} does not belong to the group {}, header {}",
            thisNode,
            partitionGroup,
            header);
        throw new NotInSameGroupException(partitionGroup, thisNode);
      }
    }
    return member;
  }

  public void preAddNodeForDataGroup(AddNodeLog log, DataGroupMember targetDataGroupMember) {

    // Make sure the previous add/remove node log has applied
    metaGroupMember.syncLocalApply(log.getMetaLogIndex() - 1, false);

    // Check the validity of the partition table
    if (!metaGroupMember.getPartitionTable().deserialize(log.getPartitionTable())) {
      return;
    }

    targetDataGroupMember.preAddNode(log.getNewNode());
  }

  /**
   * Try adding the node into the group of each DataGroupMember, and if the DataGroupMember no
   * longer stays in that group, also remove and stop it. If the new group contains this node, also
   * create and add a new DataGroupMember for it.
   */
  public void addNode(Node node, NodeAdditionResult result) {
    // If the node executed adding itself to the cluster, it's unnecessary to add new groups because
    // they already exist.
    if (node.equals(thisNode)) {
      return;
    }
    Iterator<Entry<RaftNode, DataGroupMember>> entryIterator = headerGroupMap.entrySet().iterator();
    synchronized (headerGroupMap) {
      while (entryIterator.hasNext()) {
        Entry<RaftNode, DataGroupMember> entry = entryIterator.next();
        DataGroupMember dataGroupMember = entry.getValue();
        // the member may be extruded from the group, remove and stop it if so
        boolean shouldLeave = dataGroupMember.addNode(node, result);
        if (shouldLeave) {
          logger.info("This node does not belong to {} any more", dataGroupMember.getAllNodes());
          removeMember(entry.getKey(), entry.getValue(), false);
          entryIterator.remove();
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Data cluster server: start to handle new groups when adding new node {}", node);
      }
      for (PartitionGroup newGroup : result.getNewGroupList()) {
        if (newGroup.contains(thisNode)) {
          RaftNode header = newGroup.getHeader();
          logger.info("Adding this node into a new group {}", newGroup);
          DataGroupMember dataGroupMember = dataMemberFactory.create(newGroup);
          dataGroupMember = addDataGroupMember(dataGroupMember, header);
          dataGroupMember.pullNodeAdditionSnapshots(
              ((SlotPartitionTable) partitionTable).getNodeSlots(header), node);
        }
      }
    }
  }

  /**
   * When the node joins a cluster, it also creates a new data group and a corresponding member
   * which has no data. This is to make that member pull data from other nodes.
   */
  public void pullSnapshots() {
    for (int raftId = 0;
        raftId < ClusterDescriptor.getInstance().getConfig().getMultiRaftFactor();
        raftId++) {
      RaftNode raftNode = new RaftNode(thisNode, raftId);
      List<Integer> slots = ((SlotPartitionTable) partitionTable).getNodeSlots(raftNode);
      DataGroupMember dataGroupMember = headerGroupMap.get(raftNode);
      dataGroupMember.pullNodeAdditionSnapshots(slots, thisNode);
    }
  }

  /** Make sure the group will not receive new raft logs. */
  private void removeMember(
      RaftNode header, DataGroupMember dataGroupMember, boolean removedGroup) {
    dataGroupMember.setReadOnly();
    if (!removedGroup) {
      dataGroupMember.stop();
    } else {
      if (dataGroupMember.getCharacter() != NodeCharacter.LEADER) {
        new Thread(
                () -> {
                  try {
                    dataGroupMember.syncLeader(null);
                    dataGroupMember.stop();
                  } catch (CheckConsistencyException e) {
                    logger.warn("Failed to check consistency.", e);
                  }
                })
            .start();
      }
    }
    stoppedMemberManager.put(header, dataGroupMember);
    logger.info(
        "Data group member has removed, header {}, group is {}.",
        header,
        dataGroupMember.getAllNodes());
  }

  /**
   * Set the partition table as the in-use one and build a DataGroupMember for each local group (the
   * group which the local node is in) and start them.
   */
  @SuppressWarnings("java:S1135")
  public void buildDataGroupMembers(PartitionTable partitionTable) {
    setPartitionTable(partitionTable);
    // TODO-Cluster: if there are unchanged members, do not stop and restart them
    // clear previous members if the partition table is reloaded
    for (DataGroupMember value : headerGroupMap.values()) {
      value.stop();
    }

    for (DataGroupMember value : headerGroupMap.values()) {
      value.setUnchanged(false);
    }

    List<PartitionGroup> partitionGroups = partitionTable.getLocalGroups();
    for (PartitionGroup partitionGroup : partitionGroups) {
      RaftNode header = partitionGroup.getHeader();
      DataGroupMember prevMember = headerGroupMap.get(header);
      if (prevMember == null || !prevMember.getAllNodes().equals(partitionGroup)) {
        logger.info("Building member of data group: {}", partitionGroup);
        // no previous member or member changed
        DataGroupMember dataGroupMember = dataMemberFactory.create(partitionGroup);
        // the previous member will be replaced here
        addDataGroupMember(dataGroupMember, header);
        dataGroupMember.setUnchanged(true);
      } else {
        prevMember.setUnchanged(true);
        prevMember.start();
        // TODO do we nedd call other functions in addDataGroupMember() ?
      }
    }

    // remove out-dated members of this node
    headerGroupMap.entrySet().removeIf(e -> !e.getValue().isUnchanged());

    logger.info("Data group members are ready");
  }

  public void preRemoveNodeForDataGroup(RemoveNodeLog log, DataGroupMember targetDataGroupMember) {

    // Make sure the previous add/remove node log has applied
    metaGroupMember.syncLocalApply(log.getMetaLogIndex() - 1, false);

    // Check the validity of the partition table
    if (!metaGroupMember.getPartitionTable().deserialize(log.getPartitionTable())) {
      return;
    }

    logger.debug(
        "Pre removing a node {} from {}",
        log.getRemovedNode(),
        targetDataGroupMember.getAllNodes());
    targetDataGroupMember.preRemoveNode(log.getRemovedNode());
  }

  /**
   * Try removing a node from the groups of each DataGroupMember. If the node is the header of some
   * group, set the member to read only so that it can still provide data for other nodes that has
   * not yet pulled its data. Otherwise, just change the node list of the member and pull new data.
   * And create a new DataGroupMember if this node should join a new group because of this removal.
   *
   * @param node
   * @param removalResult cluster changes due to the node removal
   */
  public void removeNode(Node node, NodeRemovalResult removalResult) {
    Iterator<Entry<RaftNode, DataGroupMember>> entryIterator = headerGroupMap.entrySet().iterator();
    synchronized (headerGroupMap) {
      while (entryIterator.hasNext()) {
        Entry<RaftNode, DataGroupMember> entry = entryIterator.next();
        DataGroupMember dataGroupMember = entry.getValue();
        if (dataGroupMember.getHeader().getNode().equals(node) || node.equals(thisNode)) {
          entryIterator.remove();
          removeMember(
              entry.getKey(), dataGroupMember, dataGroupMember.getHeader().getNode().equals(node));
        } else {
          // the group should be updated
          dataGroupMember.removeNode(node);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Data cluster server: start to handle new groups and pulling data when removing node {}",
            node);
      }
      // if the removed group contains the local node, the local node should join a new group to
      // preserve the replication number
      for (PartitionGroup group : partitionTable.getLocalGroups()) {
        RaftNode header = group.getHeader();
        if (!headerGroupMap.containsKey(header)) {
          logger.info("{} should join a new group {}", thisNode, group);
          DataGroupMember dataGroupMember = dataMemberFactory.create(group);
          addDataGroupMember(dataGroupMember, header);
        }
        // pull new slots from the removed node
        headerGroupMap.get(header).pullSlots(removalResult);
      }
    }
  }

  public void setPartitionTable(PartitionTable partitionTable) {
    this.partitionTable = partitionTable;
  }

  /** @return The reports of every DataGroupMember in this node. */
  public List<DataMemberReport> genMemberReports() {
    List<DataMemberReport> dataMemberReports = new ArrayList<>();
    for (DataGroupMember value : headerGroupMap.values()) {

      dataMemberReports.add(value.genReport());
    }
    return dataMemberReports;
  }

  public Map<RaftNode, DataGroupMember> getHeaderGroupMap() {
    return headerGroupMap;
  }

  public static void setProtocolFactory(TProtocolFactory protocolFactory) {
    DataGroupEngine.protocolFactory = protocolFactory;
  }

  public static void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    DataGroupEngine.metaGroupMember = metaGroupMember;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final DataGroupEngine Instance = new DataGroupEngine();
  }

  @Override
  public String getHeaderGroupMapAsString() {
    return headerGroupMap.toString();
  }

  @Override
  public int getAsyncServiceMapSize() {
    return asyncServiceMap.size();
  }

  @Override
  public int getSyncServiceMapSize() {
    return syncServiceMap.size();
  }

  @Override
  public String getPartitionTable() {
    return partitionTable.toString();
  }
}
