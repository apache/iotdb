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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.exception.AddConsensusGroupException;
import org.apache.iotdb.confignode.exception.AddPeerException;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.node.heartbeat.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionGroupCache;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionHeartbeatSample;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.TActiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInactiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateConfigNodeGroupReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ConfigNodeProcedureEnv {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigNodeProcedureEnv.class);

  /** add or remove node lock */
  private final LockQueue nodeLock = new LockQueue();

  /** pipe operation lock */
  private final LockQueue pipeLock = new LockQueue();

  private final ReentrantLock schedulerLock = new ReentrantLock();

  private final ConfigManager configManager;

  private final ProcedureScheduler scheduler;

  private final DataNodeRemoveHandler dataNodeRemoveHandler;

  private final ReentrantLock removeConfigNodeLock;

  public ConfigNodeProcedureEnv(ConfigManager configManager, ProcedureScheduler scheduler) {
    this.configManager = configManager;
    this.scheduler = scheduler;
    this.dataNodeRemoveHandler = new DataNodeRemoveHandler(configManager);
    this.removeConfigNodeLock = new ReentrantLock();
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  /**
   * Delete ConfigNode cache, includes ClusterSchemaInfo and PartitionInfo
   *
   * @param name database name
   * @return tsStatus
   */
  public TSStatus deleteConfig(String name) {
    DeleteDatabasePlan deleteDatabasePlan = new DeleteDatabasePlan(name);
    return getClusterSchemaManager().deleteStorageGroup(deleteDatabasePlan);
  }

  /**
   * Pre delete a database
   *
   * @param preDeleteType execute/rollback
   * @param deleteSgName database name
   */
  public void preDelete(PreDeleteDatabasePlan.PreDeleteType preDeleteType, String deleteSgName) {
    getPartitionManager().preDeleteStorageGroup(deleteSgName, preDeleteType);
  }

  /**
   * @param storageGroupName database name
   * @return ALL SUCCESS OR NOT
   * @throws IOException IOE
   * @throws TException Thrift IOE
   */
  public boolean invalidateCache(String storageGroupName) throws IOException, TException {
    NodeManager nodeManager = configManager.getNodeManager();
    List<TDataNodeConfiguration> allDataNodes = nodeManager.getRegisteredDataNodes();
    TInvalidateCacheReq invalidateCacheReq = new TInvalidateCacheReq();
    invalidateCacheReq.setStorageGroup(true);
    invalidateCacheReq.setFullPath(storageGroupName);
    for (TDataNodeConfiguration dataNodeConfiguration : allDataNodes) {
      int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();

      // if the node is not alive, sleep 1 second and try again
      NodeStatus nodeStatus = nodeManager.getNodeStatusByNodeId(dataNodeId);
      if (nodeStatus == NodeStatus.Unknown) {
        try {
          TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
          LOG.error("Sleep failed in ConfigNodeProcedureEnv: ", e);
        }
        nodeStatus = nodeManager.getNodeStatusByNodeId(dataNodeId);
      }

      if (nodeStatus == NodeStatus.Running) {
        final TSStatus invalidateSchemaStatus =
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithRetry(
                    dataNodeConfiguration.getLocation().getInternalEndPoint(),
                    invalidateCacheReq,
                    DataNodeRequestType.INVALIDATE_SCHEMA_CACHE);
        final TSStatus invalidatePartitionStatus =
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithRetry(
                    dataNodeConfiguration.getLocation().getInternalEndPoint(),
                    invalidateCacheReq,
                    DataNodeRequestType.INVALIDATE_PARTITION_CACHE);
        if (!verifySucceed(invalidatePartitionStatus, invalidateSchemaStatus)) {
          LOG.error(
              "Invalidate cache failed, invalidate partition cache status is {}, invalidate schema cache status is {}",
              invalidatePartitionStatus,
              invalidateSchemaStatus);
          return false;
        }
      } else if (nodeStatus == NodeStatus.Unknown) {
        LOG.warn(
            "Invalidate cache failed, because DataNode {} is Unknown",
            dataNodeConfiguration.getLocation().getInternalEndPoint());
      }
    }
    return true;
  }

  public boolean verifySucceed(TSStatus... status) {
    return Arrays.stream(status)
        .allMatch(tsStatus -> tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public boolean doubleCheckReplica(TDataNodeLocation removedDatanode) {
    return configManager
                .getNodeManager()
                .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly)
                .size()
            - Boolean.compare(
                configManager
                        .getNodeManager()
                        .getNodeStatusByNodeId(removedDatanode.getDataNodeId())
                    != NodeStatus.Unknown,
                false)
        >= NodeInfo.getMinimumDataNode();
  }

  /**
   * Let the remotely new ConfigNode build the ConsensusGroup. Actually, the parameter of this
   * method can be empty, adding new raft peer to exist group should invoke createPeer(groupId,
   * emptyList).
   *
   * @param tConfigNodeLocation New ConfigNode's location
   */
  public void addConsensusGroup(TConfigNodeLocation tConfigNodeLocation)
      throws AddConsensusGroupException {
    List<TConfigNodeLocation> configNodeLocations =
        new ArrayList<>(configManager.getNodeManager().getRegisteredConfigNodes());
    configNodeLocations.add(tConfigNodeLocation);
    TSStatus status =
        (TSStatus)
            SyncConfigNodeClientPool.getInstance()
                .sendSyncRequestToConfigNodeWithRetry(
                    tConfigNodeLocation.getInternalEndPoint(),
                    new TAddConsensusGroupReq(configNodeLocations),
                    ConfigNodeRequestType.ADD_CONSENSUS_GROUP);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AddConsensusGroupException(tConfigNodeLocation);
    }
  }

  /**
   * Leader will add the new ConfigNode Peer into ConfigRegion
   *
   * @param configNodeLocation The new ConfigNode
   * @throws AddPeerException When addPeer doesn't success
   */
  public void addConfigNodePeer(TConfigNodeLocation configNodeLocation) throws AddPeerException {
    configManager.getConsensusManager().addConfigNodePeer(configNodeLocation);
  }

  /**
   * Remove peer in Leader node
   *
   * @param tConfigNodeLocation node is removed
   * @throws ProcedureException if failed status
   */
  public void removeConfigNodePeer(TConfigNodeLocation tConfigNodeLocation)
      throws ProcedureException {
    removeConfigNodeLock.lock();
    TSStatus tsStatus;
    try {
      // Execute removePeer
      if (getConsensusManager().removeConfigNodePeer(tConfigNodeLocation)) {
        tsStatus =
            getConsensusManager().write(new RemoveConfigNodePlan(tConfigNodeLocation)).getStatus();
      } else {
        tsStatus =
            new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
                .setMessage(
                    "Remove ConfigNode failed because update ConsensusGroup peer information failed.");
      }
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new ProcedureException(tsStatus.getMessage());
      }
    } finally {
      removeConfigNodeLock.unlock();
    }
  }

  /**
   * Remove Consensus Group in removed node
   *
   * @param removedConfigNode config node location
   * @throws ProcedureException if failed status
   */
  public void deleteConfigNodePeer(TConfigNodeLocation removedConfigNode)
      throws ProcedureException {
    TSStatus tsStatus =
        (TSStatus)
            SyncConfigNodeClientPool.getInstance()
                .sendSyncRequestToConfigNodeWithRetry(
                    removedConfigNode.getInternalEndPoint(),
                    removedConfigNode,
                    ConfigNodeRequestType.DELETE_CONFIG_NODE_PEER);
    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ProcedureException(tsStatus.getMessage());
    }
  }

  /**
   * Stop ConfigNode and remove heartbeatCache
   *
   * @param tConfigNodeLocation config node location
   * @throws ProcedureException if failed status
   */
  public void stopConfigNode(TConfigNodeLocation tConfigNodeLocation) throws ProcedureException {
    getNodeManager().removeNodeCache(tConfigNodeLocation.getConfigNodeId());

    TSStatus tsStatus =
        (TSStatus)
            SyncConfigNodeClientPool.getInstance()
                .sendSyncRequestToConfigNodeWithRetry(
                    tConfigNodeLocation.getInternalEndPoint(),
                    tConfigNodeLocation,
                    ConfigNodeRequestType.STOP_CONFIG_NODE);

    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ProcedureException(tsStatus.getMessage());
    }
  }

  /**
   * Leader will record the new ConfigNode's information
   *
   * @param configNodeLocation The new ConfigNode
   */
  public void applyConfigNode(TConfigNodeLocation configNodeLocation) {
    configManager.getNodeManager().applyConfigNode(configNodeLocation);
  }

  /**
   * Leader will notify the new ConfigNode that registration success
   *
   * @param configNodeLocation The new ConfigNode
   */
  public void notifyRegisterSuccess(TConfigNodeLocation configNodeLocation) {
    SyncConfigNodeClientPool.getInstance()
        .sendSyncRequestToConfigNodeWithRetry(
            configNodeLocation.getInternalEndPoint(),
            null,
            ConfigNodeRequestType.NOTIFY_REGISTER_SUCCESS);
  }

  /** Notify all DataNodes when the capacity of the ConfigNodeGroup is expanded or reduced */
  public void broadCastTheLatestConfigNodeGroup() {
    List<TConfigNodeLocation> registeredConfigNodes =
        configManager.getNodeManager().getRegisteredConfigNodes();
    Map<Integer, TDataNodeLocation> registeredDataNodes =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<TUpdateConfigNodeGroupReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.BROADCAST_LATEST_CONFIG_NODE_GROUP,
            new TUpdateConfigNodeGroupReq(registeredConfigNodes),
            registeredDataNodes);

    if (registeredDataNodes.size() > 0) {
      LOG.info(
          "Begin to broadcast the latest configNodeGroup to DataNodes, ConfigNodeGroups: {}, DataNodes: {}",
          registeredConfigNodes,
          registeredDataNodes.values());
      AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
      LOG.info("Broadcast the latest configNodeGroup to DataNodes finished.");
    }
  }

  /**
   * Mark the given datanode as removing status to avoid read or write request routing to this node.
   *
   * @param dataNodeLocation the datanode to be marked as removing status
   */
  public void markDataNodeAsRemovingAndBroadcast(TDataNodeLocation dataNodeLocation) {
    // Send request to update NodeStatus on the DataNode to be removed
    if (configManager.getNodeManager().getNodeStatusByNodeId(dataNodeLocation.getDataNodeId())
        == NodeStatus.Unknown) {
      SyncDataNodeClientPool.getInstance()
          .sendSyncRequestToDataNodeWithGivenRetry(
              dataNodeLocation.getInternalEndPoint(),
              NodeStatus.Removing.getStatus(),
              DataNodeRequestType.SET_SYSTEM_STATUS,
              1);
    } else {
      SyncDataNodeClientPool.getInstance()
          .sendSyncRequestToDataNodeWithRetry(
              dataNodeLocation.getInternalEndPoint(),
              NodeStatus.Removing.getStatus(),
              DataNodeRequestType.SET_SYSTEM_STATUS);
    }

    // Force updating NodeStatus to Removing
    getNodeManager()
        .getNodeCacheMap()
        .get(dataNodeLocation.getDataNodeId())
        .forceUpdate(NodeHeartbeatSample.generateDefaultSample(NodeStatus.Removing));
  }

  /**
   * Do region creations and broadcast the CreateRegionGroupsPlan
   *
   * @return Those RegionReplicas that failed to create
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> doRegionCreation(
      TConsensusGroupType consensusGroupType, CreateRegionGroupsPlan createRegionGroupsPlan) {

    // Prepare clientHandler
    AsyncClientHandler<?, TSStatus> clientHandler;
    switch (consensusGroupType) {
      case SchemaRegion:
        clientHandler = getCreateSchemaRegionClientHandler(createRegionGroupsPlan);
        break;
      case DataRegion:
      default:
        clientHandler = getCreateDataRegionClientHandler(createRegionGroupsPlan);
        break;
    }
    if (clientHandler.getRequestIndices().isEmpty()) {
      return new HashMap<>();
    }

    // Send CreateRegion requests to DataNodes
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);

    // Filter RegionGroups that weren't created successfully
    int requestId = 0;
    Map<Integer, TSStatus> responseMap = clientHandler.getResponseMap();
    Map<TConsensusGroupId, TRegionReplicaSet> failedRegions = new HashMap<>();
    for (List<TRegionReplicaSet> regionReplicaSets :
        createRegionGroupsPlan.getRegionGroupMap().values()) {
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          if (responseMap.get(requestId).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            failedRegions
                .computeIfAbsent(
                    regionReplicaSet.getRegionId(),
                    empty -> new TRegionReplicaSet().setRegionId(regionReplicaSet.getRegionId()))
                .addToDataNodeLocations(dataNodeLocation);
          }
          requestId += 1;
        }
      }
    }
    return failedRegions;
  }

  private AsyncClientHandler<TCreateSchemaRegionReq, TSStatus> getCreateSchemaRegionClientHandler(
      CreateRegionGroupsPlan createRegionGroupsPlan) {
    AsyncClientHandler<TCreateSchemaRegionReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CREATE_SCHEMA_REGION);

    int requestId = 0;
    for (Map.Entry<String, List<TRegionReplicaSet>> sgRegionsEntry :
        createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
      String storageGroup = sgRegionsEntry.getKey();
      List<TRegionReplicaSet> regionReplicaSets = sgRegionsEntry.getValue();
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          clientHandler.putRequest(
              requestId, genCreateSchemaRegionReq(storageGroup, regionReplicaSet));
          clientHandler.putDataNodeLocation(requestId, dataNodeLocation);
          requestId += 1;
        }
      }
    }

    return clientHandler;
  }

  private AsyncClientHandler<TCreateDataRegionReq, TSStatus> getCreateDataRegionClientHandler(
      CreateRegionGroupsPlan createRegionGroupsPlan) {
    Map<String, Long> ttlMap = getTTLMap(createRegionGroupsPlan);
    AsyncClientHandler<TCreateDataRegionReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CREATE_DATA_REGION);

    int requestId = 0;
    for (Map.Entry<String, List<TRegionReplicaSet>> sgRegionsEntry :
        createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
      String storageGroup = sgRegionsEntry.getKey();
      List<TRegionReplicaSet> regionReplicaSets = sgRegionsEntry.getValue();
      long ttl = ttlMap.get(storageGroup);
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          clientHandler.putRequest(
              requestId, genCreateDataRegionReq(storageGroup, regionReplicaSet, ttl));
          clientHandler.putDataNodeLocation(requestId, dataNodeLocation);
          requestId += 1;
        }
      }
    }

    return clientHandler;
  }

  private Map<String, Long> getTTLMap(CreateRegionGroupsPlan createRegionGroupsPlan) {
    Map<String, Long> ttlMap = new HashMap<>();
    for (String storageGroup : createRegionGroupsPlan.getRegionGroupMap().keySet()) {
      try {
        ttlMap.put(
            storageGroup, getClusterSchemaManager().getDatabaseSchemaByName(storageGroup).getTTL());
      } catch (DatabaseNotExistsException e) {
        // Notice: This line will never reach since we've checked before
        LOG.error("StorageGroup doesn't exist", e);
      }
    }
    return ttlMap;
  }

  private TCreateSchemaRegionReq genCreateSchemaRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet) {
    TCreateSchemaRegionReq req = new TCreateSchemaRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    return req;
  }

  private TCreateDataRegionReq genCreateDataRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet, long TTL) {
    TCreateDataRegionReq req = new TCreateDataRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    req.setTtl(TTL);
    return req;
  }

  public long getTTL(String storageGroup) throws DatabaseNotExistsException {
    return getClusterSchemaManager().getDatabaseSchemaByName(storageGroup).getTTL();
  }

  public void persistRegionGroup(CreateRegionGroupsPlan createRegionGroupsPlan) {
    // Persist the allocation result
    getConsensusManager().write(createRegionGroupsPlan);
  }

  public void activateRegionGroup(
      TConsensusGroupId regionGroupId, Map<Integer, RegionStatus> regionStatusMap) {
    // Force activating RegionGroup
    long currentTime = System.currentTimeMillis();
    Map<Integer, RegionHeartbeatSample> heartbeatSampleMap = new HashMap<>();
    regionStatusMap.forEach(
        (dataNodeId, regionStatus) ->
            heartbeatSampleMap.put(
                dataNodeId, new RegionHeartbeatSample(currentTime, currentTime, regionStatus)));
    getPartitionManager()
        .getRegionGroupCacheMap()
        .computeIfAbsent(regionGroupId, empty -> new RegionGroupCache(regionGroupId))
        .forceUpdate(heartbeatSampleMap);

    // Select leader greedily for iot consensus protocol
    if (TConsensusGroupType.DataRegion.equals(regionGroupId.getType())
        && ConsensusFactory.IOT_CONSENSUS.equals(
            ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass())) {
      List<Integer> availableDataNodes = new ArrayList<>();
      for (Map.Entry<Integer, RegionStatus> statusEntry : regionStatusMap.entrySet()) {
        if (RegionStatus.isNormalStatus(statusEntry.getValue())) {
          availableDataNodes.add(statusEntry.getKey());
        }
      }
      getLoadManager().getRouteBalancer().greedySelectLeader(regionGroupId, availableDataNodes);
    }

    // Force update RegionRouteMap
    getLoadManager().getRouteBalancer().updateRegionRouteMap();
  }

  public void broadcastRegionGroup() {
    // Broadcast the latest RegionRouteMap
    getLoadManager().broadcastLatestRegionRouteMap();
  }

  public List<TRegionReplicaSet> getAllReplicaSets(String storageGroup) {
    return getPartitionManager().getAllReplicaSets(storageGroup);
  }

  public List<TSStatus> createTriggerOnDataNodes(
      TriggerInformation triggerInformation, Binary jarFile) throws IOException {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        nodeManager.getRegisteredDataNodeLocations();
    final TCreateTriggerInstanceReq request =
        new TCreateTriggerInstanceReq(triggerInformation.serialize());
    if (jarFile != null) {
      request.setJarFile(ByteBuffer.wrap(jarFile.getValues()));
    }

    AsyncClientHandler<TCreateTriggerInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.CREATE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> dropTriggerOnDataNodes(String triggerName, boolean needToDeleteJarFile) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        nodeManager.getRegisteredDataNodeLocations();
    final TDropTriggerInstanceReq request =
        new TDropTriggerInstanceReq(triggerName, needToDeleteJarFile);

    AsyncClientHandler<TDropTriggerInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.DROP_TRIGGER_INSTANCE, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> activeTriggerOnDataNodes(String triggerName) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        nodeManager.getRegisteredDataNodeLocations();
    final TActiveTriggerInstanceReq request = new TActiveTriggerInstanceReq(triggerName);

    AsyncClientHandler<TActiveTriggerInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.ACTIVE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> inactiveTriggerOnDataNodes(String triggerName) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        nodeManager.getRegisteredDataNodeLocations();
    final TInactiveTriggerInstanceReq request = new TInactiveTriggerInstanceReq(triggerName);

    AsyncClientHandler<TInactiveTriggerInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.INACTIVE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public LockQueue getNodeLock() {
    return nodeLock;
  }

  public LockQueue getPipeLock() {
    return pipeLock;
  }

  public ProcedureScheduler getScheduler() {
    return scheduler;
  }

  public LockQueue getRegionMigrateLock() {
    return dataNodeRemoveHandler.getRegionMigrateLock();
  }

  public ReentrantLock getSchedulerLock() {
    return schedulerLock;
  }

  public DataNodeRemoveHandler getDataNodeRemoveHandler() {
    return dataNodeRemoveHandler;
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
