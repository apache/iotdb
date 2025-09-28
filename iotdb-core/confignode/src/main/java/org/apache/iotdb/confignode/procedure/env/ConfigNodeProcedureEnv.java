1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure.env;
1
1import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
1import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
1import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
1import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
1import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
1import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
1import org.apache.iotdb.common.rpc.thrift.TSStatus;
1import org.apache.iotdb.commons.cluster.NodeStatus;
1import org.apache.iotdb.commons.cluster.NodeType;
1import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
1import org.apache.iotdb.commons.pipe.config.PipeConfig;
1import org.apache.iotdb.commons.trigger.TriggerInformation;
1import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
1import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
1import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
1import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
1import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
1import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
1import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
1import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
1import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
1import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
1import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
1import org.apache.iotdb.confignode.exception.AddConsensusGroupException;
1import org.apache.iotdb.confignode.exception.AddPeerException;
1import org.apache.iotdb.confignode.manager.ConfigManager;
1import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
1import org.apache.iotdb.confignode.manager.load.LoadManager;
1import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
1import org.apache.iotdb.confignode.manager.node.NodeManager;
1import org.apache.iotdb.confignode.manager.partition.PartitionManager;
1import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
1import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
1import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
1import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
1import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
1import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
1import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
1import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
1import org.apache.iotdb.consensus.exception.ConsensusException;
1import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
1import org.apache.iotdb.mpp.rpc.thrift.TActiveTriggerInstanceReq;
1import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
1import org.apache.iotdb.mpp.rpc.thrift.TCreatePipePluginInstanceReq;
1import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
1import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerInstanceReq;
1import org.apache.iotdb.mpp.rpc.thrift.TDropPipePluginInstanceReq;
1import org.apache.iotdb.mpp.rpc.thrift.TDropTriggerInstanceReq;
1import org.apache.iotdb.mpp.rpc.thrift.TInactiveTriggerInstanceReq;
1import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
1import org.apache.iotdb.mpp.rpc.thrift.TNotifyRegionMigrationReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
1import org.apache.iotdb.mpp.rpc.thrift.TPushMultiPipeMetaReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushMultiTopicMetaReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
1import org.apache.iotdb.mpp.rpc.thrift.TPushSingleConsumerGroupMetaReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushSinglePipeMetaReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushSingleTopicMetaReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaReq;
1import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaResp;
1import org.apache.iotdb.rpc.TSStatusCode;
1
1import org.apache.thrift.TException;
1import org.apache.tsfile.utils.Binary;
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.io.IOException;
1import java.nio.ByteBuffer;
1import java.util.ArrayList;
1import java.util.Arrays;
1import java.util.HashMap;
1import java.util.List;
1import java.util.Map;
1import java.util.concurrent.TimeUnit;
1import java.util.concurrent.locks.ReentrantLock;
1import java.util.stream.Collectors;
1
1public class ConfigNodeProcedureEnv {
1
1  private static final Logger LOG = LoggerFactory.getLogger(ConfigNodeProcedureEnv.class);
1
1  /** Add or remove node lock. */
1  private final LockQueue nodeLock = new LockQueue();
1
1  private final ReentrantLock schedulerLock = new ReentrantLock(true);
1
1  private final ReentrantLock submitRegionMigrateLock = new ReentrantLock(true);
1
1  private final ConfigManager configManager;
1
1  private final ProcedureScheduler scheduler;
1
1  private final RegionMaintainHandler regionMaintainHandler;
1
1  private final RemoveDataNodeHandler removeDataNodeHandler;
1
1  private final ReentrantLock removeConfigNodeLock;
1
1  public ConfigNodeProcedureEnv(ConfigManager configManager, ProcedureScheduler scheduler) {
1    this.configManager = configManager;
1    this.scheduler = scheduler;
1    this.regionMaintainHandler = new RegionMaintainHandler(configManager);
1    this.removeDataNodeHandler = new RemoveDataNodeHandler(configManager);
1    this.removeConfigNodeLock = new ReentrantLock();
1  }
1
1  public ConfigManager getConfigManager() {
1    return configManager;
1  }
1
1  /**
1   * Delete ConfigNode cache, includes {@link ClusterSchemaInfo} and {@link PartitionInfo}.
1   *
1   * @param name database name
1   * @param isGeneratedByPipe whether the deletion is triggered by pipe request
1   * @return tsStatus
1   */
1  public TSStatus deleteDatabaseConfig(final String name, final boolean isGeneratedByPipe) {
1    return getClusterSchemaManager()
1        .deleteDatabase(new DeleteDatabasePlan(name), isGeneratedByPipe);
1  }
1
1  /**
1   * Pre delete a database.
1   *
1   * @param preDeleteType execute/rollback
1   * @param deleteSgName database name
1   */
1  public void preDeleteDatabase(
1      final PreDeleteDatabasePlan.PreDeleteType preDeleteType, final String deleteSgName) {
1    getPartitionManager().preDeleteDatabase(deleteSgName, preDeleteType);
1  }
1
1  /**
1   * @param databaseName database name
1   * @return ALL SUCCESS OR NOT
1   * @throws IOException IOE
1   * @throws TException Thrift IOE
1   */
1  public boolean invalidateCache(final String databaseName) throws IOException, TException {
1    final List<TDataNodeConfiguration> allDataNodes = getNodeManager().getRegisteredDataNodes();
1    final TInvalidateCacheReq invalidateCacheReq = new TInvalidateCacheReq();
1    invalidateCacheReq.setStorageGroup(true);
1    invalidateCacheReq.setFullPath(databaseName);
1    for (final TDataNodeConfiguration dataNodeConfiguration : allDataNodes) {
1      final int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
1
1      // If the node is not alive, retry for up to 10 times
1      NodeStatus nodeStatus = getLoadManager().getNodeStatus(dataNodeId);
1      int retryNum = 10;
1      if (nodeStatus == NodeStatus.Unknown) {
1        for (int i = 0; i < retryNum && nodeStatus == NodeStatus.Unknown; i++) {
1          try {
1            TimeUnit.MILLISECONDS.sleep(500);
1          } catch (final InterruptedException e) {
1            LOG.error("Sleep failed in ConfigNodeProcedureEnv: ", e);
1            Thread.currentThread().interrupt();
1            break;
1          }
1          nodeStatus = getLoadManager().getNodeStatus(dataNodeId);
1        }
1      }
1
1      if (nodeStatus == NodeStatus.Unknown) {
1        LOG.warn(
1            "Invalidate cache failed, because DataNode {} is Unknown",
1            dataNodeConfiguration.getLocation().getInternalEndPoint());
1        return false;
1      }
1
1      // Always invalidate PartitionCache first
1      final TSStatus invalidatePartitionStatus =
1          (TSStatus)
1              SyncDataNodeClientPool.getInstance()
1                  .sendSyncRequestToDataNodeWithRetry(
1                      dataNodeConfiguration.getLocation().getInternalEndPoint(),
1                      invalidateCacheReq,
1                      CnToDnSyncRequestType.INVALIDATE_PARTITION_CACHE);
1
1      final TSStatus invalidateSchemaStatus =
1          (TSStatus)
1              SyncDataNodeClientPool.getInstance()
1                  .sendSyncRequestToDataNodeWithRetry(
1                      dataNodeConfiguration.getLocation().getInternalEndPoint(),
1                      invalidateCacheReq,
1                      CnToDnSyncRequestType.INVALIDATE_SCHEMA_CACHE);
1
1      if (!verifySucceed(invalidatePartitionStatus, invalidateSchemaStatus)) {
1        LOG.error(
1            "Invalidate cache failed, invalidate partition cache status is {}, invalidate schemaengine cache status is {}",
1            invalidatePartitionStatus,
1            invalidateSchemaStatus);
1        return false;
1      }
1    }
1    return true;
1  }
1
1  public boolean verifySucceed(TSStatus... status) {
1    return Arrays.stream(status)
1        .allMatch(tsStatus -> tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode());
1  }
1
1  /**
1   * Let the remotely new ConfigNode build the ConsensusGroup.
1   *
1   * <p>Actually, the parameter of this method can be empty, adding new raft peer to exist group
1   * should invoke createPeer(groupId, emptyList).
1   *
1   * @param tConfigNodeLocation New ConfigNode's location
1   */
1  public void addConsensusGroup(TConfigNodeLocation tConfigNodeLocation)
1      throws AddConsensusGroupException {
1    List<TConfigNodeLocation> configNodeLocations =
1        new ArrayList<>(configManager.getNodeManager().getRegisteredConfigNodes());
1    configNodeLocations.add(tConfigNodeLocation);
1    TSStatus status =
1        (TSStatus)
1            SyncConfigNodeClientPool.getInstance()
1                .sendSyncRequestToConfigNodeWithRetry(
1                    tConfigNodeLocation.getInternalEndPoint(),
1                    new TAddConsensusGroupReq(configNodeLocations),
1                    CnToCnNodeRequestType.ADD_CONSENSUS_GROUP);
1    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
1      throw new AddConsensusGroupException(tConfigNodeLocation);
1    }
1  }
1
1  /**
1   * Leader will add the new ConfigNode Peer into ConfigRegion.
1   *
1   * @param configNodeLocation The new ConfigNode
1   * @throws AddPeerException When addPeer doesn't success
1   */
1  public void addConfigNodePeer(TConfigNodeLocation configNodeLocation) throws AddPeerException {
1    configManager.getConsensusManager().addConfigNodePeer(configNodeLocation);
1  }
1
1  /**
1   * Remove peer in Leader node.
1   *
1   * @param tConfigNodeLocation node is removed
1   * @throws ProcedureException if failed status
1   */
1  public void removeConfigNodePeer(TConfigNodeLocation tConfigNodeLocation)
1      throws ProcedureException {
1    removeConfigNodeLock.lock();
1    TSStatus tsStatus;
1    try {
1      // Execute removePeer
1      if (getConsensusManager().removeConfigNodePeer(tConfigNodeLocation)) {
1        tsStatus = getConsensusManager().write(new RemoveConfigNodePlan(tConfigNodeLocation));
1      } else {
1        tsStatus =
1            new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
1                .setMessage(
1                    "Remove ConfigNode failed because update ConsensusGroup peer information failed.");
1      }
1    } catch (ConsensusException e) {
1      LOG.warn("Failed in the write API executing the consensus layer due to: ", e);
1      tsStatus = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
1      tsStatus.setMessage(e.getMessage());
1    }
1    try {
1      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
1        throw new ProcedureException(tsStatus.getMessage());
1      }
1    } finally {
1      removeConfigNodeLock.unlock();
1    }
1  }
1
1  /**
1   * Remove Consensus Group in removed node.
1   *
1   * @param removedConfigNode config node location
1   * @throws ProcedureException if failed status
1   */
1  public void deleteConfigNodePeer(TConfigNodeLocation removedConfigNode)
1      throws ProcedureException {
1    TSStatus tsStatus =
1        (TSStatus)
1            SyncConfigNodeClientPool.getInstance()
1                .sendSyncRequestToConfigNodeWithRetry(
1                    removedConfigNode.getInternalEndPoint(),
1                    removedConfigNode,
1                    CnToCnNodeRequestType.DELETE_CONFIG_NODE_PEER);
1    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
1      throw new ProcedureException(tsStatus.getMessage());
1    }
1  }
1
1  /**
1   * Stop ConfigNode and remove heartbeatCache.
1   *
1   * @param tConfigNodeLocation config node location
1   * @throws ProcedureException if failed status
1   */
1  public void stopAndClearConfigNode(TConfigNodeLocation tConfigNodeLocation)
1      throws ProcedureException {
1    TSStatus tsStatus =
1        (TSStatus)
1            SyncConfigNodeClientPool.getInstance()
1                .sendSyncRequestToConfigNodeWithRetry(
1                    tConfigNodeLocation.getInternalEndPoint(),
1                    tConfigNodeLocation,
1                    CnToCnNodeRequestType.STOP_AND_CLEAR_CONFIG_NODE);
1
1    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
1      throw new ProcedureException(tsStatus.getMessage());
1    }
1
1    getLoadManager().removeNodeCache(tConfigNodeLocation.getConfigNodeId());
1  }
1
1  /**
1   * Leader will record the new ConfigNode's information.
1   *
1   * @param configNodeLocation The new ConfigNode
1   * @param versionInfo The new ConfigNode's versionInfo
1   */
1  public void applyConfigNode(
1      TConfigNodeLocation configNodeLocation, TNodeVersionInfo versionInfo) {
1    configManager.getNodeManager().applyConfigNode(configNodeLocation, versionInfo);
1  }
1
1  /**
1   * Leader will notify the new ConfigNode that registration success.
1   *
1   * @param configNodeLocation The new ConfigNode
1   */
1  public void notifyRegisterSuccess(TConfigNodeLocation configNodeLocation) {
1    SyncConfigNodeClientPool.getInstance()
1        .sendSyncRequestToConfigNodeWithRetry(
1            configNodeLocation.getInternalEndPoint(),
1            null,
1            CnToCnNodeRequestType.NOTIFY_REGISTER_SUCCESS);
1  }
1
1  /**
1   * Create a new ConfigNodeHeartbeatCache
1   *
1   * @param nodeId the index of the new ConfigNode
1   */
1  public void createConfigNodeHeartbeatCache(int nodeId) {
1    getLoadManager().getLoadCache().createNodeHeartbeatCache(NodeType.ConfigNode, nodeId);
1    // TODO: invoke a force heartbeat to update new ConfigNode's status immediately
1  }
1
1  /**
1   * Do region creations and broadcast the {@link CreateRegionGroupsPlan}.
1   *
1   * @return Those RegionReplicas that failed to create
1   */
1  public Map<TConsensusGroupId, TRegionReplicaSet> doRegionCreation(
1      final TConsensusGroupType consensusGroupType,
1      final CreateRegionGroupsPlan createRegionGroupsPlan) {
1
1    // Prepare clientHandler
1    DataNodeAsyncRequestContext<?, TSStatus> clientHandler;
1    switch (consensusGroupType) {
1      case SchemaRegion:
1        clientHandler = getCreateSchemaRegionClientHandler(createRegionGroupsPlan);
1        break;
1      case DataRegion:
1      default:
1        clientHandler = getCreateDataRegionClientHandler(createRegionGroupsPlan);
1        break;
1    }
1    if (clientHandler.getRequestIndices().isEmpty()) {
1      return new HashMap<>();
1    }
1
1    // Send CreateRegion requests to DataNodes
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1
1    // Filter RegionGroups that weren't created successfully
1    int requestId = 0;
1    Map<Integer, TSStatus> responseMap = clientHandler.getResponseMap();
1    Map<TConsensusGroupId, TRegionReplicaSet> failedRegions = new HashMap<>();
1    for (List<TRegionReplicaSet> regionReplicaSets :
1        createRegionGroupsPlan.getRegionGroupMap().values()) {
1      for (final TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
1        for (final TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
1          if (responseMap.get(requestId).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
1            failedRegions
1                .computeIfAbsent(
1                    regionReplicaSet.getRegionId(),
1                    empty -> new TRegionReplicaSet().setRegionId(regionReplicaSet.getRegionId()))
1                .addToDataNodeLocations(dataNodeLocation);
1          }
1          requestId += 1;
1        }
1      }
1    }
1    return failedRegions;
1  }
1
1  private DataNodeAsyncRequestContext<TCreateSchemaRegionReq, TSStatus>
1      getCreateSchemaRegionClientHandler(CreateRegionGroupsPlan createRegionGroupsPlan) {
1    DataNodeAsyncRequestContext<TCreateSchemaRegionReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.CREATE_SCHEMA_REGION);
1
1    int requestId = 0;
1    for (Map.Entry<String, List<TRegionReplicaSet>> sgRegionsEntry :
1        createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
1      String storageGroup = sgRegionsEntry.getKey();
1      List<TRegionReplicaSet> regionReplicaSets = sgRegionsEntry.getValue();
1      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
1        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
1          clientHandler.putRequest(
1              requestId, genCreateSchemaRegionReq(storageGroup, regionReplicaSet));
1          clientHandler.putNodeLocation(requestId, dataNodeLocation);
1          requestId += 1;
1        }
1      }
1    }
1
1    return clientHandler;
1  }
1
1  private DataNodeAsyncRequestContext<TCreateDataRegionReq, TSStatus>
1      getCreateDataRegionClientHandler(CreateRegionGroupsPlan createRegionGroupsPlan) {
1    DataNodeAsyncRequestContext<TCreateDataRegionReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.CREATE_DATA_REGION);
1
1    int requestId = 0;
1    for (Map.Entry<String, List<TRegionReplicaSet>> sgRegionsEntry :
1        createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
1      String storageGroup = sgRegionsEntry.getKey();
1      List<TRegionReplicaSet> regionReplicaSets = sgRegionsEntry.getValue();
1      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
1        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
1          clientHandler.putRequest(
1              requestId, genCreateDataRegionReq(storageGroup, regionReplicaSet));
1          clientHandler.putNodeLocation(requestId, dataNodeLocation);
1          requestId += 1;
1        }
1      }
1    }
1
1    return clientHandler;
1  }
1
1  private TCreateSchemaRegionReq genCreateSchemaRegionReq(
1      String storageGroup, TRegionReplicaSet regionReplicaSet) {
1    TCreateSchemaRegionReq req = new TCreateSchemaRegionReq();
1    req.setStorageGroup(storageGroup);
1    req.setRegionReplicaSet(regionReplicaSet);
1    return req;
1  }
1
1  private TCreateDataRegionReq genCreateDataRegionReq(
1      String storageGroup, TRegionReplicaSet regionReplicaSet) {
1    TCreateDataRegionReq req = new TCreateDataRegionReq();
1    req.setStorageGroup(storageGroup);
1    req.setRegionReplicaSet(regionReplicaSet);
1    return req;
1  }
1
1  public List<TSStatus> notifyRegionMigrationToAllDataNodes(
1      TConsensusGroupId consensusGroupId, boolean isStart) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TNotifyRegionMigrationReq request =
1        new TNotifyRegionMigrationReq(
1            configManager
1                .getConsensusManager()
1                .getConsensusImpl()
1                .getLogicalClock(ConfigNodeInfo.CONFIG_REGION_ID),
1            System.nanoTime(),
1            configManager.getProcedureManager().getRegionOperationConsensusIds());
1    request.setRegionId(consensusGroupId);
1    request.setIsStart(isStart);
1
1    final DataNodeAsyncRequestContext<TNotifyRegionMigrationReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.NOTIFY_REGION_MIGRATION, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList();
1  }
1
1  public void persistRegionGroup(CreateRegionGroupsPlan createRegionGroupsPlan) {
1    // Persist the allocation result
1    try {
1      getConsensusManager().write(createRegionGroupsPlan);
1    } catch (ConsensusException e) {
1      LOG.warn("Failed in the write API executing the consensus layer due to: ", e);
1    }
1  }
1
1  /**
1   * Force activating RegionGroup by setting status to Running, therefore the ConfigNode-leader can
1   * select leader for it and use it to allocate new Partitions
1   *
1   * @param activateRegionGroupMap Map<Database, Map<RegionGroupId, Map<DataNodeId, activate
1   *     heartbeat sample>>>
1   */
1  public void activateRegionGroup(
1      Map<String, Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>>>
1          activateRegionGroupMap) {
1    // Create RegionGroup heartbeat Caches
1    activateRegionGroupMap.forEach(
1        (database, regionGroupSampleMap) ->
1            regionGroupSampleMap.forEach(
1                (regionGroupId, regionSampleMap) ->
1                    getLoadManager()
1                        .getLoadCache()
1                        .createRegionGroupHeartbeatCache(
1                            database, regionGroupId, regionSampleMap.keySet())));
1    // Force update first heartbeat samples
1    getLoadManager()
1        .forceUpdateRegionGroupCache(
1            activateRegionGroupMap.values().stream()
1                .flatMap(innerMap -> innerMap.entrySet().stream())
1                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b)));
1    // Wait for leader and priority redistribution
1    getLoadManager()
1        .waitForRegionGroupReady(
1            activateRegionGroupMap.values().stream()
1                .flatMap(innterMap -> innterMap.keySet().stream())
1                .collect(Collectors.toList()));
1  }
1
1  public List<TRegionReplicaSet> getAllReplicaSets(String storageGroup) {
1    return getPartitionManager().getAllReplicaSets(storageGroup);
1  }
1
1  public List<TSStatus> createTriggerOnDataNodes(
1      TriggerInformation triggerInformation, Binary jarFile) throws IOException {
1    NodeManager nodeManager = configManager.getNodeManager();
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        nodeManager.getRegisteredDataNodeLocations();
1    final TCreateTriggerInstanceReq request =
1        new TCreateTriggerInstanceReq(triggerInformation.serialize());
1    if (jarFile != null) {
1      request.setJarFile(ByteBuffer.wrap(jarFile.getValues()));
1    }
1
1    DataNodeAsyncRequestContext<TCreateTriggerInstanceReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.CREATE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList();
1  }
1
1  public List<TSStatus> dropTriggerOnDataNodes(String triggerName, boolean needToDeleteJarFile) {
1    NodeManager nodeManager = configManager.getNodeManager();
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        nodeManager.getRegisteredDataNodeLocations();
1    final TDropTriggerInstanceReq request =
1        new TDropTriggerInstanceReq(triggerName, needToDeleteJarFile);
1
1    DataNodeAsyncRequestContext<TDropTriggerInstanceReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.DROP_TRIGGER_INSTANCE, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList();
1  }
1
1  public List<TSStatus> activeTriggerOnDataNodes(String triggerName) {
1    NodeManager nodeManager = configManager.getNodeManager();
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        nodeManager.getRegisteredDataNodeLocations();
1    final TActiveTriggerInstanceReq request = new TActiveTriggerInstanceReq(triggerName);
1
1    DataNodeAsyncRequestContext<TActiveTriggerInstanceReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.ACTIVE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList();
1  }
1
1  public List<TSStatus> inactiveTriggerOnDataNodes(String triggerName) {
1    NodeManager nodeManager = configManager.getNodeManager();
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        nodeManager.getRegisteredDataNodeLocations();
1    final TInactiveTriggerInstanceReq request = new TInactiveTriggerInstanceReq(triggerName);
1
1    DataNodeAsyncRequestContext<TInactiveTriggerInstanceReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.INACTIVE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList();
1  }
1
1  public List<TSStatus> createPipePluginOnDataNodes(PipePluginMeta pipePluginMeta, byte[] jarFile)
1      throws IOException {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TCreatePipePluginInstanceReq request =
1        new TCreatePipePluginInstanceReq(pipePluginMeta.serialize(), ByteBuffer.wrap(jarFile));
1
1    final DataNodeAsyncRequestContext<TCreatePipePluginInstanceReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.CREATE_PIPE_PLUGIN, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList();
1  }
1
1  public List<TSStatus> dropPipePluginOnDataNodes(
1      String pipePluginName, boolean needToDeleteJarFile) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TDropPipePluginInstanceReq request =
1        new TDropPipePluginInstanceReq(pipePluginName, needToDeleteJarFile);
1
1    DataNodeAsyncRequestContext<TDropPipePluginInstanceReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.DROP_PIPE_PLUGIN, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList();
1  }
1
1  public Map<Integer, TPushPipeMetaResp> pushAllPipeMetaToDataNodes(
1      List<ByteBuffer> pipeMetaBinaryList) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushPipeMetaReq request = new TPushPipeMetaReq().setPipeMetas(pipeMetaBinaryList);
1
1    final DataNodeAsyncRequestContext<TPushPipeMetaReq, TPushPipeMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.PIPE_PUSH_ALL_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes(ByteBuffer pipeMetaBinary) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushSinglePipeMetaReq request = new TPushSinglePipeMetaReq().setPipeMeta(pipeMetaBinary);
1
1    final DataNodeAsyncRequestContext<TPushSinglePipeMetaReq, TPushPipeMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.PIPE_PUSH_SINGLE_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public Map<Integer, TPushPipeMetaResp> dropSinglePipeOnDataNodes(String pipeNameToDrop) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushSinglePipeMetaReq request =
1        new TPushSinglePipeMetaReq().setPipeNameToDrop(pipeNameToDrop);
1
1    final DataNodeAsyncRequestContext<TPushSinglePipeMetaReq, TPushPipeMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.PIPE_PUSH_SINGLE_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public Map<Integer, TPushPipeMetaResp> pushMultiPipeMetaToDataNodes(
1      List<ByteBuffer> pipeMetaBinaryList) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushMultiPipeMetaReq request =
1        new TPushMultiPipeMetaReq().setPipeMetas(pipeMetaBinaryList);
1
1    final DataNodeAsyncRequestContext<TPushMultiPipeMetaReq, TPushPipeMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.PIPE_PUSH_MULTI_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public Map<Integer, TPushPipeMetaResp> dropMultiPipeOnDataNodes(List<String> pipeNamesToDrop) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushMultiPipeMetaReq request =
1        new TPushMultiPipeMetaReq().setPipeNamesToDrop(pipeNamesToDrop);
1
1    final DataNodeAsyncRequestContext<TPushMultiPipeMetaReq, TPushPipeMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.PIPE_PUSH_MULTI_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public Map<Integer, TPushTopicMetaResp> pushAllTopicMetaToDataNodes(
1      List<ByteBuffer> topicMetaBinaryList) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushTopicMetaReq request = new TPushTopicMetaReq().setTopicMetas(topicMetaBinaryList);
1
1    final DataNodeAsyncRequestContext<TPushTopicMetaReq, TPushTopicMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.TOPIC_PUSH_ALL_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public List<TSStatus> pushSingleTopicOnDataNode(ByteBuffer topicMeta) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushSingleTopicMetaReq request = new TPushSingleTopicMetaReq().setTopicMeta(topicMeta);
1
1    final DataNodeAsyncRequestContext<TPushSingleTopicMetaReq, TPushTopicMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.TOPIC_PUSH_SINGLE_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList().stream()
1        .map(TPushTopicMetaResp::getStatus)
1        .collect(Collectors.toList());
1  }
1
1  public List<TSStatus> dropSingleTopicOnDataNode(String topicNameToDrop) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushSingleTopicMetaReq request =
1        new TPushSingleTopicMetaReq().setTopicNameToDrop(topicNameToDrop);
1
1    final DataNodeAsyncRequestContext<TPushSingleTopicMetaReq, TPushTopicMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.TOPIC_PUSH_SINGLE_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList().stream()
1        .map(TPushTopicMetaResp::getStatus)
1        .collect(Collectors.toList());
1  }
1
1  public Map<Integer, TPushTopicMetaResp> pushMultiTopicMetaToDataNodes(
1      List<ByteBuffer> topicMetaBinaryList) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushMultiTopicMetaReq request =
1        new TPushMultiTopicMetaReq().setTopicMetas(topicMetaBinaryList);
1
1    final DataNodeAsyncRequestContext<TPushMultiTopicMetaReq, TPushTopicMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.TOPIC_PUSH_MULTI_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public Map<Integer, TPushTopicMetaResp> dropMultiTopicOnDataNodes(List<String> topicNamesToDrop) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushMultiTopicMetaReq request =
1        new TPushMultiTopicMetaReq().setTopicNamesToDrop(topicNamesToDrop);
1
1    final DataNodeAsyncRequestContext<TPushMultiTopicMetaReq, TPushTopicMetaResp> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.TOPIC_PUSH_MULTI_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public Map<Integer, TPushConsumerGroupMetaResp> pushAllConsumerGroupMetaToDataNodes(
1      List<ByteBuffer> consumerGroupMetaBinaryList) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushConsumerGroupMetaReq request =
1        new TPushConsumerGroupMetaReq().setConsumerGroupMetas(consumerGroupMetaBinaryList);
1
1    final DataNodeAsyncRequestContext<TPushConsumerGroupMetaReq, TPushConsumerGroupMetaResp>
1        clientHandler =
1            new DataNodeAsyncRequestContext<>(
1                CnToDnAsyncRequestType.CONSUMER_GROUP_PUSH_ALL_META, request, dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
1            clientHandler,
1            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
1    return clientHandler.getResponseMap();
1  }
1
1  public List<TSStatus> pushSingleConsumerGroupOnDataNode(ByteBuffer consumerGroupMeta) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushSingleConsumerGroupMetaReq request =
1        new TPushSingleConsumerGroupMetaReq().setConsumerGroupMeta(consumerGroupMeta);
1
1    final DataNodeAsyncRequestContext<TPushSingleConsumerGroupMetaReq, TPushConsumerGroupMetaResp>
1        clientHandler =
1            new DataNodeAsyncRequestContext<>(
1                CnToDnAsyncRequestType.CONSUMER_GROUP_PUSH_SINGLE_META,
1                request,
1                dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList().stream()
1        .map(TPushConsumerGroupMetaResp::getStatus)
1        .collect(Collectors.toList());
1  }
1
1  public List<TSStatus> dropSingleConsumerGroupOnDataNode(String consumerGroupNameToDrop) {
1    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
1        configManager.getNodeManager().getRegisteredDataNodeLocations();
1    final TPushSingleConsumerGroupMetaReq request =
1        new TPushSingleConsumerGroupMetaReq().setConsumerGroupNameToDrop(consumerGroupNameToDrop);
1
1    final DataNodeAsyncRequestContext<TPushSingleConsumerGroupMetaReq, TPushConsumerGroupMetaResp>
1        clientHandler =
1            new DataNodeAsyncRequestContext<>(
1                CnToDnAsyncRequestType.CONSUMER_GROUP_PUSH_SINGLE_META,
1                request,
1                dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseList().stream()
1        .map(TPushConsumerGroupMetaResp::getStatus)
1        .collect(Collectors.toList());
1  }
1
1  public LockQueue getNodeLock() {
1    return nodeLock;
1  }
1
1  public ProcedureScheduler getScheduler() {
1    return scheduler;
1  }
1
1  public ReentrantLock getSchedulerLock() {
1    return schedulerLock;
1  }
1
1  public ReentrantLock getSubmitRegionMigrateLock() {
1    return submitRegionMigrateLock;
1  }
1
1  public RegionMaintainHandler getRegionMaintainHandler() {
1    return regionMaintainHandler;
1  }
1
1  public RemoveDataNodeHandler getRemoveDataNodeHandler() {
1    return removeDataNodeHandler;
1  }
1
1  private ConsensusManager getConsensusManager() {
1    return configManager.getConsensusManager();
1  }
1
1  private NodeManager getNodeManager() {
1    return configManager.getNodeManager();
1  }
1
1  private ClusterSchemaManager getClusterSchemaManager() {
1    return configManager.getClusterSchemaManager();
1  }
1
1  private PartitionManager getPartitionManager() {
1    return configManager.getPartitionManager();
1  }
1
1  private LoadManager getLoadManager() {
1    return configManager.getLoadManager();
1  }
1}
1