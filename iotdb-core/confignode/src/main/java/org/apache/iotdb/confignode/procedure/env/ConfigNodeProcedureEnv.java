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
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.AddConsensusGroupException;
import org.apache.iotdb.confignode.exception.AddPeerException;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TActiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropPipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInactiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleConsumerGroupMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSinglePipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.Binary;
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
import java.util.stream.Collectors;

public class ConfigNodeProcedureEnv {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigNodeProcedureEnv.class);

  /** Add or remove node lock. */
  private final LockQueue nodeLock = new LockQueue();

  private final ReentrantLock schedulerLock = new ReentrantLock(true);

  private final ReentrantLock submitRegionMigrateLock = new ReentrantLock(true);

  private final ConfigManager configManager;

  private final ProcedureScheduler scheduler;

  private final RegionMaintainHandler regionMaintainHandler;

  private final RemoveDataNodeHandler removeDataNodeHandler;

  private final ReentrantLock removeConfigNodeLock;

  public ConfigNodeProcedureEnv(ConfigManager configManager, ProcedureScheduler scheduler) {
    this.configManager = configManager;
    this.scheduler = scheduler;
    this.regionMaintainHandler = new RegionMaintainHandler(configManager);
    this.removeDataNodeHandler = new RemoveDataNodeHandler(configManager);
    this.removeConfigNodeLock = new ReentrantLock();
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  /**
   * Delete ConfigNode cache, includes {@link ClusterSchemaInfo} and {@link PartitionInfo}.
   *
   * @param name database name
   * @param isGeneratedByPipe whether the deletion is triggered by pipe request
   * @return tsStatus
   */
  public TSStatus deleteDatabaseConfig(String name, boolean isGeneratedByPipe) {
    DeleteDatabasePlan deleteDatabasePlan = new DeleteDatabasePlan(name);
    return getClusterSchemaManager().deleteDatabase(deleteDatabasePlan, isGeneratedByPipe);
  }

  /**
   * Pre delete a database.
   *
   * @param preDeleteType execute/rollback
   * @param deleteSgName database name
   */
  public void preDeleteDatabase(
      PreDeleteDatabasePlan.PreDeleteType preDeleteType, String deleteSgName) {
    getPartitionManager().preDeleteDatabase(deleteSgName, preDeleteType);
  }

  /**
   * @param storageGroupName database name
   * @return ALL SUCCESS OR NOT
   * @throws IOException IOE
   * @throws TException Thrift IOE
   */
  public boolean invalidateCache(String storageGroupName) throws IOException, TException {
    List<TDataNodeConfiguration> allDataNodes = getNodeManager().getRegisteredDataNodes();
    TInvalidateCacheReq invalidateCacheReq = new TInvalidateCacheReq();
    invalidateCacheReq.setStorageGroup(true);
    invalidateCacheReq.setFullPath(storageGroupName);
    for (TDataNodeConfiguration dataNodeConfiguration : allDataNodes) {
      int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();

      // If the node is not alive, sleep 1 second and try again
      NodeStatus nodeStatus = getLoadManager().getNodeStatus(dataNodeId);
      if (nodeStatus == NodeStatus.Unknown) {
        try {
          TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
          LOG.error("Sleep failed in ConfigNodeProcedureEnv: ", e);
          Thread.currentThread().interrupt();
        }
        nodeStatus = getLoadManager().getNodeStatus(dataNodeId);
      }

      if (nodeStatus == NodeStatus.Running) {
        // Always invalidate PartitionCache first
        final TSStatus invalidatePartitionStatus =
            (TSStatus)
                SyncDataNodeClientPool.getInstance()
                    .sendSyncRequestToDataNodeWithRetry(
                        dataNodeConfiguration.getLocation().getInternalEndPoint(),
                        invalidateCacheReq,
                        CnToDnSyncRequestType.INVALIDATE_PARTITION_CACHE);

        final TSStatus invalidateSchemaStatus =
            (TSStatus)
                SyncDataNodeClientPool.getInstance()
                    .sendSyncRequestToDataNodeWithRetry(
                        dataNodeConfiguration.getLocation().getInternalEndPoint(),
                        invalidateCacheReq,
                        CnToDnSyncRequestType.INVALIDATE_SCHEMA_CACHE);

        if (!verifySucceed(invalidatePartitionStatus, invalidateSchemaStatus)) {
          LOG.error(
              "Invalidate cache failed, invalidate partition cache status is {}, invalidate schemaengine cache status is {}",
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

  /**
   * Let the remotely new ConfigNode build the ConsensusGroup.
   *
   * <p>Actually, the parameter of this method can be empty, adding new raft peer to exist group
   * should invoke createPeer(groupId, emptyList).
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
                    CnToCnNodeRequestType.ADD_CONSENSUS_GROUP);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AddConsensusGroupException(tConfigNodeLocation);
    }
  }

  /**
   * Leader will add the new ConfigNode Peer into ConfigRegion.
   *
   * @param configNodeLocation The new ConfigNode
   * @throws AddPeerException When addPeer doesn't success
   */
  public void addConfigNodePeer(TConfigNodeLocation configNodeLocation) throws AddPeerException {
    configManager.getConsensusManager().addConfigNodePeer(configNodeLocation);
  }

  /**
   * Remove peer in Leader node.
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
        tsStatus = getConsensusManager().write(new RemoveConfigNodePlan(tConfigNodeLocation));
      } else {
        tsStatus =
            new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
                .setMessage(
                    "Remove ConfigNode failed because update ConsensusGroup peer information failed.");
      }
    } catch (ConsensusException e) {
      LOG.warn("Failed in the write API executing the consensus layer due to: ", e);
      tsStatus = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      tsStatus.setMessage(e.getMessage());
    }
    try {
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new ProcedureException(tsStatus.getMessage());
      }
    } finally {
      removeConfigNodeLock.unlock();
    }
  }

  /**
   * Remove Consensus Group in removed node.
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
                    CnToCnNodeRequestType.DELETE_CONFIG_NODE_PEER);
    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ProcedureException(tsStatus.getMessage());
    }
  }

  /**
   * Stop ConfigNode and remove heartbeatCache.
   *
   * @param tConfigNodeLocation config node location
   * @throws ProcedureException if failed status
   */
  public void stopConfigNode(TConfigNodeLocation tConfigNodeLocation) throws ProcedureException {
    TSStatus tsStatus =
        (TSStatus)
            SyncConfigNodeClientPool.getInstance()
                .sendSyncRequestToConfigNodeWithRetry(
                    tConfigNodeLocation.getInternalEndPoint(),
                    tConfigNodeLocation,
                    CnToCnNodeRequestType.STOP_CONFIG_NODE);

    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ProcedureException(tsStatus.getMessage());
    }

    getLoadManager().removeNodeCache(tConfigNodeLocation.getConfigNodeId());
  }

  /**
   * Leader will record the new ConfigNode's information.
   *
   * @param configNodeLocation The new ConfigNode
   * @param versionInfo The new ConfigNode's versionInfo
   */
  public void applyConfigNode(
      TConfigNodeLocation configNodeLocation, TNodeVersionInfo versionInfo) {
    configManager.getNodeManager().applyConfigNode(configNodeLocation, versionInfo);
  }

  /**
   * Leader will notify the new ConfigNode that registration success.
   *
   * @param configNodeLocation The new ConfigNode
   */
  public void notifyRegisterSuccess(TConfigNodeLocation configNodeLocation) {
    SyncConfigNodeClientPool.getInstance()
        .sendSyncRequestToConfigNodeWithRetry(
            configNodeLocation.getInternalEndPoint(),
            null,
            CnToCnNodeRequestType.NOTIFY_REGISTER_SUCCESS);
  }

  /**
   * Create a new ConfigNodeHeartbeatCache
   *
   * @param nodeId the index of the new ConfigNode
   */
  public void createConfigNodeHeartbeatCache(int nodeId) {
    getLoadManager().getLoadCache().createNodeHeartbeatCache(NodeType.ConfigNode, nodeId);
    // TODO: invoke a force heartbeat to update new ConfigNode's status immediately
  }

  /**
   * Do region creations and broadcast the {@link CreateRegionGroupsPlan}.
   *
   * @return Those RegionReplicas that failed to create
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> doRegionCreation(
      TConsensusGroupType consensusGroupType, CreateRegionGroupsPlan createRegionGroupsPlan) {

    // Prepare clientHandler
    DataNodeAsyncRequestContext<?, TSStatus> clientHandler;
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
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);

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

  private DataNodeAsyncRequestContext<TCreateSchemaRegionReq, TSStatus>
      getCreateSchemaRegionClientHandler(CreateRegionGroupsPlan createRegionGroupsPlan) {
    DataNodeAsyncRequestContext<TCreateSchemaRegionReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.CREATE_SCHEMA_REGION);

    int requestId = 0;
    for (Map.Entry<String, List<TRegionReplicaSet>> sgRegionsEntry :
        createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
      String storageGroup = sgRegionsEntry.getKey();
      List<TRegionReplicaSet> regionReplicaSets = sgRegionsEntry.getValue();
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          clientHandler.putRequest(
              requestId, genCreateSchemaRegionReq(storageGroup, regionReplicaSet));
          clientHandler.putNodeLocation(requestId, dataNodeLocation);
          requestId += 1;
        }
      }
    }

    return clientHandler;
  }

  private DataNodeAsyncRequestContext<TCreateDataRegionReq, TSStatus>
      getCreateDataRegionClientHandler(CreateRegionGroupsPlan createRegionGroupsPlan) {
    DataNodeAsyncRequestContext<TCreateDataRegionReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.CREATE_DATA_REGION);

    int requestId = 0;
    for (Map.Entry<String, List<TRegionReplicaSet>> sgRegionsEntry :
        createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
      String storageGroup = sgRegionsEntry.getKey();
      List<TRegionReplicaSet> regionReplicaSets = sgRegionsEntry.getValue();
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          clientHandler.putRequest(
              requestId, genCreateDataRegionReq(storageGroup, regionReplicaSet));
          clientHandler.putNodeLocation(requestId, dataNodeLocation);
          requestId += 1;
        }
      }
    }

    return clientHandler;
  }

  private TCreateSchemaRegionReq genCreateSchemaRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet) {
    TCreateSchemaRegionReq req = new TCreateSchemaRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    return req;
  }

  private TCreateDataRegionReq genCreateDataRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet) {
    TCreateDataRegionReq req = new TCreateDataRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    return req;
  }

  public void persistRegionGroup(CreateRegionGroupsPlan createRegionGroupsPlan) {
    // Persist the allocation result
    try {
      getConsensusManager().write(createRegionGroupsPlan);
    } catch (ConsensusException e) {
      LOG.warn("Failed in the write API executing the consensus layer due to: ", e);
    }
  }

  /**
   * Force activating RegionGroup by setting status to Running, therefore the ConfigNode-leader can
   * select leader for it and use it to allocate new Partitions
   *
   * @param activateRegionGroupMap Map<Database, Map<RegionGroupId, Map<DataNodeId, activate
   *     heartbeat sample>>>
   */
  public void activateRegionGroup(
      Map<String, Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>>>
          activateRegionGroupMap) {
    // Create RegionGroup heartbeat Caches
    activateRegionGroupMap.forEach(
        (database, regionGroupSampleMap) ->
            regionGroupSampleMap.forEach(
                (regionGroupId, regionSampleMap) ->
                    getLoadManager()
                        .getLoadCache()
                        .createRegionGroupHeartbeatCache(
                            database, regionGroupId, regionSampleMap.keySet())));
    // Force update first heartbeat samples
    getLoadManager()
        .forceUpdateRegionGroupCache(
            activateRegionGroupMap.values().stream()
                .flatMap(innerMap -> innerMap.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b)));
    // Wait for leader and priority redistribution
    getLoadManager()
        .waitForRegionGroupReady(
            activateRegionGroupMap.values().stream()
                .flatMap(innterMap -> innterMap.keySet().stream())
                .collect(Collectors.toList()));
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

    DataNodeAsyncRequestContext<TCreateTriggerInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.CREATE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> dropTriggerOnDataNodes(String triggerName, boolean needToDeleteJarFile) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        nodeManager.getRegisteredDataNodeLocations();
    final TDropTriggerInstanceReq request =
        new TDropTriggerInstanceReq(triggerName, needToDeleteJarFile);

    DataNodeAsyncRequestContext<TDropTriggerInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.DROP_TRIGGER_INSTANCE, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> activeTriggerOnDataNodes(String triggerName) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        nodeManager.getRegisteredDataNodeLocations();
    final TActiveTriggerInstanceReq request = new TActiveTriggerInstanceReq(triggerName);

    DataNodeAsyncRequestContext<TActiveTriggerInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.ACTIVE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> inactiveTriggerOnDataNodes(String triggerName) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        nodeManager.getRegisteredDataNodeLocations();
    final TInactiveTriggerInstanceReq request = new TInactiveTriggerInstanceReq(triggerName);

    DataNodeAsyncRequestContext<TInactiveTriggerInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.INACTIVE_TRIGGER_INSTANCE, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> createPipePluginOnDataNodes(PipePluginMeta pipePluginMeta, byte[] jarFile)
      throws IOException {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TCreatePipePluginInstanceReq request =
        new TCreatePipePluginInstanceReq(pipePluginMeta.serialize(), ByteBuffer.wrap(jarFile));

    final DataNodeAsyncRequestContext<TCreatePipePluginInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.CREATE_PIPE_PLUGIN, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> dropPipePluginOnDataNodes(
      String pipePluginName, boolean needToDeleteJarFile) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TDropPipePluginInstanceReq request =
        new TDropPipePluginInstanceReq(pipePluginName, needToDeleteJarFile);

    DataNodeAsyncRequestContext<TDropPipePluginInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.DROP_PIPE_PLUGIN, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public Map<Integer, TPushPipeMetaResp> pushAllPipeMetaToDataNodes(
      List<ByteBuffer> pipeMetaBinaryList) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushPipeMetaReq request = new TPushPipeMetaReq().setPipeMetas(pipeMetaBinaryList);

    final DataNodeAsyncRequestContext<TPushPipeMetaReq, TPushPipeMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.PIPE_PUSH_ALL_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes(ByteBuffer pipeMetaBinary) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSinglePipeMetaReq request = new TPushSinglePipeMetaReq().setPipeMeta(pipeMetaBinary);

    final DataNodeAsyncRequestContext<TPushSinglePipeMetaReq, TPushPipeMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.PIPE_PUSH_SINGLE_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushPipeMetaResp> dropSinglePipeOnDataNodes(String pipeNameToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSinglePipeMetaReq request =
        new TPushSinglePipeMetaReq().setPipeNameToDrop(pipeNameToDrop);

    final DataNodeAsyncRequestContext<TPushSinglePipeMetaReq, TPushPipeMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.PIPE_PUSH_SINGLE_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushPipeMetaResp> pushMultiPipeMetaToDataNodes(
      List<ByteBuffer> pipeMetaBinaryList) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushMultiPipeMetaReq request =
        new TPushMultiPipeMetaReq().setPipeMetas(pipeMetaBinaryList);

    final DataNodeAsyncRequestContext<TPushMultiPipeMetaReq, TPushPipeMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.PIPE_PUSH_MULTI_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushPipeMetaResp> dropMultiPipeOnDataNodes(List<String> pipeNamesToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushMultiPipeMetaReq request =
        new TPushMultiPipeMetaReq().setPipeNamesToDrop(pipeNamesToDrop);

    final DataNodeAsyncRequestContext<TPushMultiPipeMetaReq, TPushPipeMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.PIPE_PUSH_MULTI_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushTopicMetaResp> pushAllTopicMetaToDataNodes(
      List<ByteBuffer> topicMetaBinaryList) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushTopicMetaReq request = new TPushTopicMetaReq().setTopicMetas(topicMetaBinaryList);

    final DataNodeAsyncRequestContext<TPushTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.TOPIC_PUSH_ALL_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public List<TSStatus> pushSingleTopicOnDataNode(ByteBuffer topicMeta) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSingleTopicMetaReq request = new TPushSingleTopicMetaReq().setTopicMeta(topicMeta);

    final DataNodeAsyncRequestContext<TPushSingleTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.TOPIC_PUSH_SINGLE_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList().stream()
        .map(TPushTopicMetaResp::getStatus)
        .collect(Collectors.toList());
  }

  public List<TSStatus> dropSingleTopicOnDataNode(String topicNameToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSingleTopicMetaReq request =
        new TPushSingleTopicMetaReq().setTopicNameToDrop(topicNameToDrop);

    final DataNodeAsyncRequestContext<TPushSingleTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.TOPIC_PUSH_SINGLE_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList().stream()
        .map(TPushTopicMetaResp::getStatus)
        .collect(Collectors.toList());
  }

  public Map<Integer, TPushTopicMetaResp> pushMultiTopicMetaToDataNodes(
      List<ByteBuffer> topicMetaBinaryList) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushMultiTopicMetaReq request =
        new TPushMultiTopicMetaReq().setTopicMetas(topicMetaBinaryList);

    final DataNodeAsyncRequestContext<TPushMultiTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.TOPIC_PUSH_MULTI_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushTopicMetaResp> dropMultiTopicOnDataNodes(List<String> topicNamesToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushMultiTopicMetaReq request =
        new TPushMultiTopicMetaReq().setTopicNamesToDrop(topicNamesToDrop);

    final DataNodeAsyncRequestContext<TPushMultiTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.TOPIC_PUSH_MULTI_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushConsumerGroupMetaResp> pushAllConsumerGroupMetaToDataNodes(
      List<ByteBuffer> consumerGroupMetaBinaryList) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushConsumerGroupMetaReq request =
        new TPushConsumerGroupMetaReq().setConsumerGroupMetas(consumerGroupMetaBinaryList);

    final DataNodeAsyncRequestContext<TPushConsumerGroupMetaReq, TPushConsumerGroupMetaResp>
        clientHandler =
            new DataNodeAsyncRequestContext<>(
                CnToDnAsyncRequestType.CONSUMER_GROUP_PUSH_ALL_META, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public List<TSStatus> pushSingleConsumerGroupOnDataNode(ByteBuffer consumerGroupMeta) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSingleConsumerGroupMetaReq request =
        new TPushSingleConsumerGroupMetaReq().setConsumerGroupMeta(consumerGroupMeta);

    final DataNodeAsyncRequestContext<TPushSingleConsumerGroupMetaReq, TPushConsumerGroupMetaResp>
        clientHandler =
            new DataNodeAsyncRequestContext<>(
                CnToDnAsyncRequestType.CONSUMER_GROUP_PUSH_SINGLE_META,
                request,
                dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList().stream()
        .map(TPushConsumerGroupMetaResp::getStatus)
        .collect(Collectors.toList());
  }

  public List<TSStatus> dropSingleConsumerGroupOnDataNode(String consumerGroupNameToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSingleConsumerGroupMetaReq request =
        new TPushSingleConsumerGroupMetaReq().setConsumerGroupNameToDrop(consumerGroupNameToDrop);

    final DataNodeAsyncRequestContext<TPushSingleConsumerGroupMetaReq, TPushConsumerGroupMetaResp>
        clientHandler =
            new DataNodeAsyncRequestContext<>(
                CnToDnAsyncRequestType.CONSUMER_GROUP_PUSH_SINGLE_META,
                request,
                dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList().stream()
        .map(TPushConsumerGroupMetaResp::getStatus)
        .collect(Collectors.toList());
  }

  public LockQueue getNodeLock() {
    return nodeLock;
  }

  public ProcedureScheduler getScheduler() {
    return scheduler;
  }

  public LockQueue getRegionMigrateLock() {
    return regionMaintainHandler.getRegionMigrateLock();
  }

  public ReentrantLock getSchedulerLock() {
    return schedulerLock;
  }

  public ReentrantLock getSubmitRegionMigrateLock() {
    return submitRegionMigrateLock;
  }

  public RegionMaintainHandler getRegionMaintainHandler() {
    return regionMaintainHandler;
  }

  public RemoveDataNodeHandler getRemoveDataNodeHandler() {
    return removeDataNodeHandler;
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
