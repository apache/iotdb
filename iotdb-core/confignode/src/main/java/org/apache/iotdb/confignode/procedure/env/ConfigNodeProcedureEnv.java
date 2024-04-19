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
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.AddConsensusGroupException;
import org.apache.iotdb.confignode.exception.AddPeerException;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
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
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ConfigNodeProcedureEnv {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigNodeProcedureEnv.class);

  /** Add or remove node lock. */
  private final LockQueue nodeLock = new LockQueue();

  private final ReentrantLock schedulerLock = new ReentrantLock(true);

  private final ConfigManager configManager;

  private final ProcedureScheduler scheduler;

  private final RegionMaintainHandler regionMaintainHandler;

  private final ReentrantLock removeConfigNodeLock;

  public ConfigNodeProcedureEnv(ConfigManager configManager, ProcedureScheduler scheduler) {
    this.configManager = configManager;
    this.scheduler = scheduler;
    this.regionMaintainHandler = new RegionMaintainHandler(configManager);
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
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithRetry(
                    dataNodeConfiguration.getLocation().getInternalEndPoint(),
                    invalidateCacheReq,
                    DataNodeRequestType.INVALIDATE_PARTITION_CACHE);

        final TSStatus invalidateSchemaStatus =
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithRetry(
                    dataNodeConfiguration.getLocation().getInternalEndPoint(),
                    invalidateCacheReq,
                    DataNodeRequestType.INVALIDATE_SCHEMA_CACHE);

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

  public boolean checkEnoughDataNodeAfterRemoving(TDataNodeLocation removedDatanode) {
    final int existedDataNodeNum =
        getNodeManager()
            .filterDataNodeThroughStatus(
                NodeStatus.Running, NodeStatus.ReadOnly, NodeStatus.Removing)
            .size();
    int dataNodeNumAfterRemoving;
    if (getLoadManager().getNodeStatus(removedDatanode.getDataNodeId()) != NodeStatus.Unknown) {
      dataNodeNumAfterRemoving = existedDataNodeNum - 1;
    } else {
      dataNodeNumAfterRemoving = existedDataNodeNum;
    }
    return dataNodeNumAfterRemoving >= NodeInfo.getMinimumDataNode();
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
                    ConfigNodeRequestType.ADD_CONSENSUS_GROUP);
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
                    ConfigNodeRequestType.DELETE_CONFIG_NODE_PEER);
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
                    ConfigNodeRequestType.STOP_CONFIG_NODE);

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
            ConfigNodeRequestType.NOTIFY_REGISTER_SUCCESS);
  }

  /**
   * Mark the given datanode as removing status to avoid read or write request routing to this node.
   *
   * @param dataNodeLocation the datanode to be marked as removing status
   */
  public void markDataNodeAsRemovingAndBroadcast(TDataNodeLocation dataNodeLocation) {
    // Send request to update NodeStatus on the DataNode to be removed
    if (getLoadManager().getNodeStatus(dataNodeLocation.getDataNodeId()) == NodeStatus.Unknown) {
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

    long currentTime = System.nanoTime();
    // Force updating NodeStatus to Removing
    getLoadManager()
        .forceUpdateNodeCache(
            NodeType.DataNode,
            dataNodeLocation.getDataNodeId(),
            new NodeHeartbeatSample(currentTime, NodeStatus.Removing));
    Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>> removingHeartbeatSampleMap =
        new TreeMap<>();
    // Force update RegionStatus to Removing
    getPartitionManager()
        .getAllReplicaSets(dataNodeLocation.getDataNodeId())
        .forEach(
            replicaSet ->
                removingHeartbeatSampleMap.put(
                    replicaSet.getRegionId(),
                    Collections.singletonMap(
                        dataNodeLocation.getDataNodeId(),
                        new RegionHeartbeatSample(currentTime, RegionStatus.Removing))));
    getLoadManager().forceUpdateRegionGroupCache(removingHeartbeatSampleMap);
  }

  /**
   * Do region creations and broadcast the {@link CreateRegionGroupsPlan}.
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
   * @param activateRegionGroupMap Map<RegionGroupId, Map<DataNodeId, activate heartbeat sample>>
   */
  public void activateRegionGroup(
      Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>> activateRegionGroupMap) {
    getLoadManager().forceUpdateRegionGroupCache(activateRegionGroupMap);
    // Wait for leader and priority redistribution
    getLoadManager().waitForRegionGroupReady(new ArrayList<>(activateRegionGroupMap.keySet()));
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

  public List<TSStatus> createPipePluginOnDataNodes(PipePluginMeta pipePluginMeta, byte[] jarFile)
      throws IOException {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TCreatePipePluginInstanceReq request =
        new TCreatePipePluginInstanceReq(pipePluginMeta.serialize(), ByteBuffer.wrap(jarFile));

    final AsyncClientHandler<TCreatePipePluginInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.CREATE_PIPE_PLUGIN, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> dropPipePluginOnDataNodes(
      String pipePluginName, boolean needToDeleteJarFile) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TDropPipePluginInstanceReq request =
        new TDropPipePluginInstanceReq(pipePluginName, needToDeleteJarFile);

    AsyncClientHandler<TDropPipePluginInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.DROP_PIPE_PLUGIN, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public Map<Integer, TPushPipeMetaResp> pushAllPipeMetaToDataNodes(
      List<ByteBuffer> pipeMetaBinaryList) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushPipeMetaReq request = new TPushPipeMetaReq().setPipeMetas(pipeMetaBinaryList);

    final AsyncClientHandler<TPushPipeMetaReq, TPushPipeMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.PIPE_PUSH_ALL_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes(ByteBuffer pipeMetaBinary) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSinglePipeMetaReq request = new TPushSinglePipeMetaReq().setPipeMeta(pipeMetaBinary);

    final AsyncClientHandler<TPushSinglePipeMetaReq, TPushPipeMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.PIPE_PUSH_SINGLE_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushPipeMetaResp> dropSinglePipeOnDataNodes(String pipeNameToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSinglePipeMetaReq request =
        new TPushSinglePipeMetaReq().setPipeNameToDrop(pipeNameToDrop);

    final AsyncClientHandler<TPushSinglePipeMetaReq, TPushPipeMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.PIPE_PUSH_SINGLE_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
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

    final AsyncClientHandler<TPushMultiPipeMetaReq, TPushPipeMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.PIPE_PUSH_MULTI_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushPipeMetaResp> dropMultiPipeOnDataNodes(List<String> pipeNamesToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushMultiPipeMetaReq request =
        new TPushMultiPipeMetaReq().setPipeNamesToDrop(pipeNamesToDrop);

    final AsyncClientHandler<TPushMultiPipeMetaReq, TPushPipeMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.PIPE_PUSH_MULTI_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushTopicMetaResp> pushAllTopicMetaToDataNodes(
      List<ByteBuffer> topicMetaBinaryList) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushTopicMetaReq request = new TPushTopicMetaReq().setTopicMetas(topicMetaBinaryList);

    final AsyncClientHandler<TPushTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.TOPIC_PUSH_ALL_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public List<TSStatus> pushSingleTopicOnDataNode(ByteBuffer topicMeta) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSingleTopicMetaReq request = new TPushSingleTopicMetaReq().setTopicMeta(topicMeta);

    final AsyncClientHandler<TPushSingleTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.TOPIC_PUSH_SINGLE_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList().stream()
        .map(TPushTopicMetaResp::getStatus)
        .collect(Collectors.toList());
  }

  public List<TSStatus> dropSingleTopicOnDataNode(String topicNameToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSingleTopicMetaReq request =
        new TPushSingleTopicMetaReq().setTopicNameToDrop(topicNameToDrop);

    final AsyncClientHandler<TPushSingleTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.TOPIC_PUSH_SINGLE_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
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

    final AsyncClientHandler<TPushMultiTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.TOPIC_PUSH_MULTI_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public Map<Integer, TPushTopicMetaResp> dropMultiTopicOnDataNodes(List<String> topicNamesToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushMultiTopicMetaReq request =
        new TPushMultiTopicMetaReq().setTopicNamesToDrop(topicNamesToDrop);

    final AsyncClientHandler<TPushMultiTopicMetaReq, TPushTopicMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.TOPIC_PUSH_MULTI_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
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

    final AsyncClientHandler<TPushConsumerGroupMetaReq, TPushConsumerGroupMetaResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.CONSUMER_GROUP_PUSH_ALL_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 * 2 / 3);
    return clientHandler.getResponseMap();
  }

  public List<TSStatus> pushSingleConsumerGroupOnDataNode(ByteBuffer consumerGroupMeta) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSingleConsumerGroupMetaReq request =
        new TPushSingleConsumerGroupMetaReq().setConsumerGroupMeta(consumerGroupMeta);

    final AsyncClientHandler<TPushSingleConsumerGroupMetaReq, TPushConsumerGroupMetaResp>
        clientHandler =
            new AsyncClientHandler<>(
                DataNodeRequestType.CONSUMER_GROUP_PUSH_SINGLE_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList().stream()
        .map(TPushConsumerGroupMetaResp::getStatus)
        .collect(Collectors.toList());
  }

  public List<TSStatus> dropSingleConsumerGroupOnDataNode(String consumerGroupNameToDrop) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPushSingleConsumerGroupMetaReq request =
        new TPushSingleConsumerGroupMetaReq().setConsumerGroupNameToDrop(consumerGroupNameToDrop);

    final AsyncClientHandler<TPushSingleConsumerGroupMetaReq, TPushConsumerGroupMetaResp>
        clientHandler =
            new AsyncClientHandler<>(
                DataNodeRequestType.CONSUMER_GROUP_PUSH_SINGLE_META, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
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

  public RegionMaintainHandler getRegionMaintainHandler() {
    return regionMaintainHandler;
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
