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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionMigrateFailedType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.enums.DataNodeRemoveState;
import org.apache.iotdb.commons.enums.RegionMigrateState;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.sync.datanode.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.response.DataNodeToStatusResp;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.mpp.rpc.thrift.TDisableDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class DataNodeRemoveManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeRemoveManager.class);
  private static final int QUEUE_SIZE_LIMIT = 200;

  private boolean hasSetUp = false;

  private ConfigManager configManager;

  private final LinkedBlockingQueue<RemoveDataNodePlan> removeQueue;

  // which request is running
  private RemoveDataNodePlan headRequest = null;

  // which data node is removing in `headRequest`, [0, req TDataNodeLocation list size)
  private int headNodeIndex = -1;

  private DataNodeRemoveState headNodeState = DataNodeRemoveState.NORMAL;

  // the region ids belongs to head node
  private List<TConsensusGroupId> headNodeRegionIds = new ArrayList<>();

  // which region is migrating on head node
  private int headRegionIndex = -1;

  private RegionMigrateState headRegionState = RegionMigrateState.ONLINE;

  private volatile boolean stopped = false;

  private volatile boolean isLeader = false;
  private final Object leaderLock = new Object();
  private Thread workThread;
  private Thread waitLeaderThread;

  private final Object regionMigrateLock = new Object();
  private TRegionMigrateResultReportReq lastRegionMigrateResult = null;

  public DataNodeRemoveManager(ConfigManager configManager) {
    this.configManager = configManager;
    removeQueue = new LinkedBlockingQueue<>(QUEUE_SIZE_LIMIT);

    createWaitLeaderThread();
    createWorkThread();
  }

  /** start the manager when Config node startup */
  public void start() {
    if (!hasSetUp) {
      // TODO 1. when restart，reload info from NoInfo and continue
      setUp();
      hasSetUp = true;
    }
    LOGGER.info("Data Node remove service start");
    // 2. if it is not leader, loop check
    // configManager.getConsensusManager().isLeader())
    waitUntilBeLeader();

    // 3. Take and exec request one by one
    // 4. When Data Node's state or Region's state change, then modify the request and write it to
    // Consensus.
    // 5. TODO if leader change?
    execRequestFromQueue();
  }

  private void createWaitLeaderThread() {
    waitLeaderThread =
        new Thread(
            () -> {
              while (!stopped && !configManager.getConsensusManager().isLeader()) {
                try {
                  Thread.sleep(2000);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
              LOGGER.info("the config node is leader now!");
              isLeader = true;
              synchronized (leaderLock) {
                leaderLock.notify();
              }
            });
    waitLeaderThread.setName("Wait-Leader-Thread");
  }

  private void createWorkThread() {
    workThread =
        new Thread(
            () -> {
              while (!stopped) {
                RemoveDataNodePlan req = null;
                try {
                  while (!isLeader) {
                    LOGGER.warn("the ConfigNode is not leader, waiting...");
                    synchronized (leaderLock) {
                      leaderLock.wait();
                    }
                  }
                  LOGGER.info("the ConfigNode is leader now, will take request and run");
                  req = removeQueue.take();
                  LOGGER.info("exec the request : {}", req);
                  prepareHeadRequestInfo(req);
                  TSStatus status = execRemoveDataNodeRequest(headRequest);
                  LOGGER.info("exec the request: {}, result:  {}", req, status);
                  // unRegisterRequest(req);
                } catch (InterruptedException e) {
                  LOGGER.warn("work thread interrupted", e);
                  Thread.currentThread().interrupt();
                } catch (Exception e) {
                  LOGGER.warn("the request run failed", e);
                } finally {
                  if (req != null) {
                    unRegisterRequest(req);
                  }
                }
              }
            });
    workThread.setName("Exec-RemoveDataNode-Thread");
  }

  private void waitUntilBeLeader() {
    if (waitLeaderThread != null) {
      waitLeaderThread.start();
    }
  }

  private void execRequestFromQueue() {
    if (workThread != null) {
      workThread.start();
    }
  }

  public void beLeader() {
    // TODO
    /** this.isLeader = true; synchronized (leaderLock) { leaderLock.notify(); } */
  }

  public void beFollower() {
    // TODO
    /** this.isLeader = false; */
  }

  /**
   * prepare for the request. from first node, first region to exec
   *
   * @param req RemoveDataNodeReq
   */
  private void prepareHeadRequestInfo(RemoveDataNodePlan req) {
    LOGGER.info("start to prepare for request: {}", req);
    this.headRequest = req;
    // to exec the request, will change it's state at different stage.
    this.headNodeIndex = 0;
    this.headNodeState = DataNodeRemoveState.NORMAL;

    TDataNodeLocation headNode = req.getDataNodeLocations().get(headNodeIndex);
    this.headNodeRegionIds =
        configManager.getPartitionManager().getAllReplicaSets().stream()
            .filter(rg -> rg.getDataNodeLocations().contains(headNode))
            .filter(rg -> rg.regionId.getType() != TConsensusGroupType.PartitionRegion)
            .map(TRegionReplicaSet::getRegionId)
            .collect(Collectors.toList());
    this.headRegionIndex = 0;
    this.headRegionState = RegionMigrateState.ONLINE;

    // modify head quest
    this.headRequest.setUpdate(true);
    this.headRequest.setExecDataNodeRegionIds(headNodeRegionIds);
    this.headRequest.setExecDataNodeIndex(headNodeIndex);
    this.headRequest.setExecRegionIndex(headRegionIndex);
    this.headRequest.setExecDataNodeState(headNodeState);
    this.headRequest.setExecRegionState(headRegionState);
    configManager.getConsensusManager().write(headRequest);
    LOGGER.info("finished to prepare for request: {}", req);
  }

  private void setUp() {
    removeQueue.addAll(configManager.getNodeManager().getDataNodeRemoveRequestQueue());
    headRequest = configManager.getNodeManager().getHeadRequestForDataNodeRemove();
    if (headRequest == null) {
      return;
    }

    // avoid duplication
    if (removeQueue.contains(headRequest)) {
      removeQueue.remove(headRequest);
    }

    headNodeIndex = headRequest.getExecDataNodeIndex();
    headNodeState = headRequest.getExecDataNodeState();
    headNodeRegionIds = headRequest.getExecDataNodeRegionIds();
    headRegionIndex = headRequest.getExecRegionIndex();
    headRegionState = headRequest.getExecRegionState();
  }

  /**
   * exec the request loop all removed node 1: brocast it to cluster 2: loop region on it and
   * migrate region 3 stop the node or roll back
   *
   * @param req RemoveDataNodeReq
   */
  private TSStatus execRemoveDataNodeRequest(RemoveDataNodePlan req) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

    for (TDataNodeLocation dataNodeLocation : req.getDataNodeLocations()) {
      headNodeIndex = req.getDataNodeLocations().indexOf(dataNodeLocation);
      status = broadcastDisableDataNode(req, dataNodeLocation);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Disable Data Node Error {}", status);
        return status;
      }

      // fetch data/schema region from one datanode
      status = migrateSingleDataNodeRegions(req, dataNodeLocation);
      if (isSucceed(status)) {
        status = stopDataNode(req, dataNodeLocation);
        if (isFailed(status)) {
          LOGGER.error(
              "send rpc to stop the Data Node {} error, please stop it use command",
              dataNodeLocation);
        }
      } else {
        LOGGER.error("the request run failed {}, result {}. will roll back", req, status);
        rollBackSingleNode(req, dataNodeLocation);
      }
    }
    return status;
  }

  private void updateRegionLocationCache(
      TConsensusGroupId regionId, TDataNodeLocation oldNode, TDataNodeLocation newNode) {
    LOGGER.debug(
        "start to update region {} location from {} to {} when it migrate succeed",
        regionId,
        oldNode.getInternalEndPoint().getIp(),
        newNode.getInternalEndPoint().getIp());
    UpdateRegionLocationPlan req = new UpdateRegionLocationPlan(regionId, oldNode, newNode);
    TSStatus status = configManager.getPartitionManager().updateRegionLocation(req);
    LOGGER.debug(
        "update region {} location finished, result:{}, old:{}, new:{}",
        regionId,
        status,
        oldNode.getInternalEndPoint().getIp(),
        newNode.getInternalEndPoint().getIp());
  }

  private void rollBackSingleNode(RemoveDataNodePlan req, TDataNodeLocation node) {
    LOGGER.warn("roll back remove data node {} in the request {}", node, req);
    if (headRegionState == RegionMigrateState.DATA_COPY_FAILED) {
      // TODO delete target node the head region data
      TConsensusGroupId tRegionId = lastRegionMigrateResult.getRegionId();
      for (Map.Entry<TDataNodeLocation, TRegionMigrateFailedType> entry :
          lastRegionMigrateResult.getFailedNodeAndReason().entrySet()) {
        TDataNodeLocation failedNode = entry.getKey();
        TRegionMigrateFailedType failedReason = entry.getValue();
        switch (failedReason) {
            // TODO how to impl roll back
          case AddPeerFailed:
            LOGGER.warn(
                "add new peer node {} for region {} failed, will roll back",
                failedNode.getInternalEndPoint().getIp(),
                tRegionId);
            break;
          case RemovePeerFailed:
            LOGGER.warn(
                "remove old peer node {} for region {} failed, will roll back",
                failedNode.getInternalEndPoint().getIp(),
                tRegionId);
            break;
          case RemoveConsensusGroupFailed:
            LOGGER.warn(
                "remove consensus group on node {} for region {} failed, will roll back",
                failedNode.getInternalEndPoint().getIp(),
                tRegionId);
            break;
          case DeleteRegionFailed:
            LOGGER.warn(
                "create region {} instance on {} failed, will roll back",
                failedNode.getInternalEndPoint().getIp(),
                tRegionId);
            break;
          default:
            LOGGER.warn(
                "UnSupport reason {} for region {} migrate failed", failedReason, tRegionId);
        }
      }
    }
    // TODO if roll back failed, FAILED
    storeDataNodeState(req, DataNodeRemoveState.REMOVE_FAILED);
  }

  /**
   * broadcast these datanode in RemoveDataNodeReq are disabled, so they will not accept read/write
   * request
   *
   * @param req RemoveDataNodeReq
   * @param disabledDataNode TDataNodeLocation
   */
  private TSStatus broadcastDisableDataNode(
      RemoveDataNodePlan req, TDataNodeLocation disabledDataNode) {
    LOGGER.info(
        "DataNodeRemoveService start send disable the Data Node to cluster, {}", disabledDataNode);
    storeDataNodeState(req, DataNodeRemoveState.REMOVE_START);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<TEndPoint> otherOnlineDataNodes =
        configManager.getLoadManager().getOnlineDataNodes(-1).stream()
            .map(TDataNodeConfiguration::getLocation)
            .filter(loc -> !loc.equals(disabledDataNode))
            .map(TDataNodeLocation::getInternalEndPoint)
            .collect(Collectors.toList());

    for (TEndPoint server : otherOnlineDataNodes) {
      TDisableDataNodeReq disableReq = new TDisableDataNodeReq(disabledDataNode);
      status =
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  server, disableReq, DataNodeRequestType.DISABLE_DATA_NODE);
      if (!isSucceed(status)) {
        return status;
      }
    }
    LOGGER.info(
        "DataNodeRemoveService finished send disable the Data Node to cluster, {}",
        disabledDataNode);
    status.setMessage("Succeed disable the Data Node from cluster");
    return status;
  }

  private void storeRegionState(RemoveDataNodePlan req, RegionMigrateState state) {
    req.setExecRegionIndex(headRegionIndex);
    headRegionState = state;
    req.setExecRegionState(headRegionState);
    configManager.getConsensusManager().write(req);
  }

  private void storeDataNodeState(RemoveDataNodePlan req, DataNodeRemoveState state) {
    req.setExecDataNodeIndex(headNodeIndex);
    headNodeState = state;
    req.setExecDataNodeState(headNodeState);
    configManager.getConsensusManager().write(req);
  }

  private TSStatus migrateSingleDataNodeRegions(
      RemoveDataNodePlan req, TDataNodeLocation dataNodeLocation) {
    LOGGER.info("start to migrate regions on the Data Node: {}", dataNodeLocation);
    TSStatus status;
    storeDataNodeState(req, DataNodeRemoveState.REGION_MIGRATING);
    for (TConsensusGroupId regionId : headNodeRegionIds) {
      headRegionIndex = headNodeRegionIds.indexOf(regionId);
      // impl migrate region with twice rpc: CN-->DN(send), DN-->CN(report)
      status = migrateSingleRegion(req, dataNodeLocation, regionId);
      // if has one region migrate failed, the node remove failed
      if (isFailed(status)) {
        storeDataNodeState(req, DataNodeRemoveState.REGION_MIGRATE_FAILED);
        return status;
      }
    }

    // all regions on the node migrate succeed
    storeDataNodeState(req, DataNodeRemoveState.REGION_MIGRATE_SUCCEED);
    status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    status.setMessage("The Data Node migrate regions succeed");
    LOGGER.info("finished to migrate regions on the Data Node: {}", dataNodeLocation);
    return status;
  }

  private TSStatus migrateSingleRegion(
      RemoveDataNodePlan req, TDataNodeLocation node, TConsensusGroupId regionId) {
    // change region leader:  change leader and then add peer, will cause ratis server exit
    // so modify policy: add peer firs, then change region leader, see:
    // RegionMigrateService.changeLeader()

    // do migrate region
    TSStatus status = doMigrateSingleRegion(req, node, regionId);
    if (isFailed(status)) {
      storeRegionState(req, RegionMigrateState.DATA_COPY_FAILED);
      return status;
    }
    storeRegionState(req, RegionMigrateState.DATA_COPY_SUCCEED);
    return status;
  }

  private TSStatus changeSingleRegionLeader(
      RemoveDataNodePlan req, TDataNodeLocation node, TConsensusGroupId regionId) {
    storeRegionState(req, RegionMigrateState.LEADER_CHANGING);
    LOGGER.debug("start to send region leader change. {}", regionId);
    TSStatus status;
    // pick a node in same raft group to be new region leader
    List<TRegionReplicaSet> regionReplicaSets =
        configManager.getPartitionManager().getAllReplicaSets().stream()
            .filter(rg -> rg.getRegionId().equals(regionId))
            .collect(Collectors.toList());
    if (regionReplicaSets.isEmpty()) {
      LOGGER.warn("not find TRegionReplica for region: {}, ignore it", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("not find TRegionReplica for region, ignore");
      return status;
    }
    Optional<TDataNodeLocation> newLeaderNode =
        regionReplicaSets.get(0).getDataNodeLocations().stream()
            .filter(e -> !e.equals(node))
            .findAny();
    if (!newLeaderNode.isPresent()) {
      LOGGER.warn("No enough Data node to change leader for region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("No enough Data node to change leader for region " + regionId);
      return status;
    }
    status =
        SyncDataNodeClientPool.getInstance()
            .changeRegionLeader(regionId, node.getInternalEndPoint(), newLeaderNode.get());
    LOGGER.debug("finished to send region leader change. {}", regionId);
    return status;
  }

  private TSStatus doMigrateSingleRegion(
      RemoveDataNodePlan req, TDataNodeLocation node, TConsensusGroupId regionId) {
    storeRegionState(req, RegionMigrateState.DATA_COPYING);
    LOGGER.debug("start to migrate region {}", regionId);
    TSStatus status;
    List<TDataNodeLocation> regionReplicaNodes = findRegionReplicaNodes(regionId);
    if (regionReplicaNodes.isEmpty()) {
      LOGGER.warn("Not find region replica nodes, region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("not find region replica nodes, region: " + regionId);
      return status;
    }

    // will migrate the region to the new node, which should not be same raft
    Optional<TDataNodeLocation> newNode = pickNewReplicaNodeForRegion(regionReplicaNodes);
    if (!newNode.isPresent()) {
      LOGGER.warn("No enough Data node to migrate region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("No enough Data node to migrate region, region: " + regionId);
      return status;
    }

    status = addNewNodeToRegionConsensusGroup(regionId, regionReplicaNodes, newNode.get());
    if (isFailed(status)) {
      return status;
    }

    // TODO if region replica is 1, the new leader is null, it also need to migrate
    Optional<TDataNodeLocation> newLeaderNode =
        regionReplicaNodes.stream().filter(e -> !e.equals(node)).findAny();
    if (!newLeaderNode.isPresent()) {
      LOGGER.warn(
          "No other Node to change region leader, check by show regions, region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("No other Node to change region leader, check by show regions");
      return status;
    }

    TMigrateRegionReq migrateRegionReq = new TMigrateRegionReq(regionId, node, newNode.get());
    migrateRegionReq.setNewLeaderNode(newLeaderNode.get());
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                node.getInternalEndPoint(), migrateRegionReq, DataNodeRequestType.MIGRATE_REGION);
    // maybe send rpc failed
    if (isFailed(status)) {
      return status;
    }
    LOGGER.debug("send region {} migrate action to {}, wait it finished", regionId, node);
    // wait DN report the region migrate result, when DN reported, then will notify and continue
    status = waitForTheRegionMigrateFinished();
    // interrupt wait
    if (isFailed(status)) {
      return status;
    }
    LOGGER.debug(
        "wait region {} migrate finished. migrate result: {}", regionId, lastRegionMigrateResult);
    status = lastRegionMigrateResult.migrateResult;
    if (isSucceed(status)) {
      updateRegionLocationCache(regionId, node, newNode.get());
    }
    return status;
  }

  private List<TDataNodeLocation> findRegionReplicaNodes(TConsensusGroupId regionId) {
    List<TRegionReplicaSet> regionReplicaSets =
        configManager.getPartitionManager().getAllReplicaSets().stream()
            .filter(rg -> rg.regionId.equals(regionId))
            .collect(Collectors.toList());
    if (regionReplicaSets.isEmpty()) {
      LOGGER.warn("not find TRegionReplica for region: {}", regionId);
      return Collections.emptyList();
    }

    return regionReplicaSets.get(0).getDataNodeLocations();
  }

  private Optional<TDataNodeLocation> pickNewReplicaNodeForRegion(
      List<TDataNodeLocation> regionReplicaNodes) {
    return configManager.getLoadManager().getOnlineDataNodes(-1).stream()
        .map(TDataNodeConfiguration::getLocation)
        .filter(e -> !regionReplicaNodes.contains(e))
        .findAny();
  }

  private TSStatus addNewNodeToRegionConsensusGroup(
      TConsensusGroupId regionId,
      List<TDataNodeLocation> regionReplicaNodes,
      TDataNodeLocation newNode) {
    String storageGroup = configManager.getPartitionManager().getRegionStorageGroup(regionId);
    TSStatus status =
        SyncDataNodeClientPool.getInstance()
            .addToRegionConsensusGroup(
                // TODO replace with real ttl
                regionReplicaNodes, regionId, newNode, storageGroup, Long.MAX_VALUE);
    LOGGER.debug("send add region {} consensus group to {}", regionId, newNode);
    if (isFailed(status)) {
      LOGGER.error(
          "add new node {} to region {} consensus group failed,  result: {}",
          newNode,
          regionId,
          status);
    }
    return status;
  }

  private boolean isSucceed(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private boolean isFailed(TSStatus status) {
    return !isSucceed(status);
  }

  /**
   * register a RemoveDataNodeReq
   *
   * @param req RemoveDataNodeReq
   * @return true if register succeed.
   */
  public synchronized boolean registerRequest(RemoveDataNodePlan req) {
    if (!removeQueue.add(req)) {
      LOGGER.error("register request failed");
      return false;
    }
    ConsensusWriteResponse resp = configManager.getConsensusManager().write(req);
    LOGGER.info("write register request to Consensus result : {} for req {}", resp, req);
    return resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  /**
   * remove a RemoveDataNodeReq
   *
   * @param req RemoveDataNodeReq
   */
  public void unRegisterRequest(RemoveDataNodePlan req) {
    req.setFinished(true);
    configManager.getConsensusManager().write(req);
    reset();
    LOGGER.info("unregister request succeed, remain {} request", removeQueue.size());
  }

  public void reportRegionMigrateResult(TRegionMigrateResultReportReq req) {
    LOGGER.debug("accept region {} migrate result, result: {}", req.getRegionId(), req);
    notifyTheRegionMigrateFinished(req);
  }

  private TSStatus waitForTheRegionMigrateFinished() {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    synchronized (regionMigrateLock) {
      try {
        // TODO set timeOut?
        regionMigrateLock.wait();
      } catch (InterruptedException e) {
        LOGGER.error("region migrate {} interrupt", headNodeRegionIds.get(headRegionIndex), e);
        Thread.currentThread().interrupt();
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage("wait region migrate interrupt," + e.getMessage());
      }
    }
    return status;
  }

  private void notifyTheRegionMigrateFinished(TRegionMigrateResultReportReq req) {
    lastRegionMigrateResult = req;
    synchronized (regionMigrateLock) {
      regionMigrateLock.notify();
    }
  }

  private void reset() {
    this.headRequest = null;
    this.headNodeIndex = -1;
    this.headNodeState = DataNodeRemoveState.NORMAL;
    this.headNodeRegionIds.clear();
    this.headRegionIndex = -1;
    this.headRegionState = RegionMigrateState.ONLINE;
  }

  private TSStatus stopDataNode(RemoveDataNodePlan req, TDataNodeLocation dataNode) {
    LOGGER.info("begin to stop Data Node {} in request {}", dataNode, req);
    storeDataNodeState(req, DataNodeRemoveState.STOP);
    AsyncDataNodeClientPool.getInstance().resetClient(dataNode.getInternalEndPoint());
    TSStatus status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                dataNode.getInternalEndPoint(), dataNode, DataNodeRequestType.STOP_DATA_NODE);
    LOGGER.info("stop Data Node {} result: {}", dataNode, status);
    return status;
  }

  /** stop the manager */
  public void stop() {
    stopped = true;
    if (waitLeaderThread != null) {
      waitLeaderThread.interrupt();
    }
    if (workThread != null) {
      workThread.interrupt();
    }
    LOGGER.info("Data Node remove service is stopped");
  }

  public void setConfigManager(ConfigManager configManager) {
    this.configManager = configManager;
  }

  /**
   * check if the remove datanode request illegal
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS when request is legal.
   */
  public DataNodeToStatusResp checkRemoveDataNodeRequest(RemoveDataNodePlan removeDataNodePlan) {
    DataNodeToStatusResp dataSet = new DataNodeToStatusResp();
    dataSet.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    TSStatus status = checkRegionReplication(removeDataNodePlan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(status);
      return dataSet;
    }

    status = checkRequestLimit();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(status);
      return dataSet;
    }

    status = checkDataNodeExist(removeDataNodePlan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(status);
      return dataSet;
    }

    status = checkDuplicateRequest(removeDataNodePlan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(status);
      return dataSet;
    }

    status = checkDuplicateDataNodeAcrossRequests(removeDataNodePlan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(status);
      return dataSet;
    }
    return dataSet;
  }

  /**
   * check if request exceed threshold
   *
   * @return SUCCEED_STATUS if not exceed threshold
   */
  private TSStatus checkRequestLimit() {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (removeQueue.size() >= QUEUE_SIZE_LIMIT) {
      status.setCode(TSStatusCode.REQUEST_SIZE_EXCEED.getStatusCode());
      status.setMessage("remove Data Node request exceed threshold, reject this request");
    }
    return status;
  }

  /**
   * check if the request repeat
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS if not repeat
   */
  private TSStatus checkDuplicateRequest(RemoveDataNodePlan removeDataNodePlan) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (removeQueue.contains(removeDataNodePlan)) {
      status.setCode(TSStatusCode.DUPLICATE_REMOVE.getStatusCode());
      status.setMessage(
          "the remove datanode request is duplicate, wait the last same request finished");
    }
    return status;
  }

  /**
   * check if has same Data Node amount different request
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS if not has
   */
  private TSStatus checkDuplicateDataNodeAcrossRequests(RemoveDataNodePlan removeDataNodePlan) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    boolean hasDuplicateDataNodeAcrossRequests =
        removeQueue.stream()
            .map(RemoveDataNodePlan::getDataNodeLocations)
            .anyMatch(loc -> removeDataNodePlan.getDataNodeLocations().contains(loc));
    if (hasDuplicateDataNodeAcrossRequests) {
      TSStatus dataNodeDuplicate = new TSStatus(TSStatusCode.DUPLICATE_REMOVE.getStatusCode());
      dataNodeDuplicate.setMessage(
          "there exist duplicate Data Node between this request and other requests, can't run");
    }
    return status;
  }

  /**
   * check if has removed Data Node but not exist in cluster
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS if not has
   */
  private TSStatus checkDataNodeExist(RemoveDataNodePlan removeDataNodePlan) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

    List<TDataNodeLocation> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes(-1).stream()
            .map(TDataNodeConfiguration::getLocation)
            .collect(Collectors.toList());
    boolean hasNotExistNode =
        removeDataNodePlan.getDataNodeLocations().stream()
            .anyMatch(loc -> !allDataNodes.contains(loc));
    if (hasNotExistNode) {
      status.setCode(TSStatusCode.DATANODE_NOT_EXIST.getStatusCode());
      status.setMessage("there exist Data Node in request but not in cluster");
    }
    return status;
  }

  /**
   * check if has enought replication in cluster
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS if not has
   */
  private TSStatus checkRegionReplication(RemoveDataNodePlan removeDataNodePlan) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    int removedDataNodeSize = removeDataNodePlan.getDataNodeLocations().size();
    int allDataNodeSize = configManager.getNodeManager().getRegisteredDataNodeCount();
    if (allDataNodeSize - removedDataNodeSize < NodeInfo.getMinimumDataNode()) {
      status.setCode(TSStatusCode.LACK_REPLICATION.getStatusCode());
      status.setMessage(
          "lack replication, allow most removed Data Node size : "
              + (allDataNodeSize - NodeInfo.getMinimumDataNode()));
    }
    return status;
  }
}
