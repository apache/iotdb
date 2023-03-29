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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.exception.sync.PipeSinkNotExistException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.PreCreatePipePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.RecordPipeMessagePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.SetPipeStatusPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.ShowPipePlan;
import org.apache.iotdb.confignode.consensus.response.pipe.PipeResp;
import org.apache.iotdb.confignode.consensus.response.pipe.PipeSinkResp;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.observer.NodeStatisticsEvent;
import org.apache.iotdb.confignode.persistence.sync.ClusterSyncInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipeOnDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TOperatePipeOnDataNodeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SyncManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncManager.class);

  private final IManager configManager;
  private final ClusterSyncInfo clusterSyncInfo;

  public SyncManager(IManager configManager, ClusterSyncInfo clusterSyncInfo) {
    this.configManager = configManager;
    this.clusterSyncInfo = clusterSyncInfo;
  }

  public void lockSyncMetadata() {
    clusterSyncInfo.lockSyncMetadata();
  }

  public void unlockSyncMetadata() {
    clusterSyncInfo.unlockSyncMetadata();
  }

  // ======================================================
  // region Implement of PipeSink
  // ======================================================

  public TSStatus createPipeSink(CreatePipeSinkPlan plan) {
    try {
      clusterSyncInfo.checkAddPipeSink(plan);
      return getConsensusManager().write(plan).getStatus();
    } catch (PipeSinkException e) {
      LOGGER.error(e.getMessage());
      return new TSStatus(TSStatusCode.CREATE_PIPE_SINK_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus dropPipeSink(DropPipeSinkPlan plan) {
    try {
      clusterSyncInfo.checkDropPipeSink(plan.getPipeSinkName());
      return getConsensusManager().write(plan).getStatus();
    } catch (PipeSinkException e) {
      LOGGER.error(e.getMessage());
      return new TSStatus(TSStatusCode.CREATE_PIPE_SINK_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TGetPipeSinkResp getPipeSink(String pipeSinkName) {
    GetPipeSinkPlan getPipeSinkPlan = new GetPipeSinkPlan(pipeSinkName);
    PipeSinkResp pipeSinkResp =
        (PipeSinkResp) getConsensusManager().read(getPipeSinkPlan).getDataset();
    TGetPipeSinkResp resp = new TGetPipeSinkResp();
    resp.setStatus(pipeSinkResp.getStatus());
    if (pipeSinkResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setPipeSinkInfoList(
          pipeSinkResp.getPipeSinkList().stream()
              .map(PipeSink::getTPipeSinkInfo)
              .collect(Collectors.toList()));
    }
    return resp;
  }

  // endregion

  // ======================================================
  // region Implement of Pipe
  // ======================================================

  public void checkAddPipe(PipeInfo pipeInfo) throws PipeException, PipeSinkNotExistException {
    clusterSyncInfo.checkAddPipe(pipeInfo);
  }

  public TSStatus preCreatePipe(PipeInfo pipeInfo) {
    pipeInfo.setStatus(PipeStatus.PARTIAL_CREATE);
    return getConsensusManager().write(new PreCreatePipePlan(pipeInfo)).getStatus();
  }

  public TSStatus setPipeStatus(String pipeName, PipeStatus pipeStatus) {
    return getConsensusManager().write(new SetPipeStatusPlan(pipeName, pipeStatus)).getStatus();
  }

  public TSStatus dropPipe(String pipeName) {
    return getConsensusManager().write(new DropPipePlan(pipeName)).getStatus();
  }

  public TSStatus recordPipeMessage(String pipeName, PipeMessage pipeMessage) {
    return getConsensusManager()
        .write(new RecordPipeMessagePlan(pipeName, pipeMessage))
        .getStatus();
  }

  public TShowPipeResp showPipe(String pipeName) {
    ShowPipePlan showPipePlan = new ShowPipePlan(pipeName);
    PipeResp pipeResp = (PipeResp) getConsensusManager().read(showPipePlan).getDataset();
    TShowPipeResp resp = new TShowPipeResp();
    resp.setStatus(pipeResp.getStatus());
    if (pipeResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setPipeInfoList(
          pipeResp.getPipeInfoList().stream()
              .map(PipeInfo::getTShowPipeInfo)
              .collect(Collectors.toList()));
    }
    return resp;
  }

  public PipeInfo getPipeInfo(String pipeName) throws PipeException {
    return clusterSyncInfo.getPipeInfo(pipeName);
  }

  public TGetAllPipeInfoResp getAllPipeInfo() {
    try {
      // Should lock SyncMetadata to block operation PIPE procedure
      lockSyncMetadata();
      TGetAllPipeInfoResp resp = new TGetAllPipeInfoResp();
      resp.setStatus(StatusUtils.OK);
      resp.setAllPipeInfo(
          clusterSyncInfo.getAllPipeInfos().stream()
              .map(PipeInfo::serializeToByteBuffer)
              .collect(Collectors.toList()));
      return resp;
    } finally {
      unlockSyncMetadata();
    }
  }

  /**
   * Broadcast DataNodes to operate PIPE operation.
   *
   * @param pipeName name of PIPE
   * @param operation only support {@link SyncOperation#START_PIPE}, {@link SyncOperation#STOP_PIPE}
   *     and {@link SyncOperation#DROP_PIPE}
   * @return Map key is DataNodeId and value is TSStatus
   */
  public Map<Integer, TSStatus> operatePipeOnDataNodes(String pipeName, SyncOperation operation) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    nodeManager
        .filterDataNodeThroughStatus(NodeStatus.Running)
        .forEach(
            dataNodeConfiguration ->
                dataNodeLocationMap.put(
                    dataNodeConfiguration.getLocation().getDataNodeId(),
                    dataNodeConfiguration.getLocation()));
    final TOperatePipeOnDataNodeReq request =
        new TOperatePipeOnDataNodeReq(pipeName, (byte) operation.ordinal());

    AsyncClientHandler<TOperatePipeOnDataNodeReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.OPERATE_PIPE, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);

    return clientHandler.getResponseMap();
  }

  /**
   * Broadcast DataNodes to operate PIPE operation for roll back procedure.
   *
   * @param pipeName name of PIPE
   * @param createTime specifies the version of the pipe that needs to be rolled back to avoid
   *     concurrent errors caused by retry requests
   * @param operation only support {@link SyncOperation#START_PIPE}, {@link SyncOperation#STOP_PIPE}
   *     and {@link SyncOperation#DROP_PIPE}
   * @param dataNodeIds target DataNodeId set
   */
  public void operatePipeOnDataNodesForRollback(
      String pipeName, long createTime, SyncOperation operation, Set<Integer> dataNodeIds) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    nodeManager.filterDataNodeThroughStatus(NodeStatus.Running).stream()
        .filter(
            dataNodeConfiguration ->
                dataNodeIds.contains(dataNodeConfiguration.getLocation().dataNodeId))
        .forEach(
            dataNodeConfiguration ->
                dataNodeLocationMap.put(
                    dataNodeConfiguration.getLocation().getDataNodeId(),
                    dataNodeConfiguration.getLocation()));
    final TOperatePipeOnDataNodeReq request =
        new TOperatePipeOnDataNodeReq(pipeName, (byte) operation.ordinal())
            .setCreateTime(createTime);

    AsyncClientHandler<TOperatePipeOnDataNodeReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.ROLLBACK_OPERATE_PIPE, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);

    List<Integer> failedRollbackDataNodeId = new ArrayList<>();
    for (Map.Entry<Integer, TSStatus> responseEntry : clientHandler.getResponseMap().entrySet()) {
      if (responseEntry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        failedRollbackDataNodeId.add(responseEntry.getKey());
      }
    }
    configManager.getRetryFailedTasksThread().retryRollbackReq(failedRollbackDataNodeId, request);
  }

  /**
   * Broadcast DataNodes to pre create PIPE
   *
   * @param pipeInfo pipeInfo
   * @return Map key is DataNodeId and value is TSStatus
   */
  public Map<Integer, TSStatus> preCreatePipeOnDataNodes(PipeInfo pipeInfo) {
    NodeManager nodeManager = configManager.getNodeManager();
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    nodeManager
        .filterDataNodeThroughStatus(NodeStatus.Running)
        .forEach(
            dataNodeConfiguration ->
                dataNodeLocationMap.put(
                    dataNodeConfiguration.getLocation().getDataNodeId(),
                    dataNodeConfiguration.getLocation()));
    final TCreatePipeOnDataNodeReq request =
        new TCreatePipeOnDataNodeReq(pipeInfo.serializeToByteBuffer());

    AsyncClientHandler<TCreatePipeOnDataNodeReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.PRE_CREATE_PIPE, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);

    return clientHandler.getResponseMap();
  }

  /**
   * When some Nodes' states changed during a heartbeat loop, the eventbus in LoadManager will post
   * the different NodeStatstics event to SyncManager and ClusterSchemaManager.
   *
   * @param nodeStatisticsEvent nodeStatistics that changed in a heartbeat loop
   */
  @Subscribe
  @AllowConcurrentEvents
  public void handleNodeStatistics(NodeStatisticsEvent nodeStatisticsEvent) {
    // TODO
  }

  // endregion

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
