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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSender;
import org.apache.iotdb.common.rpc.thrift.TServiceProvider;
import org.apache.iotdb.common.rpc.thrift.TServiceType;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncConfigNodeClientPool;
import org.apache.iotdb.confignode.client.async.AsyncConfigNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.async.handlers.AsyncConfigNodeClientHandler;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateClusterIdPlan;
import org.apache.iotdb.confignode.persistence.ClusterInfo;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class ClusterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManager.class);

  private final IManager configManager;
  private final ClusterInfo clusterInfo;

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  public ClusterManager(IManager configManager, ClusterInfo clusterInfo) {
    this.configManager = configManager;
    this.clusterInfo = clusterInfo;
  }

  public void checkClusterId() {
    if (clusterInfo.getClusterId() != null) {
      LOGGER.info("clusterID: {}", clusterInfo.getClusterId());
      return;
    }
    generateClusterId();
  }

  public String getClusterId() {
    return clusterInfo.getClusterId();
  }

  public String getClusterIdWithRetry(long maxWaitTime) {
    long startTime = System.currentTimeMillis();
    while (clusterInfo.getClusterId() == null
        && System.currentTimeMillis() - startTime < maxWaitTime) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return clusterInfo.getClusterId();
  }

  private void generateClusterId() {
    String clusterId = String.valueOf(UUID.randomUUID());
    UpdateClusterIdPlan updateClusterIdPlan = new UpdateClusterIdPlan(clusterId);
    try {
      configManager.getConsensusManager().write(updateClusterIdPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }
  }

  public TTestConnectionResp submitTestConnectionTaskToEveryNode() {
    TTestConnectionResp resp = new TTestConnectionResp();
    resp.resultList = new ArrayList<>();
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    TNodeLocations nodeLocations = new TNodeLocations();
    nodeLocations.setConfigNodeLocations(configManager.getNodeManager().getRegisteredConfigNodes());
    nodeLocations.setDataNodeLocations(
        configManager.getNodeManager().getRegisteredDataNodes().stream()
            .map(TDataNodeConfiguration::getLocation)
            .collect(Collectors.toList()));
    Map<Integer, TConfigNodeLocation> configNodeLocationMap =
            configManager.getNodeManager().getRegisteredConfigNodes().stream()
                    .collect(Collectors.toMap(TConfigNodeLocation::getConfigNodeId, location -> location));
    AsyncConfigNodeClientHandler<TNodeLocations, TTestConnectionResp> configNodeClientHandler =
            new AsyncConfigNodeClientHandler<>(ConfigNodeRequestType.SUBMIT_TEST_CONNECTION_TASK, nodeLocations, configNodeLocationMap);
    AsyncConfigNodeClientPool.getInstance().sendAsyncRequestToConfigNode(configNodeClientHandler);
    configNodeClientHandler.getResponseMap().values().forEach(configNodeResp -> {
      if (configNodeResp.isSetResultList()) {
        merge(resp, configNodeResp);
      } else {
        // TODO: 展示错误
        LOGGER.warn("Some problem");
      }
    });
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
            configManager.getNodeManager().getRegisteredDataNodes().stream().map(TDataNodeConfiguration::getLocation)
                    .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, location -> location));
    AsyncClientHandler<TNodeLocations, TTestConnectionResp> dataNodeClientHandler =
            new AsyncClientHandler<>(DataNodeRequestType.SUBMIT_TEST_CONNECTION_TASK, nodeLocations, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNode(dataNodeClientHandler);
    dataNodeClientHandler.getResponseMap().values().forEach(dataNodeResp -> {
      if (dataNodeResp.isSetResultList()) {
        merge(resp, dataNodeResp);
      } else {
        // TODO: 展示错误
        LOGGER.warn("Some problem");
      }
    });
    return resp;
  }

  private static void merge(TTestConnectionResp resp1, TTestConnectionResp resp2) {
    resp1.resultList.addAll(resp2.getResultList());
  }

  private void submitTestConnectionTaskToAllDataNode() {

    AsyncClientHandler<TNodeLocations, TTestConnectionResp> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.SUBMIT_TEST_CONNECTION_TASK,
            new TNodeLocations(),
            configManager.getNodeManager().getRegisteredDataNodeLocations());
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    clientHandler.getResponseMap();
  }

  private void submitTestConnectionTaskToAllConfigNode() {}

  public List<TTestConnectionResult> doConnectionTest(TNodeLocations nodeLocations) {
    List<TTestConnectionResult> configNodeResult = testAllConfigNodeConnection(nodeLocations.getConfigNodeLocations());
    List<TTestConnectionResult> dataNodeResult =
        testAllDataNodeConnection(nodeLocations.getDataNodeLocations());
    configNodeResult.addAll(dataNodeResult);
    return configNodeResult;
  }

  private List<TTestConnectionResult> testConfigNodeConnection(
      TConfigNodeLocation configNodeLocation) {
    final TSender sender =
        new TSender()
            .setConfigNodeLocation(
                ConfigNodeDescriptor.getInstance().getConf().generateLocalConfigNodeLocation());
    final TTestConnectionResult result = new TTestConnectionResult();
    result.setServiceProvider(
        new TServiceProvider(
            configNodeLocation.getInternalEndPoint(), TServiceType.ConfigNodeInternalService));
    result.setSender(sender);
    List<TTestConnectionResult> results = new ArrayList<>();
    try {
      TSStatus status =
          (TSStatus)
              SyncConfigNodeClientPool.getInstance()
                  .sendSyncRequestToConfigNodeWithRetry(
                      configNodeLocation.getInternalEndPoint(),
                      null,
                      ConfigNodeRequestType.TEST_CONNECTION);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        result.setSuccess(true);
      } else {
        result.setSuccess(false);
        result.setReason("unknown");
      }
    } catch (Exception e) {
      LOGGER.error("Test connection fail", e);
      result.setSuccess(false);
      result.setReason(e.getMessage());
    }
    results.add(result);
    return results;
  }

  private List<TTestConnectionResult> testAllConfigNodeConnection(
          List<TConfigNodeLocation> configNodeLocations) {
    final TSender sender =
            new TSender()
                    .setConfigNodeLocation(
                            ConfigNodeDescriptor.getInstance().getConf().generateLocalConfigNodeLocation());
    Map<Integer, TConfigNodeLocation> configNodeLocationMap =
            configNodeLocations.stream()
                    .collect(Collectors.toMap(TConfigNodeLocation::getConfigNodeId, location -> location));
    AsyncConfigNodeClientHandler<Object, TSStatus> clientHandler =
            new AsyncConfigNodeClientHandler<>(ConfigNodeRequestType.TEST_CONNECTION, new Object(), configNodeLocationMap);
    AsyncConfigNodeClientPool.getInstance().sendAsyncRequestToConfigNodeWithRetry(clientHandler);
    Map<Integer, TConfigNodeLocation> anotherConfigNodeLocationMap =
            configNodeLocations.stream()
                    .collect(Collectors.toMap(TConfigNodeLocation::getConfigNodeId, location -> location));
    List<TTestConnectionResult> results = new ArrayList<>();
    clientHandler
            .getResponseMap()
            .forEach(
                    (configNodeId, status) -> {
                      TEndPoint endPoint = anotherConfigNodeLocationMap.get(configNodeId).getInternalEndPoint();
                      TServiceProvider serviceProvider =
                              new TServiceProvider(endPoint, TServiceType.ConfigNodeInternalService);
                      TTestConnectionResult result = new TTestConnectionResult();
                      result.setSender(sender);
                      result.setServiceProvider(serviceProvider);
                      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        result.setSuccess(true);
                      } else {
                        result.setSuccess(false);
                        result.setReason(status.getMessage());
                      }
                      results.add(result);
                    });
    return results;
  }

  private List<TTestConnectionResult> testAllDataNodeConnection(
      List<TDataNodeLocation> dataNodeLocations) {
    final TSender sender =
        new TSender()
            .setConfigNodeLocation(
                ConfigNodeDescriptor.getInstance().getConf().generateLocalConfigNodeLocation());
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        dataNodeLocations.stream()
            .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, location -> location));
    AsyncClientHandler<Object, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.TEST_CONNECTION, new Object(), dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TDataNodeLocation> anotherDataNodeLocationMap =
            dataNodeLocations.stream()
                    .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, location -> location));
    List<TTestConnectionResult> results = new ArrayList<>();
    clientHandler
        .getResponseMap()
        .forEach(
            (dataNodeId, status) -> {
              TEndPoint endPoint = anotherDataNodeLocationMap.get(dataNodeId).getInternalEndPoint();
              TServiceProvider serviceProvider =
                  new TServiceProvider(endPoint, TServiceType.DataNodeInternalService);
              TTestConnectionResult result = new TTestConnectionResult();
              result.setSender(sender);
              result.setServiceProvider(serviceProvider);
              if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                result.setSuccess(true);
              } else {
                result.setSuccess(false);
                result.setReason(status.getMessage());
              }
              results.add(result);
            });
    return results;
  }
}
