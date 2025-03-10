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

package org.apache.iotdb.confignode.manager.load.service;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.IFailureDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.FixedDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.PhiAccrualDetector;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TopologyService {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyService.class);
  private static final long PROBING_INTERVAL_MS = 5_000L;
  private static final long PROBING_TIMEOUT_MS = 1_000L;
  private final ScheduledExecutorService topologyExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.CONFIG_NODE_TOPOLOGY_SERVICE.getName());

  private final Consumer<Map<Integer, Set<Integer>>> topologyChangeListener;
  private final IManager configManager;
  private ScheduledFuture<?> future;

  /* (fromDataNodeId, toDataNodeId) -> heartbeat history */
  private final Map<Pair<Integer, Integer>, List<AbstractHeartbeatSample>> heartbeats;

  private final IFailureDetector failureDetector;
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  public TopologyService(
      IManager configManager, Consumer<Map<Integer, Set<Integer>>> topologyChangeListener) {
    this.configManager = configManager;
    this.topologyChangeListener = topologyChangeListener;
    this.heartbeats = new HashMap<>();

    // here we use the same failure
    switch (CONF.getFailureDetector()) {
      case IFailureDetector.PHI_ACCRUAL_DETECTOR:
        this.failureDetector =
            new PhiAccrualDetector(
                CONF.getFailureDetectorPhiThreshold(),
                CONF.getFailureDetectorPhiAcceptablePauseInMs() * 1000_000L,
                CONF.getHeartbeatIntervalInMs() * 200_000L,
                60,
                new FixedDetector(CONF.getFailureDetectorFixedThresholdInMs() * 1000_000L));
        break;
      case IFailureDetector.FIXED_DETECTOR:
      default:
        this.failureDetector =
            new FixedDetector(CONF.getFailureDetectorFixedThresholdInMs() * 1000_000L);
    }
  }

  public void startTopologyService() {
    future =
        ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
            topologyExecutor, this::topologyProbing, 0, PROBING_INTERVAL_MS, TimeUnit.MILLISECONDS);
    LOGGER.info("Topology Probing has started successfully");
  }

  public void stopTopologyService() {
    future.cancel(true);
    future = null;
    LOGGER.info("Topology Probing has stopped successfully");
  }

  private void topologyProbing() {
    // 1. get the latest datanode list
    final List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    final Set<Integer> dataNodeIds = new HashSet<>();
    final Map<TEndPoint, Integer> endPoint2IdMap = new HashMap<>();
    for (final TDataNodeConfiguration dataNodeConf :
        configManager.getNodeManager().getRegisteredDataNodes()) {
      final TDataNodeLocation location = dataNodeConf.getLocation();
      dataNodeLocations.add(location);
      dataNodeIds.add(location.getDataNodeId());
      endPoint2IdMap.put(location.getInternalEndPoint(), location.getDataNodeId());
    }

    // 2. send the verify connection RPC to all datanode
    final TNodeLocations nodeLocations = new TNodeLocations();
    nodeLocations.setDataNodeLocations(dataNodeLocations);
    nodeLocations.setConfigNodeLocations(Collections.emptyList());
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodes().stream()
            .map(TDataNodeConfiguration::getLocation)
            .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, location -> location));
    final DataNodeAsyncRequestContext<TNodeLocations, TTestConnectionResp>
        dataNodeAsyncRequestContext =
            new DataNodeAsyncRequestContext<>(
                CnToDnAsyncRequestType.SUBMIT_TEST_DN_INTERNAL_CONNECTION_TASK,
                nodeLocations,
                dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithTimeoutInMs(dataNodeAsyncRequestContext, PROBING_TIMEOUT_MS);
    final List<TTestConnectionResult> results = new ArrayList<>();
    dataNodeAsyncRequestContext
        .getResponseMap()
        .forEach(
            (nodeId, resp) -> {
              if (resp.isSetResultList()) {
                results.addAll(resp.getResultList());
              }
            });

    // 3. collect results and update the heartbeat timestamps
    for (final TTestConnectionResult result : results) {
      final int fromDataNodeId =
          Optional.ofNullable(result.getSender().getDataNodeLocation())
              .map(TDataNodeLocation::getDataNodeId)
              .orElse(-1);
      final int toDataNodeId =
          Optional.ofNullable(endPoint2IdMap.get(result.getServiceProvider().getEndPoint()))
              .orElse(-1);
      if (result.isSuccess()
          && dataNodeIds.contains(fromDataNodeId)
          && dataNodeIds.contains(toDataNodeId)) {
        // testAllDataNodeConnectionWithTimeout ensures the heartbeats are Dn-Dn internally. Here we
        // just double-check.
        heartbeats
            .computeIfAbsent(new Pair<>(fromDataNodeId, toDataNodeId), p -> new ArrayList<>())
            .add(new NodeHeartbeatSample(NodeStatus.Running));
      }
    }

    // 4. use failure detector to identify potential network partitions
    final Map<Integer, Set<Integer>> latestTopology =
        dataNodeLocations.stream()
            .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, k -> new HashSet<>()));
    for (final Map.Entry<Pair<Integer, Integer>, List<AbstractHeartbeatSample>> entry :
        heartbeats.entrySet()) {
      final int fromId = entry.getKey().getLeft();
      final int toId = entry.getKey().getRight();
      if (entry.getValue().isEmpty() || failureDetector.isAvailable(entry.getValue())) {
        // when the first heartbeat is not received, we still consider this node reachable
        latestTopology.get(fromId).add(toId);
      } else {
        LOGGER.info(
            String.format("Connection from DataNode %d to DataNode %d is broken", fromId, toId));
      }
    }

    // 5. notify the listeners on topology change
    topologyChangeListener.accept(latestTopology);
  }
}
