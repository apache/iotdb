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
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
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
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;

import org.apache.ratis.util.AwaitForSignal;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TopologyService implements Runnable, IClusterStatusSubscriber {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyService.class);
  private static final long PROBING_INTERVAL_MS = 5_000L;
  private static final long PROBING_TIMEOUT_MS = PROBING_INTERVAL_MS;
  private static final int SAMPLING_WINDOW_SIZE = 100;

  private final ExecutorService topologyThread =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.CONFIG_NODE_TOPOLOGY_SERVICE.getName());

  private Future<?> future = null;
  private final Consumer<Map<Integer, Set<Integer>>> topologyChangeListener;

  private final AwaitForSignal awaitForSignal;
  private final IManager configManager;

  private final AtomicBoolean shouldRun;

  /* (fromDataNodeId, toDataNodeId) -> heartbeat history */
  private final Map<Pair<Integer, Integer>, List<AbstractHeartbeatSample>> heartbeats;
  private final List<Integer> startingDataNodes = new CopyOnWriteArrayList<>();

  private final IFailureDetector failureDetector;
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  public TopologyService(
      IManager configManager, Consumer<Map<Integer, Set<Integer>>> topologyChangeListener) {
    this.configManager = configManager;
    this.topologyChangeListener = topologyChangeListener;
    this.heartbeats = new ConcurrentHashMap<>();
    this.shouldRun = new AtomicBoolean(false);
    this.awaitForSignal = new AwaitForSignal(this.getClass().getSimpleName());

    // here we use the same failure
    switch (CONF.getFailureDetector()) {
      case IFailureDetector.PHI_ACCRUAL_DETECTOR:
        this.failureDetector =
            new PhiAccrualDetector(
                CONF.getFailureDetectorPhiThreshold(),
                CONF.getFailureDetectorPhiAcceptablePauseInMs() * 1000_000L,
                CONF.getHeartbeatIntervalInMs() * 200_000L,
                IFailureDetector.PHI_COLD_START_THRESHOLD,
                new FixedDetector(CONF.getFailureDetectorFixedThresholdInMs() * 1000_000L));
        break;
      case IFailureDetector.FIXED_DETECTOR:
      default:
        this.failureDetector =
            new FixedDetector(CONF.getFailureDetectorFixedThresholdInMs() * 1000_000L);
    }
  }

  public synchronized void startTopologyService() {
    if (future == null) {
      future = this.topologyThread.submit(this);
    }
    shouldRun.set(true);
    LOGGER.info("Topology Probing has started successfully");
  }

  public synchronized void stopTopologyService() {
    shouldRun.set(false);
    future.cancel(true);
    future = null;
    heartbeats.clear();
    LOGGER.info("Topology Probing has stopped successfully");
  }

  /**
   * Schedule the {@link #topologyProbing} task either: 1. every PROBING_INTERVAL_MS interval. 2.
   * Manually triggered by outside events (node restart / register, etc.).
   */
  private boolean mayWait() {
    try {
      this.awaitForSignal.await(PROBING_INTERVAL_MS, TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException e) {
      // we don't reset the interrupt flag here since we may reuse this thread again.
      return false;
    }
  }

  @Override
  public void run() {
    while (shouldRun.get() && mayWait()) {
      topologyProbing();
    }
  }

  private synchronized void topologyProbing() {
    // 1. get the latest datanode list
    final List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    final Set<Integer> dataNodeIds = new HashSet<>();
    for (final TDataNodeConfiguration dataNodeConf :
        configManager.getNodeManager().getRegisteredDataNodes()) {
      final TDataNodeLocation location = dataNodeConf.getLocation();
      if (startingDataNodes.contains(location.getDataNodeId())) {
        continue; // we shall wait for internal endpoint to be ready
      }
      dataNodeLocations.add(location);
      dataNodeIds.add(location.getDataNodeId());
    }

    // 2. send the verify connection RPC to all datanodes
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
      final int toDataNodeId = result.getServiceProvider().getNodeId();
      if (result.isSuccess()
          && dataNodeIds.contains(fromDataNodeId)
          && dataNodeIds.contains(toDataNodeId)) {
        // testAllDataNodeConnectionWithTimeout ensures the heartbeats are Dn-Dn internally. Here we
        // just double-check.
        final List<AbstractHeartbeatSample> heartbeatHistory =
            heartbeats.computeIfAbsent(
                new Pair<>(fromDataNodeId, toDataNodeId), p -> new LinkedList<>());
        heartbeatHistory.add(new NodeHeartbeatSample(NodeStatus.Running));
        if (heartbeatHistory.size() > SAMPLING_WINDOW_SIZE) {
          heartbeatHistory.remove(0);
        }
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

      if (!entry.getValue().isEmpty()
          && !failureDetector.isAvailable(entry.getKey(), entry.getValue())) {
        LOGGER.debug("Connection from DataNode {} to DataNode {} is broken", fromId, toId);
      } else {
        Optional.ofNullable(latestTopology.get(fromId)).ifPresent(s -> s.add(toId));
      }
    }

    logAsymmetricPartition(latestTopology);

    // 5. notify the listeners on topology change
    if (shouldRun.get()) {
      topologyChangeListener.accept(latestTopology);
    }
  }

  /**
   * We only consider warning (one vs remaining) network partition. If we need to cover more
   * complicated scenarios like (many vs many) network partition, we shall use graph algorithms
   * then.
   */
  private void logAsymmetricPartition(final Map<Integer, Set<Integer>> topology) {
    final Set<Integer> nodes = topology.keySet();
    if (nodes.size() == 1) {
      // 1 DataNode
      return;
    }

    for (int from : nodes) {
      for (int to : nodes) {
        if (from >= to) {
          continue;
        }

        // whether we have asymmetric partition [from -> to]
        final Set<Integer> reachableFrom = topology.get(from);
        final Set<Integer> reachableTo = topology.get(to);
        if (reachableFrom.size() <= 1 || reachableTo.size() <= 1) {
          // symmetric partition for (from) or (to)
          continue;
        }
        if (!reachableTo.contains(from) && !reachableFrom.contains(to)) {
          LOGGER.debug("[Topology] Asymmetric network partition from {} to {}", from, to);
        }
      }
    }
  }

  /** We only listen to datanode remove / restart / register events */
  @Override
  public void onNodeStatisticsChanged(NodeStatisticsChangeEvent event) {
    final Set<Integer> datanodeIds =
        configManager.getNodeManager().getRegisteredDataNodeLocations().keySet();
    final Map<Integer, Pair<NodeStatistics, NodeStatistics>> changes =
        event.getDifferentNodeStatisticsMap();
    for (final Map.Entry<Integer, Pair<NodeStatistics, NodeStatistics>> entry :
        changes.entrySet()) {
      final Integer nodeId = entry.getKey();
      final Pair<NodeStatistics, NodeStatistics> changeEvent = entry.getValue();
      if (!datanodeIds.contains(nodeId)) {
        continue;
      }
      if (changeEvent.getLeft() == null) {
        // if a new datanode registered, DO NOT trigger probing immediately
        startingDataNodes.add(nodeId);
        continue;
      } else {
        startingDataNodes.remove(nodeId);
      }

      final Set<Pair<Integer, Integer>> affectedPairs =
          heartbeats.keySet().stream()
              .filter(
                  pair ->
                      Objects.equals(pair.getLeft(), nodeId)
                          || Objects.equals(pair.getRight(), nodeId))
              .collect(Collectors.toSet());

      if (changeEvent.getRight() == null) {
        // datanode removed from cluster, clean up probing history
        affectedPairs.forEach(heartbeats::remove);
      } else {
        // we only trigger probing immediately if node comes around from UNKNOWN to RUNNING
        if (NodeStatus.Unknown.equals(changeEvent.getLeft().getStatus())
            && NodeStatus.Running.equals(changeEvent.getRight().getStatus())) {
          // let's clear the history when a new node comes around
          affectedPairs.forEach(pair -> heartbeats.put(pair, new ArrayList<>()));
          awaitForSignal.signal();
        }
      }
    }
  }
}
