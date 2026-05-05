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
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
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
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

import org.apache.ratis.util.AwaitForSignal;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

  /** Rotation index for sqrt(N) prober selection. */
  private int proberRotationIndex = 0;

  /** Last topology pushed to each DataNode, used to detect changes. */
  private final Map<Integer, Set<Integer>> lastPushedTopology = new ConcurrentHashMap<>();

  /** Client manager for pushing topology updates to DataNodes via heartbeat RPC. */
  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      topologyPushClientManager;

  public TopologyService(
      IManager configManager, Consumer<Map<Integer, Set<Integer>>> topologyChangeListener) {
    this.configManager = configManager;
    this.topologyChangeListener = topologyChangeListener;
    this.heartbeats = new ConcurrentHashMap<>();
    this.shouldRun = new AtomicBoolean(false);
    this.awaitForSignal = new AwaitForSignal(this.getClass().getSimpleName());
    this.topologyPushClientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeHeartbeatServiceClientPoolFactory());

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
   * Schedule the {@link #topologyProbing} task either: 1. every adaptive probing interval. 2.
   * Manually triggered by outside events (node restart / register, etc.).
   */
  private boolean mayWait() {
    try {
      long baseInterval = CONF.getTopologyProbingBaseIntervalInMs();
      int dataNodeCount =
          configManager.getNodeManager().getRegisteredDataNodes().size() - startingDataNodes.size();
      int referenceNodeCount = CONF.getTopologyProbingReferenceNodeCount();
      long interval = Math.max(baseInterval, baseInterval * dataNodeCount / referenceNodeCount);
      this.awaitForSignal.await(interval, TimeUnit.MILLISECONDS);
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

  /**
   * Select sqrt(N) DataNodes as probers, rotating through all DataNodes across cycles so that every
   * DataNode gets to be a prober over sqrt(N) cycles.
   */
  private List<TDataNodeLocation> selectProbers(List<TDataNodeLocation> allDataNodes) {
    int n = allDataNodes.size();
    if (n <= 1) {
      return allDataNodes;
    }
    int sqrtN = (int) Math.ceil(Math.sqrt(n));
    List<TDataNodeLocation> sorted = new ArrayList<>(allDataNodes);
    sorted.sort(Comparator.comparingInt(TDataNodeLocation::getDataNodeId));
    int startIndex = (proberRotationIndex * sqrtN) % n;
    proberRotationIndex++;
    List<TDataNodeLocation> probers = new ArrayList<>(sqrtN);
    for (int i = 0; i < sqrtN && i < n; i++) {
      probers.add(sorted.get((startIndex + i) % n));
    }
    return probers;
  }

  private synchronized void topologyProbing() {
    // 1. get the latest datanode list, filter out starting ones
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

    // 2. compute adaptive interval and timeout from N = dataNodeLocations.size()
    final long baseInterval = CONF.getTopologyProbingBaseIntervalInMs();
    final int referenceNodeCount = CONF.getTopologyProbingReferenceNodeCount();
    final double timeoutRatio = CONF.getTopologyProbingTimeoutRatio();
    final int dataNodeCount = dataNodeLocations.size();
    final long interval = Math.max(baseInterval, baseInterval * dataNodeCount / referenceNodeCount);
    final long timeout = (long) (interval * timeoutRatio);

    // 3. select sqrt(N) probers via rotating selection
    final List<TDataNodeLocation> probers = selectProbers(dataNodeLocations);

    // 4. build TNodeLocations with ALL DataNode locations (so probers test all targets)
    final TNodeLocations nodeLocations = new TNodeLocations();
    nodeLocations.setDataNodeLocations(dataNodeLocations);
    nodeLocations.setConfigNodeLocations(Collections.emptyList());

    // 5. build proberLocationMap containing only the selected probers
    final Map<Integer, TDataNodeLocation> proberLocationMap =
        probers.stream()
            .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, location -> location));

    // 6. send async requests ONLY to probers (not all DataNodes) with computed timeout
    final DataNodeAsyncRequestContext<TNodeLocations, TTestConnectionResp>
        dataNodeAsyncRequestContext =
            new DataNodeAsyncRequestContext<>(
                CnToDnAsyncRequestType.SUBMIT_TEST_DN_INTERNAL_CONNECTION_TASK,
                nodeLocations,
                proberLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithTimeoutInMs(dataNodeAsyncRequestContext, timeout);
    final List<TTestConnectionResult> results = new ArrayList<>();
    dataNodeAsyncRequestContext
        .getResponseMap()
        .forEach(
            (nodeId, resp) -> {
              if (resp.isSetResultList()) {
                results.addAll(resp.getResultList());
              }
            });

    // 7. collect results and update the heartbeat timestamps
    for (final TTestConnectionResult result : results) {
      final int fromDataNodeId =
          Optional.ofNullable(result.getSender().getDataNodeLocation())
              .map(TDataNodeLocation::getDataNodeId)
              .orElse(-1);
      final int toDataNodeId = result.getServiceProvider().getNodeId();
      if (result.isSuccess()
          && dataNodeIds.contains(fromDataNodeId)
          && dataNodeIds.contains(toDataNodeId)) {
        final List<AbstractHeartbeatSample> heartbeatHistory =
            heartbeats.computeIfAbsent(
                new Pair<>(fromDataNodeId, toDataNodeId), p -> new LinkedList<>());
        heartbeatHistory.add(new NodeHeartbeatSample(NodeStatus.Running));
        if (heartbeatHistory.size() > SAMPLING_WINDOW_SIZE) {
          heartbeatHistory.remove(0);
        }
      }
    }

    // 8. use failure detector to identify potential network partitions (on ALL heartbeat pairs)
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

    // 9. notify the listeners on topology change
    if (shouldRun.get()) {
      topologyChangeListener.accept(latestTopology);
    }

    // 10. push topology changes to DataNodes
    pushTopologyToDataNodes(latestTopology, dataNodeLocations);
  }

  /**
   * Push topology changes to DataNodes via heartbeat RPC. Each DataNode only receives a push when
   * its own reachable set has changed since the last push.
   */
  private void pushTopologyToDataNodes(
      Map<Integer, Set<Integer>> latestTopology, List<TDataNodeLocation> dataNodeLocations) {
    // Build dataNodes map once for all pushes
    final Map<Integer, TDataNodeLocation> dataNodesMap =
        dataNodeLocations.stream()
            .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, loc -> loc));

    for (final TDataNodeLocation location : dataNodeLocations) {
      final int nodeId = location.getDataNodeId();
      final Set<Integer> reachableSet = latestTopology.getOrDefault(nodeId, Collections.emptySet());
      final Set<Integer> lastPushed = lastPushedTopology.get(nodeId);

      if (lastPushed != null && lastPushed.equals(reachableSet)) {
        continue;
      }

      final TDataNodeHeartbeatReq req =
          new TDataNodeHeartbeatReq(System.nanoTime(), false, false, 0L, 0L);
      req.setTopology(latestTopology);
      req.setDataNodes(dataNodesMap);

      final TEndPoint endPoint = location.getInternalEndPoint();
      try {
        topologyPushClientManager
            .borrowClient(endPoint)
            .getDataNodeHeartBeat(
                req,
                new AsyncMethodCallback<TDataNodeHeartbeatResp>() {
                  @Override
                  public void onComplete(TDataNodeHeartbeatResp response) {
                    // No-op: topology push is fire-and-forget
                  }

                  @Override
                  public void onError(Exception exception) {
                    // No-op: topology push failures are silently ignored
                  }
                });
        lastPushedTopology.put(nodeId, new HashSet<>(reachableSet));
      } catch (Exception e) {
        LOGGER.debug(
            "Failed to push topology to DataNode {} at {}: {}", nodeId, endPoint, e.getMessage());
      }
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
