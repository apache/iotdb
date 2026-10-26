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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
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
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.IFailureDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.FixedDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.PhiAccrualDetector;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateClusterTopologyReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.util.AwaitForSignal;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TopologyService implements Runnable, IClusterStatusSubscriber {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyService.class);
  private static final int SAMPLING_WINDOW_SIZE = 100;
  private static final int TOPOLOGY_PROBING_RETRY_NUM = 1;

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

  private final IFailureDetector failureDetector;
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private int proberRotationIndex = 0;

  /** Last topology pushed to each DataNode, updated only on successful push. */
  private final Map<Integer, Set<Integer>> lastPushedTopology = new ConcurrentHashMap<>();

  /** Latest computed topology, updated each probing round. */
  private final Map<Integer, Set<Integer>> latestTopology = new ConcurrentHashMap<>();

  public TopologyService(
      IManager configManager, Consumer<Map<Integer, Set<Integer>>> topologyChangeListener) {
    this.configManager = configManager;
    this.topologyChangeListener = topologyChangeListener;
    this.heartbeats = new ConcurrentHashMap<>();
    this.shouldRun = new AtomicBoolean(false);
    this.awaitForSignal = new AwaitForSignal(this.getClass().getSimpleName());

    switch (CONF.getFailureDetector()) {
      case IFailureDetector.PHI_ACCRUAL_DETECTOR:
        this.failureDetector =
            new PhiAccrualDetector(
                CONF.getFailureDetectorPhiThreshold(),
                CONF.getFailureDetectorPhiAcceptablePauseInMs() * 1000_000L,
                CONF.getFailureDetectorHeartbeatIntervalInMs() * 200_000L,
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
    LOGGER.info(ManagerMessages.TOPOLOGY_PROBING_HAS_STARTED_SUCCESSFULLY);
  }

  public synchronized void stopTopologyService() {
    shouldRun.set(false);
    if (future != null) {
      future.cancel(true);
      future = null;
    }
    heartbeats.clear();
    latestTopology.clear();
    LOGGER.info(ManagerMessages.TOPOLOGY_PROBING_HAS_STOPPED_SUCCESSFULLY);
  }

  private boolean mayWait() {
    try {
      this.awaitForSignal.await(CONF.getTopologyProbingBaseIntervalInMs(), TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException e) {
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
    // 1. get Running DataNodes only
    final List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    final Set<Integer> dataNodeIds = new HashSet<>();
    for (final TDataNodeConfiguration dataNodeConf :
        configManager.getNodeManager().getRegisteredDataNodes()) {
      final TDataNodeLocation location = dataNodeConf.getLocation();
      if (configManager.getLoadManager().getNodeStatus(location.getDataNodeId())
          != NodeStatus.Running) {
        continue;
      }
      dataNodeLocations.add(location);
      dataNodeIds.add(location.getDataNodeId());
    }

    // 2. compute probing timeout
    final long timeout =
        (long) (CONF.getTopologyProbingBaseIntervalInMs() * CONF.getTopologyProbingTimeoutRatio());

    // 3. select sqrt(N) probers via rotating selection
    final List<TDataNodeLocation> probers = selectProbers(dataNodeLocations);
    final Set<Integer> proberIds =
        probers.stream().map(TDataNodeLocation::getDataNodeId).collect(Collectors.toSet());

    // 4. build TNodeLocations with ALL DataNode locations (so probers test all targets)
    final TNodeLocations nodeLocations = new TNodeLocations();
    nodeLocations.setDataNodeLocations(dataNodeLocations);
    nodeLocations.setConfigNodeLocations(Collections.emptyList());

    // 5. build proberLocationMap containing only the selected probers
    final Map<Integer, TDataNodeLocation> proberLocationMap =
        probers.stream()
            .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, location -> location));

    // 6. send async requests ONLY to probers with computed timeout
    final DataNodeAsyncRequestContext<TNodeLocations, TTestConnectionResp>
        dataNodeAsyncRequestContext =
            new DataNodeAsyncRequestContext<>(
                CnToDnAsyncRequestType.SUBMIT_TEST_DN_INTERNAL_CONNECTION_TASK,
                nodeLocations,
                proberLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(dataNodeAsyncRequestContext, TOPOLOGY_PROBING_RETRY_NUM, timeout, true);
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

    // 8. build topology: for non-probers carry forward previous results (default all-connected),
    //    for probers run failure detector
    final Map<Integer, Set<Integer>> computedTopology = new HashMap<>();
    for (int nodeId : dataNodeIds) {
      if (proberIds.contains(nodeId)) {
        computedTopology.put(nodeId, new HashSet<>());
      } else {
        Set<Integer> prev = latestTopology.get(nodeId);
        if (prev != null) {
          Set<Integer> carried = new HashSet<>(prev);
          carried.retainAll(dataNodeIds);
          computedTopology.put(nodeId, carried);
        } else {
          computedTopology.put(nodeId, new HashSet<>(dataNodeIds));
        }
      }
    }

    for (final Map.Entry<Pair<Integer, Integer>, List<AbstractHeartbeatSample>> entry :
        heartbeats.entrySet()) {
      final int fromId = entry.getKey().getLeft();
      final int toId = entry.getKey().getRight();

      if (!proberIds.contains(fromId)
          || !dataNodeIds.contains(fromId)
          || !dataNodeIds.contains(toId)) {
        continue;
      }

      if (!entry.getValue().isEmpty()
          && !failureDetector.isAvailable(entry.getKey(), entry.getValue())) {
        LOGGER.debug(ManagerMessages.CONNECTION_FROM_DATANODE_TO_DATANODE_IS_BROKEN, fromId, toId);
      } else {
        computedTopology.get(fromId).add(toId);
      }
    }

    // For prober nodes: pairs with no heartbeat history default to connected
    for (int fromId : proberIds) {
      if (!dataNodeIds.contains(fromId)) {
        continue;
      }
      Set<Integer> reachableSet = computedTopology.get(fromId);
      for (int toId : dataNodeIds) {
        if (!heartbeats.containsKey(new Pair<>(fromId, toId))) {
          reachableSet.add(toId);
        }
      }
    }

    // Save computed topology for next round
    latestTopology.clear();
    for (Map.Entry<Integer, Set<Integer>> entry : computedTopology.entrySet()) {
      latestTopology.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }

    logAsymmetricPartition(computedTopology);

    // 9. notify the listeners on topology change
    if (shouldRun.get()) {
      topologyChangeListener.accept(computedTopology);
    }

    // 10. push topology changes to DataNodes
    pushTopologyToDataNodes(computedTopology, dataNodeLocations);
  }

  /**
   * Push topology changes to DataNodes via PUSH_TOPOLOGY request. Each DataNode receives only its
   * own reachable set. lastPushedTopology is updated only on successful push.
   */
  private void pushTopologyToDataNodes(
      Map<Integer, Set<Integer>> computedTopology, List<TDataNodeLocation> dataNodeLocations) {
    final Map<Integer, TDataNodeLocation> dataNodesMap =
        dataNodeLocations.stream()
            .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, loc -> loc));

    final Map<Integer, TDataNodeLocation> targetMap = new HashMap<>();
    for (final TDataNodeLocation location : dataNodeLocations) {
      final int nodeId = location.getDataNodeId();
      final Set<Integer> reachableSet =
          computedTopology.getOrDefault(nodeId, Collections.emptySet());
      final Set<Integer> lastPushed = lastPushedTopology.get(nodeId);
      if (lastPushed != null && lastPushed.equals(reachableSet)) {
        continue;
      }
      targetMap.put(nodeId, location);
    }

    if (targetMap.isEmpty()) {
      return;
    }

    final DataNodeAsyncRequestContext<TUpdateClusterTopologyReq, TSStatus> context =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.PUSH_TOPOLOGY, targetMap);
    for (final Map.Entry<Integer, TDataNodeLocation> entry : targetMap.entrySet()) {
      final int nodeId = entry.getKey();
      final Set<Integer> reachableSet =
          computedTopology.getOrDefault(nodeId, Collections.emptySet());
      final Map<Integer, Set<Integer>> perNodeTopology = new HashMap<>();
      perNodeTopology.put(nodeId, new HashSet<>(reachableSet));
      final TUpdateClusterTopologyReq req =
          new TUpdateClusterTopologyReq(dataNodesMap, perNodeTopology);
      context.putRequest(nodeId, req);
    }

    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            context, TOPOLOGY_PROBING_RETRY_NUM, CONF.getTopologyProbingBaseIntervalInMs(), true);

    context
        .getResponseMap()
        .forEach(
            (nodeId, resp) -> {
              if (resp.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                Set<Integer> reachableSet =
                    computedTopology.getOrDefault(nodeId, Collections.emptySet());
                lastPushedTopology.put(nodeId, new HashSet<>(reachableSet));
              }
            });
  }

  private void logAsymmetricPartition(final Map<Integer, Set<Integer>> topology) {
    final Set<Integer> nodes = topology.keySet();
    if (nodes.size() == 1) {
      return;
    }

    for (int from : nodes) {
      for (int to : nodes) {
        if (from >= to) {
          continue;
        }

        final Set<Integer> reachableFrom = topology.get(from);
        final Set<Integer> reachableTo = topology.get(to);
        if (reachableFrom.size() <= 1 || reachableTo.size() <= 1) {
          continue;
        }
        if (!reachableTo.contains(from) && !reachableFrom.contains(to)) {
          LOGGER.debug(ManagerMessages.TOPOLOGY_ASYMMETRIC_NETWORK_PARTITION_FROM_TO, from, to);
        }
      }
    }
  }

  /** Clean up heartbeat and push state when a node is removed or entering Removing status. */
  @Override
  public void onNodeStatisticsChanged(NodeStatisticsChangeEvent event) {
    for (final Map.Entry<Integer, Pair<NodeStatistics, NodeStatistics>> entry :
        event.getDifferentNodeStatisticsMap().entrySet()) {
      final Integer nodeId = entry.getKey();
      final Pair<NodeStatistics, NodeStatistics> change = entry.getValue();
      if (change.getRight() == null || NodeStatus.Removing.equals(change.getRight().getStatus())) {
        heartbeats
            .keySet()
            .removeIf(
                pair ->
                    Objects.equals(pair.getLeft(), nodeId)
                        || Objects.equals(pair.getRight(), nodeId));
        lastPushedTopology.remove(nodeId);
        latestTopology.remove(nodeId);
      }
    }
  }
}
