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

package org.apache.iotdb.confignode.it.cluster;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBNodeStatusPersistenceIT {

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testStoppedNodesSurviveLeaderFailureAndLogRecovery() throws Exception {
    initCluster(5, Integer.MAX_VALUE);
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    final int stoppedConfigNodeIndex = (leaderIndex + 1) % 5;
    stopAndReportNodes(stoppedConfigNodeIndex);
    assertNodeStatuses(stoppedConfigNodeIndex, NodeStatus.Stopped);

    // Five ConfigNodes retain a quorum with both a stopped follower and a failed leader.
    EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex).stopForcibly();
    assertNotEquals(leaderIndex, EnvFactory.getEnv().getLeaderConfigNodeIndex());
    assertNodeStatuses(stoppedConfigNodeIndex, NodeStatus.Stopped);

    EnvFactory.getEnv().startConfigNode(leaderIndex);
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex)),
            Collections.singletonList(NodeStatus.Running));

    // Restart every surviving ConfigNode and restore the stopped states from consensus logs.
    restartLiveConfigNodes();
    assertNodeStatuses(stoppedConfigNodeIndex, NodeStatus.Stopped);

    EnvFactory.getEnv().startDataNode(0);
    assertNodeStatuses(stoppedConfigNodeIndex, NodeStatus.Running);

    // Keep the DataNode offline during the next election so fresh heartbeats cannot hide a
    // resurrected Stopped status. A crash without a shutdown report must become Unknown.
    EnvFactory.getEnv().getDataNodeWrapper(0).stopForcibly();
    assertNodeStatuses(stoppedConfigNodeIndex, NodeStatus.Unknown);
    final int nextLeaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv().getConfigNodeWrapper(nextLeaderIndex).stopForcibly();
    assertNotEquals(nextLeaderIndex, EnvFactory.getEnv().getLeaderConfigNodeIndex());
    assertNodeStatuses(stoppedConfigNodeIndex, NodeStatus.Unknown);
  }

  @Test
  public void testStoppedNodesSurviveSnapshotRecovery() throws Exception {
    initCluster(3, 1);
    final int stoppedConfigNodeIndex = (EnvFactory.getEnv().getLeaderConfigNodeIndex() + 1) % 3;
    stopAndReportNodes(stoppedConfigNodeIndex);
    assertNodeStatuses(stoppedConfigNodeIndex, NodeStatus.Stopped);

    final List<ConfigNodeWrapper> liveConfigNodes = getLiveConfigNodes();
    final TShowClusterResp cluster;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      cluster = client.showCluster();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), cluster.getStatus().getCode());
    }
    final int stoppedConfigNodeId =
        findConfigNodeLocation(cluster, stoppedConfigNodeIndex).getConfigNodeId();
    final int stoppedDataNodeId = cluster.getDataNodeList().get(0).getDataNodeId();

    // Verify actual completed snapshots, including their contents, before restarting the quorum.
    // A low snapshot threshold alone would not prove snapshot recovery was exercised.
    for (ConfigNodeWrapper configNode : liveConfigNodes) {
      awaitStoppedNodeSnapshot(configNode, stoppedConfigNodeId, stoppedDataNodeId);
    }
    restartLiveConfigNodes();
    assertNodeStatuses(stoppedConfigNodeIndex, NodeStatus.Stopped);
  }

  @Test
  public void testRemovingNodeSurvivesLeaderFailure() throws Exception {
    initCluster(3, Integer.MAX_VALUE);
    // Use the removal procedure's DataNode RPC. The leader learns Removing through heartbeats.
    setDataNodeSystemStatus(NodeStatus.Removing);
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(0)),
            Collections.singletonList(NodeStatus.Removing));

    // The new leader must restore Removing without any help from fresh DataNode heartbeats.
    EnvFactory.getEnv().getDataNodeWrapper(0).stopForcibly();
    assertFalse(EnvFactory.getEnv().getDataNodeWrapper(0).isAlive());
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex).stopForcibly();
    assertFalse(EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex).isAlive());
    assertNotEquals(leaderIndex, EnvFactory.getEnv().getLeaderConfigNodeIndex());
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(0)),
            Collections.singletonList(NodeStatus.Removing));
  }

  @Test
  public void testReadOnlyReasonRebuiltAfterLeaderFailureAndClearedAfterCrash() throws Exception {
    initCluster(3, Integer.MAX_VALUE);
    setDataNodeSystemStatus(NodeStatus.ReadOnly);
    awaitDataNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);

    // ReadOnly and its reason belong to the live DataNode and reach the new leader in heartbeats.
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex).stopForcibly();
    assertFalse(EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex).isAlive());
    assertNotEquals(leaderIndex, EnvFactory.getEnv().getLeaderConfigNodeIndex());
    awaitDataNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);

    // Without a shutdown report there is no durable Stopped marker. An unreachable ReadOnly
    // DataNode must become Unknown, with its old reason cleared in both node display RPCs.
    EnvFactory.getEnv().getDataNodeWrapper(0).stopForcibly();
    assertFalse(EnvFactory.getEnv().getDataNodeWrapper(0).isAlive());
    awaitDataNodeStatusWithReason(NodeStatus.Unknown, null);
  }

  private void setDataNodeSystemStatus(NodeStatus status) throws Exception {
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try (SyncDataNodeInternalServiceClient client =
        clientManager.borrowClient(
            new TEndPoint(
                EnvFactory.getEnv().getDataNodeWrapper(0).getIp(),
                EnvFactory.getEnv().getDataNodeWrapper(0).getInternalPort()))) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.setSystemStatus(status.getStatus()).getCode());
    } finally {
      clientManager.close();
    }
  }

  private void awaitDataNodeStatusWithReason(NodeStatus expectedStatus, String expectedReason)
      throws InterruptedException {
    Throwable lastFailure = null;
    for (int retry = 0; retry < 60; retry++) {
      try (SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        final TShowClusterResp cluster = client.showCluster();
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), cluster.getStatus().getCode());
        final int dataNodeId = cluster.getDataNodeList().get(0).getDataNodeId();
        assertEquals(expectedStatus.getStatus(), cluster.getNodeStatus().get(dataNodeId));
        assertEquals(expectedReason, cluster.getNodeStatusReason().get(dataNodeId));

        final TShowDataNodesResp dataNodes = client.showDataNodes();
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), dataNodes.getStatus().getCode());
        final TDataNodeInfo dataNode =
            dataNodes.getDataNodesInfoList().stream()
                .filter(node -> node.getDataNodeId() == dataNodeId)
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertEquals(expectedStatus.getStatus(), dataNode.getStatus());
        assertEquals(expectedReason, dataNode.getStatusReason());
        return;
      } catch (IOException | ClientManagerException | TException | AssertionError e) {
        lastFailure = e;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new AssertionError(
        "DataNode did not reach " + expectedStatus + " with reason " + expectedReason, lastFailure);
  }

  private void initCluster(int configNodeCount, int snapshotThreshold) throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setConfigNodeRatisSnapshotTriggerThreshold(snapshotThreshold)
        .setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(4000);
    EnvFactory.getEnv().initClusterEnvironment(configNodeCount, 1);
  }

  private void stopAndReportNodes(int configNodeIndex) throws Exception {
    final TConfigNodeLocation configNodeLocation;
    final TDataNodeLocation dataNodeLocation;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TShowClusterResp cluster = client.showCluster();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), cluster.getStatus().getCode());
      configNodeLocation = findConfigNodeLocation(cluster, configNodeIndex);
      dataNodeLocation = cluster.getDataNodeList().get(0);
    }

    EnvFactory.getEnv().getConfigNodeWrapper(configNodeIndex).stopForcibly();
    EnvFactory.getEnv().getDataNodeWrapper(0).stopForcibly();
    assertFalse(EnvFactory.getEnv().getConfigNodeWrapper(configNodeIndex).isAlive());
    assertFalse(EnvFactory.getEnv().getDataNodeWrapper(0).isAlive());

    // Deliver the shutdown hooks' reports explicitly: Process.destroy() does not execute JVM
    // shutdown hooks on every platform. These tests exercise the reports' durable handling.
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.reportConfigNodeShutdown(configNodeLocation).getCode());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.reportDataNodeShutdown(dataNodeLocation).getCode());
    }
  }

  private TConfigNodeLocation findConfigNodeLocation(
      TShowClusterResp cluster, int configNodeIndex) {
    return cluster.getConfigNodeList().stream()
        .filter(
            node ->
                node.getInternalEndPoint().getPort()
                    == EnvFactory.getEnv().getConfigNodeWrapper(configNodeIndex).getPort())
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private void assertNodeStatuses(int stoppedConfigNodeIndex, NodeStatus dataNodeStatus) {
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Arrays.asList(
                EnvFactory.getEnv().getConfigNodeWrapper(stoppedConfigNodeIndex),
                EnvFactory.getEnv().getDataNodeWrapper(0)),
            Arrays.asList(NodeStatus.Stopped, dataNodeStatus));
  }

  private List<ConfigNodeWrapper> getLiveConfigNodes() {
    return EnvFactory.getEnv().getConfigNodeWrapperList().stream()
        .filter(ConfigNodeWrapper::isAlive)
        .collect(Collectors.toList());
  }

  private void restartLiveConfigNodes() {
    final List<ConfigNodeWrapper> liveConfigNodes = getLiveConfigNodes();
    liveConfigNodes.forEach(ConfigNodeWrapper::stopForcibly);
    liveConfigNodes.forEach(node -> assertFalse(node.isAlive()));
    liveConfigNodes.forEach(ConfigNodeWrapper::start);
  }

  private void awaitStoppedNodeSnapshot(
      ConfigNodeWrapper configNode, int stoppedConfigNodeId, int stoppedDataNodeId)
      throws Exception {
    for (int retry = 0; retry < 60; retry++) {
      final List<Path> snapshotFiles = new ArrayList<>();
      try {
        final List<Path> groups;
        try (Stream<Path> paths =
            Files.list(Paths.get(configNode.getNodePath(), "data", "confignode", "consensus"))) {
          groups = paths.filter(Files::isDirectory).collect(Collectors.toList());
        }
        // Never walk temporary snapshot directories: an open directory handle can prevent
        // Ratis from atomically renaming them on Windows.
        for (Path group : groups) {
          try (Stream<Path> paths = Files.list(group.resolve("sm"))) {
            snapshotFiles.addAll(
                paths
                    .filter(path -> path.getFileName().toString().matches("\\d+_\\d+"))
                    .map(path -> path.resolve("node_info.bin"))
                    .collect(Collectors.toList()));
          }
        }
      } catch (IOException | UncheckedIOException e) {
        // Ratis can delete an older snapshot while this directory tree is being enumerated.
        TimeUnit.SECONDS.sleep(1);
        continue;
      }
      for (Path snapshotFile : snapshotFiles) {
        final NodeInfo recovered = new NodeInfo();
        try {
          recovered.processLoadSnapshot(snapshotFile.getParent().toFile());
        } catch (IOException e) {
          // A newer snapshot may have replaced this one since the directory was enumerated.
          continue;
        }
        if (NodeStatus.Stopped.equals(recovered.getPersistedNodeStatus(stoppedConfigNodeId))
            && NodeStatus.Stopped.equals(recovered.getPersistedNodeStatus(stoppedDataNodeId))) {
          return;
        }
      }
      TimeUnit.SECONDS.sleep(1);
    }
    fail("No completed snapshot contains both Stopped nodes on " + configNode.getId());
  }
}
