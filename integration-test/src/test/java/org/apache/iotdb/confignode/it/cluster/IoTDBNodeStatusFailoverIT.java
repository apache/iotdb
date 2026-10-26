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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/** Node status recovery observed through production ConfigNode RPCs and actual process failures. */
@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class IoTDBNodeStatusFailoverIT {
  private IClientManager<TEndPoint, SyncConfigNodeIServiceClient> configNodeClients;

  @After
  public void tearDown() throws InterruptedException {
    List<AbstractNodeWrapper> nodes = new ArrayList<>(EnvFactory.getEnv().getNodeWrapperList());
    try {
      // Stop every process before directory cleanup can fail and skip the remaining nodes.
      for (AbstractNodeWrapper node : nodes) {
        node.stopForcibly();
      }
      for (AbstractNodeWrapper node : nodes) {
        if (node.getInstance() != null) {
          assertTrue(node.getId(), node.getInstance().waitFor(60, TimeUnit.SECONDS));
          assertFalse(node.getId(), node.isAlive());
        }
      }
    } finally {
      try {
        if (configNodeClients != null) {
          configNodeClients.close();
        }
      } finally {
        EnvFactory.getEnv().cleanClusterEnvironment();
      }
    }
  }

  @Test
  public void testReportedConfigNodeShutdownClearsAfterRestart() throws Exception {
    init(1);
    int follower = (awaitLeader() + 1) % 3;
    TConfigNodeLocation location = configLocation(cluster(), follower);
    stopForcibly(EnvFactory.getEnv().getConfigNodeWrapper(follower));
    // Exercise the production shutdown report, without depending on platform-specific JVM hooks.
    try (SyncConfigNodeIServiceClient client = leaderClient()) {
      assertSuccess(client.reportConfigNodeShutdown(location));
    }
    awaitConfigNodeStatus(location.getConfigNodeId(), NodeStatus.Stopped);

    EnvFactory.getEnv().startConfigNode(follower);
    awaitConfigNodeStatus(location.getConfigNodeId(), NodeStatus.Running);
    stopForcibly(EnvFactory.getEnv().getConfigNodeWrapper(follower));
    awaitConfigNodeStatus(location.getConfigNodeId(), NodeStatus.Unknown);
  }

  @Test
  public void testStoppedDataNodeSurvivesLeaderFailure() throws Exception {
    init(1);
    int stopped = stopAndReportDataNode(0);
    awaitDataNodeStatus(stopped, NodeStatus.Stopped, null);
    int leader = awaitLeader();
    stopForcibly(EnvFactory.getEnv().getConfigNodeWrapper(leader));
    assertNotEquals(leader, awaitLeader());
    // The DataNode stays offline, so fresh heartbeats cannot hide lost persisted state.
    awaitDataNodeStatus(stopped, NodeStatus.Stopped, null);

    EnvFactory.getEnv().startConfigNode(leader);
    awaitConfigNodeStatus(configLocation(cluster(), leader).getConfigNodeId(), NodeStatus.Running);
    awaitDataNodeStatus(stopped, NodeStatus.Stopped, null);
  }

  @Test
  public void testReadOnlyReasonSurvivesLeaderFailureAndClearsAfterRestart() throws Exception {
    init(1);
    TDataNodeLocation dataNode = dataLocation(cluster(), 0);
    IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try (SyncDataNodeInternalServiceClient client =
        clients.borrowClient(dataNode.getInternalEndPoint())) {
      assertSuccess(client.setSystemStatus(NodeStatus.ReadOnly.getStatus()));
    } finally {
      clients.close();
    }
    int nodeId = dataNode.getDataNodeId();
    awaitDataNodeStatus(nodeId, NodeStatus.ReadOnly, NodeStatus.MANUAL);
    int leader = awaitLeader();
    stopForcibly(EnvFactory.getEnv().getConfigNodeWrapper(leader));
    assertNotEquals(leader, awaitLeader());
    // This checks the visible status and reason; live heartbeats may rebuild both.
    awaitDataNodeStatus(nodeId, NodeStatus.ReadOnly, NodeStatus.MANUAL);

    assertEquals(nodeId, stopAndReportDataNode(0));
    awaitDataNodeStatus(nodeId, NodeStatus.Stopped, null);
    EnvFactory.getEnv().startDataNode(0);
    awaitDataNodeStatus(nodeId, NodeStatus.Running, null);
  }

  @Test
  public void testStoppedStatusRecoversAndClearsAcrossConfigNodeRestarts() throws Exception {
    init(1);
    int nodeId = stopAndReportDataNode(0);
    awaitDataNodeStatus(nodeId, NodeStatus.Stopped, null);
    restartConfigNodes();
    awaitDataNodeStatus(nodeId, NodeStatus.Stopped, null);

    EnvFactory.getEnv().startDataNode(0);
    awaitDataNodeStatus(nodeId, NodeStatus.Running, null);
    // Do not report the second stop. The old Stopped marker must not return after recovery.
    stopForcibly(EnvFactory.getEnv().getDataNodeWrapper(0));
    awaitDataNodeStatus(nodeId, NodeStatus.Unknown, null);
    restartConfigNodes();
    awaitDataNodeStatus(nodeId, NodeStatus.Unknown, null);
  }

  @Test
  public void testRemovedDataNodeStaysUnregisteredAfterLeaderFailure() throws Exception {
    // The production removal API does not support SimpleConsensus.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1);
    init(2);
    TShowClusterResp before = cluster();
    TDataNodeLocation victim = dataLocation(before, 0);
    int survivor = dataLocation(before, 1).getDataNodeId();
    try (SyncConfigNodeIServiceClient client = leaderClient()) {
      assertSuccess(
          client
              .removeDataNode(new TDataNodeRemoveReq(Collections.singletonList(victim)))
              .getStatus());
    }
    // Observe the completed procedure; Removing is transient and need not be sampled.
    awaitRemoved(victim.getDataNodeId(), survivor);
    int leader = awaitLeader();
    stopForcibly(EnvFactory.getEnv().getConfigNodeWrapper(leader));
    assertNotEquals(leader, awaitLeader());
    awaitRemoved(victim.getDataNodeId(), survivor);
  }

  private void init(int dataNodes) throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setConfigNodeRatisSnapshotTriggerThreshold(Integer.MAX_VALUE)
        .setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(4000);
    EnvFactory.getEnv().initClusterEnvironment(3, dataNodes);
    configNodeClients =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    // Fault-window observations use CN RPCs, which do not open a DataNode login session.
    // This suite does not inspect individual replicas or assert a particular snapshot boundary.
  }

  private int stopAndReportDataNode(int index) throws Exception {
    TDataNodeLocation node = dataLocation(cluster(), index);
    stopForcibly(EnvFactory.getEnv().getDataNodeWrapper(index));
    try (SyncConfigNodeIServiceClient client = leaderClient()) {
      // Success acknowledges the persisted report, unlike observing a heartbeat-derived status.
      assertSuccess(client.reportDataNodeShutdown(node));
    }
    return node.getDataNodeId();
  }

  private void stopForcibly(AbstractNodeWrapper node) throws InterruptedException {
    node.stopForcibly();
    assertTrue(node.getId(), node.getInstance().waitFor(60, TimeUnit.SECONDS));
    assertFalse(node.getId(), node.isAlive());
  }

  private void restartConfigNodes() throws Exception {
    List<ConfigNodeWrapper> nodes = EnvFactory.getEnv().getConfigNodeWrapperList();
    for (ConfigNodeWrapper node : nodes) {
      stopForcibly(node);
    }
    configNodeClients.clearAll();
    nodes.forEach(ConfigNodeWrapper::start);
    awaitCluster(
        "All ConfigNodes must recover",
        (client, cluster) -> {
          assertEquals(3, cluster.getConfigNodeListSize());
          for (TConfigNodeLocation node : cluster.getConfigNodeList()) {
            assertEquals(
                cluster.toString(),
                NodeStatus.Running.getStatus(),
                cluster.getNodeStatus().get(node.getConfigNodeId()));
          }
        });
  }

  private void awaitDataNodeStatus(int nodeId, NodeStatus status, String reason)
      throws InterruptedException {
    awaitCluster(
        "DataNode " + nodeId + " must become " + status + " with reason " + reason,
        (client, cluster) -> {
          assertTrue(
              cluster.toString(),
              cluster.getDataNodeList().stream().anyMatch(n -> n.getDataNodeId() == nodeId));
          assertEquals(cluster.toString(), status.getStatus(), cluster.getNodeStatus().get(nodeId));
          assertEquals(cluster.toString(), reason, cluster.getNodeStatusReason().get(nodeId));
          TShowDataNodesResp response = client.showDataNodes();
          assertSuccess(response.getStatus());
          TDataNodeInfo node =
              response.getDataNodesInfoList().stream()
                  .filter(n -> n.getDataNodeId() == nodeId)
                  .findFirst()
                  .orElseThrow(() -> new AssertionError(response));
          assertEquals(response.toString(), status.getStatus(), node.getStatus());
          assertEquals(response.toString(), reason, node.getStatusReason());
        });
  }

  private void awaitConfigNodeStatus(int nodeId, NodeStatus status) throws InterruptedException {
    awaitCluster(
        "ConfigNode " + nodeId + " must become " + status,
        (client, cluster) -> {
          assertTrue(
              cluster.toString(),
              cluster.getConfigNodeList().stream().anyMatch(n -> n.getConfigNodeId() == nodeId));
          assertEquals(cluster.toString(), status.getStatus(), cluster.getNodeStatus().get(nodeId));
          TShowConfigNodesResp response = client.showConfigNodes();
          assertSuccess(response.getStatus());
          TConfigNodeInfo node =
              response.getConfigNodesInfoList().stream()
                  .filter(n -> n.getConfigNodeId() == nodeId)
                  .findFirst()
                  .orElseThrow(() -> new AssertionError(response));
          assertEquals(response.toString(), status.getStatus(), node.getStatus());
        });
  }

  private void awaitRemoved(int removed, int remaining) throws InterruptedException {
    awaitCluster(
        "DataNode " + removed + " must stay unregistered",
        (client, cluster) -> {
          assertEquals(
              cluster.toString(),
              Collections.singletonList(remaining),
              cluster.getDataNodeList().stream()
                  .map(TDataNodeLocation::getDataNodeId)
                  .collect(Collectors.toList()));
          assertFalse(cluster.toString(), cluster.getNodeStatus().containsKey(removed));
          TShowDataNodesResp response = client.showDataNodes();
          assertSuccess(response.getStatus());
          assertEquals(
              response.toString(),
              Collections.singletonList(remaining),
              response.getDataNodesInfoList().stream()
                  .map(TDataNodeInfo::getDataNodeId)
                  .collect(Collectors.toList()));
        });
  }

  private int awaitLeader() throws InterruptedException {
    return awaitCluster("A ConfigNode leader must be ready", (client, cluster) -> {});
  }

  private int awaitCluster(String description, ClusterAssertion assertion)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
    Throwable lastFailure = null;
    List<ConfigNodeWrapper> nodes = EnvFactory.getEnv().getConfigNodeWrapperList();
    do {
      for (int i = 0; i < nodes.size(); i++) {
        if (!nodes.get(i).isAlive()) {
          continue;
        }
        try (SyncConfigNodeIServiceClient client = configNodeClient(i)) {
          TShowClusterResp response = client.showCluster();
          assertSuccess(response.getStatus());
          // A successful showCluster also checks consensus and leader-service readiness.
          assertion.check(client, response);
          return i;
        } catch (ClientManagerException | TException | AssertionError e) {
          lastFailure = e;
        }
      }
      TimeUnit.SECONDS.sleep(1);
    } while (System.nanoTime() < deadline);
    throw new AssertionError(description, lastFailure);
  }

  private SyncConfigNodeIServiceClient configNodeClient(int index) throws ClientManagerException {
    ConfigNodeWrapper node = EnvFactory.getEnv().getConfigNodeWrapper(index);
    return configNodeClients.borrowClient(new TEndPoint(node.getIp(), node.getPort()));
  }

  private SyncConfigNodeIServiceClient leaderClient()
      throws InterruptedException, ClientManagerException {
    return configNodeClient(awaitLeader());
  }

  private TShowClusterResp cluster() throws Exception {
    try (SyncConfigNodeIServiceClient client = leaderClient()) {
      TShowClusterResp response = client.showCluster();
      assertSuccess(response.getStatus());
      return response;
    }
  }

  private TDataNodeLocation dataLocation(TShowClusterResp cluster, int index) {
    return cluster.getDataNodeList().stream()
        .filter(
            n ->
                n.getInternalEndPoint().getPort()
                    == EnvFactory.getEnv().getDataNodeWrapper(index).getInternalPort())
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private TConfigNodeLocation configLocation(TShowClusterResp cluster, int index) {
    return cluster.getConfigNodeList().stream()
        .filter(
            n ->
                n.getInternalEndPoint().getPort()
                    == EnvFactory.getEnv().getConfigNodeWrapper(index).getPort())
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private void assertSuccess(TSStatus status) {
    assertEquals(status.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @FunctionalInterface
  private interface ClusterAssertion {
    void check(SyncConfigNodeIServiceClient client, TShowClusterResp cluster) throws TException;
  }
}
