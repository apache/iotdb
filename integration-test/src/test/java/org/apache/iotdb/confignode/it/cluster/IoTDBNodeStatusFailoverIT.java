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
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import com.sun.tools.attach.VirtualMachine;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Process-level status persistence checks with three actual ConfigRegion voters. */
@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class IoTDBNodeStatusFailoverIT {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();
  @Rule public ErrorCollector failures = new ErrorCollector();
  private Path agentJar;
  private int queryNode;

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testGracefulLeaderShutdownPreservesStoppedAndRemoving() throws Exception {
    init(3);
    int stopped = stoppedFixture(0);
    int removing = removingFixture(1);
    int leader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    Path marker = submit(leader, "shutdown");
    awaitFile(marker);
    assertTrue(
        "System.exit must finish all JVM shutdown hooks",
        EnvFactory.getEnv()
            .getConfigNodeWrapper(leader)
            .getInstance()
            .waitFor(60, TimeUnit.SECONDS));
    assertNotEquals(leader, EnvFactory.getEnv().getLeaderConfigNodeIndex());
    assertStatus(0, stopped, NodeStatus.Stopped);
    assertStatus(1, removing, NodeStatus.Removing);
    EnvFactory.getEnv().startConfigNode(leader);
    awaitConfigNodeRunning(leader);
    awaitPersisted(leader, stopped, NodeStatus.Stopped);
    transfer(leader);
    assertStatus(0, stopped, NodeStatus.Stopped);
    assertStatus(1, removing, NodeStatus.Removing);
  }

  @Test
  public void testGracefulSeedShutdownReportsStopped() throws Exception {
    init(1);
    transfer(1);
    int seedId = configLocation(cluster(), 0).getConfigNodeId();
    // Use System.exit so the actual shutdown hook runs on Windows as well as Unix.
    awaitFile(submit(0, "shutdown"));
    assertTrue(
        EnvFactory.getEnv().getConfigNodeWrapper(0).getInstance().waitFor(60, TimeUnit.SECONDS));
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getConfigNodeWrapper(0)),
            Collections.singletonList(NodeStatus.Stopped));
    awaitPersisted(1, seedId, NodeStatus.Stopped);
    awaitPersisted(2, seedId, NodeStatus.Stopped);

    EnvFactory.getEnv().startConfigNode(0);
    awaitConfigNodeRunning(0);
    awaitPersisted(0, seedId, null);
    awaitPersisted(1, seedId, null);
    awaitPersisted(2, seedId, null);
  }

  @Test
  public void testForcedLeaderShutdownWithTwoDataNodes() throws Exception {
    init(2);
    int stopped = stoppedFixture(0);
    int leader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv().getConfigNodeWrapper(leader).stopForcibly();
    assertFalse(EnvFactory.getEnv().getConfigNodeWrapper(leader).isAlive());
    assertNotEquals(leader, EnvFactory.getEnv().getLeaderConfigNodeIndex());
    assertStatus(0, stopped, NodeStatus.Stopped);
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getConfigNodeWrapper(leader)),
            Collections.singletonList(NodeStatus.Unknown));
    EnvFactory.getEnv().startConfigNode(leader);
    awaitConfigNodeRunning(leader);
    awaitPersisted(leader, stopped, NodeStatus.Stopped);
    transfer(leader);
    assertStatus(0, stopped, NodeStatus.Stopped);
  }

  @Test
  public void testNewLeaderChangesSurviveReturnToOriginalLeader() throws Exception {
    init(3);
    int stopped = stoppedFixture(0);
    int removing = removingFixture(1);
    int first = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    int second = (first + 1) % 3;
    transfer(second);
    assertStatus(0, stopped, NodeStatus.Stopped);
    assertStatus(1, removing, NodeStatus.Removing);
    EnvFactory.getEnv().startDataNode(0);
    assertStatus(0, stopped, NodeStatus.Running);
    assertSuccess(control(second, "update:" + removing + ":Running"));
    awaitPersisted(first, stopped, null);
    awaitPersisted(first, removing, null);
    transfer((first + 2) % 3);
    transfer(first);
    EnvFactory.getEnv().getDataNodeWrapper(0).stopForcibly();
    assertStatus(0, stopped, NodeStatus.Unknown);
    transfer(second);
    assertStatus(0, stopped, NodeStatus.Unknown);
    assertStatus(1, removing, NodeStatus.Unknown);
  }

  @Test
  public void testReadOnlyReasonPersistsAcrossLeaderTransferAndClearsAfterRestart()
      throws Exception {
    init(2);
    TDataNodeLocation dataNode = dataLocation(cluster(), 0);
    int dataNodeId = dataNode.getDataNodeId();
    IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> pool =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try (SyncDataNodeInternalServiceClient client =
        pool.borrowClient(dataNode.getInternalEndPoint())) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.setSystemStatus(NodeStatus.ReadOnly.getStatus()).getCode());
    } finally {
      pool.close();
    }
    assertStatus(0, dataNodeId, NodeStatus.ReadOnly);
    assertEquals(NodeStatus.MANUAL, cluster().getNodeStatusReason().get(dataNodeId));
    // Inspect every voter's applied record before election; SHOW alone could pass on heartbeats.
    for (int i = 0; i < 3; i++) {
      awaitPersisted(i, dataNodeId, NodeStatus.ReadOnly, NodeStatus.MANUAL);
    }

    int nextLeader = (EnvFactory.getEnv().getLeaderConfigNodeIndex() + 1) % 3;
    transfer(nextLeader);
    assertEquals(nextLeader, EnvFactory.getEnv().getLeaderConfigNodeIndex());
    for (int i = 0; i < 3; i++) {
      awaitPersisted(i, dataNodeId, NodeStatus.ReadOnly, NodeStatus.MANUAL);
    }
    assertStatus(0, dataNodeId, NodeStatus.ReadOnly);
    assertEquals(NodeStatus.MANUAL, cluster().getNodeStatusReason().get(dataNodeId));

    // Explicitly report after stopping: Windows process termination does not run shutdown hooks.
    assertEquals(dataNodeId, stoppedFixture(0));
    assertNull(cluster().getNodeStatusReason().get(dataNodeId));
    for (int i = 0; i < 3; i++) {
      awaitPersisted(i, dataNodeId, NodeStatus.Stopped, null);
    }

    EnvFactory.getEnv().startDataNode(0);
    assertStatus(0, dataNodeId, NodeStatus.Running);
    assertNull(cluster().getNodeStatusReason().get(dataNodeId));
    for (int i = 0; i < 3; i++) {
      awaitPersisted(i, dataNodeId, null, null);
    }
  }

  @Test
  public void testLiveOldLeaderPartitionAndQuorumRecovery() throws Exception {
    init(3);
    int stopped = stoppedFixture(0);
    int first = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    TConfigNodeLocation firstLocation = configLocation(cluster(), first);
    for (int i = 0; i < 3; i++) {
      control(i, "partition:" + firstLocation.getConfigNodeId());
    }
    int majorityLeader = awaitOtherLeader(first);
    assertTrue(
        "Partition must leave the old leader process alive",
        EnvFactory.getEnv().getConfigNodeWrapper(first).isAlive());
    assertSuccess(control(majorityLeader, "update:" + stopped + ":Removing"));
    try {
      assertStatus(0, stopped, NodeStatus.Removing);
    } catch (SQLException e) {
      // Preserve a failure for SQL routing without skipping consensus recovery assertions.
      failures.addError(
          new AssertionError(
              "SHOW through the healthy DataNode cannot reach the elected majority", e));
    }
    heal();
    awaitPersisted(first, stopped, NodeStatus.Removing);
    transfer(first);
    assertStatus(0, stopped, NodeStatus.Removing);

    // Keep every process alive but prevent any pair of ConfigRegion voters communicating.
    for (int i = 0; i < 3; i++) {
      control(i, "partition:*");
    }
    Properties noQuorum = control(first, "update:" + stopped + ":Running");
    assertNotEquals(
        "No quorum cannot confirm clearing Removing", "200", noQuorum.getProperty("code"));
    assertEquals(
        "Removing", control(first, "inspect:" + stopped).getProperty("persisted." + stopped));
    heal();
    int recovered = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    assertSuccess(control(recovered, "update:" + stopped + ":Running"));
    for (int i = 0; i < 3; i++) {
      awaitPersisted(i, stopped, null);
    }
    transfer((recovered + 1) % 3);
    assertStatus(0, stopped, NodeStatus.Unknown);
  }

  private void init(int dataNodes) throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setConfigNodeRatisSnapshotTriggerThreshold(Integer.MAX_VALUE)
        .setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(4000);
    EnvFactory.getEnv().initClusterEnvironment(3, dataNodes);
    queryNode = dataNodes - 1;
    agentJar = temporary.getRoot().toPath().resolve("node-status-agent.jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().putValue("Agent-Class", NodeStatusTestAgent.class.getName());
    String resource = NodeStatusTestAgent.class.getName().replace('.', '/') + ".class";
    try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(agentJar), manifest);
        InputStream bytecode = getClass().getClassLoader().getResourceAsStream(resource)) {
      assertNotNull(bytecode);
      jar.putNextEntry(new JarEntry(resource));
      bytecode.transferTo(jar);
      jar.closeEntry();
    }
  }

  @Test
  public void testNoQuorumTransitionMatrix() throws Exception {
    init(2);
    int nodeId = stoppedFixture(0);
    NodeStatus[][] transitions = {
      {NodeStatus.Running, NodeStatus.Stopped},
      {NodeStatus.Stopped, NodeStatus.Running},
      {NodeStatus.Removing, NodeStatus.Running},
      {NodeStatus.Removing, NodeStatus.Stopped}
    };
    for (NodeStatus[] transition : transitions) {
      int leader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
      assertSuccess(control(leader, "update:" + nodeId + ":" + transition[0]));
      NodeStatus persisted = transition[0] == NodeStatus.Running ? null : transition[0];
      for (int i = 0; i < 3; i++) {
        awaitPersisted(i, nodeId, persisted);
      }
      try {
        for (int i = 0; i < 3; i++) {
          control(i, "partition:*");
        }
        Properties result = awaitFile(submit(leader, "update:" + nodeId + ":" + transition[1]));
        assertNotEquals(
            "No quorum: " + transition[0] + " -> " + transition[1] + " " + result,
            "200",
            result.getProperty("code"));
        for (int i = 0; i < 3; i++) {
          assertEquals(
              String.valueOf(persisted),
              control(i, "inspect:" + nodeId).getProperty("persisted." + nodeId));
        }
        // A read may reject or return a local cache before the old leader steps down. Record
        // the actual direct RPC response rather than assert an undocumented linearizable read.
        try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(leader)) {
          TShowClusterResp read = client.showCluster();
          System.out.println(
              "No-quorum SHOW RPC: " + transition[0] + " -> " + transition[1] + ", " + read);
        }
        try (Connection connection = queryConnection();
            Statement statement = connection.createStatement()) {
          try (ResultSet rows = statement.executeQuery("SHOW DATANODES")) {
            while (rows.next()) {
              if (rows.getInt(1) == nodeId) {
                System.out.println("No-quorum SHOW SQL cached status: " + rows.getString("Status"));
              }
            }
          }
        } catch (SQLException unavailable) {
          System.out.println("No-quorum SHOW SQL unavailable: " + unavailable.getMessage());
        }
      } finally {
        heal();
      }
      int recovered = EnvFactory.getEnv().getLeaderConfigNodeIndex();
      assertSuccess(control(recovered, "update:" + nodeId + ":" + transition[1]));
      NodeStatus finalPersisted = transition[1] == NodeStatus.Running ? null : transition[1];
      for (int i = 0; i < 3; i++) {
        awaitPersisted(i, nodeId, finalPersisted);
      }
    }
  }

  @Test
  public void testSnapshotAndFollowingClearLogRecoverTogether() throws Exception {
    init(3);
    int stopped = stoppedFixture(0);
    int removing = removingFixture(1);
    for (int i = 0; i < 3; i++) {
      awaitPersisted(i, stopped, NodeStatus.Stopped);
      awaitPersisted(i, removing, NodeStatus.Removing);
      control(i, "snapshot");
      assertSnapshot(i, stopped, NodeStatus.Stopped, removing, NodeStatus.Removing);
    }
    int leader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    assertSuccess(control(leader, "update:" + stopped + ":Running"));
    assertSuccess(control(leader, "update:" + removing + ":Stopped"));
    for (int i = 0; i < 3; i++) {
      awaitPersisted(i, stopped, null);
      awaitPersisted(i, removing, NodeStatus.Stopped);
    }
    restartConfigNodes();
    assertStatus(0, stopped, NodeStatus.Unknown);
    assertStatus(1, removing, NodeStatus.Stopped);
    for (int i = 0; i < 3; i++) {
      control(i, "snapshot");
      assertSnapshot(i, stopped, null, removing, NodeStatus.Stopped);
    }
    restartConfigNodes();
    assertStatus(0, stopped, NodeStatus.Unknown);
    assertStatus(1, removing, NodeStatus.Stopped);
  }

  private void assertSnapshot(
      int configNode, int firstId, NodeStatus first, int secondId, NodeStatus second)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    do {
      List<Path> snapshots;
      // Stop at sm/<term>_<index>, without opening temporary snapshot directories on Windows.
      try (Stream<Path> paths =
          Files.walk(
              Paths.get(
                  EnvFactory.getEnv().getConfigNodeWrapper(configNode).getNodePath(),
                  "data",
                  "confignode",
                  "consensus"),
              3)) {
        snapshots =
            paths
                .filter(
                    p ->
                        p.getFileName().toString().matches("\\d+_\\d+")
                            && p.getParent().getFileName().toString().equals("sm"))
                .collect(Collectors.toList());
      } catch (IOException | UncheckedIOException e) {
        // Ratis may delete an old snapshot while the directories are being enumerated.
        TimeUnit.MILLISECONDS.sleep(200);
        continue;
      }
      for (Path snapshot : snapshots) {
        NodeInfo restored = new NodeInfo();
        try {
          restored.processLoadSnapshot(snapshot.toFile());
        } catch (IOException e) {
          // The snapshot may have been replaced since it was enumerated.
          continue;
        }
        if (java.util.Objects.equals(
                first == null ? null : new Pair<>(first, null),
                restored.getPersistedNodeStatus(firstId))
            && java.util.Objects.equals(
                second == null ? null : new Pair<>(second, null),
                restored.getPersistedNodeStatus(secondId))) {
          return;
        }
      }
      TimeUnit.MILLISECONDS.sleep(200);
    } while (System.nanoTime() < deadline);
    fail("No completed snapshot has the expected durable markers on ConfigNode " + configNode);
  }

  private void restartConfigNodes() throws Exception {
    EnvFactory.getEnv().getConfigNodeWrapperList().forEach(n -> n.stopForcibly());
    EnvFactory.getEnv().getConfigNodeWrapperList().forEach(n -> n.start());
    EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv()
        .ensureNodeStatus(
            new java.util.ArrayList<>(EnvFactory.getEnv().getConfigNodeWrapperList()),
            java.util.Arrays.asList(NodeStatus.Running, NodeStatus.Running, NodeStatus.Running));
  }

  private void awaitConfigNodeRunning(int index) {
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getConfigNodeWrapper(index)),
            Collections.singletonList(NodeStatus.Running));
  }

  @Test
  public void testRealRemovalPersistsIntentAndDeletesRegistration() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaReplicationFactor(2)
        .setDataReplicationFactor(2);
    init(3);
    int leader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    int victim = -1;
    int victimIndex = -1;
    try (Connection connection = queryConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.status_remove");
      statement.execute(
          "CREATE TIMESERIES root.status_remove.d.s WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("INSERT INTO root.status_remove.d(time,s) VALUES (1,1)");
      TShowClusterResp before = cluster();
      int queryId = dataLocation(before, queryNode).getDataNodeId();
      try (ResultSet regions = statement.executeQuery("SHOW REGIONS")) {
        while (regions.next()) {
          int candidate = regions.getInt("DataNodeId");
          if (candidate != queryId) {
            victim = candidate;
            break;
          }
        }
      }
      assertTrue("The victim must host a real region replica", victim >= 0);
      for (int i = 0; i < 3; i++) {
        if (dataLocation(before, i).getDataNodeId() == victim) {
          victimIndex = i;
        }
      }
      assertTrue(victimIndex >= 0);
      Path barrier = submit(leader, "barrier");
      assertNull(awaitFile(barrier).getProperty("error"));
      try {
        statement.execute("REMOVE DATANODE " + victim);
        awaitFile(Paths.get(barrier + ".reached"));
        assertStatus(victimIndex, victim, NodeStatus.Removing);
        for (int i = 0; i < 3; i++) {
          awaitPersisted(i, victim, NodeStatus.Removing);
        }
      } finally {
        Files.createFile(Paths.get(barrier + ".release"));
      }
      long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
      boolean removed = false;
      do {
        final int victimId = victim;
        removed =
            cluster().getDataNodeList().stream().noneMatch(n -> n.getDataNodeId() == victimId);
        if (!removed) {
          TimeUnit.MILLISECONDS.sleep(200);
        }
      } while (!removed && System.nanoTime() < deadline);
      assertTrue("RemoveDataNodesProcedure must unregister the node", removed);
      for (int i = 0; i < 3; i++) {
        awaitPersisted(i, victim, null);
      }
      transfer((leader + 1) % 3);
      try (ResultSet rows = statement.executeQuery("SHOW DATANODES")) {
        while (rows.next()) {
          assertNotEquals(victim, rows.getInt(1));
        }
      }
      try (ResultSet rows = statement.executeQuery("SELECT s FROM root.status_remove.d")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getInt(2));
        assertFalse(rows.next());
      }
    }
  }

  private int stoppedFixture(int index) throws Exception {
    TDataNodeLocation node = dataLocation(cluster(), index);
    EnvFactory.getEnv().getDataNodeWrapper(index).stopForcibly();
    assertFalse(EnvFactory.getEnv().getDataNodeWrapper(index).isAlive());
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      assertEquals(200, client.reportDataNodeShutdown(node).getCode());
    }
    assertStatus(index, node.getDataNodeId(), NodeStatus.Stopped);
    return node.getDataNodeId();
  }

  private int removingFixture(int index) throws Exception {
    TDataNodeLocation node = dataLocation(cluster(), index);
    IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> pool =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try (SyncDataNodeInternalServiceClient client = pool.borrowClient(node.getInternalEndPoint())) {
      assertEquals(200, client.setSystemStatus("Removing").getCode());
    } finally {
      pool.close();
    }
    assertStatus(index, node.getDataNodeId(), NodeStatus.Removing);
    EnvFactory.getEnv().getDataNodeWrapper(index).stopForcibly();
    return node.getDataNodeId();
  }

  private void assertStatus(int index, int nodeId, NodeStatus expected) throws Exception {
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(index)),
            Collections.singletonList(expected));
    assertEquals(expected.getStatus(), cluster().getNodeStatus().get(nodeId));
    // Pin the SQL connection to the healthy query node; do not fan out to stopped DataNodes.
    try (Connection connection = queryConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : new String[] {"SHOW CLUSTER", "SHOW DATANODES"}) {
        boolean found = false;
        try (ResultSet rows = statement.executeQuery(sql)) {
          while (rows.next()) {
            if (rows.getInt(1) == nodeId) {
              assertEquals(sql, expected.getStatus(), rows.getString("Status"));
              found = true;
            }
          }
        }
        assertTrue(sql + " did not contain DataNode " + nodeId, found);
      }
    }
  }

  private void transfer(int target) throws Exception {
    int leader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    if (leader == target) {
      return;
    }
    long previousTerm = Long.parseLong(control(leader, "inspect").getProperty("term"));
    TConfigNodeLocation location = configLocation(cluster(), target);
    control(
        leader,
        "transfer:" + location.getConfigNodeId() + ":" + location.getConsensusEndPoint().getPort());
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    do {
      if (EnvFactory.getEnv().getLeaderConfigNodeIndex() == target) {
        assertTrue(Long.parseLong(control(target, "inspect").getProperty("term")) > previousTerm);
        return;
      }
      TimeUnit.MILLISECONDS.sleep(200);
    } while (System.nanoTime() < deadline);
    fail("Leadership did not transfer to ConfigNode " + target);
  }

  private int awaitOtherLeader(int excluded) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    do {
      for (int i = 0; i < 3; i++) {
        if (i != excluded) {
          try (SyncConfigNodeIServiceClient client =
              (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(i)) {
            if (client.showCluster().getStatus().getCode() == 200) {
              return i;
            }
          } catch (Exception ignored) {
            /* Election can temporarily reject a read. */
          }
        }
      }
      TimeUnit.MILLISECONDS.sleep(200);
    } while (System.nanoTime() < deadline);
    throw new AssertionError("Majority did not elect a ready leader");
  }

  private void heal() throws Exception {
    for (int i = 0; i < 3; i++) {
      control(i, "heal");
    }
  }

  private void awaitPersisted(int node, int dataNode, NodeStatus expected) throws Exception {
    awaitPersisted(node, dataNode, expected, null);
  }

  private void awaitPersisted(int node, int dataNode, NodeStatus expected, String expectedReason)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    do {
      Properties state = control(node, "inspect:" + dataNode);
      if (String.valueOf(expected).equals(state.getProperty("persisted." + dataNode))
          && String.valueOf(expectedReason)
              .equals(state.getProperty("persistedReason." + dataNode))) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(200);
    } while (System.nanoTime() < deadline);
    fail(
        "ConfigNode "
            + node
            + " did not persist "
            + expected
            + " with reason "
            + expectedReason
            + " for "
            + dataNode);
  }

  private Path submit(int node, String command) throws Exception {
    Path output =
        temporary.getRoot().toPath().resolve("control-" + System.nanoTime() + ".properties");
    VirtualMachine vm =
        VirtualMachine.attach(
            Long.toString(EnvFactory.getEnv().getConfigNodeWrapper(node).getInstance().pid()));
    try {
      vm.loadAgent(agentJar.toString(), command + "|" + output);
    } finally {
      vm.detach();
    }
    return output;
  }

  private Properties control(int node, String command) throws Exception {
    Properties result = awaitFile(submit(node, command));
    assertNull(command + " failed: " + result, result.getProperty("error"));
    return result;
  }

  private Properties awaitFile(Path output) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    while (!Files.exists(output) && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
    assertTrue("Test control timed out: " + output, Files.exists(output));
    Properties result = new Properties();
    try (InputStream stream = Files.newInputStream(output)) {
      result.load(stream);
    }
    return result;
  }

  private TShowClusterResp cluster() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TShowClusterResp result = client.showCluster();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getStatus().getCode());
      return result;
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

  private void assertSuccess(Properties result) {
    assertEquals(result.toString(), "200", result.getProperty("code"));
  }

  private Connection queryConnection() throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", "root");
    properties.setProperty("network_timeout", "15000");
    properties.setProperty("sql_dialect", "tree");
    return DriverManager.getConnection(
        "jdbc:iotdb://"
            + EnvFactory.getEnv().getDataNodeWrapper(queryNode).getIp()
            + ":"
            + EnvFactory.getEnv().getDataNodeWrapper(queryNode).getPort()
            + "/",
        properties);
  }
}
