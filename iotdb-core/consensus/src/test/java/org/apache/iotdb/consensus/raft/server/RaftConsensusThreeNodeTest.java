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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.raft.RaftConsensus;
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine.StringRequest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Three {@link RaftConsensus} nodes on localhost with real Thrift, real timers and real log files,
 * loaded through {@link ConsensusFactory} by class name exactly as IoTDB would.
 */
public class RaftConsensusThreeNodeTest {

  private static final ConsensusGroupId GID = new DataRegionId(1);

  private final List<IConsensus> servers = new ArrayList<>();
  private final List<RecordingStateMachine> machines = new ArrayList<>();
  private final List<Peer> peers = new ArrayList<>();
  private final List<File> dirs = new ArrayList<>();

  private static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  @Before
  public void setUp() throws Exception {
    System.setProperty("raft.election_timeout_min_ms", "300");
    System.setProperty("raft.election_timeout_max_ms", "600");
    System.setProperty("raft.first_election_timeout_min_ms", "50");
    System.setProperty("raft.first_election_timeout_max_ms", "200");
    System.setProperty("raft.heartbeat_interval_ms", "100");
    for (int i = 1; i <= 3; i++) {
      peers.add(new Peer(GID, i, new TEndPoint("127.0.0.1", freePort())));
      dirs.add(Files.createTempDirectory("raft-node-" + i).toFile());
      machines.add(new RecordingStateMachine());
    }
    for (int i = 0; i < 3; i++) {
      servers.add(newServer(i));
    }
    for (IConsensus s : servers) {
      s.start();
    }
    for (IConsensus s : servers) {
      s.createLocalPeer(GID, peers);
    }
  }

  private IConsensus newServer(int i) {
    return ConsensusFactory.getConsensusImpl(
            RaftConsensus.CLASS_NAME,
            ConsensusConfig.newBuilder()
                .setThisNodeId(peers.get(i).getNodeId())
                .setThisNode(peers.get(i).getEndpoint())
                .setStorageDir(dirs.get(i).getAbsolutePath())
                .setConsensusGroupType(TConsensusGroupType.DataRegion)
                .build(),
            gid -> machines.get(i))
        .orElseThrow(
            () -> new IllegalStateException("cannot construct " + RaftConsensus.CLASS_NAME));
  }

  @After
  public void tearDown() throws Exception {
    for (IConsensus s : servers) {
      if (s != null) {
        s.stop();
      }
    }
    for (File d : dirs) {
      org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(d);
    }
    for (String k : new ArrayList<>(System.getProperties().stringPropertyNames())) {
      if (k.startsWith("raft.")) {
        System.clearProperty(k);
      }
    }
  }

  private static void await(String what, long timeoutMs, BooleanSupplier cond)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(20);
    }
  }

  private int leaderIndex() {
    for (int i = 0; i < servers.size(); i++) {
      if (servers.get(i) != null && servers.get(i).isLeader(GID)) {
        return i;
      }
    }
    return -1;
  }

  private int awaitLeader() throws InterruptedException {
    await(
        "a leader",
        15_000,
        () -> leaderIndex() >= 0 && servers.get(leaderIndex()).isLeaderReady(GID));
    return leaderIndex();
  }

  private void awaitApplied(int count) throws InterruptedException {
    await(
        count + " applied on every node",
        15_000,
        () -> {
          for (int i = 0; i < machines.size(); i++) {
            if (servers.get(i) != null && machines.get(i).applied.size() < count) {
              return false;
            }
          }
          return true;
        });
  }

  @Test
  public void electsLeaderReplicatesAndForwardsWrites() throws Exception {
    int leader = awaitLeader();
    assertNotNull(servers.get(leader).getLeader(GID));
    assertEquals(peers.get(leader).getNodeId(), servers.get(leader).getLeader(GID).getNodeId());
    assertTrue(servers.get(leader).getLogicalClock(GID) >= 1);
    assertEquals(3, servers.get(leader).getReplicationNum(GID));

    for (int i = 0; i < 20; i++) {
      TSStatus st = servers.get(leader).write(GID, new StringRequest("w" + i));
      assertEquals(st.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), st.getCode());
    }
    int follower = (leader + 1) % 3;
    TSStatus forwarded = servers.get(follower).write(GID, new StringRequest("via-follower"));
    assertEquals(
        forwarded.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), forwarded.getCode());
    assertNotNull(forwarded.getRedirectNode());

    awaitApplied(21);
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      expected.add("w" + i);
    }
    expected.add("via-follower");
    for (RecordingStateMachine m : machines) {
      assertEquals(expected, m.snapshotApplied());
    }
    assertEquals(0, machines.get(leader).remoteApplied.get());
    assertTrue(machines.get(follower).remoteApplied.get() >= 21);
    for (int i = 0; i < 3; i++) {
      assertEquals(peers.get(leader).getNodeId(), machines.get(i).leaderId);
    }
    assertTrue(machines.get(leader).leaderReady);
  }

  @Test
  public void survivesLeaderCrashAndRestart() throws Exception {
    int leader = awaitLeader();
    for (int i = 0; i < 5; i++) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          servers.get(leader).write(GID, new StringRequest("a" + i)).getCode());
    }
    awaitApplied(5);
    servers.get(leader).stop();
    servers.set(leader, null);
    int old = leader;
    await(
        "a new leader",
        15_000,
        () ->
            leaderIndex() >= 0
                && leaderIndex() != old
                && servers.get(leaderIndex()).isLeaderReady(GID));
    int newLeader = leaderIndex();
    for (int i = 0; i < 5; i++) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          servers.get(newLeader).write(GID, new StringRequest("b" + i)).getCode());
    }
    // the crashed node comes back from its own log directory and catches up
    machines.set(old, new RecordingStateMachine());
    servers.set(old, newServer(old));
    servers.get(old).start();
    assertEquals(Arrays.asList(GID), servers.get(old).getAllConsensusGroupIds());
    awaitApplied(10);
    List<String> expected =
        Arrays.asList("a0", "a1", "a2", "a3", "a4", "b0", "b1", "b2", "b3", "b4");
    for (RecordingStateMachine m : machines) {
      assertEquals(expected, m.snapshotApplied());
    }
    assertTrue(
        servers.get(old).getLogicalClock(GID) >= servers.get(newLeader).getLogicalClock(GID));
  }

  @Test
  public void deleteLocalPeerRemovesTheGroupDirectory() throws Exception {
    awaitLeader();
    IConsensus s = servers.get(0);
    File dir = new File(s.getRegionDirFromConsensusGroupId(GID));
    assertTrue(dir.exists());
    s.deleteLocalPeer(GID);
    assertTrue(!dir.exists());
    assertTrue(s.getAllConsensusGroupIds().isEmpty());
  }
}
