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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Real-Thrift membership changes and snapshots: a learner joins through addRemotePeer (learner ->
 * catch-up -> promotion), a peer is removed through a forwarded admin RPC, leadership is
 * transferred, and a fresh node catches up through InstallSnapshot after log compaction.
 */
public class RaftConsensusMembershipTest {

  private static final ConsensusGroupId GID = new DataRegionId(7);

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
    System.setProperty("raft.snapshot_trigger_entries", "10");
    System.setProperty("raft.log_preserve_entries_after_snapshot", "0");
    for (int i = 1; i <= 5; i++) {
      peers.add(new Peer(GID, i, new TEndPoint("127.0.0.1", freePort())));
      dirs.add(Files.createTempDirectory("raft-m-" + i).toFile());
      machines.add(new RecordingStateMachine());
      servers.add(null);
    }
    for (int i = 0; i < 3; i++) {
      servers.set(i, newServer(i));
      servers.get(i).start();
    }
    List<Peer> initial = peers.subList(0, 3);
    for (int i = 0; i < 3; i++) {
      servers.get(i).createLocalPeer(GID, initial);
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
        "a ready leader",
        15_000,
        () -> leaderIndex() >= 0 && servers.get(leaderIndex()).isLeaderReady(GID));
    return leaderIndex();
  }

  private void write(int i, String value) throws Exception {
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        servers.get(i).write(GID, new StringRequest(value)).getCode());
  }

  private void awaitApplied(int node, int count) throws InterruptedException {
    await(
        "N" + (node + 1) + " applied " + count,
        20_000,
        () -> machines.get(node).applied.size() >= count);
  }

  @Test
  public void addRemovePeersAndTransferLeadership() throws Exception {
    int leader = awaitLeader();
    for (int i = 0; i < 5; i++) {
      write(leader, "a" + i);
    }
    // node 4 joins as an empty learner, exactly as ConfigNode's migration path creates peers
    servers.set(3, newServer(3));
    servers.get(3).start();
    servers.get(3).createLocalPeer(GID, Collections.emptyList());
    servers.get(leader).addRemotePeer(GID, peers.get(3));
    awaitApplied(3, 5);
    assertEquals(4, servers.get(leader).getReplicationNum(GID));
    assertEquals(4, servers.get(3).getReplicationNum(GID));
    write(3, "via-new-peer"); // forwarded to the leader
    for (int i = 0; i < 4; i++) {
      awaitApplied(i, 6);
    }
    // remove a non-leader through another non-leader: the request is forwarded to the leader
    int removed = (leader + 1) % 3;
    int requester = (leader + 2) % 3;
    servers.get(requester).removeRemotePeer(GID, peers.get(removed));
    await(
        "membership without the removed peer",
        10_000,
        () -> servers.get(leader).getReplicationNum(GID) == 3);
    servers.get(removed).deleteLocalPeer(GID);
    servers.get(removed).stop();
    servers.set(removed, null);
    write(leader, "after-remove");
    int[] remaining = new int[] {leader, requester, 3};
    for (int i : remaining) {
      awaitApplied(i, 7);
    }
    // transfer leadership to node 4
    servers.get(leader).transferLeader(GID, peers.get(3));
    await(
        "N4 leads",
        15_000,
        () -> servers.get(3).isLeader(GID) && servers.get(3).isLeaderReady(GID));
    write(3, "after-transfer");
    for (int i : remaining) {
      awaitApplied(i, 8);
      assertEquals(machines.get(3).snapshotApplied(), machines.get(i).snapshotApplied());
    }
  }

  @Test
  public void snapshotCompactsAndInstallsToAFreshNode() throws Exception {
    int leader = awaitLeader();
    for (int i = 0; i < 25; i++) {
      write(leader, "s" + i);
    }
    RaftConsensus rc = (RaftConsensus) servers.get(leader);
    await("compaction on the leader", 15_000, () -> rc.groupStatus(GID).getFirstIndex() > 0);
    assertTrue(machines.get(leader).snapshotsTaken.get() >= 1);
    // a fresh node joins: its nextIndex falls below the leader's firstIndex, so it gets a snapshot
    servers.set(4, newServer(4));
    servers.get(4).start();
    servers.get(4).createLocalPeer(GID, Collections.emptyList());
    servers.get(leader).addRemotePeer(GID, peers.get(4));
    awaitApplied(4, 25);
    assertTrue(machines.get(4).snapshotsLoaded.get() >= 1);
    assertEquals(machines.get(leader).snapshotApplied(), machines.get(4).snapshotApplied());
    write(leader, "post-snapshot");
    awaitApplied(4, 26);
    assertEquals("post-snapshot", machines.get(4).applied.get(25));
    // the new node restarts from its own snapshot + log
    servers.get(4).stop();
    servers.set(4, newServer(4));
    servers.get(4).start();
    write(leader, "after-restart");
    awaitApplied(4, 27);
    assertEquals(machines.get(leader).snapshotApplied(), machines.get(4).snapshotApplied());
  }
}
