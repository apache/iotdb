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
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine.AppliedView;
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine.StringRequest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Read consistency over real Thrift: READ_INDEX reads on a follower see every write the leader has
 * acknowledged, LEASE reads work on the leader, and an isolated leader resigns its lease.
 */
public class RaftReadConsistencyTest {

  private static final ConsensusGroupId GID = new DataRegionId(21);

  private final List<IConsensus> servers = new ArrayList<>();
  private final List<RecordingStateMachine> machines = new ArrayList<>();
  private final List<Peer> peers = new ArrayList<>();
  private final List<File> dirs = new ArrayList<>();

  private static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  private void start(String consistency) throws Exception {
    System.setProperty("raft.election_timeout_min_ms", "300");
    System.setProperty("raft.election_timeout_max_ms", "600");
    System.setProperty("raft.first_election_timeout_min_ms", "50");
    System.setProperty("raft.first_election_timeout_max_ms", "200");
    System.setProperty("raft.heartbeat_interval_ms", "100");
    System.setProperty("raft.lease_check_interval_ms", "100");
    System.setProperty("raft.read_consistency", consistency);
    Set<Integer> allocated = new HashSet<>();
    for (int i = 1; i <= 3; i++) {
      int port;
      do {
        port = freePort();
      } while (!allocated.add(port));
      peers.add(new Peer(GID, i, new TEndPoint("127.0.0.1", port)));
      dirs.add(Files.createTempDirectory("raft-read-" + i).toFile());
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
    InboundFaults.install(null);
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

  private int applied(int node) throws Exception {
    return ((AppliedView) servers.get(node).read(GID, new StringRequest("q"))).applied.size();
  }

  @Test
  public void readIndexOnAFollowerSeesEveryAcknowledgedWrite() throws Exception {
    start("READ_INDEX");
    int leader = awaitLeader();
    int follower = (leader + 1) % 3;
    for (int i = 0; i < 20; i++) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          servers.get(leader).write(GID, new StringRequest("v" + i)).getCode());
      assertTrue(
          "follower read behind at " + i,
          applied(follower) >= i + 1); // read index catches up first
    }
    assertEquals(20, applied(leader));
    RaftConsensus rc = (RaftConsensus) servers.get(leader);
    assertTrue(rc.getGroup(GID).leaseValid());
    assertTrue(rc.getGroup(GID).metrics().count("client_ack") >= 20);
  }

  @Test
  public void delayedRepliesToEarlierRequestsCannotConfirmANewRead() throws Exception {
    System.setProperty("raft.read_index_timeout_ms", "250");
    start("READ_INDEX");
    int leader = awaitLeader();
    RaftGroupServer group = ((RaftConsensus) servers.get(leader)).getGroup(GID);
    int leaderId = peers.get(leader).getNodeId();
    int followerId = peers.get((leader + 1) % 3).getNodeId();
    InboundFaults.install(
        new InboundFaults() {
          @Override
          public boolean drop(
              int self, int from, org.apache.iotdb.consensus.raft.core.msg.Message message) {
            return self == leaderId
                && message instanceof org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes
                && ((org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes) message).msgId
                    != -77;
          }
        });
    java.util.concurrent.CompletableFuture<Boolean> read =
        java.util.concurrent.CompletableFuture.supplyAsync(
            () -> {
              try {
                group.read(
                    new StringRequest("q"),
                    org.apache.iotdb.consensus.raft.config.RaftConfig.ReadConsistency.READ_INDEX);
                return true;
              } catch (org.apache.iotdb.consensus.exception.ConsensusException expected) {
                return false;
              }
            });
    // These delayed, current-term answers keep refreshing contact timestamps. Their unrelated
    // request ids must not satisfy the read's fresh quorum-confirmation round.
    for (int i = 0; i < 20 && !read.isDone(); i++) {
      group.onEvent(
          new org.apache.iotdb.consensus.raft.core.event.Event.Inbound(
              followerId,
              new org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes(
                  group.term(), java.util.Collections.emptyList(), -1, -1, -1, Long.MIN_VALUE, -77),
              null));
      Thread.sleep(20);
    }
    assertFalse(
        "an unrelated delayed response must not authorize a read", read.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void leaseReadsOnTheLeaderAndIsolationResignsTheLease() throws Exception {
    start("LEASE");
    int leader = awaitLeader();
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        servers.get(leader).write(GID, new StringRequest("x")).getCode());
    assertEquals(1, applied(leader)); // lease read on the leader
    int follower = (leader + 1) % 3;
    assertEquals(1, applied(follower)); // a follower falls back to the leader's commit index
    // isolate the leader: both followers stop; no majority answers, so the lease lapses
    for (int i = 0; i < 3; i++) {
      if (i != leader) {
        servers.get(i).stop();
        servers.set(i, null);
      }
    }
    final int ldr = leader;
    await("lease step-down", 10_000, () -> !servers.get(ldr).isLeader(GID));
    assertFalse(servers.get(ldr).isLeader(GID));
  }
}
