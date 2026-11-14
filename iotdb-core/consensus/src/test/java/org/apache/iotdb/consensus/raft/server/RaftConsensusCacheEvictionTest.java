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
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Real three-node RPC and durable-log regression: payload eviction must not lose client replies.
 */
public class RaftConsensusCacheEvictionTest {
  private static final ConsensusGroupId GID = new DataRegionId(73);
  private final List<IConsensus> servers = new ArrayList<>();
  private final List<RecordingStateMachine> machines = new ArrayList<>();
  private final List<File> dirs = new ArrayList<>();
  private final List<ServerSocket> reservedPorts = new ArrayList<>();
  private final Map<String, String> savedProperties = new LinkedHashMap<>();

  private void property(String key, String value) {
    savedProperties.put(key, System.getProperty(key));
    System.setProperty(key, value);
  }

  @After
  public void tearDown() throws Exception {
    try {
      for (IConsensus server : servers) {
        server.stop();
      }
    } finally {
      for (ServerSocket socket : reservedPorts) {
        socket.close();
      }
      for (File dir : dirs) {
        org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(dir);
      }
      for (Map.Entry<String, String> property : savedProperties.entrySet()) {
        if (property.getValue() == null) {
          System.clearProperty(property.getKey());
        } else {
          System.setProperty(property.getKey(), property.getValue());
        }
      }
    }
  }

  @Test
  public void zeroCacheCompletesLocalAndForwardedWritesAfterApply() throws Exception {
    property("raft.log.cache_bytes", "0");
    property("raft.log.fsync_max_batch_entries", "1");
    property("raft.log.fsync_max_delay_ms", "0");
    property("raft.write_timeout_ms", "2000");
    property("raft.election_timeout_min_ms", "400");
    property("raft.election_timeout_max_ms", "800");
    property("raft.first_election_timeout_min_ms", "50");
    property("raft.first_election_timeout_max_ms", "200");
    property("raft.heartbeat_interval_ms", "100");
    List<Peer> peers = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      ServerSocket socket = new ServerSocket(0);
      reservedPorts.add(socket);
      peers.add(new Peer(GID, i, new TEndPoint("127.0.0.1", socket.getLocalPort())));
      dirs.add(Files.createTempDirectory("raft-cache-zero-" + i).toFile());
      machines.add(new RecordingStateMachine());
    }
    for (int i = 0; i < 3; i++) {
      final RecordingStateMachine machine = machines.get(i);
      IConsensus server =
          ConsensusFactory.getConsensusImpl(
                  RaftConsensus.CLASS_NAME,
                  ConsensusConfig.newBuilder()
                      .setThisNodeId(peers.get(i).getNodeId())
                      .setThisNode(peers.get(i).getEndpoint())
                      .setStorageDir(dirs.get(i).getAbsolutePath())
                      .setConsensusGroupType(TConsensusGroupType.DataRegion)
                      .build(),
                  gid -> machine)
              .orElseThrow(() -> new IllegalStateException("cannot construct Raft"));
      servers.add(server);
      reservedPorts.get(i).close();
      server.start();
    }
    for (IConsensus server : servers) {
      server.createLocalPeer(GID, peers);
    }
    await("ready leader", () -> leaderIndex() >= 0);
    int leader = leaderIndex();
    TSStatus local = servers.get(leader).write(GID, new StringRequest("local"));
    // Preserve the stronger regression evidence: application succeeded even when the old
    // completion path lost the reply because the disk-decoded entry had NO_REQUEST.
    await("local application", () -> machines.get(leader).applied.contains("local"));
    assertEquals(local.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), local.getCode());
    assertEquals(Arrays.asList("local"), machines.get(leader).snapshotApplied());
    int follower = (leader + 1) % 3;
    TSStatus forwarded = servers.get(follower).write(GID, new StringRequest("forwarded"));
    assertEquals(
        forwarded.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), forwarded.getCode());
    assertNotNull(forwarded.getRedirectNode());
    await(
        "application on all voters", () -> machines.stream().allMatch(m -> m.applied.size() == 2));
    for (RecordingStateMachine machine : machines) {
      assertEquals(Arrays.asList("local", "forwarded"), machine.snapshotApplied());
    }
  }

  private int leaderIndex() {
    for (int i = 0; i < servers.size(); i++) {
      if (servers.get(i).isLeader(GID) && servers.get(i).isLeaderReady(GID)) {
        return i;
      }
    }
    return -1;
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(10);
    }
  }
}
