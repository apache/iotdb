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

package org.apache.iotdb.consensus.traft;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.TRaftConfig;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Regression coverage for the Raft safety and TRaft-specific metadata behavior added here. */
public class TRaftConsensusTest {

  private static final ConsensusGroupId GROUP_ID = new DataRegionId(1);
  private static final AtomicInteger NEXT_PORT = new AtomicInteger(18_000);

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final List<AutoCloseable> closeables = new ArrayList<>();

  @After
  public void tearDown() throws Exception {
    for (int i = closeables.size() - 1; i >= 0; i--) {
      closeables.get(i).close();
    }
    closeables.clear();
  }

  @Test
  public void testAutomaticElectionAndCommittedWrite() throws Exception {
    // Verifies the happy path: automatic election, quorum commit, and apply-after-commit.
    TestCluster cluster = new TestCluster(3);
    closeables.add(cluster);

    int leader = cluster.waitLeaderReady();
    TSStatus status =
        cluster
            .getConsensus(leader)
            .write(GROUP_ID, TestRequest.withTime(1, 100L, 100L));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    waitUntil(
        () -> cluster.stateMachineValues().stream().allMatch(value -> value == 1),
        TimeUnit.SECONDS.toMillis(10),
        "TRaft cluster did not replicate committed write to every peer");
  }

  @Test
  public void testStaleCandidateCannotWinElection() throws Exception {
    // A follower that missed committed entries must lose the election to the up-to-date follower.
    TestCluster cluster = new TestCluster(3);
    closeables.add(cluster);

    int leader = cluster.waitLeaderReady();
    int staleFollower = (leader + 1) % 3;
    int healthyFollower = (leader + 2) % 3;

    cluster.getImpl(staleFollower).setActive(false);
    TSStatus status =
        cluster
            .getConsensus(leader)
            .write(GROUP_ID, TestRequest.withTime(1, 200L, 200L));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    waitUntil(
        () ->
            cluster.getStateMachine(leader).getValue() == 1
                && cluster.getStateMachine(healthyFollower).getValue() == 1,
        TimeUnit.SECONDS.toMillis(10),
        "Committed entry was not replicated to the healthy quorum");

    cluster.getImpl(leader).setActive(false);
    cluster.getImpl(staleFollower).setActive(true);
    cluster.getImpl(staleFollower).triggerElection();

    waitUntil(
        () -> cluster.getConsensus(healthyFollower).isLeader(GROUP_ID),
        TimeUnit.SECONDS.toMillis(10),
        "Up-to-date follower did not win re-election. " + cluster.describeState());
    Assert.assertFalse(cluster.getConsensus(staleFollower).isLeader(GROUP_ID));
  }

  @Test
  public void testConflictRepairAndApplyAfterCommit() throws Exception {
    // Verifies suffix truncation on conflict and ensures follower apply waits for leaderCommit.
    CounterStateMachine stateMachine = new CounterStateMachine();
    File storageDir = temporaryFolder.newFolder("standalone-server");
    Peer peer = new Peer(GROUP_ID, 1, new TEndPoint("127.0.0.1", NEXT_PORT.getAndIncrement()));
    TRaftServerImpl server =
        new TRaftServerImpl(
            storageDir.getAbsolutePath(),
            peer,
            new java.util.TreeSet<>(Collections.singletonList(peer)),
            stateMachine,
            testConfig(),
            new TRaftLocalTransport());
    closeables.add(server::stop);
    server.start();

    TRaftLogEntry firstEntry =
        new TRaftLogEntry(
            TRaftEntryType.DATA,
            10L,
            0L,
            1L,
            1L,
            0L,
            0L,
            TestRequest.withTime(1, 10L, 10L).serializeToByteBuffer().array());
    TRaftAppendEntriesResponse response =
        server.receiveAppendEntries(
            new TRaftAppendEntriesRequest(2, 1L, 0L, 0L, 0L, Collections.singletonList(firstEntry)));
    Assert.assertTrue(response.isSuccess());
    Assert.assertEquals(0, stateMachine.getValue());

    TRaftLogEntry conflictingEntry =
        new TRaftLogEntry(
            TRaftEntryType.DATA,
            20L,
            0L,
            1L,
            2L,
            0L,
            0L,
            TestRequest.withTime(1, 20L, 20L).serializeToByteBuffer().array());
    response =
        server.receiveAppendEntries(
            new TRaftAppendEntriesRequest(
                3, 2L, 0L, 0L, 0L, Collections.singletonList(conflictingEntry)));
    Assert.assertTrue(response.isSuccess());
    Assert.assertEquals(2L, server.getLogEntries().get(0).getLogTerm());
    Assert.assertEquals(0, stateMachine.getValue());

    response =
        server.receiveAppendEntries(new TRaftAppendEntriesRequest(3, 2L, 1L, 2L, 1L, Collections.emptyList()));
    Assert.assertTrue(response.isSuccess());
    Assert.assertEquals(1, stateMachine.getValue());
  }

  @Test
  public void testUntimedRequestDoesNotDerivePartitionFromPayload() throws Exception {
    // Requests without explicit timestamps should reuse the current partition instead of guessing.
    TestCluster cluster = new TestCluster(1, testConfigWithSnapshotThreshold(100));
    closeables.add(cluster);

    int leader = cluster.waitLeaderReady();
    cluster.getConsensus(leader).write(GROUP_ID, TestRequest.withTime(1, 300L, 300L));
    cluster.getConsensus(leader).write(GROUP_ID, TestRequest.withoutTime(1, 1L));

    List<TRaftLogEntry> dataEntries = new ArrayList<>();
    for (TRaftLogEntry entry : cluster.getImpl(leader).getLogEntries()) {
      if (entry.getEntryType() == TRaftEntryType.DATA) {
        dataEntries.add(entry);
      }
    }
    Assert.assertEquals(2, dataEntries.size());
    Assert.assertEquals(
        dataEntries.get(0).getPartitionIndex(), dataEntries.get(1).getPartitionIndex());
  }

  @Test
  public void testSnapshotRecovery() throws Exception {
    // Snapshot metadata must be sufficient to restore both state and Raft bookkeeping after restart.
    File storageDir = temporaryFolder.newFolder("snapshot-recovery");
    CounterStateMachine firstStateMachine = new CounterStateMachine();
    TRaftConsensus firstConsensus = buildConsensus(0, storageDir, firstStateMachine);
    closeables.add(firstConsensus::stop);
    firstConsensus.start();
    firstConsensus.createLocalPeer(GROUP_ID, Collections.singletonList(buildPeer(0)));
    waitUntil(
        () -> firstConsensus.isLeaderReady(GROUP_ID),
        TimeUnit.SECONDS.toMillis(10),
        "Single-node TRaft group never became leader");

    firstConsensus.write(GROUP_ID, TestRequest.withTime(1, 10L, 10L));
    firstConsensus.write(GROUP_ID, TestRequest.withTime(1, 11L, 11L));
    firstConsensus.write(GROUP_ID, TestRequest.withTime(1, 12L, 12L));
    firstConsensus.triggerSnapshot(GROUP_ID, true);
    firstConsensus.stop();

    CounterStateMachine recoveredStateMachine = new CounterStateMachine();
    TRaftConsensus recoveredConsensus = buildConsensus(0, storageDir, recoveredStateMachine);
    closeables.add(recoveredConsensus::stop);
    recoveredConsensus.start();

    waitUntil(
        () -> recoveredStateMachine.getValue() == 3,
        TimeUnit.SECONDS.toMillis(10),
        "TRaft snapshot recovery did not restore state machine");
    Assert.assertEquals(3, recoveredStateMachine.getValue());
  }

  @Test
  public void testAddPeerCatchesUpHistory() throws Exception {
    File node0Dir = temporaryFolder.newFolder("add-peer-node0");
    File node1Dir = temporaryFolder.newFolder("add-peer-node1");

    Peer node0 = buildPeer(10);
    Peer node1 = buildPeer(11);
    CounterStateMachine node0StateMachine = new CounterStateMachine();
    CounterStateMachine node1StateMachine = new CounterStateMachine();

    TRaftConsensus node0Consensus = buildConsensus(node0, node0Dir, node0StateMachine);
    TRaftConsensus node1Consensus = buildConsensus(node1, node1Dir, node1StateMachine);
    closeables.add(node0Consensus::stop);
    closeables.add(node1Consensus::stop);

    node0Consensus.start();
    node1Consensus.start();
    node0Consensus.createLocalPeer(GROUP_ID, Collections.singletonList(node0));
    waitUntil(
        () -> node0Consensus.isLeaderReady(GROUP_ID),
        TimeUnit.SECONDS.toMillis(10),
        "Single-node TRaft group never became leader before add-peer");

    node0Consensus.write(GROUP_ID, TestRequest.withTime(1, 400L, 400L));
    node0Consensus.write(GROUP_ID, TestRequest.withTime(1, 401L, 401L));

    node1Consensus.createLocalPeer(GROUP_ID, java.util.Arrays.asList(node0, node1));
    node0Consensus.addRemotePeer(GROUP_ID, node1);

    waitUntil(
        () -> node1StateMachine.getValue() == 2,
        TimeUnit.SECONDS.toMillis(10),
        "New TRaft peer did not catch up historical entries");
    Assert.assertEquals(2, node1StateMachine.getValue());
  }

  private TRaftConsensus buildConsensus(int nodeId, File storageDir, CounterStateMachine stateMachine) {
    return buildConsensus(buildPeer(nodeId), storageDir, stateMachine);
  }

  private TRaftConsensus buildConsensus(Peer peer, File storageDir, CounterStateMachine stateMachine) {
    return new TRaftConsensus(
        ConsensusConfig.newBuilder()
            .setThisNode(peer.getEndpoint())
            .setThisNodeId(peer.getNodeId())
            .setStorageDir(storageDir.getAbsolutePath())
            .setConsensusGroupType(TConsensusGroupType.DataRegion)
            .setTRaftConfig(testConfig())
            .build(),
        gid -> stateMachine);
  }

  private static TRaftConfig testConfig() {
    return testConfigWithSnapshotThreshold(2);
  }

  private static TRaftConfig testConfigWithSnapshotThreshold(long autoTriggerLogThreshold) {
    return TRaftConfig.newBuilder()
        .setReplication(
            TRaftConfig.Replication.newBuilder()
                .setRequestTimeoutMs(TimeUnit.SECONDS.toMillis(5))
                .setWaitingReplicationTimeMs(50L)
                .setMaxEntriesPerAppend(16)
                .build())
        .setElection(
            TRaftConfig.Election.newBuilder()
                .setHeartbeatIntervalMs(80L)
                .setTimeoutMinMs(200L)
                .setTimeoutMaxMs(400L)
                .setRandomSeed(7L)
                .build())
        .setSnapshot(
            TRaftConfig.Snapshot.newBuilder()
                .setAutoTriggerLogThreshold(autoTriggerLogThreshold)
                .build())
        .build();
  }

  private static Peer buildPeer(int nodeId) {
    return new Peer(GROUP_ID, nodeId, new TEndPoint("127.0.0.1", 19_000 + nodeId));
  }

  private static void waitUntil(CheckedCondition condition, long timeoutMs, String message)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.evaluate()) {
        return;
      }
      Thread.sleep(50L);
    }
    Assert.fail(message);
  }

  @FunctionalInterface
  private interface CheckedCondition {
    boolean evaluate() throws Exception;
  }

  private static class TestCluster implements AutoCloseable {

    private final List<Peer> peers = new ArrayList<>();
    private final List<CounterStateMachine> stateMachines = new ArrayList<>();
    private final List<TRaftConsensus> consensuses = new ArrayList<>();

    private TestCluster(int nodeCount) throws Exception {
      this(nodeCount, testConfig());
    }

    private TestCluster(int nodeCount, TRaftConfig config) throws Exception {
      for (int i = 0; i < nodeCount; i++) {
        peers.add(new Peer(GROUP_ID, i, new TEndPoint("127.0.0.1", NEXT_PORT.getAndIncrement())));
      }
      for (int i = 0; i < nodeCount; i++) {
        CounterStateMachine stateMachine = new CounterStateMachine();
        stateMachines.add(stateMachine);
        TRaftConsensus consensus =
            new TRaftConsensus(
                ConsensusConfig.newBuilder()
                    .setThisNode(peers.get(i).getEndpoint())
                    .setThisNodeId(i)
                    .setStorageDir(temporaryStorageDir(i).getAbsolutePath())
                    .setConsensusGroupType(TConsensusGroupType.DataRegion)
                    .setTRaftConfig(config)
                    .build(),
                gid -> stateMachine);
        consensus.start();
        consensuses.add(consensus);
      }
      for (TRaftConsensus consensus : consensuses) {
        consensus.createLocalPeer(GROUP_ID, peers);
      }
    }

    private File temporaryStorageDir(int index) throws IOException {
      File dir = Files.createTempDirectory("traft-test-" + index + "-").toFile();
      dir.deleteOnExit();
      return dir;
    }

    private int waitLeaderReady() throws Exception {
      waitUntil(
          () -> consensuses.stream().filter(c -> c.isLeaderReady(GROUP_ID)).count() == 1,
          TimeUnit.SECONDS.toMillis(10),
          "TRaft cluster did not elect a ready leader");
      for (int i = 0; i < consensuses.size(); i++) {
        if (consensuses.get(i).isLeaderReady(GROUP_ID)) {
          return i;
        }
      }
      throw new IllegalStateException("Leader disappeared after election");
    }

    private TRaftConsensus getConsensus(int index) {
      return consensuses.get(index);
    }

    private TRaftServerImpl getImpl(int index) {
      return consensuses.get(index).getImpl(GROUP_ID);
    }

    private CounterStateMachine getStateMachine(int index) {
      return stateMachines.get(index);
    }

    private List<Integer> stateMachineValues() {
      List<Integer> values = new ArrayList<>();
      for (CounterStateMachine stateMachine : stateMachines) {
        values.add(stateMachine.getValue());
      }
      return values;
    }

    private String describeState() {
      List<String> states = new ArrayList<>();
      for (int i = 0; i < consensuses.size(); i++) {
        TRaftServerImpl impl = getImpl(i);
        Peer leader = impl.getLeader();
        states.add(
            String.format(
                "node=%d active=%s leader=%s leaderReady=%s term=%d commit=%d logSize=%d knownLeader=%s applied=%d",
                i,
                impl.isActive(),
                impl.isLeader(),
                impl.isLeaderReady(),
                impl.getCurrentTerm(),
                impl.getCommitIndex(),
                impl.getLogEntries().size(),
                leader == null ? -1 : leader.getNodeId(),
                getStateMachine(i).getValue()));
      }
      return states.toString();
    }

    @Override
    public void close() {
      for (TRaftConsensus consensus : consensuses) {
        consensus.stop();
      }
    }
  }

  private static class CounterStateMachine implements IStateMachine, IStateMachine.EventApi {

    private final AtomicInteger value = new AtomicInteger();

    @Override
    public void start() {
      value.set(0);
    }

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest request) {
      TestRequest testRequest = (TestRequest) request;
      if (testRequest.getOp() == 1) {
        value.incrementAndGet();
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      if (request instanceof TestRequest) {
        return request;
      }
      return new TestRequest(request.serializeToByteBuffer());
    }

    @Override
    public DataSet read(IConsensusRequest request) {
      return null;
    }

    @Override
    public boolean takeSnapshot(File snapshotDir) {
      File snapshot = new File(snapshotDir, "snapshot.txt");
      try {
        Files.write(
            snapshot.toPath(),
            String.valueOf(value.get()).getBytes(StandardCharsets.UTF_8));
        return true;
      } catch (IOException e) {
        return false;
      }
    }

    @Override
    public void loadSnapshot(File latestSnapshotRootDir) {
      File snapshot = new File(latestSnapshotRootDir, "snapshot.txt");
      if (!snapshot.exists()) {
        value.set(0);
        return;
      }
      try {
        value.set(
            Integer.parseInt(
                new String(Files.readAllBytes(snapshot.toPath()), StandardCharsets.UTF_8)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    int getValue() {
      return value.get();
    }
  }

  private static class TestRequest implements IConsensusRequest {

    private final int op;
    private final boolean hasTime;
    private final long time;
    private final long prefix;

    private TestRequest(ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      this.prefix = duplicate.getLong();
      this.hasTime = duplicate.get() == 1;
      this.time = duplicate.getLong();
      this.op = duplicate.getInt();
    }

    private TestRequest(int op, boolean hasTime, long time, long prefix) {
      this.op = op;
      this.hasTime = hasTime;
      this.time = time;
      this.prefix = prefix;
    }

    static ByteBufferConsensusRequest withTime(int op, long time, long prefix) {
      return new ByteBufferConsensusRequest(new TestRequest(op, true, time, prefix).serializeToByteBuffer());
    }

    static ByteBufferConsensusRequest withoutTime(int op, long prefix) {
      return new ByteBufferConsensusRequest(
          new TestRequest(op, false, Long.MIN_VALUE, prefix).serializeToByteBuffer());
    }

    int getOp() {
      return op;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1 + Long.BYTES + Integer.BYTES);
      buffer.putLong(prefix);
      buffer.put((byte) (hasTime ? 1 : 0));
      buffer.putLong(time);
      buffer.putInt(op);
      buffer.flip();
      return buffer;
    }

    @Override
    public boolean hasTime() {
      return hasTime;
    }

    @Override
    public long getTime() {
      return time;
    }
  }
}
