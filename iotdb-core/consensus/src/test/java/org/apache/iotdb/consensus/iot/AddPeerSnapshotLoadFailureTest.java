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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.iot.util.TestEntry;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Regression test for the snapshot-load-failure bug (a failed snapshot load on the AddPeer target
 * was silently swallowed, so the new peer was activated and the migration falsely reported
 * successful, losing data on the new replica).
 *
 * <p>It builds a real two-node IoTConsensus group, forces the target peer's {@link
 * org.apache.iotdb.consensus.IStateMachine#loadSnapshot} to fail, and verifies that {@code
 * addRemotePeer}:
 *
 * <ul>
 *   <li>actually reaches the snapshot-load step (so the failure is the one under test, not an
 *       earlier step),
 *   <li>fails with a {@link ConsensusException} instead of silently succeeding,
 *   <li>does not leave the target peer active with an incompletely-loaded snapshot.
 * </ul>
 */
public class AddPeerSnapshotLoadFailureTest {

  private final Logger logger = LoggerFactory.getLogger(AddPeerSnapshotLoadFailureTest.class);

  private final ConsensusGroupId gid = new DataRegionId(1);

  private final int basePort = 9200;

  private final List<Peer> peers =
      Arrays.asList(
          new Peer(gid, 1, new TEndPoint("127.0.0.1", basePort - 1)),
          new Peer(gid, 2, new TEndPoint("127.0.0.1", basePort)));

  private final List<File> peersStorage =
      Arrays.asList(
          new File("target" + File.separator + "snapshot-load-fail-1"),
          new File("target" + File.separator + "snapshot-load-fail-2"));

  private final List<List<String>> peersRecvSnapshotDirs =
      Arrays.asList(
          Arrays.asList(
              "target" + File.separator + "snapshot-load-fail-1-recv-1",
              "target" + File.separator + "snapshot-load-fail-1-recv-2"),
          Arrays.asList(
              "target" + File.separator + "snapshot-load-fail-2-recv-1",
              "target" + File.separator + "snapshot-load-fail-2-recv-2"));

  private final List<IoTConsensus> servers = new ArrayList<>();
  private final List<ControllableStateMachine> stateMachines = new ArrayList<>();

  /**
   * A {@link TestStateMachine} that takes a real (single-file) snapshot and whose snapshot load can
   * be made to fail on demand. When not forced to fail, {@link #loadSnapshot} mirrors the real
   * {@code SnapshotLoader} contract by reporting failure when the snapshot root does not exist —
   * which is exactly what happens for the receive folders that never got a fragment of a snapshot
   * in a multi-data-dir deployment.
   */
  private static class ControllableStateMachine extends TestStateMachine {
    private volatile boolean failLoadSnapshot = false;
    private volatile boolean loadSnapshotInvoked = false;
    private volatile boolean emptySnapshot = false;

    void setFailLoadSnapshot(boolean failLoadSnapshot) {
      this.failLoadSnapshot = failLoadSnapshot;
    }

    void setEmptySnapshot(boolean emptySnapshot) {
      this.emptySnapshot = emptySnapshot;
    }

    boolean isLoadSnapshotInvoked() {
      return loadSnapshotInvoked;
    }

    @Override
    public boolean loadSnapshot(File latestSnapshotRootDir) {
      loadSnapshotInvoked = true;
      if (failLoadSnapshot) {
        return false;
      }
      // Mirror SnapshotLoader: a receive folder that never received a fragment of this snapshot has
      // no snapshot root, so loading from it must report failure.
      return latestSnapshotRootDir.exists();
    }

    @Override
    public boolean takeSnapshot(File snapshotDir) {
      if (emptySnapshot) {
        // Mirror an empty region: the snapshot has no fragments to transmit. The receiver then
        // materializes no snapshot under any receive folder, which must still load successfully.
        return true;
      }
      // Write a real (single) snapshot file so the transfer actually moves data and the receiver
      // materializes the snapshot under a subset of its receive folders.
      try {
        Files.write(
            new File(snapshotDir, "snapshot.data").toPath(),
            "snapshot".getBytes(StandardCharsets.UTF_8));
        return true;
      } catch (IOException e) {
        return false;
      }
    }

    // TestStateMachine does not implement clearSnapshot (the IStateMachine default throws). The
    // AddPeer flow calls it in a finally block to clean up the local snapshot, so we provide a
    // no-op here; otherwise that cleanup would mask the ConsensusException we are asserting on.
    @Override
    public boolean clearSnapshot() {
      return true;
    }
  }

  @Before
  public void setUp() throws Exception {
    for (File file : peersStorage) {
      file.mkdirs();
      stateMachines.add(new ControllableStateMachine());
    }
    peersRecvSnapshotDirs.forEach(innerList -> innerList.forEach(dir -> new File(dir).mkdirs()));
    initServer();
  }

  @After
  public void tearDown() throws Exception {
    servers.parallelStream().forEach(IoTConsensus::stop);
    servers.clear();
    for (File file : peersStorage) {
      FileUtils.deleteFully(file);
    }
    peersRecvSnapshotDirs.forEach(
        innerList ->
            innerList.forEach(
                dir -> {
                  try {
                    FileUtils.deleteFully(new File(dir));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }));
  }

  private void initServer() throws IOException {
    Assume.assumeTrue(checkPortAvailable());
    try {
      for (int i = 0; i < peers.size(); i++) {
        int finalI = i;
        servers.add(
            (IoTConsensus)
                ConsensusFactory.getConsensusImpl(
                        ConsensusFactory.IOT_CONSENSUS,
                        ConsensusConfig.newBuilder()
                            .setThisNodeId(peers.get(i).getNodeId())
                            .setThisNode(peers.get(i).getEndpoint())
                            .setStorageDir(peersStorage.get(i).getAbsolutePath())
                            .setRecvSnapshotDirs(peersRecvSnapshotDirs.get(i))
                            .setConsensusGroupType(TConsensusGroupType.DataRegion)
                            .build(),
                        groupId -> stateMachines.get(finalI))
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                String.format(
                                    ConsensusFactory.CONSTRUCT_FAILED_MSG,
                                    ConsensusFactory.IOT_CONSENSUS))));
      }
      for (int i = 0; i < peers.size(); i++) {
        servers.get(i).start();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof StartupException) {
        // just succeed when can not bind socket
        logger.info("Can not start IoTConsensus because", e);
        Assume.assumeTrue(false);
      } else {
        logger.error("Failed because", e);
        Assert.fail("Failed because " + e.getMessage());
      }
    }
  }

  @Test
  public void addRemotePeerMustFailWhenTargetSnapshotLoadFails() throws Exception {
    // node 0 is the sole initial member; node 1 will be added as a new peer. Mirroring the real
    // region-migration flow, the destination peer (node 1) is pre-created locally with the full
    // target peer list (IoTConsensus, unlike Ratis, requires a non-empty peer list here).
    servers.get(0).createLocalPeer(gid, peers.subList(0, 1));
    servers.get(1).createLocalPeer(gid, peers);

    // Put some data into the group so the snapshot transfer is meaningful.
    for (int i = 0; i < 10; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
    }

    // Force the target peer (node 1) to fail loading the transferred snapshot.
    stateMachines.get(1).setFailLoadSnapshot(true);

    // Before the fix, addRemotePeer swallowed the load failure and returned normally, leaving the
    // target peer active with incomplete data. It must now surface the failure.
    Assert.assertThrows(
        ConsensusException.class, () -> servers.get(0).addRemotePeer(gid, peers.get(1)));

    // The failure must be the snapshot load itself, i.e. the AddPeer flow actually reached the
    // load step on the target rather than aborting earlier.
    Assert.assertTrue(
        "Target peer's loadSnapshot was never invoked; the failure came from an earlier step",
        stateMachines.get(1).isLoadSnapshotInvoked());

    // The target peer must not be left active with an incompletely-loaded snapshot.
    Assert.assertFalse(
        "Target peer was activated despite a failed snapshot load",
        servers.get(1).getImpl(gid).isActive());
  }

  /**
   * A target peer configures one receive folder per local data dir, and snapshot fragments are
   * spread across those folders by the FolderManager, so a small snapshot only materializes under a
   * subset of them. A healthy transfer must therefore still succeed even though some receive
   * folders never received any fragment of the snapshot (regression for the multi-data-dir
   * false-failure reported on the load-failure-propagation PR).
   */
  @Test
  public void addRemotePeerSucceedsWhenSnapshotSpansSubsetOfRecvDirs() throws Exception {
    servers.get(0).createLocalPeer(gid, peers.subList(0, 1));
    servers.get(1).createLocalPeer(gid, peers);

    for (int i = 0; i < 10; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
    }

    // Do NOT force a load failure: this is a healthy transfer. The (small) snapshot lands in only a
    // subset of the target's receive folders, so the others legitimately have no snapshot root.
    // Before the fix, loadSnapshot() loaded from every receive folder and treated the missing ones
    // as failures, turning this healthy multi-data-dir transfer into a spurious failure.
    servers.get(0).addRemotePeer(gid, peers.get(1));

    Assert.assertTrue(
        "Target peer's loadSnapshot was never invoked",
        stateMachines.get(1).isLoadSnapshotInvoked());
    Assert.assertTrue(
        "Target peer was not activated after a successful snapshot load",
        servers.get(1).getImpl(gid).isActive());

    // Sanity-check that the test actually exercised a partial spread: at least one of the target's
    // receive folders must hold no snapshot at all, otherwise the skipped-folder path is untested.
    long emptyRecvFolders =
        peersRecvSnapshotDirs.get(1).stream()
            .map(dir -> new File(dir, IoTConsensusServerImpl.SNAPSHOT_DIR_NAME))
            .filter(recvFolder -> isEmptyOrMissing(recvFolder))
            .count();
    Assert.assertTrue(
        "Expected at least one receive folder without the snapshot, but every folder had it",
        emptyRecvFolders > 0);
  }

  private static boolean isEmptyOrMissing(File dir) {
    String[] children = dir.list();
    return children == null || children.length == 0;
  }

  /**
   * An empty region produces a snapshot with zero fragments, so nothing is transmitted and the
   * target materializes no snapshot under any receive folder. This must still be treated as a
   * successful (no-op) load — otherwise migrating an empty region (e.g. while removing a DataNode)
   * fails. Regression for the multi-data-dir AddPeer false-failure on empty regions.
   */
  @Test
  public void addRemotePeerSucceedsWhenSnapshotIsEmpty() throws Exception {
    servers.get(0).createLocalPeer(gid, peers.subList(0, 1));
    servers.get(1).createLocalPeer(gid, peers);

    // No data is written, and the source takes an empty snapshot: zero fragments are transmitted.
    stateMachines.get(0).setEmptySnapshot(true);

    // Before the fix, loadSnapshot() reported failure when no receive folder contained the
    // snapshot, so adding a peer for an empty region failed and DataNode removal timed out.
    servers.get(0).addRemotePeer(gid, peers.get(1));

    Assert.assertTrue(
        "Target peer was not activated after an empty snapshot load",
        servers.get(1).getImpl(gid).isActive());

    // Confirm the test really exercised the empty-snapshot path: no receive folder holds a
    // snapshot.
    long nonEmptyRecvFolders =
        peersRecvSnapshotDirs.get(1).stream()
            .map(dir -> new File(dir, IoTConsensusServerImpl.SNAPSHOT_DIR_NAME))
            .filter(recvFolder -> !isEmptyOrMissing(recvFolder))
            .count();
    Assert.assertEquals(
        "Expected no receive folder to contain a snapshot for an empty region",
        0,
        nonEmptyRecvFolders);
  }

  private boolean checkPortAvailable() {
    for (Peer peer : this.peers) {
      try (ServerSocket ignored = new ServerSocket(peer.getEndpoint().port)) {
        logger.info("check port {} success for node {}", peer.getEndpoint().port, peer.getNodeId());
      } catch (IOException e) {
        logger.error("check port {} failed for node {}", peer.getEndpoint().port, peer.getNodeId());
        return false;
      }
    }
    return true;
  }
}
