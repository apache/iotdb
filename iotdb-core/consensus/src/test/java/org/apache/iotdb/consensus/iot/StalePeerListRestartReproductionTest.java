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
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher.LogDispatcherThread;
import org.apache.iotdb.consensus.iot.util.TestEntry;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/** Diagnostic reproduction: assertions describe the current faulty behavior, not a fix. */
public class StalePeerListRestartReproductionTest {

  private static final DataRegionId GROUP = new DataRegionId(91);
  private static final int INITIAL_WRITES = 8;
  private static final int WRITES_AFTER_RESTART = 8;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void removedPeerIsRecreatedByStaleStartupListAndPinsRealSyncLag() throws Exception {
    final List<Peer> peers = new ArrayList<>();
    final List<File> storageDirectories = new ArrayList<>();
    final List<TestStateMachine> stateMachines = new ArrayList<>();
    final IoTConsensus[] servers = new IoTConsensus[3];
    for (int index = 0; index < servers.length; index++) {
      peers.add(new Peer(GROUP, index + 1, new TEndPoint("127.0.0.1", unusedPort())));
      storageDirectories.add(temporaryFolder.newFolder("peer-" + index));
      stateMachines.add(new TestStateMachine());
    }
    final Peer writer = peers.get(0);
    final Peer removed = peers.get(1);
    final Peer survivor = peers.get(2);
    final List<Peer> finalPeers = Arrays.asList(writer, survivor);
    try {
      for (int index = 0; index < servers.length; index++) {
        servers[index] =
            newServer(peers.get(index), storageDirectories.get(index), stateMachines.get(index));
        servers[index].start();
        servers[index].createLocalPeer(GROUP, peers);
      }
      final IoTConsensus originalWriter = servers[0];
      write(originalWriter, writer, INITIAL_WRITES);
      await("initial replicas catch up", () -> originalWriter.getImpl(GROUP).getSyncLag() == 0);
      final LogDispatcherThread originalRemovedThread =
          dispatcher(originalWriter.getImpl(GROUP), removed);
      assertEquals(INITIAL_WRITES, originalRemovedThread.getCurrentSyncIndex());
      printState("BEFORE_REMOVE", originalWriter.getImpl(GROUP));

      // This is the real remove RPC broadcast used by removeRemotePeer. A and C acknowledge it.
      originalWriter.getImpl(GROUP).notifyPeersToRemoveSyncLogChannel(removed);
      assertEquals(finalPeers, originalWriter.getImpl(GROUP).getConfiguration());
      assertEquals(finalPeers, servers[2].getImpl(GROUP).getConfiguration());
      assertEquals(
          Collections.singletonList(survivor), dispatcherPeers(originalWriter.getImpl(GROUP)));
      assertTrue(originalRemovedThread.isStopped());
      assertEquals(0, runFinished(originalRemovedThread).getCount());
      final File regionDirectory = new File(originalWriter.getRegionDirFromConsensusGroupId(GROUP));
      assertEquals(0, regionDirectory.listFiles((dir, name) -> name.startsWith("2-")).length);
      printState("REMOVE_ACKNOWLEDGED", originalWriter.getImpl(GROUP));

      // Keep B's RPC endpoint alive: it now returns INTERNAL_SERVER_ERROR (region does not exist).
      servers[1].deleteLocalPeer(GROUP);
      originalWriter.stop();
      servers[0] = null;

      // Model a restart registration response obtained before ConfigNode commits removal.
      final IoTConsensus restartedWriter =
          newServer(writer, storageDirectories.get(0), stateMachines.get(0));
      servers[0] = restartedWriter;
      restartedWriter.recordCorrectPeerListBeforeStarting(Collections.singletonMap(GROUP, peers));
      restartedWriter.start();
      final IoTConsensusServerImpl recovered = restartedWriter.getImpl(GROUP);
      assertEquals(peers, recovered.getConfiguration());
      assertEquals(Arrays.asList(removed, survivor), dispatcherPeers(recovered));
      final LogDispatcherThread resurrectedThread = dispatcher(recovered, removed);
      assertNotSame(originalRemovedThread, resurrectedThread);
      assertFalse(resurrectedThread.isStopped());
      assertEquals(1, runFinished(resurrectedThread).getCount());
      final long removedPeerIndex = resurrectedThread.getCurrentSyncIndex();
      assertEquals(INITIAL_WRITES, removedPeerIndex);
      assertEquals(1, regionDirectory.listFiles((dir, name) -> name.startsWith("2-")).length);
      printState("STALE_STARTUP_RECREATED_REMOVED_PEER", recovered);

      write(restartedWriter, writer, WRITES_AFTER_RESTART);
      final long expectedSearchIndex = INITIAL_WRITES + WRITES_AFTER_RESTART;
      final LogDispatcherThread healthyThread = dispatcher(recovered, survivor);
      await(
          "surviving replica catches up",
          () -> healthyThread.getCurrentSyncIndex() == expectedSearchIndex);
      assertEquals(expectedSearchIndex, recovered.getSearchIndex());
      assertEquals(expectedSearchIndex, stateMachines.get(2).getRequestSet().size());
      for (int sample = 0; sample < 5; sample++) {
        assertEquals(removedPeerIndex, resurrectedThread.getCurrentSyncIndex());
        assertEquals(WRITES_AFTER_RESTART, recovered.getSyncLag());
        assertFalse(resurrectedThread.isStopped());
        assertEquals(1, runFinished(resurrectedThread).getCount());
        printState("LAG_PINNED_SAMPLE_" + sample, recovered);
        Thread.sleep(200);
      }

      // Later process shutdown changes the transport error, but does not repair membership or lag.
      servers[1].stop();
      servers[1] = null;
      Thread.sleep(300);
      assertEquals(removedPeerIndex, resurrectedThread.getCurrentSyncIndex());
      assertEquals(WRITES_AFTER_RESTART, recovered.getSyncLag());
      printState("REMOVED_ENDPOINT_DOWN_LAG_STILL_PINNED", recovered);

      // Control: explicit membership reconciliation removes the new thread and immediately unpins
      // lag.
      restartedWriter.resetPeerList(GROUP, finalPeers);
      assertEquals(Collections.singletonList(survivor), dispatcherPeers(recovered));
      assertTrue(resurrectedThread.isStopped());
      assertEquals(0, recovered.getSyncLag());
      printState("EXPLICIT_RECONCILIATION_UNPINS_LAG", recovered);

      restartedWriter.stop();
      servers[0] = null;
      final IoTConsensus freshListWriter =
          newServer(writer, storageDirectories.get(0), stateMachines.get(0));
      servers[0] = freshListWriter;
      freshListWriter.recordCorrectPeerListBeforeStarting(
          Collections.singletonMap(GROUP, finalPeers));
      freshListWriter.start();
      assertEquals(finalPeers, freshListWriter.getImpl(GROUP).getConfiguration());
      assertEquals(
          Collections.singletonList(survivor), dispatcherPeers(freshListWriter.getImpl(GROUP)));
      write(freshListWriter, writer, 1);
      await("fresh-list restart converges", () -> freshListWriter.getImpl(GROUP).getSyncLag() == 0);
      assertEquals(expectedSearchIndex + 1, freshListWriter.getImpl(GROUP).getSearchIndex());
      printState("FRESH_STARTUP_LIST_CONVERGES", freshListWriter.getImpl(GROUP));
    } finally {
      Arrays.stream(servers)
          .filter(server -> server != null)
          .parallel()
          .forEach(IoTConsensus::stop);
    }
  }

  private static IoTConsensus newServer(Peer peer, File directory, TestStateMachine stateMachine) {
    return new IoTConsensus(
        ConsensusConfig.newBuilder()
            .setThisNodeId(peer.getNodeId())
            .setThisNode(peer.getEndpoint())
            .setStorageDir(directory.getAbsolutePath())
            .setConsensusGroupType(TConsensusGroupType.DataRegion)
            .setIoTConsensusConfig(
                IoTConsensusConfig.newBuilder()
                    .setRpc(
                        IoTConsensusConfig.RPC.newBuilder().setConnectionTimeoutInMs(500).build())
                    .setReplication(
                        IoTConsensusConfig.Replication.newBuilder()
                            .setCheckpointGap(1)
                            .setBasicRetryWaitTimeMs(20)
                            .setMaxRetryWaitTimeMs(100)
                            .build())
                    .build())
            .build(),
        ignored -> stateMachine);
  }

  private static int unusedPort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void write(IoTConsensus server, Peer writer, int count) throws Exception {
    final int previousCount = (int) server.getImpl(GROUP).getSearchIndex();
    for (int index = 1; index <= count; index++) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          server.write(GROUP, new TestEntry(previousCount + index, writer)).getCode());
    }
  }

  private static void await(String condition, BooleanSupplier satisfied) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!satisfied.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(condition, satisfied.getAsBoolean());
  }

  @SuppressWarnings("unchecked")
  private static List<LogDispatcherThread> dispatchers(IoTConsensusServerImpl server)
      throws Exception {
    final LogDispatcher dispatcher = server.getLogDispatcher();
    final Field field = LogDispatcher.class.getDeclaredField("threads");
    field.setAccessible(true);
    synchronized (dispatcher) {
      return new ArrayList<>((List<LogDispatcherThread>) field.get(dispatcher));
    }
  }

  private static List<Peer> dispatcherPeers(IoTConsensusServerImpl server) throws Exception {
    return dispatchers(server).stream()
        .map(LogDispatcherThread::getPeer)
        .collect(Collectors.toList());
  }

  private static LogDispatcherThread dispatcher(IoTConsensusServerImpl server, Peer peer)
      throws Exception {
    return dispatchers(server).stream()
        .filter(thread -> thread.getPeer().equals(peer))
        .findFirst()
        .get();
  }

  private static CountDownLatch runFinished(LogDispatcherThread thread) throws Exception {
    final Field field = LogDispatcherThread.class.getDeclaredField("runFinished");
    field.setAccessible(true);
    return (CountDownLatch) field.get(thread);
  }

  private static void printState(String stage, IoTConsensusServerImpl server) throws Exception {
    System.out.printf(
        "REPRO %s searchIndex=%d minSyncIndex=%d syncLag=%d dispatchers=%s%n",
        stage,
        server.getSearchIndex(),
        server.getMinSyncIndex(),
        server.getSyncLag(),
        dispatchers(server).stream()
            .map(thread -> thread.getPeer().getNodeId() + ":" + thread.getCurrentSyncIndex())
            .collect(Collectors.toList()));
  }
}
