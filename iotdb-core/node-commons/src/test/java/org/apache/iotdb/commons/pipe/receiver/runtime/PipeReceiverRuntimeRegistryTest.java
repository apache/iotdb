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

package org.apache.iotdb.commons.pipe.receiver.runtime;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PipeReceiverRuntimeRegistryTest {

  private final PipeReceiverRuntimeRegistry registry = PipeReceiverRuntimeRegistry.getInstance();

  @Before
  public void setUp() {
    registry.clear();
  }

  @After
  public void tearDown() {
    registry.clear();
  }

  @Test
  public void testSingleSenderSingleDataNodeSingleConnectionSinglePipeBasicQuery() {
    registerDataSession("data-1", 1, "10.0.0.1", 9001, "root", "cluster-a", "pipe-a", 1, 100);
    registry.markTransfer("data-1", 200);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    final PipeReceiverRuntimeSnapshot snapshot = snapshots.get(0);
    assertEquals(PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE, snapshot.getReceiverNodeType());
    assertEquals(1, snapshot.getReceiverNodeId());
    assertEquals(PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT, snapshot.getProtocol());
    assertEquals("10.0.0.1", snapshot.getSenderAddress());
    assertEquals("9001", snapshot.getSenderPorts());
    assertEquals(1, snapshot.getConnectionCount());
    assertEquals(1, snapshot.getPipeCount());
    assertTrue(snapshot.getPipeIds().contains("pipe-a@"));
    assertEquals("root", snapshot.getUserName());
    assertEquals("cluster-a", snapshot.getSenderClusterId());
    assertEquals(100, snapshot.getLastHandshakeTime());
    assertEquals(200, snapshot.getLastTransferTime());
  }

  @Test
  public void testMultipleSendersAndMultipleDataNodesClusterAggregation() {
    registerDataSession("data-1", 1, "10.0.0.1", 9001, "root", "cluster-a", "pipe-a", 1, 100);
    registerDataSession("data-2", 2, "10.0.0.1", 9002, "root", "cluster-a", "pipe-b", 2, 200);
    registerDataSession("data-3", 2, "10.0.0.2", 9003, "root", "cluster-b", "pipe-c", 3, 300);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(3, snapshots.size());
    assertNotNull(
        findSnapshot(
            snapshots,
            PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
            1,
            PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
            "10.0.0.1"));
    assertNotNull(
        findSnapshot(
            snapshots,
            PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
            2,
            PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
            "10.0.0.1"));
    assertNotNull(
        findSnapshot(
            snapshots,
            PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
            2,
            PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
            "10.0.0.2"));
  }

  @Test
  public void testConfigNodeReceiversAreShownWithDataNodeResultsAndAirGapProtocol() {
    registerDataSession("data-1", 1, "10.0.0.1", 9001, "root", "cluster-a", "pipe-a", 1, 100);
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        0,
        PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
        "10.0.0.2",
        9002,
        "root",
        "cluster-b",
        "pipe-b",
        2,
        200);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(2, snapshots.size());
    final PipeReceiverRuntimeSnapshot configSnapshot =
        findSnapshot(
            snapshots,
            PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
            0,
            PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
            "10.0.0.2");
    assertNotNull(configSnapshot);
    assertEquals(PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP, configSnapshot.getProtocol());
    assertEquals(1, configSnapshot.getConnectionCount());
    assertEquals(1, configSnapshot.getPipeCount());
  }

  @Test
  public void testAggregateAndSortSnapshots() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        2,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    registry.markTransfer("data-1", 200);
    registry.registerOrUpdateSession(
        "data-2",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        2,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9002,
        "root",
        "cluster-b",
        "pipe-b",
        2,
        150);
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
        "127.0.0.2",
        9003,
        "root",
        PipeReceiverRuntimeRegistry.UNKNOWN,
        null,
        Long.MIN_VALUE,
        300);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(2, snapshots.size());
    assertEquals(
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE, snapshots.get(0).getReceiverNodeType());
    assertEquals(
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE, snapshots.get(1).getReceiverNodeType());

    final PipeReceiverRuntimeSnapshot dataSnapshot = snapshots.get(1);
    assertEquals(2, dataSnapshot.getConnectionCount());
    assertEquals("9001,9002", dataSnapshot.getSenderPorts());
    assertEquals(2, dataSnapshot.getPipeCount());
    assertTrue(dataSnapshot.getPipeIds().contains("pipe-a@"));
    assertTrue(dataSnapshot.getPipeIds().contains("pipe-b@"));
    assertEquals("cluster-a;cluster-b", dataSnapshot.getSenderClusterId());
    assertEquals(150, dataSnapshot.getLastHandshakeTime());
    assertEquals(200, dataSnapshot.getLastTransferTime());
  }

  @Test
  public void testDeregisterSession() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);

    assertEquals(1, registry.snapshot().size());

    registry.deregister("data-1");

    assertTrue(registry.snapshot().isEmpty());
  }

  @Test
  public void testPipeStopOrDropUpdatesPipeIdsAndPipeCount() {
    registerDataSession("data-1", 1, "10.0.0.1", 9001, "root", "cluster-a", "pipe-a", 1, 100);
    registerDataSession("data-2", 1, "10.0.0.1", 9002, "root", "cluster-a", "pipe-b", 2, 200);

    List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();
    assertEquals(1, snapshots.size());
    assertEquals(2, snapshots.get(0).getConnectionCount());
    assertEquals(2, snapshots.get(0).getPipeCount());

    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9001,
        "root",
        "cluster-a",
        null,
        Long.MIN_VALUE,
        300);

    snapshots = registry.snapshot();
    assertEquals(1, snapshots.size());
    assertEquals(2, snapshots.get(0).getConnectionCount());
    assertEquals(1, snapshots.get(0).getPipeCount());
    assertFalse(snapshots.get(0).getPipeIds().contains("pipe-a@"));
    assertTrue(snapshots.get(0).getPipeIds().contains("pipe-b@"));

    registry.deregister("data-2");

    snapshots = registry.snapshot();
    assertEquals(1, snapshots.size());
    assertEquals(1, snapshots.get(0).getConnectionCount());
    assertEquals(0, snapshots.get(0).getPipeCount());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshots.get(0).getPipeIds());
  }

  @Test
  public void testMixedVersionPipeIdsUnknownCompatibility() {
    registerDataSession(
        "data-1",
        1,
        "10.0.0.1",
        9001,
        "root",
        PipeReceiverRuntimeRegistry.UNKNOWN,
        null,
        Long.MIN_VALUE,
        100);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    assertEquals("10.0.0.1", snapshots.get(0).getSenderAddress());
    assertEquals("9001", snapshots.get(0).getSenderPorts());
    assertEquals(1, snapshots.get(0).getConnectionCount());
    assertEquals(0, snapshots.get(0).getPipeCount());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshots.get(0).getPipeIds());
  }

  @Test
  public void testLastTransferTimeDefaultsToLastHandshakeTimeBeforeTransfer() {
    registerDataSession("data-1", 1, "10.0.0.1", 9001, "root", "cluster-a", "pipe-a", 1, 100);

    List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    assertEquals(100, snapshots.get(0).getLastHandshakeTime());
    assertEquals(100, snapshots.get(0).getLastTransferTime());

    registry.markTransfer("data-1", 50);
    snapshots = registry.snapshot();

    assertEquals(100, snapshots.get(0).getLastTransferTime());

    registry.markTransfer("data-1", 200);
    snapshots = registry.snapshot();

    assertEquals(200, snapshots.get(0).getLastTransferTime());
  }

  @Test
  public void testReceiverRestartClearsRuntimeAndAllowsReconnect() {
    registerDataSession("data-1", 1, "10.0.0.1", 9001, "root", "cluster-a", "pipe-a", 1, 100);

    assertEquals(1, registry.snapshot().size());

    registry.clear();

    assertTrue(registry.snapshot().isEmpty());

    registerDataSession("data-2", 1, "10.0.0.1", 9002, "root", "cluster-a", "pipe-b", 2, 200);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();
    assertEquals(1, snapshots.size());
    assertEquals("9002", snapshots.get(0).getSenderPorts());
    assertTrue(snapshots.get(0).getPipeIds().contains("pipe-b@"));
  }

  @Test
  public void testLargeActiveSessionsSnapshotPerformanceAndTransferHotPathOverhead() {
    final int activeConnectionCount = 1000;
    final int pipeLifecycleOperationCount = 5000;
    for (int i = 0; i < activeConnectionCount; i++) {
      registerDataSession(
          "data-" + i,
          i % 10,
          "10.0." + (i / 100) + "." + (i % 100),
          9000 + i,
          "root",
          "cluster-" + (i % 5),
          "pipe-" + i,
          i,
          i);
    }

    for (int i = 0; i < pipeLifecycleOperationCount; i++) {
      final int connectionIndex = i % activeConnectionCount;
      registerDataSession(
          "data-" + connectionIndex,
          connectionIndex % 10,
          "10.0." + (connectionIndex / 100) + "." + (connectionIndex % 100),
          9000 + connectionIndex,
          "root",
          "cluster-" + (connectionIndex % 5),
          "pipe-update-" + i,
          i,
          10_000L + i);
    }

    final long transferStartTime = System.nanoTime();
    for (int i = 0; i < pipeLifecycleOperationCount; i++) {
      registry.markTransfer("data-" + (i % activeConnectionCount), 20_000L + i);
    }
    final long transferDurationNanos = System.nanoTime() - transferStartTime;

    final long snapshotStartTime = System.nanoTime();
    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();
    final long snapshotDurationNanos = System.nanoTime() - snapshotStartTime;

    assertEquals(
        activeConnectionCount,
        snapshots.stream().mapToInt(PipeReceiverRuntimeSnapshot::getConnectionCount).sum());
    assertEquals(
        20_000L + pipeLifecycleOperationCount - 1,
        snapshots.stream()
            .mapToLong(PipeReceiverRuntimeSnapshot::getLastTransferTime)
            .max()
            .orElse(0));
    assertTrue(
        "transfer updates should stay lightweight, duration ms: "
            + TimeUnit.NANOSECONDS.toMillis(transferDurationNanos),
        TimeUnit.NANOSECONDS.toMillis(transferDurationNanos) < 3000);
    assertTrue(
        "snapshot should be built from in-memory state, duration ms: "
            + TimeUnit.NANOSECONDS.toMillis(snapshotDurationNanos),
        TimeUnit.NANOSECONDS.toMillis(snapshotDurationNanos) < 3000);
  }

  @Test
  public void testRegisterOrUpdateSessionReplacesPipeId() {
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        -1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        -1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-b",
        2,
        200);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    assertEquals(1, snapshots.get(0).getPipeCount());
    assertFalse(snapshots.get(0).getPipeIds().contains("pipe-a@"));
    assertTrue(snapshots.get(0).getPipeIds().contains("pipe-b@"));
  }

  @Test
  public void testMultipleConnectionsAndDuplicatePipeAggregationDoesNotInflatePipeCount() {
    registerDataSession("data-1", 1, "10.0.0.1", 9001, "root", "cluster-a", "pipe-a", 1, 100);
    registerDataSession("data-2", 1, "10.0.0.1", 9002, "root", "cluster-a", "pipe-a", 1, 200);
    registerDataSession("data-3", 1, "10.0.0.1", -1, "root", "cluster-a", "pipe-b", 2, 150);

    registry.markTransfer("data-1", 300);
    registry.markTransfer("data-2", 250);
    registry.markTransfer("data-1", 50);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    final PipeReceiverRuntimeSnapshot snapshot = snapshots.get(0);
    assertEquals(3, snapshot.getConnectionCount());
    assertEquals("Unknown,9001,9002", snapshot.getSenderPorts());
    assertEquals(2, snapshot.getPipeCount());
    assertTrue(snapshot.getPipeIds().contains("pipe-a@"));
    assertTrue(snapshot.getPipeIds().contains("pipe-b@"));
    assertEquals(200, snapshot.getLastHandshakeTime());
    assertEquals(300, snapshot.getLastTransferTime());
  }

  @Test
  public void testSameSenderAddressWithDifferentUsersAreNotAggregated() {
    registerDataSession("data-user-a", 1, "10.0.0.1", 9001, "alice", "cluster-a", "pipe-a", 1, 100);
    registerDataSession("data-user-b", 1, "10.0.0.1", 9002, "bob", "cluster-a", "pipe-b", 2, 200);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(2, snapshots.size());
    assertEquals("alice", snapshots.get(0).getUserName());
    assertEquals("9001", snapshots.get(0).getSenderPorts());
    assertEquals(1, snapshots.get(0).getConnectionCount());
    assertEquals(1, snapshots.get(0).getPipeCount());
    assertTrue(snapshots.get(0).getPipeIds().contains("pipe-a@"));
    assertEquals("bob", snapshots.get(1).getUserName());
    assertEquals("9002", snapshots.get(1).getSenderPorts());
    assertEquals(1, snapshots.get(1).getConnectionCount());
    assertEquals(1, snapshots.get(1).getPipeCount());
    assertTrue(snapshots.get(1).getPipeIds().contains("pipe-b@"));
  }

  @Test
  public void testSameSenderAddressWithDifferentProtocolsAreNotAggregated() {
    registry.registerOrUpdateSession(
        "data-thrift",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-thrift",
        1,
        100);
    registry.registerOrUpdateSession(
        "data-air-gap",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
        "10.0.0.1",
        9002,
        "root",
        "cluster-a",
        "pipe-air-gap",
        2,
        200);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(2, snapshots.size());
    final PipeReceiverRuntimeSnapshot airGapSnapshot =
        findSnapshot(
            snapshots,
            PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
            1,
            PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
            "10.0.0.1");
    final PipeReceiverRuntimeSnapshot thriftSnapshot =
        findSnapshot(
            snapshots,
            PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
            1,
            PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
            "10.0.0.1");
    assertNotNull(airGapSnapshot);
    assertNotNull(thriftSnapshot);
    assertEquals("9002", airGapSnapshot.getSenderPorts());
    assertEquals(1, airGapSnapshot.getConnectionCount());
    assertTrue(airGapSnapshot.getPipeIds().contains("pipe-air-gap@"));
    assertEquals("9001", thriftSnapshot.getSenderPorts());
    assertEquals(1, thriftSnapshot.getConnectionCount());
    assertTrue(thriftSnapshot.getPipeIds().contains("pipe-thrift@"));
  }

  @Test
  public void testBlankAndMalformedRuntimeFieldsFallbackToUnknown() {
    registry.registerOrUpdateSession(
        "   ",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    assertTrue(registry.snapshot().isEmpty());

    registry.registerOrUpdateSession(
        "data-blank", " ", -1, "\t", "", -1, "\n", " ", "pipe-legacy", Long.MIN_VALUE, 0);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    final PipeReceiverRuntimeSnapshot snapshot = snapshots.get(0);
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshot.getReceiverNodeType());
    assertEquals(-1, snapshot.getReceiverNodeId());
    assertFalse(snapshot.isReceiverNodeIdKnown());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshot.getProtocol());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshot.getSenderAddress());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshot.getSenderPorts());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshot.getUserName());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshot.getSenderClusterId());
    assertEquals(1, snapshot.getPipeCount());
    assertEquals("pipe-legacy@Unknown", snapshot.getPipeIds());
    assertEquals(0, snapshot.getLastHandshakeTime());
    assertEquals(0, snapshot.getLastTransferTime());
  }

  @Test
  public void testRegisterOrUpdateSessionClearsPipeId() {
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        -1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        -1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        PipeReceiverRuntimeRegistry.UNKNOWN,
        null,
        Long.MIN_VALUE,
        200);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    assertEquals(0, snapshots.get(0).getPipeCount());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshots.get(0).getPipeIds());
  }

  @Test
  public void testUnknownSenderPort() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        -1,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshots.get(0).getSenderPorts());
  }

  @Test
  public void testDuplicateUnknownSenderPortsAreDeduplicated() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        -1,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    registry.registerOrUpdateSession(
        "data-2",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        -2,
        "root",
        "cluster-b",
        "pipe-b",
        2,
        200);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, snapshots.get(0).getSenderPorts());
  }

  @Test
  public void testUnknownReceiverNodeId() {
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        -1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(1, snapshots.size());
    assertEquals(-1, snapshots.get(0).getReceiverNodeId());
    assertFalse(snapshots.get(0).isReceiverNodeIdKnown());
  }

  private void registerDataSession(
      String connectionKey,
      int receiverNodeId,
      String senderAddress,
      int senderPort,
      String userName,
      String senderClusterId,
      String pipeName,
      long pipeCreationTime,
      long handshakeTime) {
    registry.registerOrUpdateSession(
        connectionKey,
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        receiverNodeId,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        senderAddress,
        senderPort,
        userName,
        senderClusterId,
        pipeName,
        pipeCreationTime,
        handshakeTime);
  }

  private static PipeReceiverRuntimeSnapshot findSnapshot(
      List<PipeReceiverRuntimeSnapshot> snapshots,
      String receiverNodeType,
      int receiverNodeId,
      String protocol,
      String senderAddress) {
    for (PipeReceiverRuntimeSnapshot snapshot : snapshots) {
      if (receiverNodeType.equals(snapshot.getReceiverNodeType())
          && receiverNodeId == snapshot.getReceiverNodeId()
          && protocol.equals(snapshot.getProtocol())
          && senderAddress.equals(snapshot.getSenderAddress())) {
        return snapshot;
      }
    }
    return null;
  }
}
