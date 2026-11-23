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
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.util.FakeConsensusReqReader;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTConsensusRecoveryTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Map<ConsensusGroupId, LifecycleStateMachine> stateMachines = new HashMap<>();
  private final Map<ConsensusGroupId, List<Peer>> peerLists = new HashMap<>();
  private BiConsumer<ConsensusGroupId, IoTConsensusServerImpl> originalNewPeerCallback;
  private BiConsumer<ConsensusGroupId, IoTConsensusServerImpl> originalRecoveringCallback;
  private ConsensusConfig config;
  private IoTConsensus consensus;

  @Before
  public void setUp() throws Exception {
    originalNewPeerCallback = IoTConsensus.onNewPeerCreated;
    originalRecoveringCallback = IoTConsensus.onPeerRecovering;
    IoTConsensus.onNewPeerCreated = null;
    IoTConsensus.onPeerRecovering = null;
    config =
        ConsensusConfig.newBuilder()
            .setThisNodeId(1)
            .setThisNode(new TEndPoint("127.0.0.1", 0))
            .setStorageDir(temporaryFolder.newFolder("consensus").getAbsolutePath())
            .setRecvSnapshotDirs(
                Collections.singletonList(temporaryFolder.newFolder("snapshots").getAbsolutePath()))
            .setConsensusGroupType(TConsensusGroupType.DataRegion)
            .build();
    consensus = newConsensus();
    consensus.start();
  }

  @After
  public void tearDown() {
    try {
      if (consensus != null) {
        consensus.stop();
      }
    } finally {
      IoTConsensus.onNewPeerCreated = originalNewPeerCallback;
      IoTConsensus.onPeerRecovering = originalRecoveringCallback;
    }
  }

  @Test
  public void testRecoveredPeersNotifyAfterStarting() throws Exception {
    final DataRegionId firstRegion = new DataRegionId(1);
    final DataRegionId secondRegion = new DataRegionId(2);
    createPeer(firstRegion);
    createPeer(secondRegion);
    consensus.stop();
    consensus = newConsensus();

    final Map<ConsensusGroupId, List<String>> notifications = new HashMap<>();
    IoTConsensus.onPeerRecovering =
        (groupId, server) -> {
          assertSame(server, consensus.getImpl(groupId));
          assertTrue(consensus.isPeerCreationCallbackPending(groupId));
          assertFalse(stateMachines.get(groupId).started);
          notifications.computeIfAbsent(groupId, ignored -> new ArrayList<>()).add("recovering");
        };
    IoTConsensus.onNewPeerCreated =
        (groupId, server) -> {
          assertSame(server, consensus.getImpl(groupId));
          assertTrue(consensus.isPeerCreationCallbackPending(groupId));
          assertTrue(stateMachines.get(groupId).started);
          notifications.computeIfAbsent(groupId, ignored -> new ArrayList<>()).add("available");
        };

    consensus.start();

    assertEquals(2, notifications.size());
    assertEquals(Arrays.asList("recovering", "available"), notifications.get(firstRegion));
    assertEquals(Arrays.asList("recovering", "available"), notifications.get(secondRegion));
    assertFalse(consensus.isPeerCreationCallbackPending(firstRegion));
    assertFalse(consensus.isPeerCreationCallbackPending(secondRegion));
  }

  @Test
  public void testRecoveryPublishesWalRetentionAfterRestoringPeers() throws Exception {
    final DataRegionId regionId = new DataRegionId(1);
    final List<Peer> peers =
        Arrays.asList(
            new Peer(regionId, 2, new TEndPoint("127.0.0.1", 1)),
            new Peer(regionId, config.getThisNodeId(), config.getThisNodeEndPoint()));
    consensus.createLocalPeer(regionId, peers);
    consensus.stop();

    final FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    when(reader.getReqIterator(anyLong())).thenReturn(mock(ConsensusReqReader.ReqIterator.class));
    consensus = newConsensus(reader);
    consensus.recordCorrectPeerListBeforeStarting(Collections.singletonMap(regionId, peers));
    IoTConsensus.onPeerRecovering =
        (groupId, server) -> {
          server.registerDetachedSubscriptionRetention(
              "test-retention",
              new SubscriptionWalRetentionPolicy(
                  "test-topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              () -> 0L);
          verify(reader, never()).setSafelyDeletedSearchIndex(anyLong());
          verify(reader, never()).setSubscriptionRetainedMinVersionId(anyLong());
        };
    consensus.start();

    verify(reader, never()).setSafelyDeletedSearchIndex(Long.MAX_VALUE);
    verify(reader).setSafelyDeletedSearchIndex(Long.MIN_VALUE + 1);
    verify(reader).setSubscriptionRetainedMinVersionId(0L);
  }

  @Test
  public void testSingleReplicaRecoveryPublishesUnboundedRetentionAfterPeerRecovery()
      throws Exception {
    final DataRegionId regionId = new DataRegionId(1);
    final List<Peer> peers =
        Collections.singletonList(
            new Peer(regionId, config.getThisNodeId(), config.getThisNodeEndPoint()));
    consensus.createLocalPeer(regionId, peers);
    consensus.stop();

    final FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    consensus = newConsensus(reader);
    consensus.recordCorrectPeerListBeforeStarting(Collections.singletonMap(regionId, peers));
    IoTConsensus.onPeerRecovering =
        (groupId, server) -> {
          verify(reader, never()).setSafelyDeletedSearchIndex(anyLong());
          verify(reader, never()).setSubscriptionRetainedMinVersionId(anyLong());
        };

    consensus.start();

    verify(reader).setSafelyDeletedSearchIndex(Long.MAX_VALUE);
    verify(reader).setSubscriptionRetainedMinVersionId(Long.MAX_VALUE);
  }

  @Test
  public void testRecoveryDoesNotPublishWalRetentionWhenPeerRestoreFails() throws Exception {
    final DataRegionId regionId = new DataRegionId(1);
    final List<Peer> peers =
        Arrays.asList(
            new Peer(regionId, 2, new TEndPoint(config.getThisNodeEndPoint().getIp(), 1)),
            new Peer(regionId, config.getThisNodeId(), config.getThisNodeEndPoint()));
    consensus.createLocalPeer(regionId, peers);
    consensus.stop();

    final FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    when(reader.getReqIterator(anyLong())).thenReturn(mock(ConsensusReqReader.ReqIterator.class));
    consensus = newConsensus(reader);
    consensus.recordCorrectPeerListBeforeStarting(Collections.singletonMap(regionId, peers));
    final IllegalStateException recoveryFailure = new IllegalStateException();
    IoTConsensus.onPeerRecovering =
        (groupId, server) -> {
          throw recoveryFailure;
        };

    final IOException exception = assertThrows(IOException.class, () -> consensus.start());
    assertSame(recoveryFailure, exception.getCause());
    verify(reader, never()).setSafelyDeletedSearchIndex(anyLong());
    verify(reader, never()).setSubscriptionRetainedMinVersionId(anyLong());
    assertTrue(consensus.getAllConsensusGroupIds().isEmpty());
    assertFalse(consensus.isPeerCreationCallbackPending(regionId));

    IoTConsensus.onPeerRecovering = null;
    consensus.start();

    verify(reader).setSafelyDeletedSearchIndex(0L);
  }

  @Test
  public void testPeerListResetFailureIsReportedAndCleanedUp() throws Exception {
    final DataRegionId regionId = new DataRegionId(1);
    final List<Peer> peers =
        Arrays.asList(
            new Peer(regionId, 2, new TEndPoint(config.getThisNodeEndPoint().getIp(), 1)),
            new Peer(regionId, config.getThisNodeId(), config.getThisNodeEndPoint()));
    consensus.createLocalPeer(regionId, peers);
    consensus.stop();

    final FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    when(reader.getReqIterator(anyLong())).thenThrow(new IllegalStateException());
    consensus = newConsensus(reader);
    consensus.recordCorrectPeerListBeforeStarting(Collections.singletonMap(regionId, peers));

    assertThrows(IOException.class, () -> consensus.start());

    verify(reader, never()).setSafelyDeletedSearchIndex(anyLong());
    verify(reader, never()).setSubscriptionRetainedMinVersionId(anyLong());
    assertTrue(consensus.getAllConsensusGroupIds().isEmpty());
  }

  @Test
  public void testRecoveryContinuesAfterPeerNotificationFailure() throws Exception {
    final DataRegionId firstRegion = new DataRegionId(1);
    final DataRegionId secondRegion = new DataRegionId(2);
    createPeer(firstRegion);
    createPeer(secondRegion);
    consensus.stop();
    consensus = newConsensus();

    final List<ConsensusGroupId> notifiedRegions = new ArrayList<>();
    IoTConsensus.onNewPeerCreated =
        (groupId, server) -> {
          assertTrue(stateMachines.get(groupId).started);
          notifiedRegions.add(groupId);
          throw new IllegalStateException("subscription binding failed");
        };

    consensus.start();

    assertEquals(2, notifiedRegions.size());
    assertTrue(notifiedRegions.containsAll(Arrays.asList(firstRegion, secondRegion)));
    assertTrue(stateMachines.get(firstRegion).started);
    assertTrue(stateMachines.get(secondRegion).started);
    assertFalse(consensus.isPeerCreationCallbackPending(firstRegion));
    assertFalse(consensus.isPeerCreationCallbackPending(secondRegion));
  }

  @Test
  public void testNewPeerRemainsPendingUntilCallbackReturns() throws Exception {
    final DataRegionId regionId = new DataRegionId(3);
    IoTConsensus.onNewPeerCreated =
        (groupId, server) -> {
          assertSame(server, consensus.getImpl(groupId));
          assertTrue(consensus.getAllConsensusGroupIds().contains(groupId));
          assertTrue(consensus.isPeerCreationCallbackPending(groupId));
          throw new IllegalStateException("subscription binding failed");
        };

    createPeer(regionId);

    assertFalse(consensus.isPeerCreationCallbackPending(regionId));
  }

  private IoTConsensus newConsensus() {
    final IoTConsensus instance =
        new IoTConsensus(
            config,
            groupId -> {
              final LifecycleStateMachine stateMachine = new LifecycleStateMachine();
              stateMachines.put(groupId, stateMachine);
              return stateMachine;
            });
    instance.recordCorrectPeerListBeforeStarting(peerLists);
    return instance;
  }

  private IoTConsensus newConsensus(final FakeConsensusReqReader reader) {
    return new IoTConsensus(
        config,
        groupId ->
            new TestStateMachine() {
              @Override
              public synchronized DataSet read(final IConsensusRequest request) {
                return reader;
              }
            });
  }

  private void createPeer(final DataRegionId regionId) throws Exception {
    final List<Peer> peers =
        Collections.singletonList(
            new Peer(regionId, config.getThisNodeId(), config.getThisNodeEndPoint()));
    consensus.createLocalPeer(regionId, peers);
    peerLists.put(regionId, peers);
  }

  private static final class LifecycleStateMachine extends TestStateMachine {

    private boolean started;

    @Override
    public void start() {
      started = true;
    }

    @Override
    public void stop() {
      started = false;
    }
  }
}
