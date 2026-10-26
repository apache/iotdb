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
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
import static org.junit.Assert.assertTrue;

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
          assertFalse(stateMachines.get(groupId).started);
          notifications.computeIfAbsent(groupId, ignored -> new ArrayList<>()).add("recovering");
        };
    IoTConsensus.onNewPeerCreated =
        (groupId, server) -> {
          assertSame(server, consensus.getImpl(groupId));
          assertTrue(stateMachines.get(groupId).started);
          notifications.computeIfAbsent(groupId, ignored -> new ArrayList<>()).add("available");
        };

    consensus.start();

    assertEquals(2, notifications.size());
    assertEquals(Arrays.asList("recovering", "available"), notifications.get(firstRegion));
    assertEquals(Arrays.asList("recovering", "available"), notifications.get(secondRegion));
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
