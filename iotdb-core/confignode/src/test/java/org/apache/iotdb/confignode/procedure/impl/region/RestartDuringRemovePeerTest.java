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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.RemoveRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS;
import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS_V2;
import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Regression coverage for the stale restart-list window and bounded CN reconciliation. */
public class RestartDuringRemovePeerTest {
  private final TConsensusGroupId regionId =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 6);
  private final TDataNodeLocation restartingNode = location(3, "11.101.10.8");
  private final TDataNodeLocation coordinator = location(5, "11.101.10.9");
  private final TDataNodeLocation removedNode = location(6, "11.101.10.10");
  private final List<TDataNodeLocation> finalPeers = Arrays.asList(restartingNode, coordinator);
  private PartitionInfo partitionInfo;
  private PartitionManager partitionManager;
  private LoadManager loadManager;
  private RegionMaintainHandler handler;
  private ConfigNodeProcedureEnv env;
  private String previousProtocol;

  @Before
  public void setUp() throws Exception {
    previousProtocol =
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass();
    ConfigNodeDescriptor.getInstance().getConf().setDataRegionConsensusProtocolClass(IOT_CONSENSUS);
    partitionInfo = new PartitionInfo();
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.restart")));
    CreateRegionGroupsPlan groups = new CreateRegionGroupsPlan();
    groups.addRegionGroup(
        "root.restart",
        new TRegionReplicaSet(
            regionId, new ArrayList<>(Arrays.asList(restartingNode, coordinator, removedNode))));
    partitionInfo.createRegionGroups(groups);

    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    partitionManager = Mockito.mock(PartitionManager.class);
    loadManager = Mockito.mock(LoadManager.class, Mockito.RETURNS_DEEP_STUBS);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(partitionManager.getRegionDatabase(regionId))
        .thenAnswer(call -> partitionInfo.getRegionDatabase(call.getArgument(0)));
    when(partitionManager.getReplicaSets("root.restart", Collections.singletonList(regionId)))
        .thenAnswer(call -> partitionInfo.getReplicaSets(call.getArgument(0), call.getArgument(1)));
    when(partitionManager.removeRegionLocation(any()))
        .thenAnswer(call -> partitionInfo.removeRegionLocation(call.getArgument(0)));
    handler = Mockito.spy(new RegionMaintainHandler(configManager));
    env = Mockito.mock(ConfigNodeProcedureEnv.class);
    when(env.getRegionMaintainHandler()).thenReturn(handler);
    doNothing().when(handler).forceUpdateRegionCache(any(), any(), any());
    doNothing().when(handler).transferRegionLeader(any(), any(), any());
    doNothing().when(handler).dropConsensusPipesForRemovePeer(any(), any());
    doReturn(success()).when(handler).submitRemoveRegionPeerTask(anyLong(), any(), any(), any());
    doReturn(success()).when(handler).submitDeleteOldRegionPeerTask(anyLong(), any(), any());
    doReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS))
        .when(handler)
        .waitTaskFinish(anyLong(), any(TDataNodeLocation.class));
    doReturn(successfulResponses()).when(handler).resetPeerList(any(), anyList(), anyMap());
  }

  @After
  public void restoreProtocol() {
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(previousProtocol);
  }

  @Test
  public void staleRestartSnapshotIsFollowedByFinalReconciliation() throws Exception {
    AccessibleRemoveProcedure procedure = newProcedure();
    procedure.doExecute(env);
    procedure.doExecute(env);
    List<TRegionReplicaSet> startupSnapshot =
        partitionInfo.getAllReplicaSets(restartingNode.getDataNodeId());
    assertTrue(startupSnapshot.get(0).getDataNodeLocations().contains(removedNode));
    verify(handler, Mockito.atLeastOnce())
        .forceUpdateRegionCache(regionId, removedNode, RegionStatus.Removing);
    verify(handler, never()).removeRegionLocation(regionId, removedNode);
    procedure.doExecute(env);
    procedure.doExecute(env);

    doAnswer(
            call -> {
              assertEquals(
                  finalPeers, partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations());
              assertEquals(finalPeers, call.getArgument(1));
              Map<Integer, TDataNodeLocation> recipients = call.getArgument(2);
              assertEquals(2, recipients.size());
              assertEquals(restartingNode, recipients.get(restartingNode.getDataNodeId()));
              assertEquals(coordinator, recipients.get(coordinator.getDataNodeId()));
              assertFalse(recipients.containsKey(removedNode.getDataNodeId()));
              return successfulResponses();
            })
        .when(handler)
        .resetPeerList(eq(regionId), anyList(), anyMap());

    assertEquals(1, procedure.doExecute(env).length);
    assertEquals(RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST, procedure.currentState());
    verify(handler, never()).resetPeerList(any(), anyList(), anyMap());
    assertEquals(0, procedure.doExecute(env).length);
    assertFalse(procedure.isWaiting());
    // Correcting future startup responses cannot change a snapshot already returned to a DN.
    assertTrue(startupSnapshot.get(0).getDataNodeLocations().contains(removedNode));
    verify(handler).resetPeerList(eq(regionId), eq(finalPeers), anyMap());
  }

  @Test
  public void metadataFailureRetriesBeforeCorrectingPeers() throws Exception {
    AccessibleRemoveProcedure procedure = beforeMetadataRemoval();
    doReturn(new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode()))
        .when(partitionManager)
        .removeRegionLocation(any());
    assertEquals(1, procedure.doExecute(env).length);
    assertEquals(ProcedureState.RUNNABLE, procedure.getState());
    assertEquals(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE, procedure.currentState());
    assertTrue(
        partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations().contains(removedNode));
    verify(handler, never()).resetPeerList(any(), anyList(), anyMap());
    verify(loadManager).removeRegionCache(regionId, removedNode.getDataNodeId());
    verify(loadManager.getRouteBalancer()).balanceRegionLeaderAndPriority();

    doAnswer(call -> partitionInfo.removeRegionLocation(call.getArgument(0)))
        .when(partitionManager)
        .removeRegionLocation(any());
    assertEquals(1, procedure.doExecute(env).length);
    assertEquals(0, procedure.doExecute(env).length);
    assertFalse(procedure.isWaiting());
    verify(handler).resetPeerList(eq(regionId), eq(finalPeers), anyMap());
    verify(loadManager, times(2)).removeRegionCache(regionId, removedNode.getDataNodeId());
    verify(loadManager.getRouteBalancer(), times(2)).balanceRegionLeaderAndPriority();
  }

  @Test
  public void v2MetadataFailureRetriesBeforeCompletingWithoutResettingPeers() throws Exception {
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(IOT_CONSENSUS_V2);
    AccessibleRemoveProcedure procedure = beforeMetadataRemoval();
    doReturn(new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode()))
        .doAnswer(call -> partitionInfo.removeRegionLocation(call.getArgument(0)))
        .when(partitionManager)
        .removeRegionLocation(any());

    assertEquals(1, procedure.doExecute(env).length);
    assertEquals(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE, procedure.currentState());
    assertFalse(procedure.isFailed());
    assertTrue(
        partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations().contains(removedNode));

    assertEquals(0, procedure.doExecute(env).length);
    assertFalse(procedure.isFailed());
    assertEquals(finalPeers, partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations());
    verify(handler, times(2)).removeRegionLocation(regionId, removedNode);
    verify(handler, never()).resetPeerList(any(), anyList(), anyMap());
  }

  @Test
  public void missingAndNotInitializedResponsesCanRetryAfterRecovery() throws Exception {
    AccessibleRemoveProcedure procedure = beforePeerListReset();
    doReturn(Collections.singletonMap(coordinator.getDataNodeId(), success()))
        .when(handler)
        .resetPeerList(any(), anyList(), anyMap());
    assertEquals(1, procedure.doExecute(env).length);
    assertEquals(ProcedureState.RUNNABLE, procedure.getState());
    assertEquals(finalPeers, partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations());

    AccessibleRemoveProcedure restored = restore(serialize(procedure));
    assertEquals(ProcedureState.RUNNABLE, restored.getState());
    assertEquals(RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST, restored.currentState());
    Map<Integer, TSStatus> notReady = successfulResponses();
    notReady.put(
        restartingNode.getDataNodeId(),
        new TSStatus(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode()));
    doReturn(notReady).when(handler).resetPeerList(any(), anyList(), anyMap());
    assertEquals(1, restored.doExecute(env).length);
    assertEquals(ProcedureState.RUNNABLE, restored.getState());

    doReturn(successfulResponses()).when(handler).resetPeerList(any(), anyList(), anyMap());
    assertEquals(0, restored.doExecute(env).length);
    assertFalse(restored.isWaiting());
    verify(handler, times(3)).resetPeerList(eq(regionId), eq(finalPeers), anyMap());
    verify(handler).removeRegionLocation(regionId, removedNode);
  }

  @Test
  public void exceptionDuringCorrectionIsRetriedInsteadOfCompleting() throws Exception {
    AccessibleRemoveProcedure procedure = beforePeerListReset();
    doThrow(new IllegalStateException("injected RPC failure"))
        .when(handler)
        .resetPeerList(any(), anyList(), anyMap());
    assertEquals(1, procedure.doExecute(env).length);
    assertEquals(ProcedureState.RUNNABLE, procedure.getState());
    assertFalse(procedure.isFailed());
    doReturn(successfulResponses()).when(handler).resetPeerList(any(), anyList(), anyMap());
    assertEquals(0, procedure.doExecute(env).length);
    assertFalse(procedure.isWaiting());
  }

  @Test
  public void replayAfterSuccessfulCorrectionIsIdempotent() throws Exception {
    AccessibleRemoveProcedure procedure = beforePeerListReset();
    byte[] checkpoint = serialize(procedure);
    assertEquals(0, procedure.doExecute(env).length);
    // Crash after successful RPCs, before persisting procedure completion.
    AccessibleRemoveProcedure restored = restore(checkpoint);
    assertEquals(0, restored.doExecute(env).length);
    assertEquals(finalPeers, partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations());
    verify(handler, times(2)).resetPeerList(eq(regionId), eq(finalPeers), anyMap());
  }

  @Test
  public void offlineReplicaCompletesRemovalAfterThreeRetries() throws Exception {
    AccessibleRemoveProcedure procedure = beforePeerListReset();
    doReturn(Collections.singletonMap(coordinator.getDataNodeId(), success()))
        .when(handler)
        .resetPeerList(any(), anyList(), anyMap());
    for (int retry = 0; retry < 3; retry++) {
      assertEquals(1, procedure.doExecute(env).length);
      assertEquals(ProcedureState.RUNNABLE, procedure.getState());
      assertFalse(procedure.isFailed());
    }
    assertEquals(0, procedure.doExecute(env).length);
    assertFalse(procedure.isFailed());
    assertNull(procedure.getException());
    assertEquals(finalPeers, partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations());
    verify(handler, times(4)).resetPeerList(eq(regionId), eq(finalPeers), anyMap());
    verify(handler).removeRegionLocation(regionId, removedNode);
  }

  @Test
  public void repeatedCorrectionExceptionsAlsoCompleteRemovalAfterThreeRetries() throws Exception {
    AccessibleRemoveProcedure procedure = beforePeerListReset();
    doThrow(new IllegalStateException("injected RPC failure"))
        .when(handler)
        .resetPeerList(any(), anyList(), anyMap());
    for (int retry = 0; retry < 3; retry++) {
      assertEquals(1, procedure.doExecute(env).length);
      assertFalse(procedure.isFailed());
    }
    assertEquals(0, procedure.doExecute(env).length);
    assertFalse(procedure.isFailed());
    assertNull(procedure.getException());
    assertEquals(finalPeers, partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations());
    verify(handler, times(4)).resetPeerList(eq(regionId), eq(finalPeers), anyMap());
    verify(handler).removeRegionLocation(regionId, removedNode);
  }

  @Test
  public void metadataFailureAlsoStopsAtRetryLimitWithoutResettingPeers() throws Exception {
    AccessibleRemoveProcedure procedure = beforeMetadataRemoval();
    doReturn(null).when(partitionManager).removeRegionLocation(any());
    for (int retry = 0; retry < 3; retry++) {
      assertEquals(1, procedure.doExecute(env).length);
      assertFalse(procedure.isWaiting());
    }
    assertEquals(0, procedure.doExecute(env).length);
    assertTrue(procedure.isFailed());
    verify(handler, times(4)).removeRegionLocation(regionId, removedNode);
    verify(handler, never()).resetPeerList(any(), anyList(), anyMap());
  }

  @Test
  public void metadataExceptionsStillFailAtRetryLimitWithoutResettingPeers() throws Exception {
    AccessibleRemoveProcedure procedure = beforeMetadataRemoval();
    IllegalStateException failure = new IllegalStateException("injected metadata failure");
    doThrow(failure).when(partitionManager).removeRegionLocation(any());
    for (int retry = 0; retry < 3; retry++) {
      assertEquals(1, procedure.doExecute(env).length);
      assertFalse(procedure.isFailed());
    }
    assertEquals(0, procedure.doExecute(env).length);
    assertTrue(procedure.isFailed());
    assertEquals(failure, procedure.getException().getCause());
    assertTrue(
        partitionInfo.getAllReplicaSets().get(0).getDataNodeLocations().contains(removedNode));
    verify(handler, times(4)).removeRegionLocation(regionId, removedNode);
    verify(handler, never()).resetPeerList(any(), anyList(), anyMap());
  }

  @Test
  public void interruptedRetryRetainsItsStepForRecovery() throws Exception {
    AccessibleRemoveProcedure procedure = beforePeerListReset();
    doReturn(Collections.emptyMap()).when(handler).resetPeerList(any(), anyList(), anyMap());
    try {
      Thread.currentThread().interrupt();
      assertEquals(1, procedure.doExecute(env).length);
      assertTrue(Thread.currentThread().isInterrupted());
      assertFalse(procedure.isFailed());
    } finally {
      Thread.interrupted();
    }
    AccessibleRemoveProcedure restored = restore(serialize(procedure));
    assertEquals(RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST, restored.currentState());
    doReturn(successfulResponses()).when(handler).resetPeerList(any(), anyList(), anyMap());
    assertEquals(0, restored.doExecute(env).length);
  }

  @Test
  public void correctionRejectsStaleMetadataAndNullResponsesButAcceptsDeletedRegion() {
    assertFalse(handler.resetPeerListAfterRemoveForIoTConsensusV1(regionId, removedNode));
    verify(handler, never()).resetPeerList(any(), anyList(), anyMap());
    partitionInfo.removeRegionLocation(new RemoveRegionLocationPlan(regionId, removedNode));
    doReturn(null).when(handler).resetPeerList(any(), anyList(), anyMap());
    assertFalse(handler.resetPeerListAfterRemoveForIoTConsensusV1(regionId, removedNode));
    when(partitionManager.getRegionDatabase(regionId)).thenReturn(null);
    assertTrue(handler.resetPeerListAfterRemoveForIoTConsensusV1(regionId, removedNode));
  }

  @Test
  public void correctionIsLimitedToIoTConsensusV1DataRegions() throws Exception {
    // New states must be appended so existing persisted state IDs remain valid.
    assertEquals(3, RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE.ordinal());
    assertEquals(4, RemoveRegionPeerState.DROP_CONSENSUS_PIPES.ordinal());
    assertEquals(5, RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST.ordinal());
    assertTrue(handler.isIoTConsensusV1DataRegion(regionId));
    assertFalse(
        handler.isIoTConsensusV1DataRegion(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 6)));
    for (String protocol : Arrays.asList(IOT_CONSENSUS_V2, RATIS_CONSENSUS)) {
      ConfigNodeDescriptor.getInstance().getConf().setDataRegionConsensusProtocolClass(protocol);
      assertFalse(handler.isIoTConsensusV1DataRegion(regionId));
      AccessibleRemoveProcedure procedure = beforeMetadataRemoval();
      assertEquals(0, procedure.doExecute(env).length);
    }
    verify(handler, never()).resetPeerList(any(), anyList(), anyMap());
  }

  private AccessibleRemoveProcedure newProcedure() {
    AccessibleRemoveProcedure procedure =
        new AccessibleRemoveProcedure(regionId, coordinator, removedNode);
    procedure.setProcId(1);
    procedure.setState(ProcedureState.RUNNABLE);
    return procedure;
  }

  private AccessibleRemoveProcedure beforeMetadataRemoval() throws Exception {
    AccessibleRemoveProcedure procedure = newProcedure();
    for (int i = 0; i < 4; i++) {
      assertEquals(1, procedure.doExecute(env).length);
    }
    return procedure;
  }

  private AccessibleRemoveProcedure beforePeerListReset() throws Exception {
    AccessibleRemoveProcedure procedure = beforeMetadataRemoval();
    assertEquals(1, procedure.doExecute(env).length);
    assertEquals(RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST, procedure.currentState());
    return procedure;
  }

  private static byte[] serialize(AccessibleRemoveProcedure procedure) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    procedure.serialize(new DataOutputStream(bytes));
    return bytes.toByteArray();
  }

  private static AccessibleRemoveProcedure restore(byte[] bytes) throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    assertEquals(ProcedureType.REMOVE_REGION_PEER_PROCEDURE.getTypeCode(), buffer.getShort());
    AccessibleRemoveProcedure restored = new AccessibleRemoveProcedure();
    restored.deserialize(buffer);
    return restored;
  }

  private Map<Integer, TSStatus> successfulResponses() {
    Map<Integer, TSStatus> responses = new HashMap<>();
    for (TDataNodeLocation peer : finalPeers) {
      responses.put(peer.getDataNodeId(), success());
    }
    return responses;
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static class AccessibleRemoveProcedure extends RemoveRegionPeerProcedure {
    private AccessibleRemoveProcedure() {}

    private AccessibleRemoveProcedure(
        TConsensusGroupId group, TDataNodeLocation coordinator, TDataNodeLocation target) {
      super(group, coordinator, target);
    }

    @Override
    public Procedure<ConfigNodeProcedureEnv>[] doExecute(ConfigNodeProcedureEnv env)
        throws InterruptedException {
      return super.doExecute(env);
    }

    @Override
    public void setState(ProcedureState state) {
      super.setState(state);
    }

    private RemoveRegionPeerState currentState() {
      return getCurrentState();
    }
  }

  private static TDataNodeLocation location(int id, String ip) {
    return new TDataNodeLocation(
        id,
        new TEndPoint(ip, 6667),
        new TEndPoint(ip, 10730),
        new TEndPoint(ip, 10740),
        new TEndPoint(ip, 10750),
        new TEndPoint(ip, 10760));
  }
}
