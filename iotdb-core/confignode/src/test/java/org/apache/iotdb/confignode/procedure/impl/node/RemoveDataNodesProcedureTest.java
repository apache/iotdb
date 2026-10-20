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
package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.AddRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.RemoveRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.procedure.NoopProcedureStore;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeHandler;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionPeerProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoveDataNodesProcedureTest {

  private TConsensusGroupId consensusGroupId =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  private TDataNodeLocation fromDataNodeLocation =
      new TDataNodeLocation(
          10,
          new TEndPoint("127.0.0.1", 6667),
          new TEndPoint("127.0.0.1", 6668),
          new TEndPoint("127.0.0.1", 6669),
          new TEndPoint("127.0.0.1", 6670),
          new TEndPoint("127.0.0.1", 6671));

  private TDataNodeLocation toDataNodeLocation =
      new TDataNodeLocation(
          11,
          new TEndPoint("127.0.0.1", 6677),
          new TEndPoint("127.0.0.1", 6678),
          new TEndPoint("127.0.0.1", 6679),
          new TEndPoint("127.0.0.1", 6680),
          new TEndPoint("127.0.0.1", 6681));

  @Test
  public void serDeTest() throws IOException {
    List<TDataNodeLocation> removedDataNodes = Collections.singletonList(fromDataNodeLocation);
    Map<Integer, NodeStatus> nodeStatusMap = new HashMap<>();
    nodeStatusMap.put(10, NodeStatus.Running);
    RemoveDataNodesProcedure procedure0 =
        new RemoveDataNodesProcedure(removedDataNodes, nodeStatusMap);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure0.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure0, ProcedureFactory.getInstance().create(buffer));
    }

    RegionMigrationPlan regionMigrationPlan =
        new RegionMigrationPlan(consensusGroupId, fromDataNodeLocation);
    regionMigrationPlan.setToDataNode(toDataNodeLocation);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      regionMigrationPlan.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(regionMigrationPlan, RegionMigrationPlan.deserialize(buffer));
    }
  }

  @Test
  public void metadataRetryExhaustionCleansUpNodeRemoval() throws Exception {
    assertFinalizationFailureCleanup(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE, false);
  }

  @Test
  public void peerListRetryExhaustionRetriesFailedNodeStatusCleanup() throws Exception {
    assertFinalizationFailureCleanup(RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST, true);
  }

  private void assertFinalizationFailureCleanup(
      RemoveRegionPeerState failingState, boolean failFirstStatusRestoration) throws Exception {
    TDataNodeLocation runningNode = fromDataNodeLocation;
    TDataNodeLocation readOnlyNode = location(12);
    TDataNodeLocation drainedNode = location(13);
    TDataNodeLocation retainedNode = location(14);
    List<TDataNodeLocation> removalTargets = Arrays.asList(runningNode, readOnlyNode, drainedNode);
    Map<Integer, NodeStatus> originalStatuses = new HashMap<>();
    originalStatuses.put(runningNode.getDataNodeId(), NodeStatus.Running);
    originalStatuses.put(readOnlyNode.getDataNodeId(), NodeStatus.ReadOnly);
    originalStatuses.put(drainedNode.getDataNodeId(), NodeStatus.Running);
    Map<Integer, NodeStatus> currentStatuses = new ConcurrentHashMap<>(originalStatuses);
    Set<Integer> registeredNodes = ConcurrentHashMap.newKeySet();
    registeredNodes.addAll(originalStatuses.keySet());
    Set<Integer> stoppedNodes = ConcurrentHashMap.newKeySet();
    AtomicInteger restorationAttempts = new AtomicInteger();
    AtomicLong firstRestorationFailureTime = new AtomicLong();
    AtomicLong restorationRetryDelay = new AtomicLong();

    TConsensusGroupId pendingRunningRegion =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    TConsensusGroupId pendingReadOnlyRegion =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, 3);
    PartitionInfo partitionInfo = new PartitionInfo();
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.remove")));
    CreateRegionGroupsPlan groups = new CreateRegionGroupsPlan();
    groups.addRegionGroup(
        "root.remove",
        new TRegionReplicaSet(
            consensusGroupId, new ArrayList<>(Arrays.asList(runningNode, retainedNode))));
    groups.addRegionGroup(
        "root.remove",
        new TRegionReplicaSet(
            pendingRunningRegion, new ArrayList<>(Arrays.asList(runningNode, retainedNode))));
    groups.addRegionGroup(
        "root.remove",
        new TRegionReplicaSet(
            pendingReadOnlyRegion, new ArrayList<>(Arrays.asList(readOnlyNode, retainedNode))));
    partitionInfo.createRegionGroups(groups);

    ConfigManager configManager = mock(ConfigManager.class, RETURNS_DEEP_STUBS);
    PartitionManager partitionManager = mock(PartitionManager.class);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(partitionManager.getAllReplicaSets())
        .thenAnswer(ignored -> partitionInfo.getAllReplicaSets());
    when(partitionManager.isDataNodeContainsRegion(anyInt(), any()))
        .thenAnswer(
            call ->
                partitionInfo.getAllReplicaSets((Integer) call.getArgument(0)).stream()
                    .anyMatch(replica -> replica.getRegionId().equals(call.getArgument(1))));

    RegionMaintainHandler regionHandler = mock(RegionMaintainHandler.class);
    when(regionHandler.filterDataNodeWithOtherRegionReplica(any(), any(TDataNodeLocation.class)))
        .thenReturn(Optional.of(retainedNode));
    doAnswer(
            call ->
                partitionInfo.addRegionLocation(
                    new AddRegionLocationPlan(call.getArgument(0), call.getArgument(1))))
        .when(regionHandler)
        .addRegionLocation(any(), any());
    when(regionHandler.createNewRegionPeer(any(), any())).thenReturn(success());
    when(regionHandler.submitAddRegionPeerTask(anyLong(), any(), any(), any()))
        .thenReturn(success());
    when(regionHandler.submitRemoveRegionPeerTask(anyLong(), any(), any(), any()))
        .thenAnswer(
            call -> {
              removalTargets.forEach(
                  node ->
                      Assert.assertEquals(
                          NodeStatus.Removing, currentStatuses.get(node.getDataNodeId())));
              return success();
            });
    when(regionHandler.submitDeleteOldRegionPeerTask(anyLong(), any(), any()))
        .thenReturn(success());
    TRegionMigrateResult taskSuccess = new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS);
    when(regionHandler.waitTaskFinish(anyLong(), any())).thenReturn(taskSuccess);
    when(regionHandler.waitTaskFinish(anyLong(), any(), any())).thenReturn(taskSuccess);
    when(regionHandler.isIoTConsensusV1DataRegion(consensusGroupId)).thenReturn(true);
    when(regionHandler.removeRegionLocation(consensusGroupId, runningNode))
        .thenAnswer(
            ignored ->
                failingState == RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE
                    ? new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode())
                    : partitionInfo.removeRegionLocation(
                        new RemoveRegionLocationPlan(consensusGroupId, runningNode)));
    when(regionHandler.resetPeerListAfterRemoveForIoTConsensusV1(consensusGroupId, runningNode))
        .thenReturn(false);

    RemoveDataNodeHandler nodeHandler = mock(RemoveDataNodeHandler.class);
    when(nodeHandler.checkEnoughDataNodeAfterRemoving(removalTargets)).thenReturn(true);
    RegionMigrationPlan migration = new RegionMigrationPlan(consensusGroupId, runningNode);
    migration.setToDataNode(toDataNodeLocation);
    // No destination was selected for the other regions, so they remain on the removal targets.
    when(nodeHandler.selectedRegionMigrationPlans(removalTargets))
        .thenReturn(Collections.singletonList(migration));
    doAnswer(
            call -> {
              List<TDataNodeLocation> nodes = call.getArgument(0);
              Map<Integer, NodeStatus> statuses = call.getArgument(1);
              boolean restoring = statuses.get(runningNode.getDataNodeId()) == NodeStatus.Running;
              if (restoring) {
                int attempt = restorationAttempts.incrementAndGet();
                Assert.assertTrue(registeredNodes.contains(readOnlyNode.getDataNodeId()));
                if (failFirstStatusRestoration && attempt == 1) {
                  currentStatuses.put(runningNode.getDataNodeId(), NodeStatus.Running);
                  firstRestorationFailureTime.set(System.nanoTime());
                  throw new IOException("Injected partial node status restoration failure");
                }
                if (failFirstStatusRestoration) {
                  restorationRetryDelay.set(System.nanoTime() - firstRestorationFailureTime.get());
                }
              }
              nodes.forEach(
                  node ->
                      currentStatuses.put(
                          node.getDataNodeId(), statuses.get(node.getDataNodeId())));
              return null;
            })
        .when(nodeHandler)
        .changeDataNodeStatus(anyList(), anyMap());
    doAnswer(
            call -> {
              List<TDataNodeLocation> nodes = call.getArgument(0);
              nodes.forEach(node -> registeredNodes.remove(node.getDataNodeId()));
              return null;
            })
        .when(nodeHandler)
        .removeDataNodePersistence(anyList());
    doAnswer(
            call -> {
              List<TDataNodeLocation> nodes = call.getArgument(0);
              nodes.forEach(node -> stoppedNodes.add(node.getDataNodeId()));
              return null;
            })
        .when(nodeHandler)
        .stopDataNodes(anyList());

    ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(env.getRegionMaintainHandler()).thenReturn(regionHandler);
    when(env.getRemoveDataNodeHandler()).thenReturn(nodeHandler);
    when(env.getSchedulerLock()).thenReturn(new ReentrantLock());
    when(env.getNodeLock()).thenReturn(new LockQueue());
    RecordingProcedureStore store = new RecordingProcedureStore();
    ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(env, store);
    when(env.getScheduler()).thenReturn(executor.getScheduler());
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      RemoveDataNodesProcedure root =
          new RemoveDataNodesProcedure(removalTargets, originalStatuses);
      long rootId = executor.submitProcedure(root);
      ProcedureTestUtil.waitForProcedure(executor, rootId);
      Assert.assertTrue("Node removal must finish failure cleanup", executor.isFinished(rootId));
      Assert.assertEquals(ProcedureState.ROLLEDBACK, root.getState());
      Assert.assertFalse(root.isSuccess());
      RemoveRegionPeerProcedure failedChild =
          store.recorded.values().stream()
              .filter(RemoveRegionPeerProcedure.class::isInstance)
              .map(RemoveRegionPeerProcedure.class::cast)
              .findFirst()
              .get();
      Procedure<?> migrationParent = store.recorded.get(failedChild.getParentProcId());
      Assert.assertTrue(migrationParent instanceof RegionMigrateProcedure);
      Assert.assertEquals(rootId, migrationParent.getParentProcId());
      Assert.assertSame(failedChild.getException(), root.getException());
      Assert.assertTrue(root.getException().getMessage().contains(failingState.name()));
      if (failingState == RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE) {
        verify(regionHandler, times(4)).removeRegionLocation(consensusGroupId, runningNode);
        verify(regionHandler, never()).resetPeerListAfterRemoveForIoTConsensusV1(any(), any());
      } else {
        verify(regionHandler).removeRegionLocation(consensusGroupId, runningNode);
        verify(regionHandler, times(4))
            .resetPeerListAfterRemoveForIoTConsensusV1(consensusGroupId, runningNode);
      }

      // Failed children bypass STOP_DATA_NODE: root rollback must still finalize node statuses.
      Assert.assertEquals(NodeStatus.Running, currentStatuses.get(runningNode.getDataNodeId()));
      Assert.assertEquals(NodeStatus.ReadOnly, currentStatuses.get(readOnlyNode.getDataNodeId()));
      Assert.assertTrue(registeredNodes.contains(runningNode.getDataNodeId()));
      Assert.assertTrue(registeredNodes.contains(readOnlyNode.getDataNodeId()));
      Assert.assertFalse(registeredNodes.contains(drainedNode.getDataNodeId()));
      Assert.assertEquals(Collections.singleton(drainedNode.getDataNodeId()), stoppedNodes);
      Assert.assertEquals(failFirstStatusRestoration ? 2 : 1, restorationAttempts.get());
      if (failFirstStatusRestoration) {
        // A node-lock wakeup must not bypass the five-second rollback retry timeout.
        Assert.assertTrue(
            "Status restoration must wait for the rollback retry timeout",
            restorationRetryDelay.get() >= TimeUnit.SECONDS.toNanos(4));
      }
      verify(nodeHandler).broadcastDataNodeStatusChange(Arrays.asList(runningNode, readOnlyNode));

      // Cleanup must not undo the completed add peer or a committed removal.
      Assert.assertTrue(
          partitionManager.isDataNodeContainsRegion(
              toDataNodeLocation.getDataNodeId(), consensusGroupId));
      Assert.assertEquals(
          failingState == RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE,
          partitionManager.isDataNodeContainsRegion(runningNode.getDataNodeId(), consensusGroupId));
      Assert.assertTrue(
          partitionManager.isDataNodeContainsRegion(
              runningNode.getDataNodeId(), pendingRunningRegion));
      Assert.assertTrue(
          partitionManager.isDataNodeContainsRegion(
              readOnlyNode.getDataNodeId(), pendingReadOnlyRegion));
    } finally {
      executor.stop();
      executor.join();
      store.stop();
      store.cleanup();
    }
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static TDataNodeLocation location(int id) {
    return new TDataNodeLocation(
        id,
        new TEndPoint("127.0.0.1", 6000 + id),
        new TEndPoint("127.0.0.1", 7000 + id),
        new TEndPoint("127.0.0.1", 8000 + id),
        new TEndPoint("127.0.0.1", 9000 + id),
        new TEndPoint("127.0.0.1", 10000 + id));
  }

  private static class RecordingProcedureStore extends NoopProcedureStore {
    private final Map<Long, Procedure<?>> recorded = new ConcurrentHashMap<>();

    @Override
    public void update(Procedure procedure) {
      recorded.put(procedure.getProcId(), procedure);
    }

    @Override
    public void update(Procedure[] procedures) {
      Arrays.stream(procedures).forEach(this::update);
    }
  }
}
