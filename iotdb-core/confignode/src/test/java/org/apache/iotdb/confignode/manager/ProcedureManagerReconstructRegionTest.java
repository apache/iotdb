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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.impl.region.ReconstructRegionProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TReconstructRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TRemoveRegionReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProcedureManagerReconstructRegionTest {

  private final TConsensusGroupId firstRegion =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 12);
  private final TConsensusGroupId secondRegion =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 14);
  private final TDataNodeLocation target = new TDataNodeLocation().setDataNodeId(7);
  private final TDataNodeLocation coordinator = new TDataNodeLocation().setDataNodeId(8);

  private ProcedureManager manager;
  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
  private NodeManager nodeManager;
  private PartitionManager partitionManager;
  private final ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> procedures =
      new ConcurrentHashMap<>();

  @Before
  public void setUp() throws Exception {
    ConfigManager configManager = mock(ConfigManager.class);
    nodeManager = mock(NodeManager.class);
    partitionManager = mock(PartitionManager.class);
    ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    RegionMaintainHandler handler = mock(RegionMaintainHandler.class);
    executor = mock(ProcedureExecutor.class);

    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(nodeManager.getRegisteredDataNode(target.getDataNodeId()))
        .thenReturn(new TDataNodeConfiguration().setLocation(target));
    when(nodeManager.filterDataNodeThroughStatus(NodeStatus.Running))
        .thenReturn(Collections.singletonList(new TDataNodeConfiguration().setLocation(target)));
    when(nodeManager.filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly))
        .thenReturn(Collections.singletonList(new TDataNodeConfiguration().setLocation(target)));
    when(partitionManager.findTConsensusGroupIdByRegionId(12)).thenReturn(Optional.of(firstRegion));
    when(partitionManager.findTConsensusGroupIdByRegionId(14))
        .thenReturn(Optional.of(secondRegion));
    when(partitionManager.findTConsensusGroupIdByRegionId(99)).thenReturn(Optional.empty());
    when(partitionManager.generateTConsensusGroupIdByRegionId(12))
        .thenReturn(Optional.of(firstRegion));
    when(partitionManager.generateTConsensusGroupIdByRegionId(14))
        .thenReturn(Optional.of(secondRegion));
    when(partitionManager.getRegionDatabase(any(TConsensusGroupId.class))).thenReturn("root.sg");

    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets = new HashMap<>();
    replicaSets.put(
        firstRegion, new TRegionReplicaSet(firstRegion, Arrays.asList(target, coordinator)));
    replicaSets.put(
        secondRegion, new TRegionReplicaSet(secondRegion, Arrays.asList(target, coordinator)));
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(replicaSets);
    when(partitionManager.getAllReplicaSets(target.getDataNodeId()))
        .thenReturn(Arrays.asList(replicaSets.get(firstRegion), replicaSets.get(secondRegion)));

    when(env.getSubmitRegionMigrateLock()).thenReturn(new ReentrantLock());
    when(env.getRegionMaintainHandler()).thenReturn(handler);
    when(handler.filterDataNodeWithOtherRegionReplica(
            any(TConsensusGroupId.class),
            eq(target),
            eq(NodeStatus.Running),
            eq(NodeStatus.Removing),
            eq(NodeStatus.ReadOnly)))
        .thenReturn(Optional.of(coordinator));
    when(executor.getProcedures()).thenReturn(procedures);

    manager = new ProcedureManager(configManager, mock(ProcedureInfo.class));
    Field envField = ProcedureManager.class.getDeclaredField("env");
    envField.setAccessible(true);
    envField.set(manager, env);
    Field executorField = ProcedureManager.class.getDeclaredField("executor");
    executorField.setAccessible(true);
    executorField.set(manager, executor);
  }

  @Test
  public void testDuplicateAndNonExistentRegionIdsAreSkippedInInputOrder() {
    TReconstructRegionReq request =
        new TReconstructRegionReq(Arrays.asList(12, 99, 14, 12, 99, 14), 7, Model.TREE);

    TSStatus status = manager.reconstructRegion(request);
    assertEquals(TSStatusCode.RECONSTRUCT_REGION_ERROR.getStatusCode(), status.getCode());
    assertTrue(status.getMessage().contains("Total regions: 3"));
    assertTrue(status.getMessage().contains("successfully submitted: 2"));
    assertTrue(status.getMessage().contains("failed to submit: 1"));

    ArgumentCaptor<ReconstructRegionProcedure> captor =
        ArgumentCaptor.forClass(ReconstructRegionProcedure.class);
    verify(executor, times(2)).submitProcedure(captor.capture());
    assertEquals(firstRegion, captor.getAllValues().get(0).getRegionId());
    assertEquals(secondRegion, captor.getAllValues().get(1).getRegionId());
    verify(partitionManager, times(1)).findTConsensusGroupIdByRegionId(12);
    verify(partitionManager, times(1)).findTConsensusGroupIdByRegionId(14);
    verify(partitionManager, times(1)).findTConsensusGroupIdByRegionId(99);
  }

  @Test
  public void testRequestWithNoUsableRegionIdsFailsWithoutSubmittingProcedure() {
    TReconstructRegionReq request = new TReconstructRegionReq(Arrays.asList(99, 99), 7, Model.TREE);

    TSStatus status = manager.reconstructRegion(request);
    assertEquals(TSStatusCode.RECONSTRUCT_REGION_ERROR.getStatusCode(), status.getCode());
    assertTrue(status.getMessage().contains("Total regions: 1"));
    assertTrue(status.getMessage().contains("failed to submit: 1"));
    verify(executor, times(0)).submitProcedure(any());
  }

  @Test
  public void testRequestWithNoRegionIdsFailsWithoutSubmittingProcedure() {
    TReconstructRegionReq request =
        new TReconstructRegionReq(Collections.emptyList(), 7, Model.TREE);

    TSStatus status = manager.reconstructRegion(request);
    assertEquals(TSStatusCode.RECONSTRUCT_REGION_ERROR.getStatusCode(), status.getCode());
    assertTrue(status.getMessage().contains("Total regions: 0"));
    verify(executor, times(0)).submitProcedure(any());
  }

  @Test
  public void testAnotherRequestCannotReconstructRegionWithActiveProcedure() {
    TReconstructRegionReq request =
        new TReconstructRegionReq(Collections.singletonList(12), 7, Model.TREE);
    ReconstructRegionProcedure activeProcedure =
        new ReconstructRegionProcedure(firstRegion, target, coordinator);
    procedures.put(1L, activeProcedure);

    TSStatus status = manager.reconstructRegion(request);
    assertEquals(TSStatusCode.RECONSTRUCT_REGION_ERROR.getStatusCode(), status.getCode());
    assertTrue(status.getMessage().contains("in progress"));
    verify(executor, times(0)).submitProcedure(any());
  }

  @Test
  public void testRemoveRegionAllowsReadOnlyTargetDataNode() {
    procedures.clear();
    when(nodeManager.filterDataNodeThroughStatus(NodeStatus.Running))
        .thenReturn(
            Collections.singletonList(new TDataNodeConfiguration().setLocation(coordinator)));
    TRemoveRegionReq request = new TRemoveRegionReq(Collections.singletonList(12), 7, Model.TREE);

    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), manager.removeRegions(request).getCode());
    verify(executor, times(1)).submitProcedure(any());
  }
}
