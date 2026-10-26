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
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.impl.region.AddRegionPeerProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.ReconstructRegionProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionOperationProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionPeerProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TExtendRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TReconstructRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TRemoveRegionReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class ProcedureManagerRegionOperationTest {

  private enum Operation {
    MIGRATE(TSStatusCode.MIGRATE_REGION_ERROR, RegionMigrateProcedure.class),
    RECONSTRUCT(TSStatusCode.RECONSTRUCT_REGION_ERROR, ReconstructRegionProcedure.class),
    EXTEND(TSStatusCode.EXTEND_REGION_ERROR, AddRegionPeerProcedure.class),
    REMOVE(TSStatusCode.REMOVE_REGION_PEER_ERROR, RemoveRegionPeerProcedure.class);

    private final TSStatusCode errorCode;
    private final Class<?> procedureClass;

    Operation(TSStatusCode errorCode, Class<?> procedureClass) {
      this.errorCode = errorCode;
      this.procedureClass = procedureClass;
    }
  }

  @Parameterized.Parameters(name = "{0}-{1}-{2}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = new ArrayList<>();
    for (Operation operation : Operation.values()) {
      for (Model model : Arrays.asList(Model.TREE, Model.TABLE)) {
        for (TConsensusGroupType type :
            Arrays.asList(TConsensusGroupType.DataRegion, TConsensusGroupType.SchemaRegion)) {
          parameters.add(new Object[] {operation, model, type});
        }
      }
    }
    return parameters;
  }

  private final Operation operation;
  private final Model model;
  private final TConsensusGroupId firstRegion;
  private final TConsensusGroupId secondRegion;
  private final TDataNodeLocation source = new TDataNodeLocation().setDataNodeId(6);
  private final TDataNodeLocation target = new TDataNodeLocation().setDataNodeId(7);
  private final TDataNodeLocation coordinator = new TDataNodeLocation().setDataNodeId(8);
  private final ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> procedures =
      new ConcurrentHashMap<>();

  private ProcedureManager manager;
  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
  private NodeManager nodeManager;
  private PartitionManager partitionManager;
  private RegionMaintainHandler handler;
  private ReentrantLock submissionLock;
  private String originalDataConsensus;
  private String originalSchemaConsensus;

  public ProcedureManagerRegionOperationTest(
      Operation operation, Model model, TConsensusGroupType type) {
    this.operation = operation;
    this.model = model;
    firstRegion = new TConsensusGroupId(type, 12);
    secondRegion = new TConsensusGroupId(type, 14);
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    originalDataConsensus =
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass();
    originalSchemaConsensus =
        ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionConsensusProtocolClass();
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);

    ConfigManager configManager = mock(ConfigManager.class);
    nodeManager = mock(NodeManager.class);
    partitionManager = mock(PartitionManager.class);
    ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    handler = mock(RegionMaintainHandler.class);
    executor = mock(ProcedureExecutor.class);
    submissionLock = new ReentrantLock();
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);

    // NodeManager returns an empty configuration for IDs that are not registered DataNodes.
    when(nodeManager.getRegisteredDataNode(anyInt())).thenReturn(new TDataNodeConfiguration());
    when(nodeManager.getRegisteredDataNode(source.getDataNodeId()))
        .thenReturn(new TDataNodeConfiguration().setLocation(source));
    when(nodeManager.getRegisteredDataNode(target.getDataNodeId()))
        .thenReturn(new TDataNodeConfiguration().setLocation(target));
    when(nodeManager.filterDataNodeThroughStatus(NodeStatus.Running))
        .thenReturn(Collections.singletonList(new TDataNodeConfiguration().setLocation(target)));
    when(nodeManager.filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly))
        .thenReturn(Collections.singletonList(new TDataNodeConfiguration().setLocation(target)));

    when(partitionManager.findTConsensusGroupIdByRegionId(anyInt())).thenReturn(Optional.empty());
    when(partitionManager.findTConsensusGroupIdByRegionId(12)).thenReturn(Optional.of(firstRegion));
    when(partitionManager.findTConsensusGroupIdByRegionId(14))
        .thenReturn(Optional.of(secondRegion));
    when(partitionManager.getRegionDatabase(any(TConsensusGroupId.class)))
        .thenReturn(model == Model.TREE ? "root.sg" : "db");

    TDataNodeLocation replica =
        operation == Operation.MIGRATE || operation == Operation.EXTEND ? source : target;
    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets = new HashMap<>();
    replicaSets.put(
        firstRegion, new TRegionReplicaSet(firstRegion, Arrays.asList(replica, coordinator)));
    replicaSets.put(
        secondRegion, new TRegionReplicaSet(secondRegion, Arrays.asList(replica, coordinator)));
    when(partitionManager.getAllReplicaSetsMap(firstRegion.getType())).thenReturn(replicaSets);
    when(partitionManager.getAllReplicaSets(replica.getDataNodeId()))
        .thenReturn(Arrays.asList(replicaSets.get(firstRegion), replicaSets.get(secondRegion)));

    when(env.getSubmitRegionMigrateLock()).thenReturn(submissionLock);
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

  @After
  public void tearDown() {
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(originalDataConsensus);
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setSchemaRegionConsensusProtocolClass(originalSchemaConsensus);
    assertFalse(submissionLock.isLocked());
  }

  private TSStatus execute(List<Integer> regionIds, int fromId, int toId) {
    switch (operation) {
      case MIGRATE:
        return manager.migrateRegion(new TMigrateRegionReq(regionIds, fromId, toId, model));
      case RECONSTRUCT:
        return manager.reconstructRegion(new TReconstructRegionReq(regionIds, toId, model));
      case EXTEND:
        return manager.extendRegions(new TExtendRegionReq(regionIds, toId, model));
      case REMOVE:
        return manager.removeRegions(new TRemoveRegionReq(regionIds, toId, model));
      default:
        throw new AssertionError(operation);
    }
  }

  private TSStatus execute(List<Integer> regionIds) {
    return execute(regionIds, source.getDataNodeId(), target.getDataNodeId());
  }

  private void assertRejected(TSStatus status) {
    assertEquals(operation.errorCode.getStatusCode(), status.getCode());
    assertNotNull(status.getMessage());
    assertFalse(status.getMessage().isEmpty());
    verify(executor, never()).submitProcedure(any());
    verify(handler, never()).removeRegionLocation(any(), any());
  }

  @Test
  public void testDuplicateRegionIdRejectsWholeRequest() {
    TSStatus status = execute(Arrays.asList(12, 14, 12));
    assertRejected(status);
    assertEquals(
        String.format(ManagerMessages.MESSAGE_DUPLICATE_REGION_ID_ARG_IN_THE_REQUEST_B6FFCCFC, 12),
        status.getMessage());
  }

  @Test
  public void testMissingLastRegionRejectsWholeRequest() {
    TSStatus status = execute(Arrays.asList(12, 14, 99));
    assertRejected(status);
    assertEquals(
        String.format(ManagerMessages.MESSAGE_REGION_ARG_DOES_NOT_EXIST_3C8400C9, 99),
        status.getMessage());
  }

  @Test
  public void testMissingFirstRegionRejectsWholeRequest() {
    assertRejected(execute(Arrays.asList(99, 12, 14)));
  }

  @Test
  public void testAllMissingRegionsRejectWholeRequest() {
    assertRejected(execute(Arrays.asList(98, 99)));
  }

  @Test
  public void testEmptyRegionIdsRejectWholeRequest() {
    TSStatus status = execute(Collections.emptyList());
    assertRejected(status);
    assertEquals(
        ManagerMessages.MESSAGE_REGION_IDS_MUST_NOT_BE_EMPTY_B42DAAFD, status.getMessage());
  }

  @Test
  public void testMissingTargetDataNodeRejectsWholeRequest() {
    TSStatus status = execute(Arrays.asList(12, 14), source.getDataNodeId(), 99);
    assertRejected(status);
    assertEquals(
        String.format(
            ManagerMessages.MESSAGE_TARGET_DATANODE_ARG_DOES_NOT_EXIST_IN_THE_CLUSTER_679D59AF, 99),
        status.getMessage());
  }

  @Test
  public void testNullTargetDataNodeConfigurationRejectsWholeRequest() {
    when(nodeManager.getRegisteredDataNode(99)).thenReturn(null);
    assertRejected(execute(Arrays.asList(12, 14), source.getDataNodeId(), 99));
  }

  @Test
  public void testMissingMigrationSourceRejectsWholeRequest() {
    assumeTrue(operation == Operation.MIGRATE);
    TSStatus status = execute(Arrays.asList(12, 14), 99, target.getDataNodeId());
    assertRejected(status);
    assertEquals(
        String.format(
            ManagerMessages.MESSAGE_SOURCE_DATANODE_ARG_DOES_NOT_EXIST_IN_THE_CLUSTER_2255633C, 99),
        status.getMessage());
  }

  @Test
  public void testIdenticalMigrationDataNodeIdsRejectWholeRequest() {
    assumeTrue(operation == Operation.MIGRATE);
    TSStatus status =
        execute(Arrays.asList(12, 14), target.getDataNodeId(), target.getDataNodeId());
    assertRejected(status);
    assertEquals(
        String.format(
            ManagerMessages.MESSAGE_SOURCE_AND_TARGET_DATANODE_IDS_MUST_BE_DIFFERENT_ARG_286D3838,
            target.getDataNodeId()),
        status.getMessage());
  }

  @Test
  public void testConflictOnLastRegionRejectsWholeRequest() {
    procedures.put(1L, new ReconstructRegionProcedure(secondRegion, target, coordinator));
    assertRejected(execute(Arrays.asList(12, 14)));
  }

  @Test
  public void testInvalidReplicaPlacementOnLastRegionRejectsWholeRequest() {
    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets =
        partitionManager.getAllReplicaSetsMap(firstRegion.getType());
    if (operation == Operation.EXTEND) {
      when(partitionManager.getAllReplicaSets(target.getDataNodeId()))
          .thenReturn(Collections.singletonList(replicaSets.get(secondRegion)));
    } else {
      int dataNodeId =
          operation == Operation.MIGRATE ? source.getDataNodeId() : target.getDataNodeId();
      when(partitionManager.getAllReplicaSets(dataNodeId))
          .thenReturn(Collections.singletonList(replicaSets.get(firstRegion)));
    }
    assertRejected(execute(Arrays.asList(12, 14)));
  }

  @Test
  public void testMissingCoordinatorOnLastRegionRejectsWholeRequest() {
    when(handler.filterDataNodeWithOtherRegionReplica(
            eq(secondRegion),
            eq(target),
            eq(NodeStatus.Running),
            eq(NodeStatus.Removing),
            eq(NodeStatus.ReadOnly)))
        .thenReturn(Optional.empty());
    assertRejected(execute(Arrays.asList(12, 14)));
  }

  @Test
  public void testValidRegionsAreSubmittedInInputOrderAfterAllChecks() {
    doAnswer(
            invocation -> {
              assertTrue(submissionLock.isHeldByCurrentThread());
              verify(partitionManager).findTConsensusGroupIdByRegionId(14);
              verify(partitionManager).findTConsensusGroupIdByRegionId(12);
              verify(partitionManager).getRegionDatabase(secondRegion);
              verify(partitionManager).getRegionDatabase(firstRegion);
              return 1L;
            })
        .when(executor)
        .submitProcedure(any());

    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), execute(Arrays.asList(14, 12)).getCode());
    ArgumentCaptor<Procedure> captor = ArgumentCaptor.forClass(Procedure.class);
    verify(executor, times(2)).submitProcedure(captor.capture());
    assertEquals(operation.procedureClass, captor.getAllValues().get(0).getClass());
    assertEquals(operation.procedureClass, captor.getAllValues().get(1).getClass());
    assertEquals(
        secondRegion, ((RegionOperationProcedure<?>) captor.getAllValues().get(0)).getRegionId());
    assertEquals(
        firstRegion, ((RegionOperationProcedure<?>) captor.getAllValues().get(1)).getRegionId());
  }

  @Test
  public void testRemoveRegionAllowsReadOnlyTargetDataNode() {
    assumeTrue(operation == Operation.REMOVE);
    when(nodeManager.filterDataNodeThroughStatus(NodeStatus.Running))
        .thenReturn(
            Collections.singletonList(new TDataNodeConfiguration().setLocation(coordinator)));
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        execute(Collections.singletonList(12)).getCode());
    verify(executor).submitProcedure(any());
  }
}
