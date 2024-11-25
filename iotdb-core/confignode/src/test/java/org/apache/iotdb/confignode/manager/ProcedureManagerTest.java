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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeHandler;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodesProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.service.RegionMigrateService.isFailed;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ProcedureManagerTest {

  private static ProcedureManager PROCEDURE_MANAGER;

  private static RemoveDataNodeHandler REMOVE_DATA_NODE_HANDLER;

  private static LoadManager LOAD_MANAGER;

  private static final ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> procedureMap =
      new ConcurrentHashMap<>();

  private final TConsensusGroupId consensusGroupId =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  private final TDataNodeLocation removeDataNodeLocationA =
      new TDataNodeLocation(
          10,
          new TEndPoint("127.0.0.1", 6667),
          new TEndPoint("127.0.0.1", 6668),
          new TEndPoint("127.0.0.1", 6669),
          new TEndPoint("127.0.0.1", 6670),
          new TEndPoint("127.0.0.1", 6671));

  private final TDataNodeLocation removeDataNodeLocationB =
      new TDataNodeLocation(
          11,
          new TEndPoint("127.0.0.1", 6677),
          new TEndPoint("127.0.0.1", 6678),
          new TEndPoint("127.0.0.1", 6679),
          new TEndPoint("127.0.0.1", 6680),
          new TEndPoint("127.0.0.1", 6681));

  private final TDataNodeLocation toDataNodeLocation =
      new TDataNodeLocation(
          12,
          new TEndPoint("127.0.0.1", 6687),
          new TEndPoint("127.0.0.1", 6688),
          new TEndPoint("127.0.0.1", 6689),
          new TEndPoint("127.0.0.1", 6690),
          new TEndPoint("127.0.0.1", 6691));

  private final TDataNodeLocation coordinatorDataNodeLocation =
      new TDataNodeLocation(
          13,
          new TEndPoint("127.0.0.1", 6697),
          new TEndPoint("127.0.0.1", 6698),
          new TEndPoint("127.0.0.1", 6699),
          new TEndPoint("127.0.0.1", 6700),
          new TEndPoint("127.0.0.1", 6701));

  private final List<TDataNodeLocation> removedDataNodes =
      new ArrayList<>(Arrays.asList(removeDataNodeLocationA, removeDataNodeLocationB));

  @BeforeClass
  public static void setUp() throws IOException {
    IManager CONFIG_MANAGER = new ConfigManager();
    ProcedureManager procedureManager = CONFIG_MANAGER.getProcedureManager();
    PROCEDURE_MANAGER = spy(procedureManager);

    ProcedureExecutor<ConfigNodeProcedureEnv> procedureExecutor = PROCEDURE_MANAGER.getExecutor();
    ProcedureExecutor<ConfigNodeProcedureEnv> PROCEDURE_EXECUTOR = spy(procedureExecutor);

    ConfigNodeProcedureEnv env = PROCEDURE_MANAGER.getEnv();
    ConfigNodeProcedureEnv ENV = spy(env);

    RemoveDataNodeHandler removeDataNodeHandler = ENV.getRemoveDataNodeHandler();
    REMOVE_DATA_NODE_HANDLER = spy(removeDataNodeHandler);

    LoadManager loadManager = CONFIG_MANAGER.getLoadManager();
    LOAD_MANAGER = spy(loadManager);

    when(PROCEDURE_MANAGER.getExecutor()).thenReturn(PROCEDURE_EXECUTOR);
    when(PROCEDURE_EXECUTOR.getProcedures()).thenReturn(procedureMap);
    when(PROCEDURE_MANAGER.getEnv()).thenReturn(ENV);
    when(ENV.getRemoveDataNodeHandler()).thenReturn(REMOVE_DATA_NODE_HANDLER);
  }

  @Test
  public void testCheckRemoveDataNodeWithAnotherRemoveProcedure() {
    Map<Integer, NodeStatus> nodeStatusMap = new HashMap<>();
    nodeStatusMap.put(10, NodeStatus.Running);
    RemoveDataNodesProcedure anotherRemoveProcedure =
        new RemoveDataNodesProcedure(removedDataNodes, nodeStatusMap);
    procedureMap.put(0L, anotherRemoveProcedure);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
  }

  @Test
  public void testCheckRemoveDataNodeWithConflictRegionMigrateProcedure() {
    RegionMigrateProcedure regionMigrateProcedure =
        new RegionMigrateProcedure(
            consensusGroupId,
            removeDataNodeLocationA,
            removeDataNodeLocationB,
            coordinatorDataNodeLocation,
            coordinatorDataNodeLocation);
    procedureMap.put(0L, regionMigrateProcedure);

    Set<TConsensusGroupId> set = new HashSet<>();
    set.add(consensusGroupId);
    when(REMOVE_DATA_NODE_HANDLER.getRemovedDataNodesRegionSet(removedDataNodes)).thenReturn(set);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
  }

  @Test
  public void testCheckRemoveDataNodeWithRegionMigrateProcedureConflictsWithEachOther() {
    RegionMigrationPlan regionMigrationPlanA =
        new RegionMigrationPlan(consensusGroupId, removeDataNodeLocationA);
    regionMigrationPlanA.setToDataNode(toDataNodeLocation);
    RegionMigrationPlan regionMigrationPlanB =
        new RegionMigrationPlan(consensusGroupId, removeDataNodeLocationB);
    regionMigrationPlanB.setToDataNode(toDataNodeLocation);

    List<RegionMigrationPlan> regionMigrationPlans =
        new ArrayList<>(Arrays.asList(regionMigrationPlanA, regionMigrationPlanB));
    when(REMOVE_DATA_NODE_HANDLER.getRegionMigrationPlans(removedDataNodes))
        .thenReturn(regionMigrationPlans);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
  }

  @Test
  public void testCheckRemoveDataNodeWithAnotherUnknownDataNode() {
    Set<TDataNodeLocation> relatedDataNodes = new HashSet<>();
    relatedDataNodes.add(removeDataNodeLocationA);
    relatedDataNodes.add(coordinatorDataNodeLocation);

    when(REMOVE_DATA_NODE_HANDLER.getRelatedDataNodeLocations(removeDataNodeLocationA))
        .thenReturn(relatedDataNodes);

    when(LOAD_MANAGER.getNodeStatus(removeDataNodeLocationA.getDataNodeId()))
        .thenReturn(NodeStatus.Running);
    when(LOAD_MANAGER.getNodeStatus(coordinatorDataNodeLocation.getDataNodeId()))
        .thenReturn(NodeStatus.Unknown);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
  }
}
