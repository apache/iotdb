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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
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
import org.apache.iotdb.confignode.procedure.impl.region.AddRegionPeerProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.ReconstructRegionProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TExtendRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TReconstructRegionReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProcedureManagerRegionOperationTest {

  private static final int REGION_ID = 1;
  private static final int CONFIG_NODE_ID = 0;
  private static final int TARGET_DATA_NODE_ID = 2;
  private static final int COORDINATOR_DATA_NODE_ID = 3;
  private static final int UNKNOWN_DATA_NODE_ID = 9999;

  private ConfigManager configManager;
  private NodeManager nodeManager;
  private PartitionManager partitionManager;
  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
  private ConfigNodeProcedureEnv env;
  private RegionMaintainHandler regionMaintainHandler;
  private ProcedureManager procedureManager;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    configManager = mock(ConfigManager.class);
    nodeManager = mock(NodeManager.class);
    partitionManager = mock(PartitionManager.class);
    executor = mock(ProcedureExecutor.class);
    env = mock(ConfigNodeProcedureEnv.class);
    regionMaintainHandler = mock(RegionMaintainHandler.class);

    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(executor.getProcedures()).thenReturn(new ConcurrentHashMap<>());
    when(env.getRegionMaintainHandler()).thenReturn(regionMaintainHandler);

    procedureManager = new ProcedureManager(configManager, mock(ProcedureInfo.class));
    procedureManager.setExecutor(executor);
    procedureManager.setEnv(env);
  }

  @Test
  public void reconstructRegionRejectsUnknownDataNodeId() {
    assertReconstructRejected(UNKNOWN_DATA_NODE_ID);
  }

  @Test
  public void reconstructRegionRejectsConfigNodeId() {
    assertReconstructRejected(CONFIG_NODE_ID);
  }

  @Test
  public void reconstructRegionSubmitsProcedureForRegisteredDataNode() {
    configureRegisteredTarget(true);

    TSStatus status =
        procedureManager.reconstructRegion(
            new TReconstructRegionReq(Collections.singletonList(REGION_ID), TARGET_DATA_NODE_ID));

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(executor, times(1)).submitProcedure(any(ReconstructRegionProcedure.class));
  }

  @Test
  public void extendRegionRejectsUnknownDataNodeId() {
    assertExtendRejected(UNKNOWN_DATA_NODE_ID);
  }

  @Test
  public void extendRegionRejectsConfigNodeId() {
    assertExtendRejected(CONFIG_NODE_ID);
  }

  @Test
  public void extendRegionSubmitsProcedureForRegisteredDataNode() {
    configureRegisteredTarget(false);

    TSStatus status =
        procedureManager.extendRegions(
            new TExtendRegionReq(Collections.singletonList(REGION_ID), TARGET_DATA_NODE_ID));

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(executor, times(1)).submitProcedure(any(AddRegionPeerProcedure.class));
  }

  private void assertReconstructRejected(int dataNodeId) {
    when(nodeManager.getRegisteredDataNode(dataNodeId)).thenReturn(new TDataNodeConfiguration());

    TSStatus status =
        procedureManager.reconstructRegion(
            new TReconstructRegionReq(Collections.singletonList(REGION_ID), dataNodeId));

    assertRejected(status, dataNodeId, TSStatusCode.RECONSTRUCT_REGION_ERROR);
  }

  private void assertExtendRejected(int dataNodeId) {
    when(nodeManager.getRegisteredDataNode(dataNodeId)).thenReturn(new TDataNodeConfiguration());

    TSStatus status =
        procedureManager.extendRegions(
            new TExtendRegionReq(Collections.singletonList(REGION_ID), dataNodeId));

    assertRejected(status, dataNodeId, TSStatusCode.EXTEND_REGION_ERROR);
  }

  private void assertRejected(TSStatus status, int dataNodeId, TSStatusCode expectedCode) {
    Assert.assertEquals(expectedCode.getStatusCode(), status.getCode());
    Assert.assertEquals(
        String.format("Target DataNode %s does not exist in the cluster", dataNodeId),
        status.getMessage());
    verify(partitionManager, never()).generateTConsensusGroupIdByRegionId(anyInt());
    verify(executor, never()).submitProcedure(any(Procedure.class));
  }

  private void configureRegisteredTarget(boolean reconstruct) {
    TConsensusGroupId consensusGroupId =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, REGION_ID);
    TDataNodeLocation targetDataNode = dataNodeLocation(TARGET_DATA_NODE_ID, 7000);
    TDataNodeLocation coordinatorDataNode = dataNodeLocation(COORDINATOR_DATA_NODE_ID, 7100);
    TDataNodeConfiguration targetDataNodeConfiguration =
        new TDataNodeConfiguration().setLocation(targetDataNode);
    TDataNodeConfiguration coordinatorDataNodeConfiguration =
        new TDataNodeConfiguration().setLocation(coordinatorDataNode);
    TRegionReplicaSet replicaSet =
        new TRegionReplicaSet(
            consensusGroupId,
            reconstruct
                ? Arrays.asList(targetDataNode, coordinatorDataNode)
                : Collections.singletonList(coordinatorDataNode));

    when(nodeManager.getRegisteredDataNode(TARGET_DATA_NODE_ID))
        .thenReturn(targetDataNodeConfiguration);
    when(nodeManager.filterDataNodeThroughStatus(NodeStatus.Running))
        .thenReturn(Arrays.asList(targetDataNodeConfiguration, coordinatorDataNodeConfiguration));
    when(partitionManager.generateTConsensusGroupIdByRegionId(REGION_ID))
        .thenReturn(Optional.of(consensusGroupId));
    when(partitionManager.getAllReplicaSets(TARGET_DATA_NODE_ID))
        .thenReturn(reconstruct ? Collections.singletonList(replicaSet) : Collections.emptyList());
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.singletonMap(consensusGroupId, replicaSet));
    when(regionMaintainHandler.filterDataNodeWithOtherRegionReplica(
            consensusGroupId,
            targetDataNode,
            NodeStatus.Running,
            NodeStatus.Removing,
            NodeStatus.ReadOnly))
        .thenReturn(Optional.of(coordinatorDataNode));
    when(env.getSubmitRegionMigrateLock()).thenReturn(new ReentrantLock());
    when(executor.submitProcedure(any(Procedure.class))).thenReturn(1L);
  }

  private TDataNodeLocation dataNodeLocation(int dataNodeId, int basePort) {
    return new TDataNodeLocation(
        dataNodeId,
        new TEndPoint("127.0.0.1", basePort),
        new TEndPoint("127.0.0.1", basePort + 1),
        new TEndPoint("127.0.0.1", basePort + 2),
        new TEndPoint("127.0.0.1", basePort + 3),
        new TEndPoint("127.0.0.1", basePort + 4));
  }
}
