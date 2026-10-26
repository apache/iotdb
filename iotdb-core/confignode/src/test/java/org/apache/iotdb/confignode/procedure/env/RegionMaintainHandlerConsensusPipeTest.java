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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.task.PipeTaskCoordinator;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS_V2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RegionMaintainHandlerConsensusPipeTest {

  private ConfigManager configManager;
  private PartitionManager partitionManager;
  private PipeManager pipeManager;
  private PipeTaskCoordinator pipeTaskCoordinator;
  private ProcedureManager procedureManager;
  private LoadManager loadManager;
  private TestRegionMaintainHandler handler;

  private String originalConsensusProtocol;

  @Before
  public void setUp() {
    originalConsensusProtocol =
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass();
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(IOT_CONSENSUS_V2);

    configManager = mock(ConfigManager.class);
    partitionManager = mock(PartitionManager.class);
    pipeManager = mock(PipeManager.class);
    pipeTaskCoordinator = mock(PipeTaskCoordinator.class);
    procedureManager = mock(ProcedureManager.class);
    loadManager = mock(LoadManager.class);

    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(configManager.getPipeManager()).thenReturn(pipeManager);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(pipeManager.getPipeTaskCoordinator()).thenReturn(pipeTaskCoordinator);

    handler = new TestRegionMaintainHandler(configManager);
  }

  @After
  public void tearDown() {
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(originalConsensusProtocol);
  }

  private TDataNodeLocation makeLocation(int nodeId, String ip, int consensusPort) {
    TDataNodeLocation location = new TDataNodeLocation();
    location.setDataNodeId(nodeId);
    location.setClientRpcEndPoint(new TEndPoint(ip, 6667));
    location.setInternalEndPoint(new TEndPoint(ip, 10730));
    location.setMPPDataExchangeEndPoint(new TEndPoint(ip, 10740));
    location.setDataRegionConsensusEndPoint(new TEndPoint(ip, consensusPort));
    location.setSchemaRegionConsensusEndPoint(new TEndPoint(ip, 10760));
    return location;
  }

  private TRegionReplicaSet makeReplicaSet(int regionId, TDataNodeLocation... locations) {
    TRegionReplicaSet replicaSet = new TRegionReplicaSet();
    replicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId));
    replicaSet.setDataNodeLocations(Arrays.asList(locations));
    return replicaSet;
  }

  @Test
  public void testNoOpWhenNotIoTConsensusV2() {
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");

    handler.checkAndRepairConsensusPipes();

    verify(partitionManager, never()).getAllReplicaSetsMap(any());
  }

  @Test
  public void testNothingToDoWhenAllPipesMatch() {
    TDataNodeLocation loc1 = makeLocation(1, "127.0.0.1", 40010);
    TDataNodeLocation loc2 = makeLocation(2, "127.0.0.2", 40010);
    TRegionReplicaSet replicaSet = makeReplicaSet(100, loc1, loc2);

    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 100);
    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets = new HashMap<>();
    replicaSets.put(groupId, replicaSet);
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(replicaSets);

    DataRegionId regionId = new DataRegionId(100);
    String pipe1to2 = new ConsensusPipeName(regionId, 1, 2).toString();
    String pipe2to1 = new ConsensusPipeName(regionId, 2, 1).toString();
    Map<String, PipeStatus> actualPipes = new HashMap<>();
    actualPipes.put(pipe1to2, PipeStatus.RUNNING);
    actualPipes.put(pipe2to1, PipeStatus.RUNNING);
    when(pipeTaskCoordinator.getConsensusPipeStatusMap()).thenReturn(actualPipes);

    handler.checkAndRepairConsensusPipes();

    verify(procedureManager, never()).createConsensusPipeAsync(any());
    verify(procedureManager, never()).dropConsensusPipeAsync(any());
    verify(procedureManager, never()).startConsensusPipe(any());
  }

  @Test
  public void testCreatesMissingPipes() {
    TDataNodeLocation loc1 = makeLocation(1, "127.0.0.1", 40010);
    TDataNodeLocation loc2 = makeLocation(2, "127.0.0.2", 40010);
    TRegionReplicaSet replicaSet = makeReplicaSet(100, loc1, loc2);

    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 100);
    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets = new HashMap<>();
    replicaSets.put(groupId, replicaSet);
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(replicaSets);

    when(pipeTaskCoordinator.getConsensusPipeStatusMap()).thenReturn(Collections.emptyMap());

    handler.checkAndRepairConsensusPipes();

    verify(procedureManager, times(2)).createConsensusPipeAsync(any(TCreatePipeReq.class));
    verify(procedureManager, never()).dropConsensusPipeAsync(any());
    verify(procedureManager, never()).startConsensusPipe(any());
  }

  @Test
  public void testDropsUnexpectedPipes() {
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.emptyMap());

    DataRegionId regionId = new DataRegionId(999);
    String unexpectedPipe = new ConsensusPipeName(regionId, 1, 2).toString();
    Map<String, PipeStatus> actualPipes = new HashMap<>();
    actualPipes.put(unexpectedPipe, PipeStatus.RUNNING);
    when(pipeTaskCoordinator.getConsensusPipeStatusMap()).thenReturn(actualPipes);

    handler.checkAndRepairConsensusPipes();

    verify(procedureManager, never()).createConsensusPipeAsync(any());
    verify(procedureManager, times(1)).dropConsensusPipeAsync(unexpectedPipe);
    verify(procedureManager, never()).startConsensusPipe(any());
  }

  @Test
  public void testRestartsStoppedPipes() {
    TDataNodeLocation loc1 = makeLocation(1, "127.0.0.1", 40010);
    TDataNodeLocation loc2 = makeLocation(2, "127.0.0.2", 40010);
    TRegionReplicaSet replicaSet = makeReplicaSet(100, loc1, loc2);

    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 100);
    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets = new HashMap<>();
    replicaSets.put(groupId, replicaSet);
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(replicaSets);

    DataRegionId regionId = new DataRegionId(100);
    String pipe1to2 = new ConsensusPipeName(regionId, 1, 2).toString();
    String pipe2to1 = new ConsensusPipeName(regionId, 2, 1).toString();
    Map<String, PipeStatus> actualPipes = new HashMap<>();
    actualPipes.put(pipe1to2, PipeStatus.RUNNING);
    actualPipes.put(pipe2to1, PipeStatus.STOPPED);
    when(pipeTaskCoordinator.getConsensusPipeStatusMap()).thenReturn(actualPipes);

    handler.checkAndRepairConsensusPipes();

    verify(procedureManager, never()).createConsensusPipeAsync(any());
    verify(procedureManager, never()).dropConsensusPipeAsync(any());
    verify(procedureManager, times(1)).startConsensusPipe(pipe2to1);
  }

  @Test
  public void testMixedScenario() {
    TDataNodeLocation loc1 = makeLocation(1, "127.0.0.1", 40010);
    TDataNodeLocation loc2 = makeLocation(2, "127.0.0.2", 40010);
    TDataNodeLocation loc3 = makeLocation(3, "127.0.0.3", 40010);
    TRegionReplicaSet replicaSet = makeReplicaSet(100, loc1, loc2, loc3);

    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 100);
    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets = new HashMap<>();
    replicaSets.put(groupId, replicaSet);
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(replicaSets);

    DataRegionId regionId100 = new DataRegionId(100);
    DataRegionId regionId999 = new DataRegionId(999);

    // 3 nodes => 6 expected pipes: 1->2, 1->3, 2->1, 2->3, 3->1, 3->2
    // Provide only 3 of the 6, one stopped; plus one unexpected pipe
    String pipe1to2 = new ConsensusPipeName(regionId100, 1, 2).toString();
    String pipe2to1 = new ConsensusPipeName(regionId100, 2, 1).toString();
    String pipe3to1 = new ConsensusPipeName(regionId100, 3, 1).toString();
    String unexpected = new ConsensusPipeName(regionId999, 5, 6).toString();

    Map<String, PipeStatus> actualPipes = new HashMap<>();
    actualPipes.put(pipe1to2, PipeStatus.RUNNING);
    actualPipes.put(pipe2to1, PipeStatus.STOPPED);
    actualPipes.put(pipe3to1, PipeStatus.RUNNING);
    actualPipes.put(unexpected, PipeStatus.RUNNING);
    when(pipeTaskCoordinator.getConsensusPipeStatusMap()).thenReturn(actualPipes);

    handler.checkAndRepairConsensusPipes();

    // Missing: 1->3, 2->3, 3->2 => 3 creates
    verify(procedureManager, times(3)).createConsensusPipeAsync(any(TCreatePipeReq.class));
    // Unexpected pipe => 1 drop
    verify(procedureManager, times(1)).dropConsensusPipeAsync(unexpected);
    // Stopped pipe 2->1 => 1 restart
    verify(procedureManager, times(1)).startConsensusPipe(pipe2to1);
  }

  @Test
  public void testDeleteOldRegionPeerUsesSingleAttemptWhenNodeUnknown() {
    TDataNodeLocation loc1 = makeLocation(1, "127.0.0.1", 40010);
    when(loadManager.getNodeStatus(loc1.getDataNodeId())).thenReturn(NodeStatus.Unknown);

    handler.submitDeleteOldRegionPeerTask(
        1L, loc1, new TConsensusGroupId(TConsensusGroupType.DataRegion, 100));

    verify(loadManager, times(1)).getNodeStatus(loc1.getDataNodeId());
    assertFalse(handler.lastUseFullRetry);
    assertEquals(CnToDnSyncRequestType.DELETE_OLD_REGION_PEER, handler.lastRequestType);
    assertEquals(loc1.getInternalEndPoint(), handler.lastEndPoint);
  }

  @Test
  public void testDeleteOldRegionPeerKeepsFullRetryWhenNodeRunning() {
    TDataNodeLocation loc1 = makeLocation(1, "127.0.0.1", 40010);
    when(loadManager.getNodeStatus(loc1.getDataNodeId())).thenReturn(NodeStatus.Running);

    handler.submitDeleteOldRegionPeerTask(
        1L, loc1, new TConsensusGroupId(TConsensusGroupType.DataRegion, 100));

    verify(loadManager, times(1)).getNodeStatus(loc1.getDataNodeId());
    assertTrue(handler.lastUseFullRetry);
    assertEquals(CnToDnSyncRequestType.DELETE_OLD_REGION_PEER, handler.lastRequestType);
    assertEquals(loc1.getInternalEndPoint(), handler.lastEndPoint);
  }

  @Test
  public void testThreeNodeReplicaSetCreatesAllSixPipes() {
    TDataNodeLocation loc1 = makeLocation(1, "127.0.0.1", 40010);
    TDataNodeLocation loc2 = makeLocation(2, "127.0.0.2", 40010);
    TDataNodeLocation loc3 = makeLocation(3, "127.0.0.3", 40010);
    TRegionReplicaSet replicaSet = makeReplicaSet(100, loc1, loc2, loc3);

    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 100);
    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets = new HashMap<>();
    replicaSets.put(groupId, replicaSet);
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(replicaSets);

    when(pipeTaskCoordinator.getConsensusPipeStatusMap()).thenReturn(Collections.emptyMap());

    handler.checkAndRepairConsensusPipes();

    // 3 nodes => 3*2 = 6 pipes to create
    verify(procedureManager, times(6)).createConsensusPipeAsync(any(TCreatePipeReq.class));
  }

  @Test
  public void testEmptyReplicaSetsAndEmptyPipes() {
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.emptyMap());
    when(pipeTaskCoordinator.getConsensusPipeStatusMap()).thenReturn(Collections.emptyMap());

    handler.checkAndRepairConsensusPipes();

    verify(procedureManager, never()).createConsensusPipeAsync(any());
    verify(procedureManager, never()).dropConsensusPipeAsync(any());
    verify(procedureManager, never()).startConsensusPipe(any());
  }

  private static class TestRegionMaintainHandler extends RegionMaintainHandler {
    private boolean lastUseFullRetry;
    private TEndPoint lastEndPoint;
    private CnToDnSyncRequestType lastRequestType;

    private TestRegionMaintainHandler(ConfigManager configManager) {
      super(configManager);
    }

    @Override
    protected TSStatus submitDataNodeSyncRequest(
        TEndPoint endPoint,
        Object request,
        CnToDnSyncRequestType requestType,
        boolean useFullRetry) {
      lastEndPoint = endPoint;
      lastRequestType = requestType;
      lastUseFullRetry = useFullRetry;
      return new TSStatus(200);
    }
  }
}
