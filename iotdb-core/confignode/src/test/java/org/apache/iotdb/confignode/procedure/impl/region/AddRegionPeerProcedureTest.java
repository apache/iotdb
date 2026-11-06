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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure.Flow;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AddRegionPeerProcedureTest {
  private final TConsensusGroupId regionId =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 10);
  private final TDataNodeLocation coordinator = location(1);
  private final TDataNodeLocation target = location(2);
  private RegionMaintainHandler handler;
  private ConfigNodeProcedureEnv env;

  @Before
  public void setUp() throws Exception {
    ConfigManager configManager = mock(ConfigManager.class);
    PartitionManager partitionManager = mock(PartitionManager.class);
    handler = mock(RegionMaintainHandler.class);
    env = mock(ConfigNodeProcedureEnv.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(env.getRegionMaintainHandler()).thenReturn(handler);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(partitionManager.getAllReplicaSets())
        .thenReturn(
            Collections.singletonList(
                new TRegionReplicaSet(
                    regionId, new ArrayList<>(Collections.singletonList(coordinator)))));
    when(handler.createNewRegionPeer(regionId, target))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    when(handler.removeRegionLocation(regionId, target))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  @Test
  public void serDeTest() throws Exception {
    AddRegionPeerProcedure procedure =
        new AddRegionPeerProcedure(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 10),
            new TDataNodeLocation(
                1,
                new TEndPoint("127.0.0.1", 0),
                new TEndPoint("127.0.0.1", 1),
                new TEndPoint("127.0.0.1", 2),
                new TEndPoint("127.0.0.1", 3),
                new TEndPoint("127.0.0.1", 4)),
            new TDataNodeLocation(
                5,
                new TEndPoint("127.0.0.1", 5),
                new TEndPoint("127.0.0.1", 6),
                new TEndPoint("127.0.0.1", 7),
                new TEndPoint("127.0.0.1", 8),
                new TEndPoint("127.0.0.1", 9)));
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure, ProcedureFactory.getInstance().create(buffer));
    }
  }

  @Test
  public void rollbackRetriesUnavailablePeerAfterDelay() throws Exception {
    when(handler.resetPeerList(eq(regionId), anyList(), anyMap()))
        .thenReturn(
            responses(TSStatusCode.CAN_NOT_CONNECT_DATANODE),
            responses(TSStatusCode.SUCCESS_STATUS));

    long start = System.nanoTime();
    executeFailedCreate();
    Assert.assertTrue(System.nanoTime() - start >= TimeUnit.SECONDS.toNanos(5));
    verify(handler, times(2)).resetPeerList(eq(regionId), anyList(), anyMap());
  }

  @Test
  public void rollbackStopsAfterFourFailedRpcBatches() throws Exception {
    when(handler.resetPeerList(eq(regionId), anyList(), anyMap()))
        .thenReturn(responses(TSStatusCode.CAN_NOT_CONNECT_DATANODE));

    long start = System.nanoTime();
    executeFailedCreate();
    Assert.assertTrue(System.nanoTime() - start >= TimeUnit.SECONDS.toNanos(15));
    verify(handler, times(4)).resetPeerList(eq(regionId), anyList(), anyMap());
  }

  @Test
  public void rollbackDoesNotRetrySuccessfulRpcBatch() throws Exception {
    when(handler.resetPeerList(eq(regionId), anyList(), anyMap()))
        .thenReturn(responses(TSStatusCode.SUCCESS_STATUS));

    executeFailedCreate();
    verify(handler, times(1)).resetPeerList(eq(regionId), anyList(), anyMap());
  }

  @Test
  public void interruptedRollbackWaitDoesNotRetry() throws Exception {
    when(handler.resetPeerList(eq(regionId), anyList(), anyMap()))
        .thenReturn(responses(TSStatusCode.CAN_NOT_CONNECT_DATANODE));

    try {
      Thread.currentThread().interrupt();
      executeFailedCreate();
      Assert.assertTrue(Thread.currentThread().isInterrupted());
      verify(handler, times(1)).resetPeerList(eq(regionId), anyList(), anyMap());
    } finally {
      Thread.interrupted();
    }
  }

  private void executeFailedCreate() throws Exception {
    AddRegionPeerProcedure procedure = new AddRegionPeerProcedure(regionId, coordinator, target);
    Assert.assertEquals(
        Flow.NO_MORE_STATE,
        procedure.executeFromState(env, AddRegionPeerState.CREATE_NEW_REGION_PEER));
    Assert.assertFalse(procedure.isFailed());
    verify(handler, times(1)).createNewRegionPeer(regionId, target);
    verify(handler, times(1)).removeRegionLocation(regionId, target);
  }

  private Map<Integer, TSStatus> responses(TSStatusCode targetStatus) {
    Map<Integer, TSStatus> responses = new HashMap<>();
    responses.put(
        coordinator.getDataNodeId(), new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    responses.put(target.getDataNodeId(), new TSStatus(targetStatus.getStatusCode()));
    return responses;
  }

  private static TDataNodeLocation location(int id) {
    return new TDataNodeLocation()
        .setDataNodeId(id)
        .setInternalEndPoint(new TEndPoint("127.0.0.1", 10730 + id));
  }
}
