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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatch;
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatchIterator;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadTsFileSchedulerTest {

  @Mock DistributedQueryPlan distributedQueryPlan;
  @Mock SubPlan subPlan;
  @Mock PlanFragment planFragment;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    when(distributedQueryPlan.getRootSubPlan()).thenReturn(subPlan);
    when(subPlan.getPlanFragment()).thenReturn(planFragment);
    when(planFragment.getId()).thenReturn(new PlanFragmentId("test", 0));
  }

  @After
  public void tearDown() {
    if (Whitebox.getInternalState(LoadTsFileMemoryManager.getInstance(), "dataCacheMemoryBlock")
        != null) {
      LoadTsFileMemoryManager.getInstance().releaseDataCacheMemoryBlock();
    }
  }

  @Test
  public void testSchedulerMetadataAccessors() {
    LoadTsFileScheduler t =
        spy(
            new LoadTsFileScheduler(
                distributedQueryPlan,
                mock(MPPQueryContext.class),
                mock(QueryStateMachine.class),
                mock(IClientManager.class),
                mock(IPartitionFetcher.class),
                false));
    t.start();
    Assert.assertNull(t.getTotalCpuTime());
    Assert.assertNull(t.getFragmentInfo());
  }

  @Test
  public void testDispatchObjectFileBatchesReturnsFalseWhenObjectPieceDispatchFails()
      throws Exception {
    final LoadTsFileScheduler scheduler = createScheduler();
    final LoadTsFileDispatcherImpl dispatcher = mock(LoadTsFileDispatcherImpl.class);
    Whitebox.setInternalState(scheduler, "dispatcher", dispatcher);

    final LoadTsFilePieceNode pieceNode =
        new LoadTsFilePieceNode(new PlanNodeId("piece"), new File("test.tsfile"));
    final ChunkData chunkData = mock(ChunkData.class);
    final LoadTsFileObjectFileBatchIterator iterator =
        mock(LoadTsFileObjectFileBatchIterator.class);
    final TSStatus failureStatus = new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
    failureStatus.setMessage("dispatch object piece failed");

    when(chunkData.getObjectFiles())
        .thenReturn(Collections.singleton(new Pair<>(new File("base"), "1/object.bin")));
    when(chunkData.getObjectFileBatchIterator(anyInt())).thenReturn(iterator);
    when(iterator.hasNext()).thenReturn(true, false);
    when(iterator.next())
        .thenReturn(
            new LoadTsFileObjectFileBatch(
                Collections.emptyList(), new TTimePartitionSlot().setStartTime(1L)));
    when(dispatcher.dispatch(isNull(), anyList()))
        .thenReturn(
            CompletableFuture.completedFuture(new FragInstanceDispatchResult(failureStatus)));

    pieceNode.addTsFileData(chunkData);

    final boolean result =
        Whitebox.invokeMethod(
            scheduler, "dispatchObjectFileBatches", pieceNode, createReplicaSet());

    Assert.assertFalse(result);
    verify(dispatcher).dispatch(isNull(), anyList());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSecondPhaseUsesRollbackCommandWhenFirstPhaseFails() throws Exception {
    final LoadTsFileScheduler scheduler = createScheduler();
    final LoadTsFileDispatcherImpl dispatcher = mock(LoadTsFileDispatcherImpl.class);
    final ArgumentCaptor<TLoadCommandReq> commandCaptor =
        ArgumentCaptor.forClass(TLoadCommandReq.class);
    Whitebox.setInternalState(scheduler, "dispatcher", dispatcher);
    ((Set<TRegionReplicaSet>) Whitebox.getInternalState(scheduler, "allReplicaSets"))
        .add(createReplicaSet());

    when(dispatcher.dispatchCommand(commandCaptor.capture(), anySet()))
        .thenReturn(CompletableFuture.completedFuture(new FragInstanceDispatchResult(true)));

    final TsFileResource tsFileResource = mock(TsFileResource.class);
    when(tsFileResource.getTsFile()).thenReturn(new File("rollback.tsfile"));

    final boolean result =
        Whitebox.invokeMethod(scheduler, "secondPhase", false, "rollback-uuid", tsFileResource);

    Assert.assertTrue(result);
    Assert.assertEquals(
        LoadTsFileScheduler.LoadCommand.ROLLBACK.ordinal(), commandCaptor.getValue().commandType);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSecondPhaseReturnsFalseWhenRollbackDispatchFails() throws Exception {
    final LoadTsFileScheduler scheduler = createScheduler();
    final LoadTsFileDispatcherImpl dispatcher = mock(LoadTsFileDispatcherImpl.class);
    final ArgumentCaptor<TLoadCommandReq> commandCaptor =
        ArgumentCaptor.forClass(TLoadCommandReq.class);
    final TSStatus failureStatus = new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
    failureStatus.setMessage("rollback failed");
    Whitebox.setInternalState(scheduler, "dispatcher", dispatcher);
    ((Set<TRegionReplicaSet>) Whitebox.getInternalState(scheduler, "allReplicaSets"))
        .add(createReplicaSet());

    when(dispatcher.dispatchCommand(commandCaptor.capture(), anySet()))
        .thenReturn(
            CompletableFuture.completedFuture(new FragInstanceDispatchResult(failureStatus)));

    final TsFileResource tsFileResource = mock(TsFileResource.class);
    when(tsFileResource.getTsFile()).thenReturn(new File("rollback.tsfile"));

    final boolean result =
        Whitebox.invokeMethod(scheduler, "secondPhase", false, "rollback-uuid", tsFileResource);

    Assert.assertFalse(result);
    Assert.assertEquals(
        LoadTsFileScheduler.LoadCommand.ROLLBACK.ordinal(), commandCaptor.getValue().commandType);
  }

  private LoadTsFileScheduler createScheduler() {
    final MPPQueryContext queryContext = mock(MPPQueryContext.class);
    when(queryContext.getTimeOut()).thenReturn(10_000L);
    when(queryContext.getStartTime()).thenReturn(0L);
    return new LoadTsFileScheduler(
        distributedQueryPlan,
        queryContext,
        mock(QueryStateMachine.class),
        mock(IClientManager.class),
        mock(IPartitionFetcher.class),
        false);
  }

  private TRegionReplicaSet createReplicaSet() {
    final TEndPoint endPoint = new TEndPoint().setIp("127.0.0.1").setPort(9000);
    final TDataNodeLocation dataNodeLocation =
        new TDataNodeLocation().setInternalEndPoint(endPoint);
    return new TRegionReplicaSet()
        .setRegionId(new DataRegionId(1).convertToTConsensusGroupId())
        .setDataNodeLocations(Collections.singletonList(dataNodeLocation));
  }
}
