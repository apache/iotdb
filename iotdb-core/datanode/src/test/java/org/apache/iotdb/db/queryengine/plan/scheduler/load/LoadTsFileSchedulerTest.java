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
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatch;
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatchIterator;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
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
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
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
    when(distributedQueryPlan.getInstances()).thenReturn(Collections.emptyList());
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
  public void testAddOrSendChunkDataAccountsMemoryByChunkDataSize() throws Exception {
    final Object tsFileDataManager = createTsFileDataManager();
    final LoadTsFileDataCacheMemoryBlock block = getTsFileDataManagerBlock(tsFileDataManager);

    final ChunkData chunkData = createChunkData(100L, mock(IDeviceID.class));

    Assert.assertTrue(
        (boolean) Whitebox.invokeMethod(tsFileDataManager, "addOrSendChunkData", chunkData));

    Assert.assertEquals(100L, getBlockMemoryUsage(block));
    Assert.assertEquals(100L, (long) Whitebox.getInternalState(tsFileDataManager, "dataSize"));
  }

  @Test
  public void testIntermediateDispatchReleasesWholePieceMemory() throws Exception {
    final TRegionReplicaSet replicaSet = createReplicaSet();
    final DataPartition dataPartition = mock(DataPartition.class);
    when(dataPartition.getDataRegionReplicaSetForWriting(any(), any())).thenReturn(replicaSet);

    final IPartitionFetcher partitionFetcher = mock(IPartitionFetcher.class);
    when(partitionFetcher.getOrCreateDataPartition(anyList(), anyString()))
        .thenReturn(dataPartition);

    final LoadTsFileScheduler scheduler = createScheduler(partitionFetcher);
    final LoadTsFileDispatcherImpl dispatcher = mock(LoadTsFileDispatcherImpl.class);
    Whitebox.setInternalState(scheduler, "dispatcher", dispatcher);
    when(dispatcher.dispatch(isNull(), anyList()))
        .thenReturn(CompletableFuture.completedFuture(new FragInstanceDispatchResult(true)));

    final Object tsFileDataManager = createTsFileDataManager(scheduler);
    final LoadTsFileDataCacheMemoryBlock block = getTsFileDataManagerBlock(tsFileDataManager);
    setBlockMemoryLimit(block, 150L);

    final IDeviceID device = mock(IDeviceID.class);
    Assert.assertTrue(
        (boolean)
            Whitebox.invokeMethod(
                tsFileDataManager, "addOrSendChunkData", createChunkData(100L, device)));
    Assert.assertEquals(100L, getBlockMemoryUsage(block));

    Assert.assertTrue(
        (boolean)
            Whitebox.invokeMethod(
                tsFileDataManager, "addOrSendChunkData", createChunkData(60L, device)));

    Assert.assertEquals(0L, getBlockMemoryUsage(block));
    Assert.assertEquals(0L, (long) Whitebox.getInternalState(tsFileDataManager, "dataSize"));
    verify(dispatcher).dispatch(isNull(), anyList());
  }

  @Test
  public void testGetPartitionQueryDatabaseForPipeGeneratedTreeModelLoad() {
    final LoadSingleTsFileNode node = mock(LoadSingleTsFileNode.class);
    when(node.isTableModel()).thenReturn(false);
    when(node.getDatabase()).thenReturn("root.test.sg");

    Assert.assertEquals("root.test.sg", LoadTsFileScheduler.getPartitionQueryDatabase(node, true));
    Assert.assertNull(LoadTsFileScheduler.getPartitionQueryDatabase(node, false));
  }

  @Test
  public void testGetPartitionQueryDatabaseForTableModelLoad() {
    final LoadSingleTsFileNode node = mock(LoadSingleTsFileNode.class);
    when(node.isTableModel()).thenReturn(true);
    when(node.getDatabase()).thenReturn("test");

    Assert.assertEquals("test", LoadTsFileScheduler.getPartitionQueryDatabase(node, false));
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

  @Test
  public void testBuildRetryTreeLoadStatementUpdatesDatabaseLevel() throws Exception {
    final LoadTsFileScheduler scheduler =
        new LoadTsFileScheduler(
            distributedQueryPlan,
            mock(MPPQueryContext.class),
            mock(QueryStateMachine.class),
            mock(IClientManager.class),
            mock(IPartitionFetcher.class),
            true);
    final Method method =
        LoadTsFileScheduler.class.getDeclaredMethod(
            "buildRetryTreeLoadStatement", String.class, boolean.class, String.class);
    method.setAccessible(true);

    final File tsFile = File.createTempFile("test", ".tsfile");
    tsFile.deleteOnExit();

    final LoadTsFileStatement statement =
        (LoadTsFileStatement)
            method.invoke(scheduler, tsFile.getAbsolutePath(), true, "root.test.sg_0");

    Assert.assertEquals("root.test.sg_0", statement.getDatabase());
    Assert.assertEquals(2, statement.getDatabaseLevel());
    Assert.assertTrue(statement.isGeneratedByPipe());
  }

  private LoadTsFileScheduler createScheduler() {
    return createScheduler(mock(IPartitionFetcher.class));
  }

  private LoadTsFileScheduler createScheduler(final IPartitionFetcher partitionFetcher) {
    final MPPQueryContext queryContext = mock(MPPQueryContext.class);
    when(queryContext.getTimeOut()).thenReturn(10_000L);
    when(queryContext.getStartTime()).thenReturn(System.currentTimeMillis());
    when(queryContext.getSession()).thenReturn(new SessionInfo(0, "root", ZoneId.systemDefault()));
    return new LoadTsFileScheduler(
        distributedQueryPlan,
        queryContext,
        mock(QueryStateMachine.class),
        mock(IClientManager.class),
        partitionFetcher,
        false);
  }

  private Object createTsFileDataManager() throws Exception {
    return createTsFileDataManager(createScheduler());
  }

  private Object createTsFileDataManager(final LoadTsFileScheduler scheduler) throws Exception {
    final LoadSingleTsFileNode singleTsFileNode = mock(LoadSingleTsFileNode.class);
    when(singleTsFileNode.getPlanNodeId()).thenReturn(new PlanNodeId("load"));
    when(singleTsFileNode.getTsFileResource())
        .thenReturn(new TsFileResource(new File("clear-test.tsfile")));

    final Class<?> tsFileDataManagerClass =
        Class.forName(
            "org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler$TsFileDataManager");
    return Whitebox.invokeConstructor(
        tsFileDataManagerClass,
        scheduler,
        singleTsFileNode,
        LoadTsFileMemoryManager.getInstance().allocateDataCacheMemoryBlock());
  }

  private LoadTsFileDataCacheMemoryBlock getTsFileDataManagerBlock(final Object tsFileDataManager)
      throws Exception {
    return Whitebox.getInternalState(tsFileDataManager, "block");
  }

  private long getBlockMemoryUsage(final LoadTsFileDataCacheMemoryBlock block) throws Exception {
    final AtomicLong memoryUsageInBytes = Whitebox.getInternalState(block, "memoryUsageInBytes");
    return memoryUsageInBytes.get();
  }

  private void setBlockMemoryLimit(final LoadTsFileDataCacheMemoryBlock block, final long limit)
      throws Exception {
    final AtomicLong limitedMemorySizeInBytes =
        Whitebox.getInternalState(block, "limitedMemorySizeInBytes");
    limitedMemorySizeInBytes.set(limit);
  }

  private ChunkData createChunkData(final long dataSize, final IDeviceID device) {
    final ChunkData chunkData = mock(ChunkData.class);
    when(chunkData.getDataSize()).thenReturn(dataSize);
    when(chunkData.getDevice()).thenReturn(device);
    when(chunkData.getTimePartitionSlot()).thenReturn(new TTimePartitionSlot(0L));
    when(chunkData.getObjectFiles()).thenReturn(Collections.emptySet());
    return chunkData;
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
