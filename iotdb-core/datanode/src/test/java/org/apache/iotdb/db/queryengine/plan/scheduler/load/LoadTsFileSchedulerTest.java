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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
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
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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
    when(distributedQueryPlan.getInstances()).thenReturn(Collections.emptyList());
  }

  @Test
  public void tt() {
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

  @Test
  public void testTsFileDataManagerClearReleasesCachedMemory() throws Exception {
    final Constructor<LoadTsFileDataCacheMemoryBlock> memoryBlockConstructor =
        LoadTsFileDataCacheMemoryBlock.class.getDeclaredConstructor(long.class);
    memoryBlockConstructor.setAccessible(true);
    final LoadTsFileDataCacheMemoryBlock memoryBlock =
        memoryBlockConstructor.newInstance(1024 * 1024L);

    final Class<?> dataManagerClass =
        Class.forName(LoadTsFileScheduler.class.getName() + "$TsFileDataManager");
    final Constructor<?> dataManagerConstructor =
        dataManagerClass.getDeclaredConstructor(
            LoadTsFileScheduler.class,
            LoadSingleTsFileNode.class,
            LoadTsFileDataCacheMemoryBlock.class);
    dataManagerConstructor.setAccessible(true);
    final Object dataManager =
        dataManagerConstructor.newInstance(
            mock(LoadTsFileScheduler.class), mock(LoadSingleTsFileNode.class), memoryBlock);

    // Simulate data buffered before split or routing aborts. clear() is the last chance to return
    // this accounting to the shared LOAD memory block.
    final long cachedMemorySize = 128L;
    memoryBlock.addMemoryUsage(cachedMemorySize);
    final Field dataSizeField = dataManagerClass.getDeclaredField("dataSize");
    dataSizeField.setAccessible(true);
    dataSizeField.setLong(dataManager, cachedMemorySize);

    final Method clearMethod = dataManagerClass.getDeclaredMethod("clear");
    clearMethod.setAccessible(true);
    clearMethod.invoke(dataManager);

    final Method getMemoryUsageMethod =
        LoadTsFileDataCacheMemoryBlock.class.getDeclaredMethod("getMemoryUsageInBytes");
    getMemoryUsageMethod.setAccessible(true);
    Assert.assertEquals(0L, getMemoryUsageMethod.invoke(memoryBlock));
    Assert.assertEquals(0L, dataSizeField.getLong(dataManager));
  }

  @Test
  public void testRouteChunkDataDeduplicatesPartitionSlots() throws Exception {
    final IPartitionFetcher partitionFetcher = mock(IPartitionFetcher.class);
    final DataPartition dataPartition = mock(DataPartition.class);
    final MPPQueryContext queryContext = mock(MPPQueryContext.class);
    final SessionInfo sessionInfo = mock(SessionInfo.class);
    when(queryContext.getSession()).thenReturn(sessionInfo);
    when(sessionInfo.getUserName()).thenReturn("root");
    when(partitionFetcher.getOrCreateDataPartition(anyList(), eq("root")))
        .thenReturn(dataPartition);

    final IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1");
    final TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(0L);
    final TRegionReplicaSet replicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 0), Collections.emptyList());
    when(dataPartition.getDataRegionReplicaSetForWriting(device, timePartitionSlot))
        .thenReturn(replicaSet);

    final LoadTsFileScheduler scheduler =
        new LoadTsFileScheduler(
            distributedQueryPlan,
            queryContext,
            mock(QueryStateMachine.class),
            mock(IClientManager.class),
            partitionFetcher,
            false);
    final LoadSingleTsFileNode node = mock(LoadSingleTsFileNode.class);
    final TsFileResource resource = mock(TsFileResource.class);
    when(node.getPlanNodeId()).thenReturn(new PlanNodeId("test"));
    when(node.getTsFileResource()).thenReturn(resource);
    when(resource.getTsFile()).thenReturn(new File("test.tsfile"));

    final Class<?> dataManagerClass =
        Class.forName(LoadTsFileScheduler.class.getName() + "$TsFileDataManager");
    final Constructor<?> dataManagerConstructor =
        dataManagerClass.getDeclaredConstructor(
            LoadTsFileScheduler.class,
            LoadSingleTsFileNode.class,
            LoadTsFileDataCacheMemoryBlock.class);
    dataManagerConstructor.setAccessible(true);
    final Object dataManager =
        dataManagerConstructor.newInstance(
            scheduler, node, mock(LoadTsFileDataCacheMemoryBlock.class));

    final ChunkData firstChunk = mock(ChunkData.class);
    when(firstChunk.getDevice()).thenReturn(device);
    when(firstChunk.getTimePartitionSlot()).thenReturn(timePartitionSlot);
    when(firstChunk.getDataSize()).thenReturn(1L);
    final ChunkData secondChunk = mock(ChunkData.class);
    when(secondChunk.getDevice()).thenReturn(device);
    when(secondChunk.getTimePartitionSlot()).thenReturn(timePartitionSlot);
    when(secondChunk.getDataSize()).thenReturn(1L);

    final Field chunkDataField = dataManagerClass.getDeclaredField("nonDirectionalChunkData");
    chunkDataField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final List<ChunkData> chunkData = (List<ChunkData>) chunkDataField.get(dataManager);
    chunkData.add(firstChunk);
    chunkData.add(secondChunk);

    final Method routeChunkData = dataManagerClass.getDeclaredMethod("routeChunkData");
    routeChunkData.setAccessible(true);
    routeChunkData.invoke(dataManager);

    verify(dataPartition, times(1)).getDataRegionReplicaSetForWriting(device, timePartitionSlot);
    Assert.assertTrue(chunkData.isEmpty());
  }
}
