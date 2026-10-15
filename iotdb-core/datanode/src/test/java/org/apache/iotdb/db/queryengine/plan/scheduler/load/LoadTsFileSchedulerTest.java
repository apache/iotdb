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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
  public void testRegionReplicaSetComparison() {
    final TDataNodeLocation dataNode1 = createDataNodeLocation(1, 10731);
    final TDataNodeLocation dataNode3 = createDataNodeLocation(3, 10733);
    final TDataNodeLocation dataNode5 = createDataNodeLocation(5, 10735);
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    final TRegionReplicaSet original =
        new TRegionReplicaSet(regionId, Arrays.asList(dataNode5, dataNode3, dataNode1));

    Assert.assertTrue(
        LoadTsFileScheduler.isSameRegionReplicaSet(
            original,
            new TRegionReplicaSet(regionId, Arrays.asList(dataNode3, dataNode5, dataNode1))));
    Assert.assertFalse(
        LoadTsFileScheduler.isSameRegionReplicaSet(
            original,
            new TRegionReplicaSet(
                regionId, Arrays.asList(dataNode3, dataNode5, createDataNodeLocation(7, 10737)))));
    Assert.assertFalse(
        LoadTsFileScheduler.isSameRegionReplicaSet(
            original,
            new TRegionReplicaSet(
                regionId, Arrays.asList(dataNode3, dataNode5, createDataNodeLocation(1, 11731)))));
    Assert.assertFalse(
        LoadTsFileScheduler.isSameRegionReplicaSet(
            original,
            new TRegionReplicaSet(
                new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
                Arrays.asList(dataNode3, dataNode5, dataNode1))));
  }

  private static TDataNodeLocation createDataNodeLocation(int dataNodeId, int internalPort) {
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setInternalEndPoint(new TEndPoint("127.0.0.1", internalPort));
  }
}
