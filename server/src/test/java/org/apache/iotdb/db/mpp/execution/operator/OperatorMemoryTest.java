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
package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.FillOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.LinearFillOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.SortOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.LinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.join.RowBasedTimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.TimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryCollectOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQuerySortOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.UpdateLastCacheOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.LastCacheScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryMergeOperator.MAP_NODE_RETRAINED_SIZE;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OperatorMemoryTest {

  @Test
  public void seriesScanOperatorTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      MeasurementPath measurementPath =
          new MeasurementPath("root.SeriesScanOperatorTest.device0.sensor0", TSDataType.INT32);
      Set<String> allSensors = Sets.newHashSet("sensor0");
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId, SeriesScanOperator.class.getSimpleName());

      SeriesScanOperator seriesScanOperator =
          new SeriesScanOperator(
              planNodeId,
              measurementPath,
              allSensors,
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              true);

      assertEquals(
          TSFileDescriptor.getInstance().getConfig().getPageSizeInByte(),
          seriesScanOperator.calculateMaxPeekMemory());
      assertEquals(
          TSFileDescriptor.getInstance().getConfig().getPageSizeInByte(),
          seriesScanOperator.calculateMaxReturnSize());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void alignedSeriesScanOperatorTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      AlignedPath alignedPath =
          new AlignedPath(
              "root.AlignedSeriesScanOperatorTest.device0",
              Arrays.asList("sensor0", "sensor1", "sensor2"));
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId, AlignedSeriesScanOperator.class.getSimpleName());

      AlignedSeriesScanOperator seriesScanOperator =
          new AlignedSeriesScanOperator(
              planNodeId,
              alignedPath,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              true);

      assertEquals(
          4 * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte(),
          seriesScanOperator.calculateMaxPeekMemory());
      assertEquals(
          4 * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte(),
          seriesScanOperator.calculateMaxReturnSize());

    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void exchangeOperatorTest() {
    ExchangeOperator exchangeOperator = new ExchangeOperator(null, null, null);

    assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, exchangeOperator.calculateMaxPeekMemory());
    assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, exchangeOperator.calculateMaxReturnSize());
  }

  @Test
  public void lastCacheScanOperatorTest() {
    TsBlock tsBlock = Mockito.mock(TsBlock.class);
    Mockito.when(tsBlock.getRetainedSizeInBytes()).thenReturn(1024L);
    LastCacheScanOperator lastCacheScanOperator = new LastCacheScanOperator(null, null, tsBlock);

    assertEquals(1024, lastCacheScanOperator.calculateMaxPeekMemory());
    assertEquals(1024, lastCacheScanOperator.calculateMaxReturnSize());
  }

  @Test
  public void fillOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);

    FillOperator fillOperator =
        new FillOperator(Mockito.mock(OperatorContext.class), new IFill[] {null, null}, child);

    assertEquals(2048 * 2, fillOperator.calculateMaxPeekMemory());
    assertEquals(1024, fillOperator.calculateMaxReturnSize());
  }

  @Test
  public void lastQueryCollectOperatorTest() {
    List<Operator> children = new ArrayList<>(4);
    Random random = new Random();
    long expectedMaxPeekMemory = 0;
    long expectedMaxReturnSize = 0;
    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      long currentMaxPeekMemory = random.nextInt(1024) + 1024;
      long currentMaxReturnSize = random.nextInt(1024);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(currentMaxPeekMemory);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(currentMaxReturnSize);
      children.add(child);
      expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, currentMaxPeekMemory);
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, currentMaxReturnSize);
    }
    LastQueryCollectOperator lastQueryCollectOperator =
        new LastQueryCollectOperator(Mockito.mock(OperatorContext.class), children);

    assertEquals(expectedMaxPeekMemory, lastQueryCollectOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, lastQueryCollectOperator.calculateMaxReturnSize());
  }

  @Test
  public void lastQueryMergeOperatorTest() {
    List<Operator> children = new ArrayList<>(4);
    Random random = new Random();
    long expectedMaxPeekMemory = 0;
    long expectedMaxReturnSize = 0;
    long childSumReturnSize = 0;
    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      long currentMaxPeekMemory = random.nextInt(1024) + 1024;
      long currentMaxReturnSize = random.nextInt(1024);
      childSumReturnSize += currentMaxReturnSize;
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(currentMaxPeekMemory);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(currentMaxReturnSize);
      children.add(child);
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, currentMaxReturnSize);
      expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, currentMaxPeekMemory);
    }
    // we need to cache all the TsBlocks of children and then return a new TsBlock as result whose
    // max possible should be equal to max return size among all its children and then we should
    // also take TreeMap memory into account.
    expectedMaxPeekMemory =
        Math.max(
            expectedMaxPeekMemory,
            childSumReturnSize
                + expectedMaxReturnSize
                + TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber()
                    * MAP_NODE_RETRAINED_SIZE);

    LastQueryMergeOperator lastQueryMergeOperator =
        new LastQueryMergeOperator(
            Mockito.mock(OperatorContext.class), children, Comparator.naturalOrder());

    assertEquals(expectedMaxPeekMemory, lastQueryMergeOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, lastQueryMergeOperator.calculateMaxReturnSize());
  }

  @Test
  public void lastQueryOperatorTest() {
    TsBlockBuilder builder = Mockito.mock(TsBlockBuilder.class);
    Mockito.when(builder.getRetainedSizeInBytes()).thenReturn(1024L);
    List<UpdateLastCacheOperator> children = new ArrayList<>(4);
    long expectedMaxPeekMemory = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    long expectedMaxReturnSize = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    for (int i = 0; i < 4; i++) {
      UpdateLastCacheOperator child = Mockito.mock(UpdateLastCacheOperator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2 * 1024 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
      children.add(child);
      expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, 2 * 1024 * 1024L);
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, 1024L);
    }
    LastQueryOperator lastQueryOperator =
        new LastQueryOperator(Mockito.mock(OperatorContext.class), children, builder);

    assertEquals(expectedMaxPeekMemory, lastQueryOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, lastQueryOperator.calculateMaxReturnSize());

    Mockito.when(builder.getRetainedSizeInBytes()).thenReturn(4 * 1024 * 1024L);
    assertEquals(4 * 1024 * 1024L, lastQueryOperator.calculateMaxPeekMemory());
    assertEquals(4 * 1024 * 1024L, lastQueryOperator.calculateMaxReturnSize());
  }

  @Test
  public void lastQuerySortOperatorTest() {
    TsBlock tsBlock = Mockito.mock(TsBlock.class);
    Mockito.when(tsBlock.getRetainedSizeInBytes()).thenReturn(16 * 1024L);
    Mockito.when(tsBlock.getPositionCount()).thenReturn(16);
    List<UpdateLastCacheOperator> children = new ArrayList<>(4);
    long expectedMaxPeekMemory =
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES + tsBlock.getRetainedSizeInBytes();
    for (int i = 0; i < 4; i++) {
      UpdateLastCacheOperator child = Mockito.mock(UpdateLastCacheOperator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
      children.add(child);
    }

    expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory + 1024, 2 * 1024);

    LastQuerySortOperator lastQuerySortOperator =
        new LastQuerySortOperator(
            Mockito.mock(OperatorContext.class), tsBlock, children, Comparator.naturalOrder());

    assertEquals(expectedMaxPeekMemory, lastQuerySortOperator.calculateMaxPeekMemory());
    assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, lastQuerySortOperator.calculateMaxReturnSize());
  }

  @Test
  public void limitOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2 * 1024L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);

    LimitOperator limitOperator =
        new LimitOperator(Mockito.mock(OperatorContext.class), 100, child);

    assertEquals(2 * 1024L, limitOperator.calculateMaxPeekMemory());
    assertEquals(1024, limitOperator.calculateMaxReturnSize());
  }

  @Test
  public void offsetOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2 * 1024L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);

    OffsetOperator offsetOperator =
        new OffsetOperator(Mockito.mock(OperatorContext.class), 100, child);

    assertEquals(2 * 1024L, offsetOperator.calculateMaxPeekMemory());
    assertEquals(1024, offsetOperator.calculateMaxReturnSize());
  }

  @Test
  public void rowBasedTimeJoinOperatorTest() {
    List<Operator> children = new ArrayList<>(4);
    List<TSDataType> dataTypeList = new ArrayList<>(2);
    dataTypeList.add(TSDataType.INT32);
    dataTypeList.add(TSDataType.INT32);
    long expectedMaxReturnSize =
        3L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    long expectedMaxPeekMemory = expectedMaxReturnSize;
    long childrenMaxPeekMemory = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      expectedMaxPeekMemory += 64 * 1024L;
      childrenMaxPeekMemory = Math.max(childrenMaxPeekMemory, child.calculateMaxPeekMemory());
      children.add(child);
    }

    expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, childrenMaxPeekMemory);

    RowBasedTimeJoinOperator rowBasedTimeJoinOperator =
        new RowBasedTimeJoinOperator(
            Mockito.mock(OperatorContext.class), children, Ordering.ASC, dataTypeList, null, null);

    assertEquals(expectedMaxPeekMemory, rowBasedTimeJoinOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, rowBasedTimeJoinOperator.calculateMaxReturnSize());
  }

  @Test
  public void sortOperatorTest() {
    SortOperator sortOperator = new SortOperator();
    assertEquals(0, sortOperator.calculateMaxPeekMemory());
    assertEquals(0, sortOperator.calculateMaxReturnSize());
  }

  @Test
  public void timeJoinOperatorTest() {
    List<Operator> children = new ArrayList<>(4);
    List<TSDataType> dataTypeList = new ArrayList<>(2);
    dataTypeList.add(TSDataType.INT32);
    dataTypeList.add(TSDataType.INT32);
    long expectedMaxReturnSize =
        3L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    long expectedMaxPeekMemory = expectedMaxReturnSize;
    long childrenMaxPeekMemory = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      expectedMaxPeekMemory += 64 * 1024L;
      childrenMaxPeekMemory = Math.max(childrenMaxPeekMemory, child.calculateMaxPeekMemory());
      children.add(child);
    }

    expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, childrenMaxPeekMemory);

    TimeJoinOperator timeJoinOperator =
        new TimeJoinOperator(
            Mockito.mock(OperatorContext.class), children, Ordering.ASC, dataTypeList, null, null);

    assertEquals(expectedMaxPeekMemory, timeJoinOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, timeJoinOperator.calculateMaxReturnSize());
  }

  @Test
  public void updateLastCacheOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);

    UpdateLastCacheOperator updateLastCacheOperator =
        new UpdateLastCacheOperator(null, child, null, TSDataType.BOOLEAN, null, true);

    assertEquals(2048, updateLastCacheOperator.calculateMaxPeekMemory());
    assertEquals(1024, updateLastCacheOperator.calculateMaxReturnSize());
  }

  @Test
  public void linearFillOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);

    LinearFillOperator linearFillOperator =
        new LinearFillOperator(
            Mockito.mock(OperatorContext.class), new LinearFill[] {null, null}, child);

    assertEquals(2048 * 3, linearFillOperator.calculateMaxPeekMemory());
    assertEquals(1024, linearFillOperator.calculateMaxReturnSize());
  }

  @Test
  public void deviceMergeOperatorTest() {
    List<Operator> children = new ArrayList<>(4);
    List<TSDataType> dataTypeList = new ArrayList<>(2);
    dataTypeList.add(TSDataType.INT32);
    dataTypeList.add(TSDataType.INT32);
    List<String> devices = new ArrayList<>(4);
    devices.add("device1");
    devices.add("device2");
    devices.add("device3");
    devices.add("device4");
    long expectedMaxReturnSize =
        3L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    long expectedMaxPeekMemory = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    long expectedRetainedSizeAfterCallingNext = 0;
    long childrenMaxPeekMemory = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(64 * 1024L);
      expectedMaxPeekMemory += 128 * 1024L;
      childrenMaxPeekMemory = Math.max(childrenMaxPeekMemory, child.calculateMaxPeekMemory());
      expectedRetainedSizeAfterCallingNext += 128 * 1024L;
      children.add(child);
    }

    expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, childrenMaxPeekMemory);

    DeviceMergeOperator deviceMergeOperator =
        new DeviceMergeOperator(
            Mockito.mock(OperatorContext.class),
            devices,
            children,
            dataTypeList,
            Mockito.mock(TimeSelector.class),
            Mockito.mock(TimeComparator.class));

    assertEquals(expectedMaxPeekMemory, deviceMergeOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, deviceMergeOperator.calculateMaxReturnSize());
    assertEquals(
        expectedRetainedSizeAfterCallingNext - 64 * 1024L,
        deviceMergeOperator.calculateRetainedSizeAfterCallingNext());
  }
}
