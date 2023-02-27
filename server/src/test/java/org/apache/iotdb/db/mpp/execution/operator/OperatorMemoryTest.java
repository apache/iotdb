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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.AggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.FillOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.LinearFillOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.RawDataAggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.SlidingWindowAggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.SortOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.LinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.join.RowBasedTimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.AbstractUpdateLastCacheOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryCollectOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQuerySortOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.UpdateLastCacheOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.CountGroupByLevelScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.NodeManageMemoryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.NodePathsConvertOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.NodePathsCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaFetchMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaFetchScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaQueryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaQueryOrderByHeatOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaQueryScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.LastCacheScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.window.TimeWindowParameter;
import org.apache.iotdb.db.mpp.execution.operator.window.WindowParameter;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.ArithmeticAdditionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.CompareLessEqualColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.TimeColumnTransformer;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.type.BooleanType;
import org.apache.iotdb.tsfile.read.common.type.LongType;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
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
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId, SeriesScanOperator.class.getSimpleName());

      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
      SeriesScanOperator seriesScanOperator =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId,
              measurementPath,
              Ordering.ASC,
              scanOptionsBuilder.build());

      assertEquals(
          TSFileDescriptor.getInstance().getConfig().getPageSizeInByte(),
          seriesScanOperator.calculateMaxPeekMemory());
      assertEquals(
          TSFileDescriptor.getInstance().getConfig().getPageSizeInByte(),
          seriesScanOperator.calculateMaxReturnSize());
      assertEquals(0, seriesScanOperator.calculateRetainedSizeAfterCallingNext());

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
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId, AlignedSeriesScanOperator.class.getSimpleName());

      AlignedSeriesScanOperator seriesScanOperator =
          new AlignedSeriesScanOperator(
              driverContext.getOperatorContexts().get(0),
              planNodeId,
              alignedPath,
              Ordering.ASC,
              SeriesScanOptions.getDefaultSeriesScanOptions(alignedPath));

      long maxPeekMemory =
          Math.max(
              TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
              4 * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
      long maxReturnMemory =
          Math.min(
              TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
              4 * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
      assertEquals(maxPeekMemory, seriesScanOperator.calculateMaxPeekMemory());
      assertEquals(maxReturnMemory, seriesScanOperator.calculateMaxReturnSize());

      assertEquals(
          maxPeekMemory - maxReturnMemory,
          seriesScanOperator.calculateRetainedSizeAfterCallingNext());

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
    assertEquals(0, exchangeOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void pipelineExchangeOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    ExchangeOperator exchangeOperator =
        new ExchangeOperator(null, null, null, child.calculateMaxReturnSize());

    assertEquals(1024L, exchangeOperator.calculateMaxPeekMemory());
    assertEquals(1024L, exchangeOperator.calculateMaxReturnSize());
    assertEquals(0, exchangeOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void lastCacheScanOperatorTest() {
    TsBlock tsBlock = Mockito.mock(TsBlock.class);
    Mockito.when(tsBlock.getRetainedSizeInBytes()).thenReturn(1024L);
    LastCacheScanOperator lastCacheScanOperator = new LastCacheScanOperator(null, null, tsBlock);

    assertEquals(1024, lastCacheScanOperator.calculateMaxPeekMemory());
    assertEquals(1024, lastCacheScanOperator.calculateMaxReturnSize());
    assertEquals(0, lastCacheScanOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void fillOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    FillOperator fillOperator =
        new FillOperator(Mockito.mock(OperatorContext.class), new IFill[] {null, null}, child);

    assertEquals(2048 * 2 + 512, fillOperator.calculateMaxPeekMemory());
    assertEquals(1024, fillOperator.calculateMaxReturnSize());
    assertEquals(512, fillOperator.calculateRetainedSizeAfterCallingNext());
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
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);
      children.add(child);
      expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, currentMaxPeekMemory);
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, currentMaxReturnSize);
    }
    LastQueryCollectOperator lastQueryCollectOperator =
        new LastQueryCollectOperator(Mockito.mock(OperatorContext.class), children);

    assertEquals(expectedMaxPeekMemory, lastQueryCollectOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, lastQueryCollectOperator.calculateMaxReturnSize());
    assertEquals(4 * 512, lastQueryCollectOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void lastQueryMergeOperatorTest() {
    List<Operator> children = new ArrayList<>(4);
    Random random = new Random();
    long expectedMaxPeekMemory = 0;
    long temp = 0;
    long expectedMaxReturnSize = 0;
    long childSumReturnSize = 0;
    long minReturnSize = Long.MAX_VALUE;
    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      long currentMaxPeekMemory = random.nextInt(1024) + 1024;
      long currentMaxReturnSize = random.nextInt(1024);
      minReturnSize = Math.min(minReturnSize, currentMaxReturnSize);
      childSumReturnSize += currentMaxReturnSize;
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(currentMaxPeekMemory);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(currentMaxReturnSize);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);
      children.add(child);
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, currentMaxReturnSize);
      expectedMaxPeekMemory =
          Math.max(expectedMaxPeekMemory, temp + child.calculateMaxPeekMemory());
      temp += (child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext());
    }
    // we need to cache all the TsBlocks of children and then return a new TsBlock as result whose
    // max possible should be equal to max return size among all its children and then we should
    // also take TreeMap memory into account.
    expectedMaxPeekMemory =
        Math.max(
            expectedMaxPeekMemory,
            temp
                + expectedMaxReturnSize
                + TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber()
                    * MAP_NODE_RETRAINED_SIZE);

    LastQueryMergeOperator lastQueryMergeOperator =
        new LastQueryMergeOperator(
            Mockito.mock(OperatorContext.class), children, Comparator.naturalOrder());

    assertEquals(expectedMaxPeekMemory, lastQueryMergeOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, lastQueryMergeOperator.calculateMaxReturnSize());
    assertEquals(
        childSumReturnSize
            - minReturnSize
            + 4 * 512
            + TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber()
                * MAP_NODE_RETRAINED_SIZE,
        lastQueryMergeOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void lastQueryOperatorTest() {
    TsBlockBuilder builder = Mockito.mock(TsBlockBuilder.class);
    Mockito.when(builder.getRetainedSizeInBytes()).thenReturn(1024L);
    List<AbstractUpdateLastCacheOperator> children = new ArrayList<>(4);
    long expectedMaxReturnSize = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    for (int i = 0; i < 4; i++) {
      UpdateLastCacheOperator child = Mockito.mock(UpdateLastCacheOperator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2 * 1024 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);
      children.add(child);
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, 1024L);
    }
    LastQueryOperator lastQueryOperator =
        new LastQueryOperator(Mockito.mock(OperatorContext.class), children, builder);

    assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES + 2 * 1024 * 1024L,
        lastQueryOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, lastQueryOperator.calculateMaxReturnSize());
    assertEquals(512L, lastQueryOperator.calculateRetainedSizeAfterCallingNext());

    Mockito.when(builder.getRetainedSizeInBytes()).thenReturn(4 * 1024 * 1024L);
    assertEquals(4 * 1024 * 1024L + 2 * 1024 * 1024L, lastQueryOperator.calculateMaxPeekMemory());
    assertEquals(4 * 1024 * 1024L, lastQueryOperator.calculateMaxReturnSize());
    assertEquals(512L, lastQueryOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void lastQuerySortOperatorTest() {
    TsBlock tsBlock = Mockito.mock(TsBlock.class);
    Mockito.when(tsBlock.getRetainedSizeInBytes()).thenReturn(16 * 1024L);
    Mockito.when(tsBlock.getPositionCount()).thenReturn(16);
    List<AbstractUpdateLastCacheOperator> children = new ArrayList<>(4);

    for (int i = 0; i < 4; i++) {
      UpdateLastCacheOperator child = Mockito.mock(UpdateLastCacheOperator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);
      children.add(child);
    }

    LastQuerySortOperator lastQuerySortOperator =
        new LastQuerySortOperator(
            Mockito.mock(OperatorContext.class), tsBlock, children, Comparator.naturalOrder());

    assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES + tsBlock.getRetainedSizeInBytes() + 2 * 1024L,
        lastQuerySortOperator.calculateMaxPeekMemory());
    assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, lastQuerySortOperator.calculateMaxReturnSize());
    assertEquals(
        16 * 1024L + 1024L + 4 * 512L,
        lastQuerySortOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void limitOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2 * 1024L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    LimitOperator limitOperator =
        new LimitOperator(Mockito.mock(OperatorContext.class), 100, child);

    assertEquals(2 * 1024L, limitOperator.calculateMaxPeekMemory());
    assertEquals(1024, limitOperator.calculateMaxReturnSize());
    assertEquals(512, limitOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void offsetOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2 * 1024L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    OffsetOperator offsetOperator =
        new OffsetOperator(Mockito.mock(OperatorContext.class), 100, child);

    assertEquals(2 * 1024L, offsetOperator.calculateMaxPeekMemory());
    assertEquals(1024, offsetOperator.calculateMaxReturnSize());
    assertEquals(512, offsetOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void rowBasedTimeJoinOperatorTest() {
    List<Operator> children = new ArrayList<>(4);
    List<TSDataType> dataTypeList = new ArrayList<>(2);
    dataTypeList.add(TSDataType.INT32);
    dataTypeList.add(TSDataType.INT32);
    long expectedMaxReturnSize =
        Math.min(
            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
            3L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    long expectedMaxPeekMemory = 0;
    long childrenMaxPeekMemory = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);
      childrenMaxPeekMemory =
          Math.max(childrenMaxPeekMemory, expectedMaxPeekMemory + child.calculateMaxPeekMemory());
      expectedMaxPeekMemory += 64 * 1024L;
      children.add(child);
    }

    expectedMaxPeekMemory =
        Math.max(expectedMaxPeekMemory + expectedMaxReturnSize, childrenMaxPeekMemory);

    RowBasedTimeJoinOperator rowBasedTimeJoinOperator =
        new RowBasedTimeJoinOperator(
            Mockito.mock(OperatorContext.class), children, Ordering.ASC, dataTypeList, null, null);

    assertEquals(expectedMaxPeekMemory, rowBasedTimeJoinOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, rowBasedTimeJoinOperator.calculateMaxReturnSize());
    assertEquals(3 * 64 * 1024L, rowBasedTimeJoinOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void sortOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    SortOperator sortOperator =
        new SortOperator(
            Mockito.mock(OperatorContext.class),
            child,
            Collections.singletonList(TSDataType.INT32),
            null);

    assertEquals(2048 + 512, sortOperator.calculateMaxPeekMemory());
    assertEquals(1024, sortOperator.calculateMaxReturnSize());
    assertEquals(512, sortOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void updateLastCacheOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    UpdateLastCacheOperator updateLastCacheOperator =
        new UpdateLastCacheOperator(null, child, null, TSDataType.BOOLEAN, null, false);

    assertEquals(2048, updateLastCacheOperator.calculateMaxPeekMemory());
    assertEquals(1024, updateLastCacheOperator.calculateMaxReturnSize());
    assertEquals(512, updateLastCacheOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void linearFillOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    LinearFillOperator linearFillOperator =
        new LinearFillOperator(
            Mockito.mock(OperatorContext.class), new LinearFill[] {null, null}, child);

    assertEquals(2048 * 3 + 512L, linearFillOperator.calculateMaxPeekMemory());
    assertEquals(1024, linearFillOperator.calculateMaxReturnSize());
    assertEquals(512, linearFillOperator.calculateRetainedSizeAfterCallingNext());
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

  @Test
  public void deviceViewOperatorTest() {
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
        2L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    long expectedMaxPeekMemory = expectedMaxReturnSize;
    long expectedRetainedSizeAfterCallingNext = 0;
    long childrenMaxPeekMemory = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(1024L);
      childrenMaxPeekMemory = Math.max(childrenMaxPeekMemory, child.calculateMaxPeekMemory());
      expectedRetainedSizeAfterCallingNext =
          Math.max(
              expectedRetainedSizeAfterCallingNext, child.calculateRetainedSizeAfterCallingNext());
      children.add(child);
    }

    expectedMaxPeekMemory =
        Math.max(expectedMaxPeekMemory, childrenMaxPeekMemory)
            + expectedRetainedSizeAfterCallingNext;

    DeviceViewOperator deviceViewOperator =
        new DeviceViewOperator(
            Mockito.mock(OperatorContext.class),
            devices,
            children,
            new ArrayList<>(),
            dataTypeList);

    assertEquals(expectedMaxPeekMemory, deviceViewOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, deviceViewOperator.calculateMaxReturnSize());
    assertEquals(
        expectedRetainedSizeAfterCallingNext,
        deviceViewOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void filterAndProjectOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);
    BooleanType booleanType = Mockito.mock(BooleanType.class);
    Mockito.when(booleanType.getTypeEnum()).thenReturn(TypeEnum.BOOLEAN);
    LongType longType = Mockito.mock(LongType.class);
    Mockito.when(longType.getTypeEnum()).thenReturn(TypeEnum.INT64);
    ColumnTransformer filterColumnTransformer =
        new CompareLessEqualColumnTransformer(
            booleanType,
            new TimeColumnTransformer(longType),
            new ConstantColumnTransformer(longType, Mockito.mock(IntColumn.class)));
    List<TSDataType> filterOutputTypes = new ArrayList<>();
    filterOutputTypes.add(TSDataType.INT32);
    filterOutputTypes.add(TSDataType.INT64);
    List<ColumnTransformer> projectColumnTransformers = new ArrayList<>();
    projectColumnTransformers.add(
        new ArithmeticAdditionColumnTransformer(
            booleanType,
            new TimeColumnTransformer(longType),
            new ConstantColumnTransformer(longType, Mockito.mock(IntColumn.class))));

    FilterAndProjectOperator operator =
        new FilterAndProjectOperator(
            Mockito.mock(OperatorContext.class),
            child,
            filterOutputTypes,
            new ArrayList<>(),
            filterColumnTransformer,
            new ArrayList<>(),
            new ArrayList<>(),
            projectColumnTransformers,
            false,
            true);

    assertEquals(
        4L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte() + 512L,
        operator.calculateMaxPeekMemory());
    assertEquals(
        2 * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte(),
        operator.calculateMaxReturnSize());
    assertEquals(512, operator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void SchemaQueryScanOperatorTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId, SchemaQueryScanOperator.class.getSimpleName());

      SchemaQueryScanOperator<?> operator =
          new SchemaQueryScanOperator<>(
              planNodeId,
              driverContext.getOperatorContexts().get(0),
              Mockito.mock(ISchemaSource.class));

      assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, operator.calculateMaxPeekMemory());
      assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, operator.calculateMaxReturnSize());
      assertEquals(0, operator.calculateRetainedSizeAfterCallingNext());

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void LevelTimeSeriesCountOperatorTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId, SeriesScanOperator.class.getSimpleName());

      CountGroupByLevelScanOperator<?> operator =
          new CountGroupByLevelScanOperator<>(
              planNodeId,
              driverContext.getOperatorContexts().get(0),
              4,
              Mockito.mock(ISchemaSource.class));

      assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, operator.calculateMaxPeekMemory());
      assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, operator.calculateMaxReturnSize());
      assertEquals(0, operator.calculateRetainedSizeAfterCallingNext());

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void SchemaCountOperatorTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId, SchemaCountOperator.class.getSimpleName());

      SchemaCountOperator<?> operator =
          new SchemaCountOperator<>(
              planNodeId,
              driverContext.getOperatorContexts().get(0),
              Mockito.mock(ISchemaSource.class));

      assertEquals(8L, operator.calculateMaxPeekMemory());
      assertEquals(8L, operator.calculateMaxReturnSize());
      assertEquals(0, operator.calculateRetainedSizeAfterCallingNext());

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void SchemaQueryMergeOperatorTest() {
    QueryId queryId = new QueryId("stub_query");
    List<Operator> children = new ArrayList<>(4);

    long expectedMaxReturnSize = 0;
    long expectedMaxPeekMemory = 0;
    long expectedRetainedSize = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);
      expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, child.calculateMaxPeekMemory());
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, child.calculateMaxReturnSize());
      expectedRetainedSize += child.calculateRetainedSizeAfterCallingNext();
      children.add(child);
    }

    SchemaQueryMergeOperator operator =
        new SchemaQueryMergeOperator(
            queryId.genPlanNodeId(), Mockito.mock(OperatorContext.class), children);

    assertEquals(expectedMaxPeekMemory, operator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, operator.calculateMaxReturnSize());
    assertEquals(expectedRetainedSize, operator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void CountMergeOperatorTest() {
    QueryId queryId = new QueryId("stub_query");
    List<Operator> children = new ArrayList<>(4);

    long expectedMaxReturnSize = 0;
    long expectedMaxPeekMemory = 0;
    long expectedRetainedSize = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);
      expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, child.calculateMaxPeekMemory());
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, child.calculateMaxReturnSize());
      expectedRetainedSize += child.calculateRetainedSizeAfterCallingNext();
      children.add(child);
    }

    CountMergeOperator operator =
        new CountMergeOperator(
            queryId.genPlanNodeId(), Mockito.mock(OperatorContext.class), children);

    assertEquals(expectedMaxPeekMemory, operator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, operator.calculateMaxReturnSize());
    assertEquals(expectedRetainedSize, operator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void SchemaFetchScanOperatorTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId, SeriesScanOperator.class.getSimpleName());

      SchemaFetchScanOperator operator =
          new SchemaFetchScanOperator(
              planNodeId, driverContext.getOperatorContexts().get(0), null, null, null, false);

      assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, operator.calculateMaxPeekMemory());
      assertEquals(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, operator.calculateMaxReturnSize());
      assertEquals(0, operator.calculateRetainedSizeAfterCallingNext());

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void SchemaFetchMergeOperatorTest() {
    List<Operator> children = new ArrayList<>(4);

    long expectedMaxReturnSize = 0;
    long expectedMaxPeekMemory = 0;
    long expectedRetainedSize = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);
      expectedMaxPeekMemory = Math.max(expectedMaxPeekMemory, child.calculateMaxPeekMemory());
      expectedMaxReturnSize = Math.max(expectedMaxReturnSize, child.calculateMaxReturnSize());
      expectedRetainedSize += child.calculateRetainedSizeAfterCallingNext();
      children.add(child);
    }

    SchemaFetchMergeOperator operator =
        new SchemaFetchMergeOperator(Mockito.mock(OperatorContext.class), children, null);

    assertEquals(expectedMaxPeekMemory, operator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, operator.calculateMaxReturnSize());
    assertEquals(expectedRetainedSize, operator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void SchemaQueryOrderByHeatOperatorTest() {
    List<Operator> children = new ArrayList<>(4);

    long expectedMaxReturnSize = 0;
    long expectedMaxPeekMemory = 0;
    long expectedRetainedSize = 0;

    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);
      expectedMaxPeekMemory += child.calculateMaxReturnSize();
      expectedMaxReturnSize += child.calculateMaxReturnSize();
      expectedRetainedSize +=
          child.calculateRetainedSizeAfterCallingNext() + child.calculateMaxReturnSize();
      children.add(child);
    }

    SchemaQueryOrderByHeatOperator operator =
        new SchemaQueryOrderByHeatOperator(Mockito.mock(OperatorContext.class), children);

    assertEquals(expectedMaxPeekMemory, operator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, operator.calculateMaxReturnSize());
    assertEquals(expectedRetainedSize, operator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void NodePathsConvertOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);

    long expectedMaxPeekMemory = child.calculateMaxPeekMemory() + child.calculateMaxReturnSize();
    long expectedMaxReturnSize = child.calculateMaxReturnSize();
    long expectedRetainedSize = child.calculateRetainedSizeAfterCallingNext();

    NodePathsConvertOperator operator =
        new NodePathsConvertOperator(Mockito.mock(OperatorContext.class), child);

    assertEquals(expectedMaxPeekMemory, operator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, operator.calculateMaxReturnSize());
    assertEquals(expectedRetainedSize, operator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void NodePathsCountOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);

    long expectedMaxPeekMemory =
        Math.max(2L * DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, child.calculateMaxPeekMemory());
    long expectedMaxReturnSize =
        Math.max(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, child.calculateMaxReturnSize());
    long expectedRetainedSize =
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES + child.calculateRetainedSizeAfterCallingNext();

    NodePathsCountOperator operator =
        new NodePathsCountOperator(Mockito.mock(OperatorContext.class), child);

    assertEquals(expectedMaxPeekMemory, operator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, operator.calculateMaxReturnSize());
    assertEquals(expectedRetainedSize, operator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void NodeManageMemoryMergeOperatorTest() {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);

    long expectedMaxPeekMemory =
        Math.max(2L * DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, child.calculateMaxPeekMemory());
    long expectedMaxReturnSize =
        Math.max(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, child.calculateMaxReturnSize());
    long expectedRetainedSize =
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES + child.calculateRetainedSizeAfterCallingNext();

    NodeManageMemoryMergeOperator operator =
        new NodeManageMemoryMergeOperator(
            Mockito.mock(OperatorContext.class), Collections.emptySet(), child);

    assertEquals(expectedMaxPeekMemory, operator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, operator.calculateMaxReturnSize());
    assertEquals(expectedRetainedSize, operator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void seriesAggregationScanOperatorTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      MeasurementPath measurementPath = new MeasurementPath("root.sg.d1.s1", TSDataType.TEXT);
      TypeProvider typeProvider = new TypeProvider();
      typeProvider.setType("count(root.sg.d1.s1)", TSDataType.INT64);
      typeProvider.setType("min_time(root.sg.d1.s1)", TSDataType.INT64);
      typeProvider.setType("first_value(root.sg.d1.s1)", TSDataType.TEXT);

      // case1: without group by, step is SINGLE
      List<AggregationDescriptor> aggregationDescriptors1 =
          Arrays.asList(
              new AggregationDescriptor(
                  TAggregationType.FIRST_VALUE.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))),
              new AggregationDescriptor(
                  TAggregationType.COUNT.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))));

      SeriesAggregationScanOperator seriesAggregationScanOperator1 =
          createSeriesAggregationScanOperator(
              instanceNotificationExecutor,
              measurementPath,
              aggregationDescriptors1,
              null,
              typeProvider);

      long expectedMaxReturnSize =
          TimeColumn.SIZE_IN_BYTES_PER_POSITION
              + 512 * Byte.BYTES
              + LongColumn.SIZE_IN_BYTES_PER_POSITION;
      long cachedRawDataSize = 2L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();

      assertEquals(
          expectedMaxReturnSize + cachedRawDataSize,
          seriesAggregationScanOperator1.calculateMaxPeekMemory());
      assertEquals(expectedMaxReturnSize, seriesAggregationScanOperator1.calculateMaxReturnSize());
      assertEquals(0, seriesAggregationScanOperator1.calculateRetainedSizeAfterCallingNext());

      // case2: without group by, step is PARTIAL
      List<AggregationDescriptor> aggregationDescriptors2 =
          Arrays.asList(
              new AggregationDescriptor(
                  TAggregationType.FIRST_VALUE.name().toLowerCase(),
                  AggregationStep.PARTIAL,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))),
              new AggregationDescriptor(
                  TAggregationType.COUNT.name().toLowerCase(),
                  AggregationStep.PARTIAL,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))));

      SeriesAggregationScanOperator seriesAggregationScanOperator2 =
          createSeriesAggregationScanOperator(
              instanceNotificationExecutor,
              measurementPath,
              aggregationDescriptors2,
              null,
              typeProvider);

      expectedMaxReturnSize =
          TimeColumn.SIZE_IN_BYTES_PER_POSITION
              + 512 * Byte.BYTES
              + 2 * LongColumn.SIZE_IN_BYTES_PER_POSITION;
      cachedRawDataSize = 2L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();

      assertEquals(
          expectedMaxReturnSize + cachedRawDataSize,
          seriesAggregationScanOperator2.calculateMaxPeekMemory());
      assertEquals(expectedMaxReturnSize, seriesAggregationScanOperator2.calculateMaxReturnSize());
      assertEquals(0, seriesAggregationScanOperator2.calculateRetainedSizeAfterCallingNext());

      long maxTsBlockLineNumber =
          TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();

      // case3: with group by, total window num < 1000
      GroupByTimeParameter groupByTimeParameter =
          new GroupByTimeParameter(
              0,
              2 * maxTsBlockLineNumber,
              maxTsBlockLineNumber / 100,
              maxTsBlockLineNumber / 100,
              true);
      List<AggregationDescriptor> aggregationDescriptors3 =
          Arrays.asList(
              new AggregationDescriptor(
                  TAggregationType.FIRST_VALUE.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))),
              new AggregationDescriptor(
                  TAggregationType.COUNT.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))));

      SeriesAggregationScanOperator seriesAggregationScanOperator3 =
          createSeriesAggregationScanOperator(
              instanceNotificationExecutor,
              measurementPath,
              aggregationDescriptors3,
              groupByTimeParameter,
              typeProvider);

      expectedMaxReturnSize =
          200
              * (TimeColumn.SIZE_IN_BYTES_PER_POSITION
                  + 512 * Byte.BYTES
                  + LongColumn.SIZE_IN_BYTES_PER_POSITION);
      cachedRawDataSize = 2L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();

      assertEquals(
          expectedMaxReturnSize + cachedRawDataSize,
          seriesAggregationScanOperator3.calculateMaxPeekMemory());
      assertEquals(expectedMaxReturnSize, seriesAggregationScanOperator3.calculateMaxReturnSize());
      assertEquals(
          cachedRawDataSize,
          seriesAggregationScanOperator3.calculateRetainedSizeAfterCallingNext());

      // case4: with group by, total window num > 1000
      groupByTimeParameter = new GroupByTimeParameter(0, 2 * maxTsBlockLineNumber, 1, 1, true);
      List<AggregationDescriptor> aggregationDescriptors4 =
          Arrays.asList(
              new AggregationDescriptor(
                  TAggregationType.FIRST_VALUE.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))),
              new AggregationDescriptor(
                  TAggregationType.COUNT.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))));

      SeriesAggregationScanOperator seriesAggregationScanOperator4 =
          createSeriesAggregationScanOperator(
              instanceNotificationExecutor,
              measurementPath,
              aggregationDescriptors4,
              groupByTimeParameter,
              typeProvider);

      expectedMaxReturnSize =
          Math.min(
              DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
              maxTsBlockLineNumber
                  * (TimeColumn.SIZE_IN_BYTES_PER_POSITION
                      + 512 * Byte.BYTES
                      + LongColumn.SIZE_IN_BYTES_PER_POSITION));
      cachedRawDataSize = 2L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();

      assertEquals(
          expectedMaxReturnSize + cachedRawDataSize,
          seriesAggregationScanOperator4.calculateMaxPeekMemory());
      assertEquals(expectedMaxReturnSize, seriesAggregationScanOperator4.calculateMaxReturnSize());
      assertEquals(
          cachedRawDataSize,
          seriesAggregationScanOperator4.calculateRetainedSizeAfterCallingNext());

      // case5: over DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES
      groupByTimeParameter = new GroupByTimeParameter(0, 2 * maxTsBlockLineNumber, 1, 1, true);
      List<AggregationDescriptor> aggregationDescriptors5 =
          Arrays.asList(
              new AggregationDescriptor(
                  TAggregationType.FIRST_VALUE.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))),
              new AggregationDescriptor(
                  TAggregationType.FIRST_VALUE.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))),
              new AggregationDescriptor(
                  TAggregationType.FIRST_VALUE.name().toLowerCase(),
                  AggregationStep.SINGLE,
                  Collections.singletonList(new TimeSeriesOperand(measurementPath))));

      SeriesAggregationScanOperator seriesAggregationScanOperator5 =
          createSeriesAggregationScanOperator(
              instanceNotificationExecutor,
              measurementPath,
              aggregationDescriptors5,
              groupByTimeParameter,
              typeProvider);

      expectedMaxReturnSize = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
      cachedRawDataSize = 2L * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();

      assertEquals(
          expectedMaxReturnSize + cachedRawDataSize,
          seriesAggregationScanOperator5.calculateMaxPeekMemory());
      assertEquals(expectedMaxReturnSize, seriesAggregationScanOperator5.calculateMaxReturnSize());
      assertEquals(
          cachedRawDataSize,
          seriesAggregationScanOperator5.calculateRetainedSizeAfterCallingNext());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  private SeriesAggregationScanOperator createSeriesAggregationScanOperator(
      ExecutorService instanceNotificationExecutor,
      MeasurementPath measurementPath,
      List<AggregationDescriptor> aggregationDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      TypeProvider typeProvider)
      throws IllegalPathException {
    Set<String> allSensors = Sets.newHashSet("s1");
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId, SeriesScanOperator.class.getSimpleName());

    List<Aggregator> aggregators = new ArrayList<>();
    aggregationDescriptors.forEach(
        o ->
            aggregators.add(
                new Aggregator(
                    AccumulatorFactory.createAccumulator(
                        o.getAggregationType(),
                        measurementPath.getSeriesType(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        true),
                    o.getStep())));

    ITimeRangeIterator timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, true, true);
    long maxReturnSize =
        AggregationUtil.calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, typeProvider);

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(allSensors);
    return new SeriesAggregationScanOperator(
        planNodeId,
        measurementPath,
        Ordering.ASC,
        scanOptionsBuilder.build(),
        driverContext.getOperatorContexts().get(0),
        aggregators,
        timeRangeIterator,
        groupByTimeParameter,
        maxReturnSize);
  }

  @Test
  public void rawDataAggregationOperatorTest() throws IllegalPathException {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    MeasurementPath measurementPath = new MeasurementPath("root.sg.d1.s1", TSDataType.TEXT);
    TypeProvider typeProvider = new TypeProvider();
    typeProvider.setType("count(root.sg.d1.s1)", TSDataType.INT64);
    typeProvider.setType("first_value(root.sg.d1.s1)", TSDataType.TEXT);

    List<AggregationDescriptor> aggregationDescriptors =
        Arrays.asList(
            new AggregationDescriptor(
                TAggregationType.FIRST_VALUE.name().toLowerCase(),
                AggregationStep.FINAL,
                Collections.singletonList(new TimeSeriesOperand(measurementPath))),
            new AggregationDescriptor(
                TAggregationType.COUNT.name().toLowerCase(),
                AggregationStep.FINAL,
                Collections.singletonList(new TimeSeriesOperand(measurementPath))));

    List<Aggregator> aggregators = new ArrayList<>();
    aggregationDescriptors.forEach(
        o ->
            aggregators.add(
                new Aggregator(
                    AccumulatorFactory.createAccumulator(
                        o.getAggregationType(),
                        measurementPath.getSeriesType(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        true),
                    o.getStep())));

    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 1000, 10, 10, true);
    ITimeRangeIterator timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, true, false);
    long maxReturnSize =
        AggregationUtil.calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, typeProvider);

    WindowParameter windowParameter = new TimeWindowParameter(false);

    RawDataAggregationOperator rawDataAggregationOperator =
        new RawDataAggregationOperator(
            Mockito.mock(OperatorContext.class),
            aggregators,
            timeRangeIterator,
            child,
            true,
            maxReturnSize,
            windowParameter);

    long expectedMaxReturnSize =
        100
            * (512 * Byte.BYTES
                + TimeColumn.SIZE_IN_BYTES_PER_POSITION
                + LongColumn.SIZE_IN_BYTES_PER_POSITION);
    long expectedMaxRetainSize = child.calculateMaxReturnSize();

    assertEquals(
        expectedMaxReturnSize
            + expectedMaxRetainSize
            + child.calculateRetainedSizeAfterCallingNext(),
        rawDataAggregationOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, rawDataAggregationOperator.calculateMaxReturnSize());
    assertEquals(
        expectedMaxRetainSize + child.calculateRetainedSizeAfterCallingNext(),
        rawDataAggregationOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void slidingWindowAggregationOperatorTest() throws IllegalPathException {
    Operator child = Mockito.mock(Operator.class);
    Mockito.when(child.calculateMaxPeekMemory()).thenReturn(2048L);
    Mockito.when(child.calculateMaxReturnSize()).thenReturn(1024L);
    Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(512L);

    MeasurementPath measurementPath = new MeasurementPath("root.sg.d1.s1", TSDataType.TEXT);
    TypeProvider typeProvider = new TypeProvider();
    typeProvider.setType("count(root.sg.d1.s1)", TSDataType.INT64);
    typeProvider.setType("first_value(root.sg.d1.s1)", TSDataType.TEXT);

    List<AggregationDescriptor> aggregationDescriptors =
        Arrays.asList(
            new AggregationDescriptor(
                TAggregationType.FIRST_VALUE.name().toLowerCase(),
                AggregationStep.FINAL,
                Collections.singletonList(new TimeSeriesOperand(measurementPath))),
            new AggregationDescriptor(
                TAggregationType.COUNT.name().toLowerCase(),
                AggregationStep.FINAL,
                Collections.singletonList(new TimeSeriesOperand(measurementPath))));

    List<Aggregator> aggregators = new ArrayList<>();
    aggregationDescriptors.forEach(
        o ->
            aggregators.add(
                new Aggregator(
                    AccumulatorFactory.createAccumulator(
                        o.getAggregationType(),
                        measurementPath.getSeriesType(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        true),
                    o.getStep())));

    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 1000, 10, 5, true);
    ITimeRangeIterator timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, true, false);
    long maxReturnSize =
        AggregationUtil.calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, typeProvider);

    SlidingWindowAggregationOperator slidingWindowAggregationOperator =
        new SlidingWindowAggregationOperator(
            Mockito.mock(OperatorContext.class),
            aggregators,
            timeRangeIterator,
            child,
            true,
            groupByTimeParameter,
            maxReturnSize);

    long expectedMaxReturnSize =
        200
            * (512 * Byte.BYTES
                + TimeColumn.SIZE_IN_BYTES_PER_POSITION
                + LongColumn.SIZE_IN_BYTES_PER_POSITION);
    long expectedMaxRetainSize = child.calculateMaxReturnSize();

    assertEquals(
        expectedMaxReturnSize
            + expectedMaxRetainSize
            + child.calculateRetainedSizeAfterCallingNext(),
        slidingWindowAggregationOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, slidingWindowAggregationOperator.calculateMaxReturnSize());
    assertEquals(
        expectedMaxRetainSize + child.calculateRetainedSizeAfterCallingNext(),
        slidingWindowAggregationOperator.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void aggregationOperatorTest() throws IllegalPathException {
    List<Operator> children = new ArrayList<>(4);
    long expectedChildrenRetainedSize = 0L;
    long expectedMaxRetainSize = 0L;
    for (int i = 0; i < 4; i++) {
      Operator child = Mockito.mock(Operator.class);
      Mockito.when(child.calculateMaxPeekMemory()).thenReturn(128 * 1024L);
      Mockito.when(child.calculateMaxReturnSize()).thenReturn(64 * 1024L);
      Mockito.when(child.calculateRetainedSizeAfterCallingNext()).thenReturn(64 * 1024L);
      expectedChildrenRetainedSize += 64 * 1024L;
      expectedMaxRetainSize += 64 * 1024L;
      children.add(child);
    }

    MeasurementPath measurementPath = new MeasurementPath("root.sg.d1.s1", TSDataType.TEXT);
    TypeProvider typeProvider = new TypeProvider();
    typeProvider.setType("count(root.sg.d1.s1)", TSDataType.INT64);
    typeProvider.setType("first_value(root.sg.d1.s1)", TSDataType.TEXT);

    List<AggregationDescriptor> aggregationDescriptors =
        Arrays.asList(
            new AggregationDescriptor(
                TAggregationType.FIRST_VALUE.name().toLowerCase(),
                AggregationStep.FINAL,
                Collections.singletonList(new TimeSeriesOperand(measurementPath))),
            new AggregationDescriptor(
                TAggregationType.COUNT.name().toLowerCase(),
                AggregationStep.FINAL,
                Collections.singletonList(new TimeSeriesOperand(measurementPath))));

    List<Aggregator> aggregators = new ArrayList<>();
    aggregationDescriptors.forEach(
        o ->
            aggregators.add(
                new Aggregator(
                    AccumulatorFactory.createAccumulator(
                        o.getAggregationType(),
                        measurementPath.getSeriesType(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        true),
                    o.getStep())));

    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 1000, 10, 10, true);
    ITimeRangeIterator timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, true, false);
    long maxReturnSize =
        AggregationUtil.calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, typeProvider);

    AggregationOperator aggregationOperator =
        new AggregationOperator(
            Mockito.mock(OperatorContext.class),
            aggregators,
            timeRangeIterator,
            children,
            maxReturnSize);

    long expectedMaxReturnSize =
        100
            * (512 * Byte.BYTES
                + TimeColumn.SIZE_IN_BYTES_PER_POSITION
                + LongColumn.SIZE_IN_BYTES_PER_POSITION);

    assertEquals(
        expectedMaxReturnSize + expectedMaxRetainSize + expectedChildrenRetainedSize,
        aggregationOperator.calculateMaxPeekMemory());
    assertEquals(expectedMaxReturnSize, aggregationOperator.calculateMaxReturnSize());
    assertEquals(
        expectedMaxRetainSize + expectedChildrenRetainedSize,
        aggregationOperator.calculateRetainedSizeAfterCallingNext());
  }
}
