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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.HashAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.StreamingHashAggregationOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.EXTREME;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.FIRST;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.LAST;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.SUM;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.getAggregationTypeByFuncName;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AccumulatorFactory.createGroupedAccumulator;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash.DEFAULT_GROUP_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AggregationCornerCaseTest {
  @Test
  // test StreamingHashOperator produces two output TsBlocks in one calculation
  public void streamingHashAggTest() {
    try (StreamingHashAggregationOperator aggregationOperator =
        genStreamingHashAggregationOperator()) {
      ListenableFuture<?> listenableFuture = aggregationOperator.isBlocked();
      listenableFuture.get();
      int resIndex = 0;
      while (!aggregationOperator.isFinished() && aggregationOperator.hasNext()) {
        TsBlock tsBlock = aggregationOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          if (resIndex == 0) {
            // produce first TsBlock because result builder is full
            assertEquals(1000, tsBlock.getPositionCount());
            assertEquals(1000, tsBlock.getColumn(0).getPositionCount());
            assertEquals(1000, tsBlock.getColumn(1).getPositionCount());
          } else if (resIndex == 1) {
            // produce second TsBlock contains the remaining part
            assertEquals(25, tsBlock.getPositionCount());
            assertEquals(25, tsBlock.getColumn(0).getPositionCount());
            assertEquals(25, tsBlock.getColumn(1).getPositionCount());
          } else {
            fail();
          }
          resIndex++;
        }
        listenableFuture = aggregationOperator.isBlocked();
        listenableFuture.get();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private StreamingHashAggregationOperator genStreamingHashAggregationOperator() {

    // Construct operator tree
    QueryId queryId = new QueryId("stub_query");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(
            instanceId,
            IoTDBThreadPoolFactory.newFixedThreadPool(
                1, "streamingAggregationHashOperator-test-instance-notification"));
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, TableScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(
        2, planNodeId2, StreamingHashAggregationOperator.class.getSimpleName());
    List<TSDataType> outputTypes = ImmutableList.of(TSDataType.INT32, TSDataType.INT32);
    // construct output TsBlock has same preGroup but has 1025 unPreGroups
    Operator childOperator =
        new Operator() {
          boolean finished = false;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder = new TsBlockBuilder(outputTypes);
            ColumnBuilder[] columnBuilders = builder.getValueColumnBuilders();
            for (int i = 0; i < 1025; i++) {
              columnBuilders[0].writeInt(1);
              columnBuilders[1].writeInt(i);
            }
            builder.declarePositions(1025);
            TsBlock result =
                builder.build(
                    new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
            finished = true;
            return result;
          }

          @Override
          public boolean hasNext() throws Exception {
            return !isFinished();
          }

          @Override
          public void close() throws Exception {}

          @Override
          public boolean isFinished() throws Exception {
            return finished;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 0;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 0;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    OperatorContext operatorContext = driverContext.getOperatorContexts().get(1);

    return new StreamingHashAggregationOperator(
        operatorContext,
        childOperator,
        Collections.singletonList(0),
        Collections.singletonList(0),
        Collections.singletonList(IntType.INT32),
        Collections.singletonList(1),
        Collections.singletonList(1),
        getComparatorForTable(
            ImmutableList.of(SortOrder.ASC_NULLS_LAST),
            ImmutableList.of(0),
            Collections.singletonList(TSDataType.INT32)),
        Collections.emptyList(),
        AggregationNode.Step.SINGLE,
        DEFAULT_GROUP_NUMBER,
        Long.MAX_VALUE,
        false,
        Long.MAX_VALUE);
  }

  @Test
  public void groupByWithLargeDataTest() {
    try (HashAggregationOperator aggregationOperator = genHashAggregationOperator()) {
      ListenableFuture<?> listenableFuture = aggregationOperator.isBlocked();
      listenableFuture.get();
      while (!aggregationOperator.isFinished() && aggregationOperator.hasNext()) {
        TsBlock tsBlock = aggregationOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          assertEquals(2, tsBlock.getPositionCount());
          Column column = tsBlock.getColumn(0);
          if (column.getLong(0) == 1) {
            assertEquals(2, column.getLong(1));
          } else {
            assertEquals(2, column.getLong(0));
            assertEquals(1, column.getLong(1));
          }
        }
        listenableFuture = aggregationOperator.isBlocked();
        listenableFuture.get();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // construct a AggregationHashOperator has more than 1024 lines in input TsBlock
  private HashAggregationOperator genHashAggregationOperator() {

    // Construct operator tree
    QueryId queryId = new QueryId("stub_query");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(
            instanceId,
            IoTDBThreadPoolFactory.newFixedThreadPool(
                1, "aggregationHashOperator-test-instance-notification"));
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, TableScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(2, planNodeId2, HashAggregationOperator.class.getSimpleName());
    Operator childOperator =
        new Operator() {
          boolean finished = false;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(Collections.singletonList(TSDataType.TIMESTAMP));
            ColumnBuilder columnBuilder = builder.getValueColumnBuilders()[0];
            for (int i = 0; i < 1000; i++) {
              columnBuilder.writeLong(1);
            }
            for (int i = 1000; i < 1025; i++) {
              columnBuilder.writeLong(2);
            }
            builder.declarePositions(1025);
            TsBlock result =
                builder.build(
                    new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
            finished = true;
            return result;
          }

          @Override
          public boolean hasNext() throws Exception {
            return !finished;
          }

          @Override
          public void close() throws Exception {}

          @Override
          public boolean isFinished() throws Exception {
            return finished;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 0;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 0;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    OperatorContext operatorContext = driverContext.getOperatorContexts().get(1);

    return new HashAggregationOperator(
        operatorContext,
        childOperator,
        Collections.singletonList(TimestampType.TIMESTAMP),
        Collections.singletonList(0),
        Collections.emptyList(),
        AggregationNode.Step.SINGLE,
        DEFAULT_GROUP_NUMBER,
        Long.MAX_VALUE,
        false,
        Long.MAX_VALUE);
  }

  @Test
  public void groupMoreThan1024Test() {
    try (HashAggregationOperator aggregationOperator = genHashAggregationOperator2()) {
      ListenableFuture<?> listenableFuture = aggregationOperator.isBlocked();
      listenableFuture.get();
      while (!aggregationOperator.isFinished() && aggregationOperator.hasNext()) {
        aggregationOperator.next();
        listenableFuture = aggregationOperator.isBlocked();
        listenableFuture.get();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // construct a AggregationHashOperator has more than 1024 groups in input TsBlock
  private HashAggregationOperator genHashAggregationOperator2() {

    // Construct operator tree
    QueryId queryId = new QueryId("stub_query");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(
            instanceId,
            IoTDBThreadPoolFactory.newFixedThreadPool(
                1, "aggregationHashOperator-test-instance-notification"));
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, TableScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(2, planNodeId2, HashAggregationOperator.class.getSimpleName());
    Operator childOperator =
        new Operator() {
          boolean finished = false;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(ImmutableList.of(TSDataType.TIMESTAMP, TSDataType.INT32));
            ColumnBuilder[] columnBuilders = builder.getValueColumnBuilders();
            for (int i = 0; i < 1025; i++) {
              columnBuilders[0].writeLong(i);
            }
            for (int i = 0; i < 1025; i++) {
              columnBuilders[1].writeInt(i);
            }
            builder.declarePositions(1025);
            TsBlock result =
                builder.build(
                    new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
            finished = true;
            return result;
          }

          @Override
          public boolean hasNext() {
            return !finished;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return finished;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 0;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 0;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    OperatorContext operatorContext = driverContext.getOperatorContexts().get(1);

    GroupedAggregator firstAggregator =
        new GroupedAggregator(
            createGroupedAccumulator(
                FIRST.getFunctionName(),
                getAggregationTypeByFuncName(FIRST.getFunctionName()),
                ImmutableList.of(TSDataType.INT32, TSDataType.TIMESTAMP),
                Collections.emptyList(),
                Collections.emptyMap(),
                true,
                false),
            AggregationNode.Step.SINGLE,
            TSDataType.INT32,
            ImmutableList.of(1, 0),
            OptionalInt.empty());
    GroupedAggregator lastAggregator =
        new GroupedAggregator(
            createGroupedAccumulator(
                LAST.getFunctionName(),
                getAggregationTypeByFuncName(LAST.getFunctionName()),
                ImmutableList.of(TSDataType.INT32, TSDataType.TIMESTAMP),
                Collections.emptyList(),
                Collections.emptyMap(),
                true,
                false),
            AggregationNode.Step.SINGLE,
            TSDataType.INT32,
            ImmutableList.of(1, 0),
            OptionalInt.empty());
    GroupedAggregator sumAggregator =
        new GroupedAggregator(
            createGroupedAccumulator(
                SUM.getFunctionName(),
                getAggregationTypeByFuncName(SUM.getFunctionName()),
                ImmutableList.of(TSDataType.INT32),
                Collections.emptyList(),
                Collections.emptyMap(),
                true,
                false),
            AggregationNode.Step.SINGLE,
            TSDataType.DOUBLE,
            ImmutableList.of(1),
            OptionalInt.empty());
    GroupedAggregator extremeAggregator =
        new GroupedAggregator(
            createGroupedAccumulator(
                EXTREME.getFunctionName(),
                getAggregationTypeByFuncName(EXTREME.getFunctionName()),
                ImmutableList.of(TSDataType.INT32),
                Collections.emptyList(),
                Collections.emptyMap(),
                true,
                false),
            AggregationNode.Step.SINGLE,
            TSDataType.INT32,
            ImmutableList.of(1),
            OptionalInt.empty());

    return new HashAggregationOperator(
        operatorContext,
        childOperator,
        ImmutableList.of(IntType.INT32),
        Collections.singletonList(1),
        ImmutableList.of(firstAggregator, lastAggregator, sumAggregator, extremeAggregator),
        AggregationNode.Step.SINGLE,
        DEFAULT_GROUP_NUMBER,
        Long.MAX_VALUE,
        false,
        Long.MAX_VALUE);
  }
}
