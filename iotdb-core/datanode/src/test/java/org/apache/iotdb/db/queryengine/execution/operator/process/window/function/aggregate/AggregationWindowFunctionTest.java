/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.TableWindowOperatorTestUtils;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.FunctionTestUtils;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.PartitionExecutor;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

// For aggregator that supports removeInputs, only SUM is tested, others are similar
public class AggregationWindowFunctionTest {
  private final List<TSDataType> inputDataTypes = Collections.singletonList(TSDataType.INT32);
  private final int[] inputs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  @Test
  public void testFrameExpansion() {
    List<TSDataType> outputDataTypes = Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE);
    double[] expected = {0, 1, 3, 6, 10, 15, 21, 28, 36, 45};

    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs);
    AggregationWindowFunction function =
        FunctionTestUtils.createAggregationWindowFunction(
            TAggregationType.SUM, TSDataType.INT32, TSDataType.DOUBLE, true);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.ROWS,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.CURRENT_ROW);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function, frameInfo);

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(expected.length, outputDataTypes);
    while (partitionExecutor.hasNext()) {
      partitionExecutor.processNextRow(tsBlockBuilder);
    }

    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    Column column = result.getColumn(1);

    Assert.assertEquals(column.getPositionCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      // This floating point are integers, no delta is needed
      Assert.assertEquals(column.getDouble(i), expected[i], 0);
    }
  }

  @Test
  public void testNotRemovableAggregationReComputation() {
    List<TSDataType> outputDataTypes = Arrays.asList(TSDataType.INT32, TSDataType.INT32);
    int[] expected = {0, 0, 0, 1, 2, 3, 4, 5, 6, 7};

    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 2, 2);
    AggregationWindowFunction function =
        FunctionTestUtils.createAggregationWindowFunction(
            TAggregationType.MIN, TSDataType.INT32, TSDataType.INT32, true);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.ROWS,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function, frameInfo);

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(expected.length, outputDataTypes);
    while (partitionExecutor.hasNext()) {
      partitionExecutor.processNextRow(tsBlockBuilder);
    }

    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    Column column = result.getColumn(1);

    Assert.assertEquals(column.getPositionCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      // This floating point are integers, no delta is needed
      Assert.assertEquals(column.getInt(i), expected[i], 0);
    }
  }

  @Test
  public void testAggregationNoReComputation() {
    List<TSDataType> outputDataTypes = Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE);
    double[] expected = {3, 6, 10, 15, 20, 25, 30, 35, 30, 24};

    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 2, 2);
    AggregationWindowFunction function =
        FunctionTestUtils.createAggregationWindowFunction(
            TAggregationType.SUM, TSDataType.INT32, TSDataType.DOUBLE, true);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.ROWS,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function, frameInfo);

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(expected.length, outputDataTypes);
    while (partitionExecutor.hasNext()) {
      partitionExecutor.processNextRow(tsBlockBuilder);
    }

    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    Column column = result.getColumn(1);

    Assert.assertEquals(column.getPositionCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      // This floating point are integers, no delta is needed
      Assert.assertEquals(column.getDouble(i), expected[i], 0);
    }
  }

  @Test
  public void testAggregationReComputation() {
    List<TSDataType> inputDataTypes = Collections.singletonList(TSDataType.INT32);
    int[] inputs = {1, 1, 1, 1, 3, 3, 3, 3, 5, 5};

    List<TSDataType> outputDataTypes = Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE);
    double[] expected = {4, 4, 4, 4, 12, 12, 12, 12, 10, 10};

    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs);
    AggregationWindowFunction function =
        FunctionTestUtils.createAggregationWindowFunction(
            TAggregationType.SUM, TSDataType.INT32, TSDataType.DOUBLE, true);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            -1,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            -1,
            0,
            SortOrder.ASC_NULLS_FIRST);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(
            tsBlock, inputDataTypes, function, frameInfo, Collections.singletonList(0));

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(expected.length, outputDataTypes);
    while (partitionExecutor.hasNext()) {
      partitionExecutor.processNextRow(tsBlockBuilder);
    }

    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    Column column = result.getColumn(1);

    Assert.assertEquals(column.getPositionCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      // This floating point are integers, no delta is needed
      Assert.assertEquals(column.getDouble(i), expected[i], 0);
    }
  }
}
