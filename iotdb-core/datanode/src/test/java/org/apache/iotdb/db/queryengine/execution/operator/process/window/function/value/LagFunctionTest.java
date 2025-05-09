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

package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.FunctionTestUtils;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.PartitionExecutor;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
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

public class LagFunctionTest {
  private final List<TSDataType> inputDataTypes = Collections.singletonList(TSDataType.INT32);
  // Inputs element less than 0 means this pos is null
  private final int[] inputs = {0, -1, -1, 1, 2, -1, 3, 4, -1, 5, 6, -1, -1, -1, -1, -1};

  private final List<TSDataType> outputDataTypes =
      Arrays.asList(TSDataType.INT32, TSDataType.INT32);

  @Test
  public void testLagFunctionIgnoreNullWithoutDefault() {
    int[] expected = {-1, -1, -1, -1, 0, 1, 1, 2, 3, 3, 4, 5, 5, 5, 5, 5};

    TsBlock tsBlock = createTsBlockWithoutDefault(inputs, 2);
    LagFunction function = new LagFunction(Arrays.asList(0, 1), true);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function);

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
      if (expected[i] < 0) {
        Assert.assertTrue(column.isNull(i));
      } else {
        Assert.assertEquals(expected[i], column.getInt(i));
      }
    }
  }

  @Test
  public void testLagFunctionIgnoreNullWithDefault() {
    int[] expected = {10, 10, 10, 10, 0, 1, 1, 2, 3, 3, 4, 5, 5, 5, 5, 5};

    TsBlock tsBlock = createTsBlockWithDefault(inputs, 2, 10);
    LagFunction function = new LagFunction(Arrays.asList(0, 1, 2), true);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function);

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
      Assert.assertEquals(expected[i], column.getInt(i));
    }
  }

  @Test
  public void testLagFunctionNotIgnoreNullWithoutDefault() {
    int[] expected = {-1, -1, 0, -1, -1, 1, 2, -1, 3, 4, -1, 5, 6, -1, -1, -1};

    TsBlock tsBlock = createTsBlockWithoutDefault(inputs, 2);
    LagFunction function = new LagFunction(Arrays.asList(0, 1), false);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function);

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
      if (expected[i] < 0) {
        Assert.assertTrue(column.isNull(i));
      } else {
        Assert.assertEquals(expected[i], column.getInt(i));
      }
    }
  }

  @Test
  public void testLagFunctionNotIgnoreNullWithDefault() {
    int[] expected = {10, 10, 0, -1, -1, 1, 2, -1, 3, 4, -1, 5, 6, -1, -1, -1};

    TsBlock tsBlock = createTsBlockWithDefault(inputs, 2, 10);
    LagFunction function = new LagFunction(Arrays.asList(0, 1, 2), false);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function);

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
      if (expected[i] < 0) {
        Assert.assertTrue(column.isNull(i));
      } else {
        Assert.assertEquals(expected[i], column.getInt(i));
      }
    }
  }

  private static TsBlock createTsBlockWithDefault(int[] inputs, int offset, int defaultValue) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      if (input >= 0) {
        columnBuilders[0].writeInt(input);
      } else {
        // Mimic null value
        columnBuilders[0].appendNull();
      }
      columnBuilders[1].writeInt(offset);
      columnBuilders[2].writeInt(defaultValue);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  private static TsBlock createTsBlockWithoutDefault(int[] inputs, int offset) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      if (input >= 0) {
        columnBuilders[0].writeInt(input);
      } else {
        // Mimic null value
        columnBuilders[0].appendNull();
      }
      columnBuilders[1].writeInt(offset);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }
}
