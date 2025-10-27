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

package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank;

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

public class NTileFunctionTest {
  private final List<TSDataType> inputDataTypes = Collections.singletonList(TSDataType.INT32);
  private final List<TSDataType> outputDataTypes =
      Arrays.asList(TSDataType.INT32, TSDataType.INT64);

  @Test
  public void testNTileFunctionWhenNIsLarge() {
    int n = 5;
    int[] inputs = {1, 2, 3};
    int[] expected = {1, 2, 3};

    TsBlock tsBlock = createTsBlockWithN(inputs, n);
    NTileFunction function = new NTileFunction(1);
    List<Integer> sortedColumns = Collections.singletonList(0);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function, sortedColumns);

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
      Assert.assertEquals(column.getLong(i), expected[i]);
    }
  }

  @Test
  public void testNTileFunctionUniform() {
    int n = 3;
    int[] inputs = {1, 2, 3, 4, 5, 6};
    int[] expected = {1, 1, 2, 2, 3, 3};

    TsBlock tsBlock = createTsBlockWithN(inputs, n);
    NTileFunction function = new NTileFunction(1);
    List<Integer> sortedColumns = Collections.singletonList(0);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function, sortedColumns);

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
      Assert.assertEquals(column.getLong(i), expected[i]);
    }
  }

  @Test
  public void testNTileFunctionNonUniform() {
    int n = 3;
    int[] inputs = {1, 2, 3, 4, 5, 6, 7};
    int[] expected = {1, 1, 1, 2, 2, 3, 3};

    TsBlock tsBlock = createTsBlockWithN(inputs, n);
    NTileFunction function = new NTileFunction(1);
    List<Integer> sortedColumns = Collections.singletonList(0);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function, sortedColumns);

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
      Assert.assertEquals(column.getLong(i), expected[i]);
    }
  }

  private static TsBlock createTsBlockWithN(int[] inputs, int n) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      columnBuilders[0].writeInt(input);
      columnBuilders[1].writeInt(n);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }
}
