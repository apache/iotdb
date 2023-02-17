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

import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SingleColumnMergerTest {

  @Test
  public void mergeTest1() {
    SingleColumnMerger merger =
        new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator());

    TsBlockBuilder inputBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    inputBuilder.getTimeColumnBuilder().writeLong(2);
    inputBuilder.getColumnBuilder(0).writeInt(20);
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(4);
    inputBuilder.getColumnBuilder(0).writeInt(40);
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(5);
    inputBuilder.getColumnBuilder(0).appendNull();
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(6);
    inputBuilder.getColumnBuilder(0).writeInt(60);
    inputBuilder.declarePosition();

    TsBlock[] inputTsBlocks = new TsBlock[] {inputBuilder.build()};
    int[] inputIndex = new int[] {1};
    int[] updatedInputIndex = new int[] {1};

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(4);
    builder.declarePosition();
    timeColumnBuilder.writeLong(5);
    builder.declarePosition();
    timeColumnBuilder.writeLong(6);
    builder.declarePosition();
    timeColumnBuilder.writeLong(7);
    builder.declarePosition();
    ColumnBuilder valueColumnBuilder = builder.getColumnBuilder(0);

    merger.mergeColumn(
        inputTsBlocks, inputIndex, updatedInputIndex, timeColumnBuilder, 7, valueColumnBuilder);

    assertEquals(4, updatedInputIndex[0]);

    Column result = valueColumnBuilder.build();

    assertEquals(4, result.getPositionCount());
    assertFalse(result.isNull(0));
    assertEquals(40, result.getInt(0));
    assertTrue(result.isNull(1));
    assertFalse(result.isNull(2));
    assertEquals(60, result.getInt(2));
    assertTrue(result.isNull(3));
  }

  /** input tsblock is null */
  @Test
  public void mergeTest2() {
    SingleColumnMerger merger =
        new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator());

    TsBlock[] inputTsBlocks = new TsBlock[1];
    int[] inputIndex = new int[] {0};
    int[] updatedInputIndex = new int[] {0};

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(4);
    builder.declarePosition();
    timeColumnBuilder.writeLong(5);
    builder.declarePosition();
    timeColumnBuilder.writeLong(6);
    builder.declarePosition();
    timeColumnBuilder.writeLong(7);
    builder.declarePosition();
    ColumnBuilder valueColumnBuilder = builder.getColumnBuilder(0);

    merger.mergeColumn(
        inputTsBlocks, inputIndex, updatedInputIndex, timeColumnBuilder, 7, valueColumnBuilder);

    assertEquals(0, updatedInputIndex[0]);

    Column result = valueColumnBuilder.build();

    assertEquals(4, result.getPositionCount());
    assertTrue(result.isNull(0));
    assertTrue(result.isNull(1));
    assertTrue(result.isNull(2));
    assertTrue(result.isNull(3));
  }

  /** current time of input tsblock is larger than endTime of timeBuilder in asc order */
  @Test
  public void mergeTest3() {
    SingleColumnMerger merger =
        new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator());

    TsBlockBuilder inputBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    inputBuilder.getTimeColumnBuilder().writeLong(8);
    inputBuilder.getColumnBuilder(0).writeInt(80);
    inputBuilder.declarePosition();

    TsBlock[] inputTsBlocks = new TsBlock[] {inputBuilder.build()};
    int[] inputIndex = new int[] {0};
    int[] updatedInputIndex = new int[] {0};

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(4);
    builder.declarePosition();
    timeColumnBuilder.writeLong(5);
    builder.declarePosition();
    timeColumnBuilder.writeLong(6);
    builder.declarePosition();
    timeColumnBuilder.writeLong(7);
    builder.declarePosition();
    ColumnBuilder valueColumnBuilder = builder.getColumnBuilder(0);

    merger.mergeColumn(
        inputTsBlocks, inputIndex, updatedInputIndex, timeColumnBuilder, 7, valueColumnBuilder);

    assertEquals(0, updatedInputIndex[0]);

    Column result = valueColumnBuilder.build();

    assertEquals(4, result.getPositionCount());
    assertTrue(result.isNull(0));
    assertTrue(result.isNull(1));
    assertTrue(result.isNull(2));
    assertTrue(result.isNull(3));
  }

  /** current time of input tsblock is less than endTime of timeBuilder in desc order */
  @Test
  public void mergeTest4() {
    SingleColumnMerger merger =
        new SingleColumnMerger(new InputLocation(0, 0), new DescTimeComparator());

    TsBlockBuilder inputBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    inputBuilder.getTimeColumnBuilder().writeLong(2);
    inputBuilder.getColumnBuilder(0).writeInt(20);
    inputBuilder.declarePosition();

    TsBlock[] inputTsBlocks = new TsBlock[] {inputBuilder.build()};
    int[] inputIndex = new int[] {0};
    int[] updatedInputIndex = new int[] {0};

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(7);
    builder.declarePosition();
    timeColumnBuilder.writeLong(6);
    builder.declarePosition();
    timeColumnBuilder.writeLong(5);
    builder.declarePosition();
    timeColumnBuilder.writeLong(4);
    builder.declarePosition();
    ColumnBuilder valueColumnBuilder = builder.getColumnBuilder(0);

    merger.mergeColumn(
        inputTsBlocks, inputIndex, updatedInputIndex, timeColumnBuilder, 4, valueColumnBuilder);

    assertEquals(0, updatedInputIndex[0]);

    Column result = valueColumnBuilder.build();

    assertEquals(4, result.getPositionCount());
    assertTrue(result.isNull(0));
    assertTrue(result.isNull(1));
    assertTrue(result.isNull(2));
    assertTrue(result.isNull(3));
  }
}
