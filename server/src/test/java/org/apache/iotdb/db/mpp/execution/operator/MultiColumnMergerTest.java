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

import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.MultiColumnMerger;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiColumnMergerTest {

  @Test
  public void mergeTest1() {
    MultiColumnMerger merger =
        new MultiColumnMerger(ImmutableList.of(new InputLocation(0, 0), new InputLocation(1, 0)));

    TsBlockBuilder inputBuilder1 = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    inputBuilder1.getTimeColumnBuilder().writeLong(2);
    inputBuilder1.getColumnBuilder(0).writeInt(20);
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(4);
    inputBuilder1.getColumnBuilder(0).writeInt(40);
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(5);
    inputBuilder1.getColumnBuilder(0).appendNull();
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(6);
    inputBuilder1.getColumnBuilder(0).writeInt(60);
    inputBuilder1.declarePosition();

    TsBlockBuilder inputBuilder2 = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    inputBuilder2.getTimeColumnBuilder().writeLong(8);
    inputBuilder2.getColumnBuilder(0).writeInt(800);
    inputBuilder2.declarePosition();
    inputBuilder2.getTimeColumnBuilder().writeLong(10);
    inputBuilder2.getColumnBuilder(0).writeInt(1000);
    inputBuilder2.declarePosition();

    TsBlock[] inputTsBlocks = new TsBlock[] {inputBuilder1.build(), inputBuilder2.build()};
    int[] inputIndex = new int[] {1, 0};
    int[] updatedInputIndex = new int[] {1, 0};

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
    timeColumnBuilder.writeLong(8);
    builder.declarePosition();
    timeColumnBuilder.writeLong(9);
    builder.declarePosition();
    timeColumnBuilder.writeLong(10);
    builder.declarePosition();
    timeColumnBuilder.writeLong(11);
    builder.declarePosition();
    ColumnBuilder valueColumnBuilder = builder.getColumnBuilder(0);

    merger.mergeColumn(
        inputTsBlocks, inputIndex, updatedInputIndex, timeColumnBuilder, 11, valueColumnBuilder);

    assertEquals(4, updatedInputIndex[0]);
    assertEquals(2, updatedInputIndex[1]);

    Column result = valueColumnBuilder.build();

    assertEquals(8, result.getPositionCount());
    assertFalse(result.isNull(0));
    assertEquals(40, result.getInt(0));
    assertTrue(result.isNull(1));
    assertFalse(result.isNull(2));
    assertEquals(60, result.getInt(2));
    assertTrue(result.isNull(3));
    assertFalse(result.isNull(4));
    assertEquals(800, result.getInt(4));
    assertTrue(result.isNull(5));
    assertFalse(result.isNull(6));
    assertEquals(1000, result.getInt(6));
    assertTrue(result.isNull(7));
  }

  @Test
  public void mergeTest2() {
    MultiColumnMerger merger =
        new MultiColumnMerger(ImmutableList.of(new InputLocation(0, 0), new InputLocation(1, 0)));

    TsBlockBuilder inputBuilder1 = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    inputBuilder1.getTimeColumnBuilder().writeLong(2);
    inputBuilder1.getColumnBuilder(0).writeInt(20);
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(4);
    inputBuilder1.getColumnBuilder(0).writeInt(40);
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(5);
    inputBuilder1.getColumnBuilder(0).appendNull();
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(6);
    inputBuilder1.getColumnBuilder(0).appendNull();
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(7);
    inputBuilder1.getColumnBuilder(0).appendNull();
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(8);
    inputBuilder1.getColumnBuilder(0).appendNull();
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(9);
    inputBuilder1.getColumnBuilder(0).appendNull();
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(10);
    inputBuilder1.getColumnBuilder(0).writeInt(100);
    inputBuilder1.declarePosition();

    TsBlockBuilder inputBuilder2 = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    inputBuilder2.getTimeColumnBuilder().writeLong(6);
    inputBuilder2.getColumnBuilder(0).writeInt(60);
    inputBuilder2.declarePosition();
    inputBuilder2.getTimeColumnBuilder().writeLong(7);
    inputBuilder2.getColumnBuilder(0).writeInt(70);
    inputBuilder2.declarePosition();
    inputBuilder2.getTimeColumnBuilder().writeLong(8);
    inputBuilder2.getColumnBuilder(0).writeInt(80);
    inputBuilder2.declarePosition();
    inputBuilder2.getTimeColumnBuilder().writeLong(9);
    inputBuilder2.getColumnBuilder(0).writeInt(90);
    inputBuilder2.declarePosition();

    TsBlock[] inputTsBlocks = new TsBlock[] {inputBuilder1.build(), inputBuilder2.build()};
    int[] inputIndex = new int[] {1, 0};
    int[] updatedInputIndex = new int[] {1, 0};

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
    timeColumnBuilder.writeLong(8);
    builder.declarePosition();
    timeColumnBuilder.writeLong(9);
    builder.declarePosition();
    timeColumnBuilder.writeLong(10);
    builder.declarePosition();
    timeColumnBuilder.writeLong(11);
    builder.declarePosition();
    ColumnBuilder valueColumnBuilder = builder.getColumnBuilder(0);

    merger.mergeColumn(
        inputTsBlocks, inputIndex, updatedInputIndex, timeColumnBuilder, 11, valueColumnBuilder);

    assertEquals(8, updatedInputIndex[0]);
    assertEquals(4, updatedInputIndex[1]);

    Column result = valueColumnBuilder.build();

    assertEquals(8, result.getPositionCount());
    assertFalse(result.isNull(0));
    assertEquals(40, result.getInt(0));
    assertTrue(result.isNull(1));
    assertFalse(result.isNull(2));
    assertEquals(60, result.getInt(2));
    assertFalse(result.isNull(3));
    assertEquals(70, result.getInt(3));
    assertFalse(result.isNull(4));
    assertEquals(80, result.getInt(4));
    assertFalse(result.isNull(5));
    assertEquals(90, result.getInt(5));
    assertFalse(result.isNull(6));
    assertEquals(100, result.getInt(6));
    assertTrue(result.isNull(7));
  }
}
