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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedExtremeAccumulator;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ExtremeAccumulatorTest {

  @Test
  public void tableExtremeAccumulatorHandlesMinValues() {
    ExtremeAccumulator intAccumulator = new ExtremeAccumulator(TSDataType.INT32);
    TsBlock intBlock = buildIntBlock(Integer.MAX_VALUE, Integer.MIN_VALUE);
    intAccumulator.addInput(
        new org.apache.tsfile.block.column.Column[] {intBlock.getColumn(0)},
        AggregationMask.createSelectAll(intBlock.getPositionCount()));
    ColumnBuilder intResult = new IntColumnBuilder(null, 1);
    intAccumulator.evaluateFinal(intResult);
    Assert.assertEquals(Integer.MIN_VALUE, intResult.build().getInt(0));

    ExtremeAccumulator longAccumulator = new ExtremeAccumulator(TSDataType.INT64);
    TsBlock longBlock = buildLongBlock(Long.MAX_VALUE, Long.MIN_VALUE);
    longAccumulator.addInput(
        new org.apache.tsfile.block.column.Column[] {longBlock.getColumn(0)},
        AggregationMask.createSelectAll(longBlock.getPositionCount()));
    ColumnBuilder longResult = new LongColumnBuilder(null, 1);
    longAccumulator.evaluateFinal(longResult);
    Assert.assertEquals(Long.MIN_VALUE, longResult.build().getLong(0));
  }

  @Test
  public void groupedExtremeAccumulatorKeepsOriginalValueAndHandlesMinValues() {
    GroupedExtremeAccumulator intAccumulator = new GroupedExtremeAccumulator(TSDataType.INT32);
    intAccumulator.setGroupCount(1);
    TsBlock intBlock = buildIntBlock(-5, 4);
    intAccumulator.addInput(
        new int[] {0, 0},
        new org.apache.tsfile.block.column.Column[] {intBlock.getColumn(0)},
        AggregationMask.createSelectAll(intBlock.getPositionCount()));
    ColumnBuilder intResult = new IntColumnBuilder(null, 1);
    intAccumulator.evaluateFinal(0, intResult);
    Assert.assertEquals(-5, intResult.build().getInt(0));

    GroupedExtremeAccumulator longAccumulator = new GroupedExtremeAccumulator(TSDataType.INT64);
    longAccumulator.setGroupCount(1);
    TsBlock longBlock = buildLongBlock(Long.MAX_VALUE, Long.MIN_VALUE);
    longAccumulator.addIntermediate(new int[] {0, 0}, longBlock.getColumn(0));
    ColumnBuilder longResult = new LongColumnBuilder(null, 1);
    longAccumulator.evaluateFinal(0, longResult);
    Assert.assertEquals(Long.MIN_VALUE, longResult.build().getLong(0));
  }

  @Test
  public void groupedExtremeAccumulatorKeepsOriginalFloatingValues() {
    GroupedExtremeAccumulator floatAccumulator = new GroupedExtremeAccumulator(TSDataType.FLOAT);
    floatAccumulator.setGroupCount(1);
    TsBlock floatBlock = buildFloatBlock(-5.5f, 4.5f);
    floatAccumulator.addInput(
        new int[] {0, 0},
        new org.apache.tsfile.block.column.Column[] {floatBlock.getColumn(0)},
        AggregationMask.createSelectAll(floatBlock.getPositionCount()));
    ColumnBuilder floatResult = new FloatColumnBuilder(null, 1);
    floatAccumulator.evaluateFinal(0, floatResult);
    Assert.assertEquals(-5.5f, floatResult.build().getFloat(0), 0.001);

    GroupedExtremeAccumulator doubleAccumulator = new GroupedExtremeAccumulator(TSDataType.DOUBLE);
    doubleAccumulator.setGroupCount(1);
    TsBlock doubleBlock = buildDoubleBlock(-10.25, 9.25);
    doubleAccumulator.addInput(
        new int[] {0, 0},
        new org.apache.tsfile.block.column.Column[] {doubleBlock.getColumn(0)},
        AggregationMask.createSelectAll(doubleBlock.getPositionCount()));
    ColumnBuilder doubleResult = new DoubleColumnBuilder(null, 1);
    doubleAccumulator.evaluateFinal(0, doubleResult);
    Assert.assertEquals(-10.25, doubleResult.build().getDouble(0), 0.001);
  }

  private TsBlock buildIntBlock(int... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    ColumnBuilder valueBuilder = builder.getValueColumnBuilders()[0];
    for (int i = 0; i < values.length; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      valueBuilder.writeInt(values[i]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private TsBlock buildLongBlock(long... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    ColumnBuilder valueBuilder = builder.getValueColumnBuilders()[0];
    for (int i = 0; i < values.length; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      valueBuilder.writeLong(values[i]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private TsBlock buildFloatBlock(float... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.FLOAT));
    ColumnBuilder valueBuilder = builder.getValueColumnBuilders()[0];
    for (int i = 0; i < values.length; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      valueBuilder.writeFloat(values[i]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private TsBlock buildDoubleBlock(double... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.DOUBLE));
    ColumnBuilder valueBuilder = builder.getValueColumnBuilders()[0];
    for (int i = 0; i < values.length; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      valueBuilder.writeDouble(values[i]);
      builder.declarePosition();
    }
    return builder.build();
  }
}
