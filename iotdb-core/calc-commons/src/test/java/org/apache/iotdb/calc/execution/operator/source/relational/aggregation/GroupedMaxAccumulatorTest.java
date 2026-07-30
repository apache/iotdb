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

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedMaxAccumulator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class GroupedMaxAccumulatorTest {

  @Test
  public void testFloatMaxWithNonPositiveInput() {
    GroupedMaxAccumulator accumulator = new GroupedMaxAccumulator(TSDataType.FLOAT);
    accumulator.setGroupCount(4);
    TsBlock input = buildFloatBlock(0.0F, 0.0F, -3.5F, -1.25F, Float.NEGATIVE_INFINITY);

    accumulator.addInput(
        new int[] {0, 0, 1, 1, 2},
        new Column[] {input.getColumn(0)},
        AggregationMask.createSelectAll(input.getPositionCount()));

    ColumnBuilder resultBuilder = new FloatColumnBuilder(null, 4);
    accumulator.evaluateFinal(0, resultBuilder);
    accumulator.evaluateFinal(1, resultBuilder);
    accumulator.evaluateFinal(2, resultBuilder);
    accumulator.evaluateFinal(3, resultBuilder);
    Column result = resultBuilder.build();

    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(0.0F, result.getFloat(0), 0.0F);
    Assert.assertEquals(-1.25F, result.getFloat(1), 0.0F);
    Assert.assertEquals(Float.NEGATIVE_INFINITY, result.getFloat(2), 0.0F);
    Assert.assertTrue(result.isNull(3));
  }

  @Test
  public void testDoubleMaxWithNonPositiveInput() {
    GroupedMaxAccumulator accumulator = new GroupedMaxAccumulator(TSDataType.DOUBLE);
    accumulator.setGroupCount(4);
    TsBlock input = buildDoubleBlock(0.0, 0.0, -8.5, -2.75, Double.NEGATIVE_INFINITY);

    accumulator.addInput(
        new int[] {0, 0, 1, 1, 2},
        new Column[] {input.getColumn(0)},
        AggregationMask.createSelectAll(input.getPositionCount()));

    assertDoubleResults(accumulator);
  }

  @Test
  public void testDoubleMaxWithNonPositiveIntermediateInput() {
    GroupedMaxAccumulator accumulator = new GroupedMaxAccumulator(TSDataType.DOUBLE);
    accumulator.setGroupCount(4);
    TsBlock input = buildDoubleBlock(0.0, 0.0, -8.5, -2.75, Double.NEGATIVE_INFINITY);

    accumulator.addIntermediate(new int[] {0, 0, 1, 1, 2}, input.getColumn(0));

    assertDoubleResults(accumulator);
  }

  private void assertDoubleResults(GroupedMaxAccumulator accumulator) {
    ColumnBuilder resultBuilder = new DoubleColumnBuilder(null, 4);
    accumulator.evaluateFinal(0, resultBuilder);
    accumulator.evaluateFinal(1, resultBuilder);
    accumulator.evaluateFinal(2, resultBuilder);
    accumulator.evaluateFinal(3, resultBuilder);
    Column result = resultBuilder.build();

    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(0.0, result.getDouble(0), 0.0);
    Assert.assertEquals(-2.75, result.getDouble(1), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, result.getDouble(2), 0.0);
    Assert.assertTrue(result.isNull(3));
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
