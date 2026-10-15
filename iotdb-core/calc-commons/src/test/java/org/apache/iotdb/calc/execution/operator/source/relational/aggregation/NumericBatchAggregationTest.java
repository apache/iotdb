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

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAvgAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedSumAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.RateFunctionType;
import org.apache.iotdb.calc.utils.TypeServices;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NumericBatchAggregationTest {
  private static final TSDataType[] TYPES = {
    TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
  };

  private Column column(TSDataType type) {
    Optional<boolean[]> nulls = Optional.of(new boolean[] {false, true, false, false});
    return switch (type) {
      case INT32 -> new IntColumn(4, nulls, new int[] {1, 99, -3, 5});
      case INT64 -> new LongColumn(4, nulls, new long[] {1, 99, -3, 5});
      case FLOAT -> new FloatColumn(4, nulls, new float[] {1, 99, -3, 5});
      case DOUBLE -> new DoubleColumn(4, nulls, new double[] {1, 99, -3, 5});
      default -> throw new AssertionError(type);
    };
  }

  @Test
  public void testAvgMaskedAddRemoveResetAndCopy() {
    for (TSDataType type : TYPES) {
      Column column = column(type);
      AvgAccumulator avg = new AvgAccumulator(type);
      avg.addInput(new Column[] {column}, AggregationMask.createSelectAll(4));
      assertEquals(1, value(avg), 0);
      avg.removeInput(new Column[] {column});
      avg.addInput(
          new Column[] {column},
          AggregationMask.createSelectedPositions(4, new int[] {1, 2, 3}, 3));
      assertEquals(1, value(avg), 0);
      TableAccumulator copy = avg.copy();
      DoubleColumnBuilder out = new DoubleColumnBuilder(null, 1);
      copy.evaluateFinal(out);
      assertTrue(out.build().isNull(0));
      avg.reset();
      avg.addInput(
          new Column[] {column}, AggregationMask.createSelectedPositions(4, new int[] {1}, 1));
      out = new DoubleColumnBuilder(null, 1);
      avg.evaluateFinal(out);
      assertTrue(out.build().isNull(0));
    }
  }

  @Test
  public void testAvgKeepsFloatingPointBatchOrder() {
    AvgAccumulator avg = new AvgAccumulator(TSDataType.DOUBLE);
    avg.addInput(
        new Column[] {new DoubleColumn(1, Optional.empty(), new double[] {1e16})},
        AggregationMask.createSelectAll(1));
    avg.addInput(
        new Column[] {new DoubleColumn(2, Optional.empty(), new double[] {-1e16, 1})},
        AggregationMask.createSelectAll(2));
    assertEquals(1.0 / 3, value(avg), 0);
  }

  @Test
  public void testGroupedMasksUseLogicalPositionAndIgnoreNulls() {
    int[] groups = {2, 1, 2, 0};
    for (TSDataType type : TYPES) {
      for (GroupedAccumulator accumulator :
          new GroupedAccumulator[] {
            new GroupedSumAccumulator(type), new GroupedAvgAccumulator(type)
          }) {
        accumulator.setGroupCount(4);
        accumulator.addInput(
            groups,
            new Column[] {column(type)},
            AggregationMask.createSelectedPositions(4, new int[] {1, 2, 3}, 3));
        assertEquals(5, groupedValue(accumulator, 0), 0);
        assertEquals(-3, groupedValue(accumulator, 2), 0);
        DoubleColumnBuilder out = new DoubleColumnBuilder(null, 1);
        accumulator.evaluateFinal(1, out);
        assertTrue(out.build().isNull(0));
        accumulator.reset();
        accumulator.addInput(
            groups, new Column[] {column(type)}, AggregationMask.createSelectAll(4));
        assertEquals(
            accumulator instanceof GroupedSumAccumulator ? -2 : -1,
            groupedValue(accumulator, 2),
            0);
      }
    }
  }

  @Test
  public void testExtremeBatchMatchesScalarForNaNZeroAndIntegerLimits() {
    Column[] columns = {
      new IntColumn(4, Optional.empty(), new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 1}),
      new LongColumn(4, Optional.empty(), new long[] {Long.MIN_VALUE, Long.MAX_VALUE, -1, 1}),
      new FloatColumn(
          4, Optional.empty(), new float[] {Float.NaN, -0.0f, 0.0f, Float.POSITIVE_INFINITY}),
      new DoubleColumn(
          4, Optional.empty(), new double[] {Double.NaN, -0.0, 0.0, Double.NEGATIVE_INFINITY})
    };
    for (int i = 0; i < TYPES.length; i++) {
      Type type = Type.fromTsDataType(TYPES[i]);
      TypeServices.ColumnValueUpdater[] scalars = {
        TypeServices.MIN_COLUMN_VALUE_UPDATER_SERVICE.call(type),
        TypeServices.MAX_COLUMN_VALUE_UPDATER_SERVICE.call(type),
        TypeServices.EXTREME_COLUMN_VALUE_UPDATER_SERVICE.call(type)
      };
      TableAccumulator[] accumulators = {
        new MinAccumulator(TYPES[i]), new MaxAccumulator(TYPES[i]), new ExtremeAccumulator(TYPES[i])
      };
      for (int k = 0; k < scalars.length; k++) {
        for (int[] positions : new int[][] {{0, 1, 2, 3}, {1, 2, 3}, {3, 0, 1}}) {
          TsPrimitiveType expected = type.getTsPrimitiveType();
          boolean initialized = false;
          for (int position : positions)
            initialized |= scalars[k].update(expected, columns[i], position, initialized);
          accumulators[k].reset();
          accumulators[k].addInput(
              new Column[] {columns[i]},
              AggregationMask.createSelectedPositions(4, positions, positions.length));
          ColumnBuilder out = type.createColumnBuilder(1);
          accumulators[k].evaluateFinal(out);
          Column result = out.build();
          if (TYPES[i] == TSDataType.FLOAT) {
            assertEquals(
                Float.floatToIntBits(expected.getFloat()),
                Float.floatToIntBits(result.getFloat(0)));
          } else if (TYPES[i] == TSDataType.DOUBLE) {
            assertEquals(
                Double.doubleToLongBits(expected.getDouble()),
                Double.doubleToLongBits(result.getDouble(0)));
          } else {
            assertEquals(expected, result.getTsPrimitiveType(0));
          }
        }
      }
    }
  }

  @Test
  public void testRateReaderValidationAndSelectedOrder() {
    for (TSDataType type : TYPES) {
      double[] sum = {0};
      TypeServices.RATE_INPUT_SERVICE
          .call(Type.fromTsDataType(type))
          .addInput(
              column(type),
              AggregationMask.createSelectedPositions(4, new int[] {1, 3, 0}, 3),
              RateFunctionType.RATE,
              (position, value) -> sum[0] = sum[0] * 10 + value);
      assertEquals(51, sum[0], 0);
      assertThrows(
          SemanticException.class,
          () ->
              TypeServices.RATE_INPUT_SERVICE
                  .call(Type.fromTsDataType(type))
                  .addInput(
                      column(type),
                      AggregationMask.createSelectAll(4),
                      RateFunctionType.RATE,
                      (position, value) -> {}));
    }
    assertThrows(
        SemanticException.class,
        () ->
            TypeServices.RATE_INPUT_SERVICE
                .call(Type.fromTsDataType(TSDataType.DOUBLE))
                .addInput(
                    new DoubleColumn(1, Optional.empty(), new double[] {Double.NaN}),
                    AggregationMask.createSelectAll(1),
                    RateFunctionType.DELTA,
                    (position, value) -> {}));
  }

  private double value(TableAccumulator accumulator) {
    DoubleColumnBuilder out = new DoubleColumnBuilder(null, 1);
    accumulator.evaluateFinal(out);
    return out.build().getDouble(0);
  }

  private double groupedValue(GroupedAccumulator accumulator, int group) {
    DoubleColumnBuilder out = new DoubleColumnBuilder(null, 1);
    accumulator.evaluateFinal(group, out);
    return out.build().getDouble(0);
  }
}
