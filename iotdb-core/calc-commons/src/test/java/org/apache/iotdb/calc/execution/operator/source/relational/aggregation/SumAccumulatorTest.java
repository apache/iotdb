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

import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SumAccumulatorTest {
  @Test
  public void testMixedNumericBatchesAndRemoval() {
    TSDataType[] types = {TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE};
    Optional<boolean[]> nulls = Optional.of(new boolean[] {false, true, false, false});
    Column[] columns = {
      new IntColumn(4, nulls, new int[] {1, 99, -3, 5}),
      new LongColumn(4, nulls, new long[] {1, 99, -3, 5}),
      new FloatColumn(4, nulls, new float[] {1, 99, -3, 5}),
      new DoubleColumn(4, nulls, new double[] {1, 99, -3, 5})
    };
    SumAccumulator[] accumulators = new SumAccumulator[4];
    for (int i = 0; i < 4; i++) {
      accumulators[i] = new SumAccumulator(types[i]);
      // AVG, grouped aggregations and rate validation still use the scalar converter API.
      assertEquals(
          -3.0,
          TypeServices.AGGREGATION_NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE
              .call(Type.fromTsDataType(types[i]))
              .create(IllegalArgumentException::new)
              .convert(columns[i], 2),
          0.0);
    }
    for (int batch = 0; batch < 32; batch++) {
      for (int i = 0; i < 4; i++) {
        SumAccumulator accumulator = accumulators[i];
        accumulator.addInput(new Column[] {columns[i]}, AggregationMask.createSelectAll(4));
        assertValue(accumulator, 3);
        accumulator.removeInput(new Column[] {columns[i]});
        assertValue(accumulator, 0);
        accumulator.reset();
        accumulator.addInput(
            new Column[] {columns[i]},
            AggregationMask.createSelectedPositions(4, new int[] {1, 2, 3}, 3));
        assertValue(accumulator, 2);
        accumulator.reset();
        DoubleColumnBuilder result = new DoubleColumnBuilder(null, 1);
        accumulator.evaluateFinal(result);
        assertTrue(result.build().isNull(0));
      }
    }
  }

  @Test
  public void testUnsupportedInputRetainsDeferredException() {
    SumAccumulator accumulator = new SumAccumulator(TSDataType.BOOLEAN);
    Column nulls = new BooleanColumn(1, Optional.of(new boolean[] {true}), new boolean[1]);
    accumulator.addInput(new Column[] {nulls}, AggregationMask.createSelectAll(1));
    accumulator.removeInput(new Column[] {nulls});
    Column value = new BooleanColumn(1, Optional.empty(), new boolean[] {true});
    assertThrows(
        IllegalArgumentException.class,
        () -> accumulator.addInput(new Column[] {value}, AggregationMask.createSelectAll(1)));
    assertThrows(
        IllegalArgumentException.class, () -> accumulator.removeInput(new Column[] {value}));
  }

  @Test
  public void testBatchBoundariesPreserveFloatingPointOrder() {
    SumAccumulator accumulator = new SumAccumulator(TSDataType.DOUBLE);
    Column seed = new DoubleColumn(1, Optional.empty(), new double[] {1e16});
    accumulator.addInput(new Column[] {seed}, AggregationMask.createSelectAll(1));
    // Summing the second batch independently would incorrectly produce 0 instead of 1.
    Column next = new DoubleColumn(2, Optional.empty(), new double[] {-1e16, 1});
    accumulator.addInput(new Column[] {next}, AggregationMask.createSelectAll(2));
    assertValue(accumulator, 1);
    accumulator.reset();
    accumulator.addInput(new Column[] {seed}, AggregationMask.createSelectAll(1));
    Column removed = new DoubleColumn(2, Optional.empty(), new double[] {1e16, 1});
    accumulator.removeInput(new Column[] {removed});
    assertValue(accumulator, -1);
  }

  @Test
  public void testNullBatchesRetainInitializationAndCopiesAreIndependent() {
    SumAccumulator accumulator = new SumAccumulator(TSDataType.INT32);
    Column nulls = new IntColumn(2, Optional.of(new boolean[] {true, true}), new int[2]);
    accumulator.addInput(new Column[] {nulls}, AggregationMask.createSelectAll(2));
    DoubleColumnBuilder result = new DoubleColumnBuilder(null, 1);
    accumulator.evaluateFinal(result);
    assertTrue(result.build().isNull(0));
    Column value = new IntColumn(1, Optional.empty(), new int[] {7});
    accumulator.addInput(new Column[] {value}, AggregationMask.createSelectAll(1));
    accumulator.addInput(new Column[] {nulls}, AggregationMask.createSelectAll(2));
    assertValue(accumulator, 7);
    TableAccumulator copy = accumulator.copy();
    copy.addInput(new Column[] {value}, AggregationMask.createSelectAll(1));
    accumulator.removeInput(new Column[] {value});
    assertValue(accumulator, 0);
    result = new DoubleColumnBuilder(null, 1);
    copy.evaluateFinal(result);
    assertEquals(7.0, result.build().getDouble(0), 0.0);
  }

  private void assertValue(SumAccumulator accumulator, double expected) {
    DoubleColumnBuilder result = new DoubleColumnBuilder(null, 1);
    accumulator.evaluateFinal(result);
    assertEquals(expected, result.build().getDouble(0), 0);
  }
}
