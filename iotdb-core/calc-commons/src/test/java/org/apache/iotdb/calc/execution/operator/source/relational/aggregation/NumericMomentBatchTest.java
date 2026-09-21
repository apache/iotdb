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

import org.apache.iotdb.calc.execution.aggregation.Accumulator;
import org.apache.iotdb.calc.execution.aggregation.CentralMomentAccumulator;
import org.apache.iotdb.calc.execution.aggregation.CentralMomentAccumulator.MomentType;
import org.apache.iotdb.calc.execution.aggregation.VarianceAccumulator;
import org.apache.iotdb.calc.execution.aggregation.VarianceAccumulator.VarianceType;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedCentralMomentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedVarianceAccumulator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.BitMap;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NumericMomentBatchTest {
  private static final TSDataType[] TYPES = {
    TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
  };

  @Test
  public void testTreeTableAndGroupedVarianceAndMoments() {
    for (TSDataType type : TYPES) {
      Column column = column(type);
      // The selected non-null distribution is [-3, -1, 1, 3].
      int[] selected = {0, 2, 3, 5, 6};
      AggregationMask mask = AggregationMask.createSelectedPositions(7, selected, selected.length);
      BitMap bitmap = new BitMap(7);
      for (int position : selected) bitmap.mark(position);
      Accumulator[] trees = {
        new VarianceAccumulator(type, VarianceType.VAR_POP),
        new CentralMomentAccumulator(type, MomentType.SKEWNESS),
        new CentralMomentAccumulator(type, MomentType.KURTOSIS)
      };
      TableAccumulator[] tables = {
        new TableVarianceAccumulator(type, VarianceType.VAR_POP),
        new TableCentralMomentAccumulator(type, MomentType.SKEWNESS),
        new TableCentralMomentAccumulator(type, MomentType.KURTOSIS)
      };
      GroupedAccumulator[] grouped = {
        new GroupedVarianceAccumulator(type, VarianceType.VAR_POP),
        new GroupedCentralMomentAccumulator(type, MomentType.SKEWNESS),
        new GroupedCentralMomentAccumulator(type, MomentType.KURTOSIS)
      };
      double[] expected = {5, 0, -1.2};
      Column times = new LongColumn(7, Optional.empty(), new long[] {0, 1, 2, 3, 4, 5, 6});
      for (int i = 0; i < trees.length; i++) {
        // Reusing each accumulator after reset also covers stale state across batches.
        for (int repeat = 0; repeat < 2; repeat++) {
          trees[i].addInput(new Column[] {times, column}, bitmap);
          ColumnBuilder out = new DoubleColumnBuilder(null, 1);
          trees[i].outputFinal(out);
          assertEquals(expected[i], out.build().getDouble(0), 1e-12);
          tables[i].addInput(new Column[] {column}, mask);
          out = new DoubleColumnBuilder(null, 1);
          tables[i].evaluateFinal(out);
          assertEquals(expected[i], out.build().getDouble(0), 1e-12);
          grouped[i].setGroupCount(3);
          grouped[i].addInput(new int[] {2, 0, 2, 1, 0, 2, 2}, new Column[] {column}, mask);
          out = new DoubleColumnBuilder(null, 1);
          grouped[i].evaluateFinal(2, out);
          assertEquals(expected[i], out.build().getDouble(0), 1e-12);
          out = new DoubleColumnBuilder(null, 1);
          grouped[i].evaluateFinal(1, out);
          assertTrue(out.build().isNull(0));
          trees[i].reset();
          tables[i].reset();
          grouped[i].reset();
        }
      }
      TableVarianceAccumulator variance = new TableVarianceAccumulator(type, VarianceType.VAR_POP);
      variance.addInput(new Column[] {column}, mask);
      variance.removeInput(new Column[] {column.getRegion(6, 1)});
      ColumnBuilder out = new DoubleColumnBuilder(null, 1);
      variance.evaluateFinal(out);
      assertEquals(8.0 / 3, out.build().getDouble(0), 1e-12);
    }
  }

  @Test
  public void testLogicalRegionAndRunLengthEncodedInputs() {
    for (TSDataType type : TYPES) {
      for (Column column :
          new Column[] {
            column(type).getRegion(2, 4),
            new RunLengthEncodedColumn(column(type).getRegion(0, 1), 8),
            new RunLengthEncodedColumn(column(type).getRegion(3, 1), 8)
          }) {
        double expected = 0;
        int count = 0;
        for (int i = 0; i < column.getPositionCount(); i++) {
          if (!column.isNull(i)) {
            expected += Type.fromTsDataType(type).getDouble(column, i);
            count++;
          }
        }
        AvgAccumulator avg = new AvgAccumulator(type);
        avg.addInput(
            new Column[] {column}, AggregationMask.createSelectAll(column.getPositionCount()));
        ColumnBuilder out = new DoubleColumnBuilder(null, 1);
        avg.evaluateFinal(out);
        Column actual = out.build();
        if (count == 0) assertTrue(actual.isNull(0));
        else assertEquals(expected / count, actual.getDouble(0), 0);
      }
    }
  }

  private static Column column(TSDataType type) {
    ColumnBuilder builder = Type.fromTsDataType(type).createColumnBuilder(7);
    int[] values = {-3, 100, -1, 99, 100, 1, 3};
    for (int i = 0; i < values.length; i++) {
      if (i == 3) {
        builder.appendNull();
        continue;
      }
      switch (type) {
        case INT32 -> builder.writeInt(values[i]);
        case INT64 -> builder.writeLong(values[i]);
        case FLOAT -> builder.writeFloat(values[i]);
        case DOUBLE -> builder.writeDouble(values[i]);
        default -> throw new AssertionError(type);
      }
    }
    return builder.build();
  }
}
