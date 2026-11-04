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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AccumulatorFactory;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedOrderedDeltaAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedOrderedIncreaseAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate.GroupedOrderedRateAccumulator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RateBatchInputTest {
  @Test
  public void testOrderedValidationAcrossBatches() {
    // An invalid later row must not undo accepted rows or advance the ordered state itself.
    for (RateFunctionType function : RateFunctionType.values()) {
      TableAccumulator single = new OrderedIrateAccumulator(TSDataType.DOUBLE);
      GroupedAccumulator grouped =
          switch (function) {
            case RATE -> new GroupedOrderedRateAccumulator(TSDataType.DOUBLE);
            case INCREASE -> new GroupedOrderedIncreaseAccumulator(TSDataType.DOUBLE);
            case DELTA -> new GroupedOrderedDeltaAccumulator(TSDataType.DOUBLE);
            case IRATE -> null;
          };
      if (grouped != null) grouped.setGroupCount(2);
      Consumer<Column[]> add =
          columns -> {
            AggregationMask mask = AggregationMask.createSelectAll(columns[0].getPositionCount());
            if (grouped == null) single.addInput(columns, mask);
            else grouped.addInput(new int[] {1, 1}, columns, mask);
          };
      for (double invalid :
          new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
        single.reset();
        if (grouped != null) grouped.reset();
        Column[] input = orderedArguments(function, new double[] {2, invalid}, new long[] {10, 20});
        assertThrows(SemanticException.class, () -> add.accept(input));
        add.accept(orderedArguments(function, new double[] {4}, new long[] {20}));
        ColumnBuilder result = new DoubleColumnBuilder(null, 1);
        if (grouped == null) single.evaluateFinal(result);
        else grouped.evaluateFinal(1, result);
        assertFalse(result.build().isNull(0));
        Column[] duplicate = orderedArguments(function, new double[] {8}, new long[] {20});
        Column[] descending = orderedArguments(function, new double[] {8}, new long[] {15});
        assertThrows(SemanticException.class, () -> add.accept(duplicate));
        assertThrows(SemanticException.class, () -> add.accept(descending));
      }
      single.reset();
      if (grouped != null) grouped.reset();
      Column[] negative = orderedArguments(function, new double[] {-2}, new long[] {10});
      if (function.isCounter()) assertThrows(SemanticException.class, () -> add.accept(negative));
      else add.accept(negative);
    }
  }

  private static Column[] orderedArguments(
      RateFunctionType function, double[] values, long[] times) {
    int count = values.length;
    Column value = new DoubleColumn(count, Optional.empty(), values);
    Column time = new LongColumn(count, Optional.empty(), times);
    long[] ends = new long[count];
    Arrays.fill(ends, 60);
    return function.isWindowed()
        ? new Column[] {
          value,
          time,
          new LongColumn(count, Optional.empty(), new long[count]),
          new LongColumn(count, Optional.empty(), ends)
        }
        : new Column[] {value, time};
  }

  @Test
  public void testAllImplementationsWithMaskedNullsAndLogicalGroupIds() {
    MemoryReservationManager memory = new NoOpMemoryReservationManager();
    for (TSDataType type :
        new TSDataType[] {
          TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
        }) {
      ColumnBuilder values = Type.fromTsDataType(type).createColumnBuilder(6);
      for (int value : new int[] {1, -1, 99, 2, 4, 8}) {
        if (value < 0) {
          values.appendNull();
          continue;
        }
        switch (type) {
          case INT32 -> values.writeInt(value);
          case INT64 -> values.writeLong(value);
          case FLOAT -> values.writeFloat(value);
          case DOUBLE -> values.writeDouble(value);
          default -> throw new AssertionError(type);
        }
      }
      Column column = values.build();
      for (RateFunctionType function : RateFunctionType.values()) {
        TAggregationType aggregation = TAggregationType.valueOf(function.name());
        Column[] arguments = arguments(column, function);
        AggregationMask mask =
            AggregationMask.createSelectedPositions(6, new int[] {0, 1, 3, 4, 5}, 5);
        TableAccumulator reference =
            AccumulatorFactory.createBuiltinAccumulator(
                aggregation,
                Collections.singletonList(type),
                AggregationNode.Step.SINGLE,
                true,
                memory);
        for (int position : new int[] {0, 1, 3, 4, 5}) {
          Column[] row = new Column[arguments.length];
          for (int i = 0; i < row.length; i++) row[i] = arguments[i].getRegion(position, 1);
          reference.addInput(row, AggregationMask.createSelectAll(1));
        }
        ColumnBuilder expectedOut = new DoubleColumnBuilder(null, 1);
        reference.evaluateFinal(expectedOut);
        double expected = expectedOut.build().getDouble(0);
        for (boolean ordered : new boolean[] {true, false}) {
          TableAccumulator single =
              AccumulatorFactory.createBuiltinAccumulator(
                  aggregation,
                  Collections.singletonList(type),
                  AggregationNode.Step.SINGLE,
                  ordered,
                  memory);
          GroupedAccumulator grouped =
              AccumulatorFactory.createGroupedAccumulator(
                  function.getFunctionName(),
                  aggregation,
                  Collections.singletonList(type),
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true,
                  false,
                  AggregationNode.Step.SINGLE,
                  ordered,
                  memory,
                  null);
          grouped.setGroupCount(3);
          single.addInput(arguments, mask);
          grouped.addInput(new int[] {2, 1, 0, 2, 2, 2}, arguments, mask);
          ColumnBuilder out = new DoubleColumnBuilder(null, 1);
          single.evaluateFinal(out);
          assertEquals(expected, out.build().getDouble(0), 0);
          out = new DoubleColumnBuilder(null, 1);
          grouped.evaluateFinal(2, out);
          assertEquals(expected, out.build().getDouble(0), 0);
          out = new DoubleColumnBuilder(null, 1);
          grouped.evaluateFinal(1, out);
          assertTrue(out.build().isNull(0));
          // A missing time is ignored for a null value, but rejected for a non-null value.
          single.reset();
          grouped.reset();
          Column[] invalid = arguments(column, function);
          invalid[1] =
              new LongColumn(
                  6,
                  Optional.of(new boolean[] {true, true, false, false, false, false}),
                  new long[] {10, 11, 20, 30, 40, 50});
          assertThrows(SemanticException.class, () -> single.addInput(invalid, mask));
          assertThrows(SemanticException.class, () -> grouped.addInput(new int[6], invalid, mask));
        }
      }
    }
  }

  private static Column[] arguments(Column values, RateFunctionType function) {
    Column times =
        new LongColumn(
            6,
            Optional.of(new boolean[] {false, true, false, false, false, false}),
            new long[] {10, 11, 20, 30, 40, 50});
    return function.isWindowed()
        ? new Column[] {
          values,
          times,
          new LongColumn(6, Optional.empty(), new long[6]),
          new LongColumn(6, Optional.empty(), new long[] {60, 60, 60, 60, 60, 60})
        }
        : new Column[] {values, times};
  }

  private static final class NoOpMemoryReservationManager implements MemoryReservationManager {

    @Override
    public void reserveMemoryCumulatively(long size) {}

    @Override
    public void reserveMemoryImmediately() {}

    @Override
    public void reserveMemoryImmediately(long size) {}

    @Override
    public void releaseMemoryCumulatively(long size) {}

    @Override
    public void releaseMemoryImmediately(long size) {}

    @Override
    public void releaseAllReservedMemory() {}

    @Override
    public Pair<Long, Long> releaseMemoryVirtually(long size) {
      return new Pair<>(0L, 0L);
    }

    @Override
    public void reserveMemoryVirtually(long bytesToBeReserved, long bytesAlreadyReserved) {}

    @Override
    public void setHighestPriority(boolean isHighestPriority) {}
  }
}
