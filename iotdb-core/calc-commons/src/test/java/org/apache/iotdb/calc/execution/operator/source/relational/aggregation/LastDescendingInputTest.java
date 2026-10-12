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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LastDescendingInputTest {
  @Test
  public void testFinalizableInputsStopAfterFirstValidTime() {
    for (boolean lastBy : new boolean[] {false, true}) {
      for (boolean masked : new boolean[] {false, true}) {
        long[] timestamps = new long[1024];
        int[] values = new int[1024];
        for (int i = 0; i < values.length; i++) {
          values[i] = i;
          timestamps[i] = 1024 - i;
        }
        boolean[] nulls = new boolean[1024];
        nulls[0] = true;
        CountingTimeColumn time = new CountingTimeColumn(timestamps, Optional.of(nulls));
        Column value = new IntColumn(1024, Optional.empty(), values);
        TableAccumulator accumulator =
            lastBy
                ? new LastByDescAccumulator(
                    TSDataType.INT32, TSDataType.INT32, false, false, true, true, true)
                : new LastDescAccumulator(TSDataType.INT32, false, true, true);
        AggregationMask mask =
            masked
                ? AggregationMask.createSelectedPositions(1024, new int[] {0, 2, 4, 6}, 4)
                : AggregationMask.createSelectAll(1024);
        accumulator.addInput(
            lastBy ? new Column[] {value, value, time} : new Column[] {value, time}, mask);
        assertTrue(accumulator.hasFinalResult());
        assertEquals(1, time.readCount);
        IntColumnBuilder result = new IntColumnBuilder(null, 1);
        accumulator.evaluateFinal(result);
        assertEquals(masked ? 2 : 1, result.build().getInt(0));
      }
    }
  }

  @Test
  public void testNonFinalizableAndAscendingInputsKeepScanning() {
    for (TableAccumulator accumulator :
        new TableAccumulator[] {
          new LastAccumulator(TSDataType.INT32),
          new LastDescAccumulator(TSDataType.INT32, false, true, false),
          new LastByAccumulator(TSDataType.INT32, TSDataType.INT32, false, false),
          new LastByDescAccumulator(
              TSDataType.INT32, TSDataType.INT32, false, false, true, true, false)
        }) {
      CountingTimeColumn time = new CountingTimeColumn(new long[] {0, 1, 2}, Optional.empty());
      Column value = new IntColumn(3, Optional.empty(), new int[] {11, 22, 33});
      accumulator.addInput(
          accumulator instanceof LastByAccumulator
              ? new Column[] {value, value, time}
              : new Column[] {value, time},
          AggregationMask.createSelectAll(3));
      assertFalse(accumulator.hasFinalResult());
      assertEquals(3, time.readCount);
      IntColumnBuilder result = new IntColumnBuilder(null, 1);
      accumulator.evaluateFinal(result);
      assertEquals(33, result.build().getInt(0));
    }
  }

  private static class CountingTimeColumn extends LongColumn {
    private int readCount;

    private CountingTimeColumn(long[] values, Optional<boolean[]> nulls) {
      super(values.length, nulls, values);
    }

    @Override
    public long getLong(int position) {
      readCount++;
      return super.getLong(position);
    }
  }
}
