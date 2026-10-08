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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class LTTBWindowMemoryTest {

  @Test
  public void testCountWindowOverlapFailsBeforeExhaustingHeap() {
    TableFunctionDataProcessor processor = processor(false, 1_000_000, 1, Type.DOUBLE);
    Record input = record(0, 1.0);
    try {
      // Without the shared budget, 5000 rows retain over 280 MiB despite every individual
      // window being far below MAX_BUFFERED_POINTS. This test also runs with a 128 MiB heap.
      assertMemoryLimit(
          () -> {
            for (int i = 0; i < 5000; i++) {
              processor.process(input, Collections.emptyList(), null);
            }
          },
          LTTBTableFunction.MAX_WINDOW_BUFFER_BYTES);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testTimeWindowCreationIsBoundedEvenForNullInput() {
    TableFunctionDataProcessor processor = processor(true, 1_000_000, 1, Type.DOUBLE);
    try {
      // One row belongs to a million overlapping time windows. Initial buffer allocation
      // must be charged even when the input column contains no eligible points.
      assertMemoryLimit(
          () -> processor.process(record(0, (Number) null), Collections.emptyList(), null),
          LTTBTableFunction.MAX_WINDOW_BUFFER_BYTES);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testColumnsShareTheWindowBudget() {
    TableFunctionDataProcessor processor = processor(false, 1_000_000, 1, Type.DOUBLE, Type.INT64);
    Record input = record(0, 1.0, Long.MAX_VALUE);
    try {
      assertMemoryLimit(
          () -> {
            for (int i = 0; i < 1800; i++) {
              processor.process(input, Collections.emptyList(), null);
            }
          },
          LTTBTableFunction.MAX_WINDOW_BUFFER_BYTES);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testArrayGrowthReservesOldAndNewIntegralArrays() {
    LTTBTableFunction.PointBuffer buffer =
        new LTTBTableFunction.PointBuffer(true, new LTTBTableFunction.WindowMemoryBudget(4096));
    for (int i = 0; i < 64; i++) {
      buffer.addIntegral(i, Long.MAX_VALUE);
    }
    // The retained 128-point buffer fits in 4096 bytes, but the old 64-point arrays and
    // replacement arrays coexist during growth. Reject before replacing any array.
    assertMemoryLimit(() -> buffer.addIntegral(64, Long.MAX_VALUE), 4096);
    assertEquals(64, buffer.size());
    assertEquals(63, buffer.time(63));
  }

  @Test
  public void testInitialBuffersShareCapacityBudget() {
    LTTBTableFunction.WindowMemoryBudget budget = new LTTBTableFunction.WindowMemoryBudget(4096);
    new LTTBTableFunction.PointBuffer(true, budget);
    new LTTBTableFunction.PointBuffer(true, budget);
    assertMemoryLimit(() -> new LTTBTableFunction.PointBuffer(true, budget), 4096);
  }

  @Test
  public void testEmittedWindowsReleaseBudgetForLongStream() {
    TableFunctionDataProcessor processor = processor(false, 1, 1, Type.DOUBLE);
    Record input = record(0, 42.0);
    List<ColumnBuilder> builders = builders(false);
    long outputRows = 0;
    try {
      // Total allocations exceed the budget, but only two windows remain live at a time.
      for (int row = 0; row < 80_000; row++) {
        processor.process(input, builders, null);
        if (row % 512 == 511) {
          outputRows += builders.get(0).getPositionCount();
          builders = builders(false);
        }
      }
      processor.finish(builders, null);
      outputRows += builders.get(0).getPositionCount();
      assertEquals(80_000, outputRows);
      assertEquals(42.0, builders.get(2).build().getDouble(0), 0.0);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testWindowResultsArePreserved() {
    for (boolean timeWindow : new boolean[] {false, true}) {
      TableFunctionDataProcessor processor = processor(timeWindow, 2, 2, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(timeWindow);
      double[] values = {0, 1, 8, 2, 0, 0};
      try {
        for (int time = 0; time < values.length; time++) {
          processor.process(record(time, values[time]), builders, null);
        }
        processor.finish(builders, null);
        ColumnBuilder times = builders.get(timeWindow ? 2 : 1);
        assertEquals(3, times.getPositionCount());
        assertEquals(1, times.build().getLong(0));
        assertEquals(2, times.build().getLong(1));
        assertEquals(4, times.build().getLong(2));
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  private static TableFunctionDataProcessor processor(
      boolean timeWindow, long size, long slide, Type... types) {
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(
                LTTBTableFunction.MODE_PROPERTY, timeWindow ? "TIME_WINDOW" : "COUNT_WINDOW")
            .addProperty(LTTBTableFunction.PARTITION_TYPES_PROPERTY, "")
            .addProperty(
                LTTBTableFunction.PARTICIPANT_TYPES_PROPERTY,
                WindowTVFUtils.joinTypes(Arrays.asList(types)))
            .addProperty(LTTBTableFunction.SIZE_PARAMETER_NAME, size)
            .addProperty(LTTBTableFunction.SLIDE_PARAMETER_NAME, slide)
            .addProperty(LTTBTableFunction.ORIGIN_PARAMETER_NAME, 0L)
            .build();
    return new LTTBTableFunction().getProcessorProvider(handle).getDataProcessor();
  }

  private static List<ColumnBuilder> builders(boolean timeWindow) {
    if (timeWindow) {
      return Arrays.asList(
          new LongColumnBuilder(null, 16),
          new LongColumnBuilder(null, 16),
          new LongColumnBuilder(null, 16),
          new DoubleColumnBuilder(null, 16));
    }
    return Arrays.asList(
        new LongColumnBuilder(null, 16),
        new LongColumnBuilder(null, 16),
        new DoubleColumnBuilder(null, 16));
  }

  private static Record record(long time, Number... values) {
    // Avoid retaining invocation histories in the long-stream memory regression test.
    Record record = mock(Record.class, withSettings().stubOnly());
    when(record.getLong(0)).thenReturn(time);
    for (int i = 0; i < values.length; i++) {
      when(record.isNull(i + 1)).thenReturn(values[i] == null);
      if (values[i] instanceof Long) {
        when(record.getLong(i + 1)).thenReturn(values[i].longValue());
      } else if (values[i] != null) {
        when(record.getDouble(i + 1)).thenReturn(values[i].doubleValue());
      }
    }
    return record;
  }

  private static void assertMemoryLimit(Runnable action, long limit) {
    try {
      action.run();
      fail("Expected the shared LTTB window memory limit");
    } catch (SemanticException e) {
      assertEquals(
          String.format(CommonMessages.LTTB_WINDOW_BUFFER_MEMORY_LIMIT_EXCEEDED, limit),
          e.getMessage());
    }
  }
}
