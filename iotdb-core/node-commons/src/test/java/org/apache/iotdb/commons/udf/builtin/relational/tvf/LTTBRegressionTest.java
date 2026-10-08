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

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LTTBRegressionTest {
  private static final long NANOSECOND_EPOCH = 1_780_000_000_000_000_000L;

  @Test
  public void testTargetCountIsInvariantUnderTimestampTranslation() {
    double[] values = {0, 1, 8, 2, 0, 0, 9, 1, -3, 0};
    long[] times = new long[values.length];
    for (int i = 0; i < times.length; i++) {
      times[i] = i;
    }
    for (int n : new int[] {3, 4, 5}) {
      int[] expected = LTTBTableFunction.selectTargetCount(times, values, times.length, n);
      for (long origin : new long[] {NANOSECOND_EPOCH, Long.MIN_VALUE, Long.MAX_VALUE - 10}) {
        long[] shifted = Arrays.stream(times).map(time -> time + origin).toArray();
        assertArrayEquals(
            expected, LTTBTableFunction.selectTargetCount(shifted, values, shifted.length, n));
      }
    }
  }

  @Test
  public void testWindowsAreInvariantUnderTimestampTranslation() {
    Double[][] rows = {{0.0}, {1.0}, {8.0}, {2.0}, {0.0}, {0.0}};
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      for (long slide : new long[] {1, 2, 3}) {
        List<ColumnBuilder> expected = sample(mode, 2, slide, 0, rows);
        List<ColumnBuilder> actual = sample(mode, 2, slide, NANOSECOND_EPOCH, rows);
        int timeIndex = actual.size() - 2;
        assertEquals(
            expected.get(timeIndex).getPositionCount(), actual.get(timeIndex).getPositionCount());
        for (int i = 0; i < actual.get(timeIndex).getPositionCount(); i++) {
          assertEquals(
              expected.get(timeIndex).build().getLong(i),
              actual.get(timeIndex).build().getLong(i) - NANOSECOND_EPOCH);
          assertEquals(
              expected.get(timeIndex + 1).build().getDouble(i),
              actual.get(timeIndex + 1).build().getDouble(i),
              0.0);
        }
      }
    }
  }

  @Test
  public void testWindowsSkipNullBucketsIndependentlyPerColumn() {
    Double[][] rows = {
      {0.0, 0.0}, {1.0, 0.0}, {2.0, 0.0},
      {null, 0.0}, {null, 1.0}, {null, 2.0},
      {null, null}, {null, null}, {null, null},
      {0.0, 0.0}, {0.0, 0.0}, {0.0, 0.0}
    };
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      List<ColumnBuilder> out = sample(mode, 3, 3, 0, rows);
      int timeIndex = mode.equals("TIME_WINDOW") ? 2 : 1;
      assertEquals(4, out.get(0).getPositionCount());
      assertEquals(2, out.get(timeIndex).build().getLong(0));
      assertTrue(out.get(timeIndex).build().isNull(1));
      assertTrue(out.get(timeIndex).build().isNull(2));
      assertEquals(9, out.get(timeIndex).build().getLong(3));
      assertEquals(2, out.get(timeIndex + 2).build().getLong(0));
      assertEquals(5, out.get(timeIndex + 2).build().getLong(1));
      assertTrue(out.get(timeIndex + 2).build().isNull(2));
      // This column keeps its own anchor at (5, 2), so the final bucket chooses t=9.
      assertEquals(9, out.get(timeIndex + 2).build().getLong(3));
    }
  }

  @Test
  public void testTrailingNullWindowsUseFinalNonemptyBucketFallback() {
    Double[][] rows = {{0.0}, {5.0}, {0.0}, {null}, {null}, {null}};
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      List<ColumnBuilder> out = sample(mode, 3, 3, 0, rows);
      int timeIndex = out.size() - 2;
      assertEquals(2, out.get(timeIndex).getPositionCount());
      assertEquals(1, out.get(timeIndex).build().getLong(0));
      assertTrue(out.get(timeIndex).build().isNull(1));
    }
  }

  @Test
  public void testTargetColumnsShareMemoryBudget() {
    TableFunctionDataProcessor processor = processor("TARGET_COUNT", 3, 3, 0, 8);
    Record input = record(0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0);
    try {
      for (int i = 0; i < 350_000; i++) {
        processor.process(input, Collections.emptyList(), null);
      }
      fail(
          "Expected a controlled memory-limit exception before buffering 350000 rows in eight columns");
    } catch (SemanticException expected) {
      // Match the same localized message as the window-memory regression suite.
      assertEquals(
          String.format(
              CommonMessages
                  .EXCEPTION_LTTB_BUFFERS_EXCEED_THE_PER_PROCESSOR_MEMORY_LIMIT_OF_ARG_BYTES_954FEF00,
              LTTBTableFunction.MAX_BUFFER_BYTES),
          expected.getMessage());
    } finally {
      processor.beforeDestroy();
    }
  }

  private static List<ColumnBuilder> sample(
      String mode, long size, long slide, long origin, Double[][] rows) {
    int columns = rows[0].length;
    TableFunctionDataProcessor processor = processor(mode, size, slide, origin, columns);
    List<ColumnBuilder> builders = new ArrayList<>();
    builders.add(new LongColumnBuilder(null, 16));
    if (mode.equals("TIME_WINDOW")) {
      builders.add(new LongColumnBuilder(null, 16));
    }
    for (int i = 0; i < columns; i++) {
      builders.add(new LongColumnBuilder(null, 16));
      builders.add(new DoubleColumnBuilder(null, 16));
    }
    try {
      for (int i = 0; i < rows.length; i++) {
        processor.process(record(origin + i, rows[i]), builders, null);
      }
      processor.finish(builders, null);
      return builders;
    } finally {
      processor.beforeDestroy();
    }
  }

  private static TableFunctionDataProcessor processor(
      String mode, long size, long slide, long origin, int columns) {
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(LTTBTableFunction.MODE_PROPERTY, mode)
            .addProperty(LTTBTableFunction.PARTITION_TYPES_PROPERTY, "")
            .addProperty(
                LTTBTableFunction.PARTICIPANT_TYPES_PROPERTY,
                String.join(",", Collections.nCopies(columns, "DOUBLE")))
            .addProperty(LTTBTableFunction.N_PARAMETER_NAME, size)
            .addProperty(LTTBTableFunction.SIZE_PARAMETER_NAME, size)
            .addProperty(LTTBTableFunction.SLIDE_PARAMETER_NAME, slide)
            .addProperty(LTTBTableFunction.ORIGIN_PARAMETER_NAME, origin)
            .build();
    return new LTTBTableFunction().getProcessorProvider(handle).getDataProcessor();
  }

  private static Record record(long time, Double... values) {
    // A lightweight row avoids Mockito invocation bookkeeping in the memory-limit test.
    return (Record)
        Proxy.newProxyInstance(
            Record.class.getClassLoader(),
            new Class<?>[] {Record.class},
            (proxy, method, arguments) -> {
              int index = (int) arguments[0];
              switch (method.getName()) {
                case "getLong":
                  return time;
                case "getDouble":
                  return values[index - 1];
                case "isNull":
                  return index != 0 && values[index - 1] == null;
                default:
                  throw new UnsupportedOperationException(method.getName());
              }
            });
  }
}
