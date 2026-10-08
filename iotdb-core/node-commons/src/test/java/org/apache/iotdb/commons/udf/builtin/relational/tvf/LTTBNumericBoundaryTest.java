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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class LTTBNumericBoundaryTest {

  @Test
  public void testInt64TargetCountPreservesPeakAtLargeBaselines() {
    Long[][] values = {{0L}, {1L}, {8L}, {2L}, {0L}, {0L}};
    for (long offset : new long[] {0, 1L << 60, Long.MIN_VALUE, Long.MAX_VALUE - 8}) {
      Column[] output = sample("TARGET_COUNT", 3, 3, 0, times(6), translate(values, offset));
      assertArrayEquals(new long[] {0, 2, 5}, longValues(output[1]));
      assertArrayEquals(new long[] {offset, offset + 8, offset}, longValues(output[2]));
    }
  }

  @Test
  public void testInt64TargetCountUsesRelativeNextBucketAverage() {
    Long[][] values = {{0L}, {1L}, {8L}, {2L}, {0L}, {0L}, {9L}, {1L}, {3L}, {0L}};
    long[][] expected = {{0, 6, 9}, {0, 2, 6, 9}, {0, 2, 4, 6, 9}};
    for (long offset : new long[] {1L << 60, Long.MIN_VALUE, Long.MAX_VALUE - 9}) {
      for (int n = 3; n <= 5; n++) {
        Column[] output = sample("TARGET_COUNT", n, n, 0, times(10), translate(values, offset));
        assertArrayEquals(expected[n - 3], longValues(output[1]));
        for (int row = 0; row < output[1].getPositionCount(); row++) {
          int selectedTime = (int) output[1].getLong(row);
          assertEquals(offset + values[selectedTime][0], output[2].getLong(row));
        }
      }
    }
  }

  @Test
  public void testInt64WindowsKeepIndependentAnchorsAcrossNullBuckets() {
    Long[][] values = {
      {0L, 0L}, {1L, 0L}, {2L, 0L},
      {null, 0L}, {null, 1L}, {null, 2L},
      {null, null}, {null, null}, {null, null},
      {0L, 0L}, {0L, 0L}, {0L, 0L}
    };
    long[] offsets = {1L << 60, Long.MIN_VALUE};
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      for (long slide : new long[] {1, 3, 4}) {
        Column[] expected = sample(mode, 3, slide, 0, times(12), values);
        Column[] actual = sample(mode, 3, slide, 0, times(12), translate(values, offsets));
        int firstValueColumn = mode.equals("TIME_WINDOW") ? 3 : 2;
        for (int column = 0; column < actual.length; column++) {
          assertEquals(expected[column].getPositionCount(), actual[column].getPositionCount());
          for (int row = 0; row < actual[column].getPositionCount(); row++) {
            assertEquals(expected[column].isNull(row), actual[column].isNull(row));
            if (!actual[column].isNull(row)) {
              long offset =
                  column >= firstValueColumn && (column - firstValueColumn) % 2 == 0
                      ? offsets[(column - firstValueColumn) / 2]
                      : 0;
              assertEquals(expected[column].getLong(row) + offset, actual[column].getLong(row));
            }
          }
        }
      }
    }
  }

  @Test
  public void testInt64DifferencesSpanningLongRangeDoNotWrap() {
    Long[][] values = {
      {Long.MIN_VALUE}, {0L}, {Long.MAX_VALUE}, {0L}, {Long.MIN_VALUE}, {Long.MIN_VALUE}
    };
    Column[] target = sample("TARGET_COUNT", 3, 3, 0, times(6), values);
    assertArrayEquals(new long[] {0, 2, 5}, longValues(target[1]));
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      Column[] output = sample(mode, 2, 2, 0, times(6), values);
      assertArrayEquals(new long[] {1, 2, 4}, longValues(output[output.length - 2]));
    }
  }

  @Test(timeout = 5000)
  public void testTimeWindowRejectsUnrepresentableEnd() {
    assertWindowOverflow(2, 2, 0, Long.MAX_VALUE - 1);
    assertWindowOverflow(1, 1, 0, Long.MAX_VALUE);
  }

  @Test(timeout = 5000)
  public void testTimeWindowRejectsUnrepresentableStart() {
    assertWindowOverflow(3, 3, 0, Long.MIN_VALUE);
  }

  @Test(timeout = 5000)
  public void testTimeWindowAllowsIntermediateOverflow() {
    for (long origin : new long[] {0, Long.MIN_VALUE, Long.MAX_VALUE - 1}) {
      Column[] output =
          sample("TIME_WINDOW", 2, 2, origin, new long[] {Long.MIN_VALUE}, new Long[][] {{7L}});
      assertEquals(1, output[0].getPositionCount());
      assertEquals(Long.MIN_VALUE, output[0].getLong(0));
      assertEquals(Long.MIN_VALUE + 2, output[1].getLong(0));
      assertEquals(Long.MIN_VALUE, output[2].getLong(0));
      assertEquals(7L, output[3].getLong(0));
    }
  }

  @Test(timeout = 5000)
  public void testTimeWindowStopsAdvancingPastLongMax() {
    Column[] output =
        sample(
            "TIME_WINDOW",
            2,
            3,
            Long.MAX_VALUE - 2,
            new long[] {Long.MAX_VALUE - 2, Long.MAX_VALUE - 1, Long.MAX_VALUE},
            new Long[][] {{0L}, {8L}, {1L}});
    assertEquals(1, output[0].getPositionCount());
    assertEquals(Long.MAX_VALUE - 2, output[0].getLong(0));
    assertEquals(Long.MAX_VALUE, output[1].getLong(0));
    // Both candidate endpoints form zero-area triangles. The last input is in the following gap.
    assertEquals(Long.MAX_VALUE - 2, output[2].getLong(0));
  }

  @Test(timeout = 5000)
  public void testTimeWindowSkipsGapBeforeUnrepresentableNextStart() {
    Column[] output =
        sample(
            "TIME_WINDOW",
            2,
            3,
            Long.MAX_VALUE - 2,
            new long[] {Long.MAX_VALUE},
            new Long[][] {{1L}});
    assertEquals(0, output[0].getPositionCount());
  }

  private static void assertWindowOverflow(long size, long slide, long origin, long time) {
    SemanticException failure =
        assertThrows(
            SemanticException.class,
            () ->
                sample("TIME_WINDOW", size, slide, origin, new long[] {time}, new Long[][] {{1L}}));
    assertEquals(
        CommonMessages.EXCEPTION_LTTB_WINDOW_BOUNDARIES_EXCEED_THE_TIMESTAMP_RANGE_7C0FC7E2,
        failure.getMessage());
  }

  private static long[] longValues(Column column) {
    long[] values = new long[column.getPositionCount()];
    for (int i = 0; i < values.length; i++) {
      values[i] = column.getLong(i);
    }
    return values;
  }

  private static long[] times(int count) {
    long[] times = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
    }
    return times;
  }

  private static Long[][] translate(Long[][] values, long... offsets) {
    Long[][] translated = new Long[values.length][offsets.length];
    for (int i = 0; i < values.length; i++) {
      for (int j = 0; j < offsets.length; j++) {
        translated[i][j] = values[i][j] == null ? null : Math.addExact(values[i][j], offsets[j]);
      }
    }
    return translated;
  }

  private static Column[] sample(
      String mode, long size, long slide, long origin, long[] times, Long[][] values) {
    int columns = values[0].length;
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(LTTBTableFunction.MODE_PROPERTY, mode)
            .addProperty(LTTBTableFunction.PARTITION_TYPES_PROPERTY, "")
            .addProperty(
                LTTBTableFunction.PARTICIPANT_TYPES_PROPERTY,
                String.join(",", Collections.nCopies(columns, "INT64")))
            .addProperty(LTTBTableFunction.N_PARAMETER_NAME, size)
            .addProperty(LTTBTableFunction.SIZE_PARAMETER_NAME, size)
            .addProperty(LTTBTableFunction.SLIDE_PARAMETER_NAME, slide)
            .addProperty(LTTBTableFunction.ORIGIN_PARAMETER_NAME, origin)
            .build();
    TableFunctionDataProcessor processor =
        new LTTBTableFunction().getProcessorProvider(handle).getDataProcessor();
    List<ColumnBuilder> builders = new ArrayList<>();
    int outputColumns = (mode.equals("TIME_WINDOW") ? 2 : 1) + 2 * columns;
    for (int i = 0; i < outputColumns; i++) {
      builders.add(new LongColumnBuilder(null, 16));
    }
    try {
      for (int i = 0; i < times.length; i++) {
        processor.process(record(times[i], values[i]), builders, null);
      }
      processor.finish(builders, null);
      return builders.stream().map(ColumnBuilder::build).toArray(Column[]::new);
    } finally {
      processor.beforeDestroy();
    }
  }

  private static Record record(long time, Long[] values) {
    return (Record)
        Proxy.newProxyInstance(
            Record.class.getClassLoader(),
            new Class<?>[] {Record.class},
            (proxy, method, arguments) -> {
              int index = (int) arguments[0];
              switch (method.getName()) {
                case "getLong":
                  return index == 0 ? time : values[index - 1];
                case "isNull":
                  return index != 0 && values[index - 1] == null;
                default:
                  throw new UnsupportedOperationException(method.getName());
              }
            });
  }
}
