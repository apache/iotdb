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
      // W0 reports F(t0) and its pick(t2); W1 has no eligible point for the first column, so the
      // first column emits no cell there while the second column keeps its own chain; the last
      // window reports its pick and then L(t11).
      assertEquals(5, out.get(0).getPositionCount());
      assertEquals(0, out.get(timeIndex).build().getLong(0));
      assertEquals(2, out.get(timeIndex).build().getLong(1));
      assertTrue(out.get(timeIndex).build().isNull(2));
      assertEquals(9, out.get(timeIndex).build().getLong(3));
      assertEquals(11, out.get(timeIndex).build().getLong(4));
      assertEquals(0, out.get(timeIndex + 2).build().getLong(0));
      assertEquals(2, out.get(timeIndex + 2).build().getLong(1));
      assertEquals(5, out.get(timeIndex + 2).build().getLong(2));
      assertEquals(9, out.get(timeIndex + 2).build().getLong(3));
      assertEquals(11, out.get(timeIndex + 2).build().getLong(4));
    }
  }

  @Test
  public void testTrailingNullWindowsUseFinalNonemptyBucketFallback() {
    Double[][] rows = {{0.0}, {5.0}, {0.0}, {null}, {null}, {null}};
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      List<ColumnBuilder> out = sample(mode, 3, 3, 0, rows);
      int timeIndex = out.size() - 2;
      // W0 reports F(t0), its pick(t1) and L(t2); W1 is all NULL and emits no row.
      assertEquals(3, out.get(timeIndex).getPositionCount());
      assertEquals(0, out.get(timeIndex).build().getLong(0));
      assertEquals(1, out.get(timeIndex).build().getLong(1));
      assertEquals(2, out.get(timeIndex).build().getLong(2));
      assertEquals(5.0, out.get(timeIndex + 1).build().getDouble(1), 0.0);
    }
  }

  /**
   * SIZE defines a bucket, not the input range of a separate standard LTTB invocation: the buckets
   * share one anchor chain per partition and column, so an overlapping window continues from the
   * point the preceding window selected instead of restarting at its own first point. Bucket {@code
   * [1,4)} selecting {@code t=1} is only reachable with a chained anchor: with a fresh anchor at
   * its own first point the largest triangle of that bucket would be {@code t=2} instead. Every
   * window emits exactly one row, so the overlapping run yields one row per window.
   */
  @Test
  public void testOverlappingWindowsContinueTheAnchorChain() {
    Double[][] rows = {{0.0}, {1.0}, {8.0}, {2.0}, {0.0}, {0.0}};

    // Count buckets [0,3), [1,4), ..., [5,8) over six rows. F(t0) is reported by the first bucket
    // and L(t5) by the last one, so the overlap yields seven rows: W0 t0,t2; W1 t1; W2 t2; W3 t3;
    // W4 t4; W5 t4,t5. W1 selecting t=1 continues from the point W0 selected; a fresh anchor at its
    // own first point would pick t=2 instead.
    List<ColumnBuilder> countWindow = sample("COUNT_WINDOW", 3, 1, 0, rows);
    Column countPicks = countWindow.get(countWindow.size() - 2).build();
    assertEquals(7, countPicks.getPositionCount());
    assertEquals(0, countPicks.getLong(0));
    assertEquals(2, countPicks.getLong(1));
    assertEquals(1, countPicks.getLong(2));
    assertEquals(2, countPicks.getLong(3));
    assertEquals(3, countPicks.getLong(4));
    assertEquals(4, countPicks.getLong(5));
    assertEquals(5, countPicks.getLong(6));

    // Time buckets [0,3), [1,4), ... with SLIDE = 1 a row also opens the buckets that started
    // before the first timestamp but have not ended yet, so eight buckets cover the six rows and
    // keep the same one-chain rule. W0 reports t0 and its pick t2, W1 picks t1 from the chained
    // anchor, and the last bucket reports only L(t5) because its own bucket holds just t4.
    List<ColumnBuilder> timeWindow = sample("TIME_WINDOW", 3, 1, 0, rows);
    Column timePicks = timeWindow.get(timeWindow.size() - 2).build();
    assertEquals(8, timePicks.getPositionCount());
  }

  /**
   * Both endpoints are buckets of their own, so two eligible points in one window emit two rows
   * even though the window bucket between them is empty: {@code F} and then {@code L}.
   */
  @Test
  public void testTwoEligiblePointsEmitTheirOwnRows() {
    Double[][] rows = {{0.0}, {8.0}};
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      List<ColumnBuilder> out = sample(mode, 3, 3, 0, rows);
      int timeIndex = out.size() - 2;
      assertEquals(2, out.get(timeIndex).getPositionCount());
      assertEquals(0, out.get(timeIndex).build().getLong(0));
      assertEquals(0.0, out.get(timeIndex + 1).build().getDouble(0), 0.0);
      assertEquals(1, out.get(timeIndex).build().getLong(1));
      assertEquals(8.0, out.get(timeIndex + 1).build().getDouble(1), 0.0);
    }
  }

  /**
   * A non-finite point stays eligible, but it never serves as {@code A} or {@code C}: the last
   * window looks ahead to the last finite eligible point instead of the non-finite last point, so
   * the selections do not depend on whether the final value is finite.
   */
  @Test
  public void testWindowModeIgnoresNonFiniteValuesAsTriangleVertices() {
    Double[][] finiteTail = {{0.0}, {5.0}, {1.0}, {2.0}, {9.0}, {3.0}, {0.0}};
    Double[][] nonFiniteTail = {{0.0}, {5.0}, {1.0}, {2.0}, {9.0}, {3.0}, {Double.NaN}};
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      int timeIndex = mode.equals("TIME_WINDOW") ? 2 : 1;
      List<ColumnBuilder> finite = sample(mode, 3, 3, 0, finiteTail);
      List<ColumnBuilder> nonFinite = sample(mode, 3, 3, 0, nonFiniteTail);
      // W0 reports F(t0) and picks t1; W1 picks t4; the last window reports L. The last row of
      // the non-finite run is the non-finite point itself, which is still returned.
      assertEquals(4, nonFinite.get(timeIndex).getPositionCount());
      assertEquals(0, nonFinite.get(timeIndex).build().getLong(0));
      assertEquals(1, nonFinite.get(timeIndex).build().getLong(1));
      assertEquals(4, nonFinite.get(timeIndex).build().getLong(2));
      assertEquals(6, nonFinite.get(timeIndex).build().getLong(3));
      assertEquals(5.0, nonFinite.get(timeIndex + 1).build().getDouble(1), 0.0);
      assertEquals(9.0, nonFinite.get(timeIndex + 1).build().getDouble(2), 0.0);
      assertTrue(Double.isNaN(nonFinite.get(timeIndex + 1).build().getDouble(3)));
      for (int i = 0; i < 3; i++) {
        assertEquals(
            finite.get(timeIndex).build().getLong(i), nonFinite.get(timeIndex).build().getLong(i));
      }
    }
  }

  /**
   * A column whose only eligible point in a window is non-finite still returns it as the pick of
   * that window, and the columns stay aligned by output position.
   */
  @Test
  public void testWindowModeReturnsNonFiniteCandidate() {
    Double[][] rows = {{0.0, 1.0}, {1.0, 2.0}, {2.0, 3.0}, {3.0, Double.NaN}};
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      List<ColumnBuilder> out = sample(mode, 2, 2, 0, rows);
      int timeIndex = mode.equals("TIME_WINDOW") ? 2 : 1;
      // W0 reports F(t0) for both columns and picks t1; W1 picks t2 and reports L(t3). The second
      // column's last eligible point is non-finite, so it is returned as L of the last window.
      assertEquals(4, out.get(timeIndex).getPositionCount());
      assertEquals(0, out.get(timeIndex).build().getLong(0));
      assertEquals(1, out.get(timeIndex).build().getLong(1));
      assertEquals(2, out.get(timeIndex).build().getLong(2));
      assertEquals(3, out.get(timeIndex).build().getLong(3));
      assertEquals(0.0, out.get(timeIndex + 1).build().getDouble(0), 0.0);
      assertEquals(1.0, out.get(timeIndex + 1).build().getDouble(1), 0.0);
      assertEquals(2.0, out.get(timeIndex + 1).build().getDouble(2), 0.0);
      assertEquals(3.0, out.get(timeIndex + 1).build().getDouble(3), 0.0);
      assertEquals(1.0, out.get(timeIndex + 3).build().getDouble(0), 0.0);
      assertEquals(2.0, out.get(timeIndex + 3).build().getDouble(1), 0.0);
      assertEquals(3.0, out.get(timeIndex + 3).build().getDouble(2), 0.0);
      assertTrue(Double.isNaN(out.get(timeIndex + 3).build().getDouble(3)));
    }
  }

  /**
   * A window without any eligible point emits no row, like M4: the number of rows is the largest
   * number of points selected by a participant column, so an all-NULL partition emits nothing.
   */
  @Test
  public void testWindowWithNoEligiblePointsEmitsNoRow() {
    Double[][] rows = {{null}, {null}, {null}, {null}};
    for (String mode : new String[] {"COUNT_WINDOW", "TIME_WINDOW"}) {
      List<ColumnBuilder> out = sample(mode, 2, 2, 0, rows);
      int timeIndex = out.size() - 2;
      assertEquals(0, out.get(timeIndex).getPositionCount());
      assertEquals(0, out.get(timeIndex + 1).getPositionCount());
    }
  }

  @Test
  public void testTargetColumnsShareMemoryBudget() {
    TableFunctionDataProcessor processor = processor("TARGET_COUNT", 3, 3, 0, 8);
    try {
      for (int i = 0; i < 350_000; i++) {
        // timestamps must be unique within the partition, so advance the input time
        processor.process(
            record(i, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), Collections.emptyList(), null);
      }
      fail(
          "Expected a controlled memory-limit exception before buffering 350000 rows in eight columns");
    } catch (SemanticException expected) {
      // Match the same localized message as the window-memory regression suite.
      assertEquals(
          String.format(
              CommonMessages
                  .EXCEPTION_LTTB_BUFFERS_EXCEED_LTTB_BUFFER_SIZE_IN_BYTES_ARG_BYTES_26BDE62D,
              LTTBTableFunction.DEFAULT_MAX_BUFFER_BYTES),
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
