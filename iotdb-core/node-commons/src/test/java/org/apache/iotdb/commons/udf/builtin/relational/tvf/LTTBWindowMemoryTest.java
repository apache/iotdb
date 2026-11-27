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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.function.BatchedTableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class LTTBWindowMemoryTest {

  @Test
  public void testCountWindowOverlapFailsBeforeExhaustingHeap() {
    TableFunctionDataProcessor processor = processor(false, 1_000_000, 1, Type.DOUBLE);
    try {
      // Without the shared budget, 5000 rows retain over 280 MiB despite every individual
      // window being far below MAX_BUFFERED_POINTS. This test also runs with a 128 MiB heap.
      assertMemoryLimit(
          () -> {
            for (int i = 0; i < 5000; i++) {
              // timestamps must be unique within the partition, so advance the input time
              processor.process(record(i, 1.0), Collections.emptyList(), null);
            }
          },
          LTTBTableFunction.DEFAULT_MAX_BUFFER_BYTES);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testAllNullRowSkipsTimeWindowAllocation() {
    TableFunctionDataProcessor processor = processor(true, 1_000_000, 1, Type.DOUBLE);
    List<ColumnBuilder> builders = builders(true);
    try {
      processor.process(record(0, (Number) null), builders, null);
      processor.finish(builders, null);
      assertEquals(0, builders.get(0).getPositionCount());
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testAllNullRowsSkipCountWindowAllocation() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    long previousBudget = config.getLttbBufferSize();
    try {
      config.setLttbBufferSize(8 * 1024);
      TableFunctionDataProcessor processor = processor(false, 100_000, 1, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(false);
      try {
        for (int t = 0; t < 64; t++) {
          processor.process(record(t, (Number) null), builders, null);
        }
        processor.finish(builders, null);
        assertEquals(0, builders.get(0).getPositionCount());
      } finally {
        processor.beforeDestroy();
      }
    } finally {
      config.setLttbBufferSize(previousBudget);
    }
  }

  @Test
  public void testCountWindowsAfterNullRowsKeepTheirOriginalIndexes() {
    TableFunctionDataProcessor processor = processor(false, 5, 2, Type.DOUBLE);
    List<ColumnBuilder> builders = builders(false);
    try {
      for (int t = 0; t <= 15; t++) {
        Double value = t == 7 || t == 14 ? (double) t : null;
        processor.process(record(t, value), builders, null);
      }
      processor.finish(builders, null);
      Column indexes = builders.get(0).build();
      Column times = builders.get(1).build();
      assertEquals(2, indexes.getPositionCount());
      // F at position 7 belongs first to [4, 9), L at 14 belongs last to [14, 19).
      assertEquals(2, indexes.getLong(0));
      assertEquals(7, times.getLong(0));
      assertEquals(7, indexes.getLong(1));
      assertEquals(14, times.getLong(1));
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testNullRowsPreserveTimeWindowAlignmentAndTimestampChecks() {
    TableFunctionDataProcessor processor = processor(true, 4, 2, Type.DOUBLE, Type.DOUBLE);
    List<ColumnBuilder> builders = new ArrayList<>(builders(true));
    builders.add(new LongColumnBuilder(null, 16));
    builders.add(new DoubleColumnBuilder(null, 16));
    try {
      processor.process(record(0, null, null), builders, null);
      processor.process(record(2, null, 5.0), builders, null);
      processor.process(record(9, null, null), builders, null);
      processor.process(record(10, null, 7.0), builders, null);
      processor.finish(builders, null);
      assertEquals(2, builders.get(0).getPositionCount());
      assertEquals(0, builders.get(0).build().getLong(0));
      assertEquals(10, builders.get(0).build().getLong(1));
      assertEquals(2, builders.get(4).build().getLong(0));
      assertEquals(10, builders.get(4).build().getLong(1));
    } finally {
      processor.beforeDestroy();
    }

    TableFunctionDataProcessor duplicates = processor(true, 4, 2, Type.DOUBLE);
    try {
      duplicates.process(record(0, (Number) null), builders, null);
      SemanticException error =
          assertThrows(
              SemanticException.class, () -> duplicates.process(record(0, 1.0), builders, null));
      assertEquals(
          String.format(
              CommonMessages
                  .EXCEPTION_LTTB_DOES_NOT_SUPPORT_DUPLICATE_TIMESTAMPS_BUT_TIMESTAMP_ARG_APPEARS_MORE_THAN_ONCE_IN_THE_SAME_PARTITION_7713FEDD,
              0),
          error.getMessage());
    } finally {
      duplicates.beforeDestroy();
    }
  }

  @Test
  public void testFirstPointOnlyNeedsItsEarliestTimeWindow() {
    TableFunctionDataProcessor processor = processor(true, 3_600_000, 10, Type.DOUBLE);
    List<ColumnBuilder> builders = builders(true);
    try {
      processor.process(record(0, 42.0), builders, null);
      processor.finish(builders, null);
      assertEquals(1, builders.get(0).getPositionCount());
      assertEquals(-3_599_990, builders.get(0).build().getLong(0));
      assertEquals(10, builders.get(1).build().getLong(0));
      assertEquals(0, builders.get(2).build().getLong(0));
      assertEquals(42.0, builders.get(3).build().getDouble(0), 0.0);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testFirstPointAfterNullPrefixOnlyNeedsItsEarliestCountWindow() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    long previousBudget = config.getLttbBufferSize();
    try {
      config.setLttbBufferSize(4 * 1024);
      TableFunctionDataProcessor processor = processor(false, 128, 1, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(false);
      try {
        for (int t = 0; t < 128; t++) {
          processor.process(record(t, (Number) null), builders, null);
        }
        processor.process(record(128, 42.0), builders, null);
        processor.finish(builders, null);
        assertEquals(1, builders.get(0).getPositionCount());
        assertEquals(1, builders.get(0).build().getLong(0));
        assertEquals(128, builders.get(1).build().getLong(0));
        assertEquals(42.0, builders.get(2).build().getDouble(0), 0.0);
      } finally {
        processor.beforeDestroy();
      }
    } finally {
      config.setLttbBufferSize(previousBudget);
    }
  }

  @Test
  public void testLaterCandidatesOpenDeferredWindowsForAllColumns() {
    TableFunctionDataProcessor processor = processor(true, 4, 2, Type.DOUBLE, Type.DOUBLE);
    List<ColumnBuilder> builders = new ArrayList<>(builders(true));
    builders.add(new LongColumnBuilder(null, 16));
    builders.add(new DoubleColumnBuilder(null, 16));
    try {
      processor.process(record(0, 0.0, null), builders, null);
      processor.process(record(2, 8.0, 5.0), builders, null);
      processor.process(record(3, 1.0, 7.0), builders, null);
      processor.process(record(4, 0.0, null), builders, null);
      processor.finish(builders, null);
      assertEquals(4, builders.get(0).getPositionCount());
      long[] firstColumnTimes = {0, 2, 3, 4};
      Column starts = builders.get(0).build();
      Column first = builders.get(2).build();
      Column second = builders.get(4).build();
      for (int row = 0; row < firstColumnTimes.length; row++) {
        assertEquals(2L * row - 2, starts.getLong(row));
        assertEquals(firstColumnTimes[row], first.getLong(row));
      }
      assertTrue(second.isNull(0));
      assertEquals(2, second.getLong(1));
      assertEquals(3, second.getLong(2));
      assertTrue(second.isNull(3));
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testTimeWindowCreationIsBoundedForBucketCandidates() {
    TableFunctionDataProcessor processor = processor(true, 1_000_000, 1, Type.DOUBLE);
    List<ColumnBuilder> builders = builders(true);
    try {
      processor.process(record(0, 1.0), builders, null);
      assertMemoryLimit(
          () -> processor.process(record(1, 2.0), builders, null),
          LTTBTableFunction.DEFAULT_MAX_BUFFER_BYTES);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testColumnsShareTheWindowBudget() {
    TableFunctionDataProcessor processor = processor(false, 1_000_000, 1, Type.DOUBLE, Type.INT64);
    try {
      assertMemoryLimit(
          () -> {
            for (int i = 0; i < 1800; i++) {
              // timestamps must be unique within the partition, so advance the input time
              processor.process(record(i, 1.0, Long.MAX_VALUE), Collections.emptyList(), null);
            }
          },
          LTTBTableFunction.DEFAULT_MAX_BUFFER_BYTES);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testArrayGrowthReservesOldAndNewIntegralArrays() {
    LTTBTableFunction.PointBuffer buffer =
        new LTTBTableFunction.PointBuffer(true, new LTTBTableFunction.BufferMemoryBudget(4096));
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
    LTTBTableFunction.BufferMemoryBudget budget = new LTTBTableFunction.BufferMemoryBudget(4096);
    LTTBTableFunction.PointBuffer first = new LTTBTableFunction.PointBuffer(true, budget);
    LTTBTableFunction.PointBuffer second = new LTTBTableFunction.PointBuffer(true, budget);
    LTTBTableFunction.PointBuffer third = new LTTBTableFunction.PointBuffer(true, budget);
    first.addIntegral(0, 1);
    second.addIntegral(0, 1);
    assertMemoryLimit(() -> third.addIntegral(0, 1), 4096);
    assertEquals(0, third.size());
  }

  @Test
  public void testEmittedWindowsReleaseBudgetForLongStream() {
    TableFunctionDataProcessor processor = processor(false, 1, 1, Type.DOUBLE);
    List<ColumnBuilder> builders = builders(false);
    long outputRows = 0;
    try {
      // Total allocations exceed the budget, but only two windows remain live at a time.
      for (int row = 0; row < 80_000; row++) {
        // timestamps must be unique within the partition, so advance the input time
        processor.process(record(row, 42.0), builders, null);
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
  public void testColumnWithASingleEligiblePointDoesNotStallEmission() {
    // Only the first row has a value, so hasLastEligible stays false and lastEligiblePositions
    // keeps its default 0. Admission must not treat that sentinel as a real last position: the
    // window holding it would then never become ready and the whole partition would be buffered.
    for (boolean timeWindow : new boolean[] {false, true}) {
      TableFunctionDataProcessor processor = processor(timeWindow, 1, 1, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(timeWindow);
      long emittedBeforeFinish = 0;
      try {
        for (int row = 0; row < 80_000; row++) {
          processor.process(record(row, row == 0 ? 42.0 : null), builders, null);
          if (row % 512 == 511) {
            emittedBeforeFinish += builders.get(0).getPositionCount();
            builders = builders(timeWindow);
          }
        }
        // Windows must stream out while the partition is still being read.
        assertTrue(emittedBeforeFinish > 0);
        processor.finish(builders, null);
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  @Test
  public void testWindowsAfterTheLastValueDoNotStallEmission() {
    // Two eligible points followed by a long NULL tail. The empty windows must release their
    // buffers as they close so a sparse partition can span more windows than fit in the budget.
    for (boolean timeWindow : new boolean[] {false, true}) {
      TableFunctionDataProcessor processor = processor(timeWindow, 1, 1, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(timeWindow);
      try {
        for (int row = 0; row < 80_000; row++) {
          Double value = null;
          if (row == 0) {
            value = 1.0;
          } else if (row == 1) {
            value = 5.0;
          }
          processor.process(record(row, value), builders, null);
        }
        processor.finish(builders, null);
        ColumnBuilder times = builders.get(timeWindow ? 2 : 1);
        ColumnBuilder valuesBuilder = builders.get(timeWindow ? 3 : 2);
        // Window 0 reports F(t0), window 1 reports L(t1); the NULL tail emits nothing.
        assertEquals(2, times.getPositionCount());
        assertEquals(0, times.build().getLong(0));
        assertEquals(1, times.build().getLong(1));
        assertEquals(1.0, valuesBuilder.build().getDouble(0), 0.0);
        assertEquals(5.0, valuesBuilder.build().getDouble(1), 0.0);
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  @Test
  public void testDisjointSingletonsStreamAcrossNullAndNonFiniteTails() {
    for (boolean timeWindow : new boolean[] {false, true}) {
      TableFunctionDataProcessor processor = processor(timeWindow, 1, 1, Type.DOUBLE, Type.DOUBLE);
      List<ColumnBuilder> builders = new ArrayList<>(builders(timeWindow));
      builders.add(new LongColumnBuilder(null, 16));
      builders.add(new DoubleColumnBuilder(null, 16));
      try {
        for (int time = 0; time < 16; time++) {
          processor.process(
              record(time, time < 2 ? (double) time : Double.NaN, time < 2 ? (double) time : null),
              builders,
              null);
          // All earlier windows closed, even though neither column has a later finite point.
          assertEquals(time, builders.get(0).getPositionCount());
        }
        processor.finish(builders, null);
        assertEquals(16, builders.get(0).getPositionCount());
        int firstTime = timeWindow ? 2 : 1;
        for (int time = 0; time < 16; time++) {
          assertEquals(time, builders.get(firstTime).build().getLong(time));
          assertEquals(
              time < 2 ? (double) time : Double.NaN,
              builders.get(firstTime + 1).build().getDouble(time),
              0.0);
          if (time < 2) {
            assertEquals(time, builders.get(firstTime + 2).build().getLong(time));
            assertEquals(time, builders.get(firstTime + 3).build().getDouble(time), 0.0);
          } else {
            assertTrue(builders.get(firstTime + 2).build().isNull(time));
            assertTrue(builders.get(firstTime + 3).build().isNull(time));
          }
        }
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  @Test
  public void testDisjointFiniteSingletonUpdatesAnchor() {
    for (boolean timeWindow : new boolean[] {false, true}) {
      TableFunctionDataProcessor processor = processor(timeWindow, 2, 2, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(timeWindow);
      try {
        Double[] values = {0.0, 10.0, Double.NaN, null, 0.0, 3.0, 0.0};
        for (int time = 0; time < values.length; time++) {
          processor.process(record(time, values[time]), builders, null);
        }
        processor.finish(builders, null);
        ColumnBuilder times = builders.get(timeWindow ? 2 : 1);
        // A is (1,10) after the singleton and the NaN, so (4,0) beats (5,3) against C=(6,0).
        long[] expected = {0, 1, 2, 4, 6};
        assertEquals(expected.length, times.getPositionCount());
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], times.build().getLong(i));
        }
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  @Test
  public void testSingleFiniteCandidateStreamsAndUpdatesAnchor() {
    for (boolean timeWindow : new boolean[] {false, true}) {
      TableFunctionDataProcessor processor = processor(timeWindow, 3, 3, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(timeWindow);
      try {
        Double[] values = {
          0.0, Double.NaN, 10.0, Double.NaN, Double.NaN, Double.NaN, 0.0, 3.0, null, 0.0
        };
        for (int time = 0; time < values.length; time++) {
          processor.process(record(time, values[time]), builders, null);
          if (time == 3) {
            // F and the sole finite candidate are known before any finite lookahead arrives.
            assertEquals(2, builders.get(0).getPositionCount());
          }
        }
        processor.finish(builders, null);
        ColumnBuilder times = builders.get(timeWindow ? 2 : 1);
        // With A=(2,10) and C=(9,0), t=6 beats t=7. The stale anchor (0,0) would pick t=7.
        long[] expected = {0, 2, 3, 6, 9};
        assertEquals(expected.length, times.getPositionCount());
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], times.build().getLong(i));
        }
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  @Test
  public void testSparseSingletonLookaheadReleasesDenseWindows() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    long previousBudget = config.getLttbBufferSize();
    try {
      config.setLttbBufferSize(32 * 1024);
      for (boolean timeWindow : new boolean[] {false, true}) {
        TableFunctionDataProcessor processor =
            processor(timeWindow, 3, 3, Type.DOUBLE, Type.DOUBLE);
        List<ColumnBuilder> builders = new ArrayList<>(builders(timeWindow));
        builders.add(new LongColumnBuilder(null, 16));
        builders.add(new DoubleColumnBuilder(null, 16));
        try {
          Double[] sparse = {0.0, 0.0, 9.0, 0.0};
          for (int t = 0; t < 120; t++) {
            processor.process(
                record(t, t < sparse.length ? sparse[t] : null, (double) t), builders, null);
          }
          processor.finish(builders, null);
          int firstTime = timeWindow ? 2 : 1;
          Column sparseTimes = builders.get(firstTime).build();
          Column sparseValues = builders.get(firstTime + 1).build();
          Column denseTimes = builders.get(firstTime + 2).build();
          assertEquals(denseTimes.getPositionCount(), sparseTimes.getPositionCount());
          assertEquals(42, denseTimes.getPositionCount());
          int[] expectedSparse = {0, 2, 3};
          for (int i = 0; i < expectedSparse.length; i++) {
            int t = expectedSparse[i];
            assertEquals(t, sparseTimes.getLong(i));
            assertEquals(sparse[t], sparseValues.getDouble(i), 0.0);
          }
          for (int i = expectedSparse.length; i < denseTimes.getPositionCount(); i++) {
            assertTrue(sparseTimes.isNull(i));
            assertTrue(sparseValues.isNull(i));
          }
          // All areas in the dense column tie. Each bucket keeps its earliest candidate.
          assertEquals(0L, denseTimes.getLong(0));
          assertEquals(1L, denseTimes.getLong(1));
          for (int i = 2; i <= 40; i++) {
            assertEquals(3L * (i - 1), denseTimes.getLong(i));
          }
          assertEquals(119L, denseTimes.getLong(41));
        } finally {
          processor.beforeDestroy();
        }
      }
    } finally {
      config.setLttbBufferSize(previousBudget);
    }
  }

  @Test
  public void testExclusiveSingletonStreamsAcrossOverlappingWindows() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    long previousBudget = config.getLttbBufferSize();
    try {
      config.setLttbBufferSize(32 * 1024);
      for (boolean timeWindow : new boolean[] {false, true}) {
        TableFunctionDataProcessor processor =
            processor(timeWindow, 3, 2, Type.DOUBLE, Type.DOUBLE);
        List<ColumnBuilder> builders = new ArrayList<>(builders(timeWindow));
        builders.add(new LongColumnBuilder(null, 16));
        builders.add(new DoubleColumnBuilder(null, 16));
        try {
          for (int t = 0; t < 120; t++) {
            processor.process(record(t, t < 2 ? (double) t : null, (double) t), builders, null);
          }
          // t=1 can only be reported by [0,3). The dense column keeps streaming.
          assertTrue(builders.get(0).getPositionCount() > 0);
          processor.finish(builders, null);
          int firstTime = timeWindow ? 2 : 1;
          Column sparseTimes = builders.get(firstTime).build();
          Column denseTimes = builders.get(firstTime + 2).build();
          assertEquals(62, denseTimes.getPositionCount());
          assertEquals(0, sparseTimes.getLong(0));
          assertEquals(1, sparseTimes.getLong(1));
          assertEquals(0, builders.get(0).build().getLong(1));
          for (int i = 2; i < sparseTimes.getPositionCount(); i++) {
            assertTrue(sparseTimes.isNull(i));
          }
          assertEquals(119, denseTimes.getLong(61));
        } finally {
          processor.beforeDestroy();
        }
      }
    } finally {
      config.setLttbBufferSize(previousBudget);
    }
  }

  @Test
  public void testSharedNonFiniteEndpointAllowsEarlierWindowToStream() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    long previousBudget = config.getLttbBufferSize();
    try {
      config.setLttbBufferSize(32 * 1024);
      for (boolean timeWindow : new boolean[] {false, true}) {
        for (boolean sparseColumnResumes : new boolean[] {false, true}) {
          assertSharedNonFiniteEndpointStreams(timeWindow, sparseColumnResumes);
        }
      }
    } finally {
      config.setLttbBufferSize(previousBudget);
    }
  }

  private static void assertSharedNonFiniteEndpointStreams(
      boolean timeWindow, boolean sparseColumnResumes) {
    TableFunctionDataProcessor processor = processor(timeWindow, 3, 2, Type.DOUBLE, Type.DOUBLE);
    List<ColumnBuilder> builders = new ArrayList<>(builders(timeWindow));
    builders.add(new LongColumnBuilder(null, 16));
    builders.add(new DoubleColumnBuilder(null, 16));
    try {
      processor.process(record(0, 0.0, 0.0), builders, null);
      processor.process(record(1, 1.0, 1.0), builders, null);
      processor.process(record(2, Double.NaN, 2.0), builders, null);
      for (int t = 3; t < 120; t++) {
        processor.process(record(t, null, (double) t), builders, null);
      }
      // [0,3) picks t=1; [2,5) reports t=2 whether or not another sparse point arrives.
      // Only the final dense windows may still be waiting for lookahead.
      int timeIndex = timeWindow ? 2 : 1;
      Column denseTimes = builders.get(timeIndex + 2).build();
      assertTrue(denseTimes.getPositionCount() > 0);
      assertTrue(denseTimes.getLong(denseTimes.getPositionCount() - 1) >= 114);
      if (sparseColumnResumes) {
        processor.process(record(120, 120.0, 120.0), builders, null);
      }
      processor.finish(builders, null);

      Column times = builders.get(timeIndex).build();
      Column values = builders.get(timeIndex + 1).build();
      Column windows = builders.get(0).build();
      List<Long> sparseTimes = new ArrayList<>();
      for (int i = 0; i < times.getPositionCount(); i++) {
        if (!times.isNull(i)) {
          long time = times.getLong(i);
          sparseTimes.add(time);
          if (time == 2) {
            assertEquals(timeWindow ? 2 : 1, windows.getLong(i));
            assertTrue(Double.isNaN(values.getDouble(i)));
          }
        }
      }
      assertEquals(
          sparseColumnResumes ? Arrays.asList(0L, 1L, 2L, 120L) : Arrays.asList(0L, 1L, 2L),
          sparseTimes);
    } finally {
      processor.beforeDestroy();
    }
  }

  @Test
  public void testSharedSingletonWaitsUntilItsEndpointStatusIsKnown() {
    for (boolean timeWindow : new boolean[] {false, true}) {
      for (boolean hasLaterPoint : new boolean[] {false, true}) {
        TableFunctionDataProcessor processor = processor(timeWindow, 3, 2, Type.DOUBLE);
        List<ColumnBuilder> builders = builders(timeWindow);
        try {
          for (int t = 0; t < 6; t++) {
            processor.process(record(t, t == 0 || t == 2 ? (double) t : null), builders, null);
          }
          if (hasLaterPoint) {
            processor.process(record(6, 6.0), builders, null);
          }
          processor.finish(builders, null);
          Column times = builders.get(timeWindow ? 2 : 1).build();
          Column windows = builders.get(0).build();
          assertEquals(hasLaterPoint ? 4 : 2, times.getPositionCount());
          assertEquals(0, times.getLong(0));
          assertEquals(2, times.getLong(1));
          if (hasLaterPoint) {
            // Both [0,3) and [2,5) select t=2 as their only candidate.
            assertEquals(0, windows.getLong(1));
            assertEquals(2, times.getLong(2));
            assertEquals(timeWindow ? 2 : 1, windows.getLong(2));
            assertEquals(6, times.getLong(3));
          } else {
            // Only [2,5) owns the final endpoint.
            assertEquals(timeWindow ? 2 : 1, windows.getLong(1));
          }
        } finally {
          processor.beforeDestroy();
        }
      }
    }
  }

  @Test
  public void testMultiPointLookaheadWaitsForTheFinalEndpoint() {
    for (boolean timeWindow : new boolean[] {false, true}) {
      TableFunctionDataProcessor processor = processor(timeWindow, 3, 3, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(timeWindow);
      try {
        Double[] values = {0.0, 0.0, 5.0, 0.0, 20.0, null, null};
        for (int t = 0; t < values.length; t++) {
          processor.process(record(t, values[t]), builders, null);
        }
        assertEquals(0, builders.get(0).getPositionCount());
        processor.finish(builders, null);
        Column times = builders.get(timeWindow ? 2 : 1).build();
        // Using t3 alone as C selects t2. Averaging t3 and the final endpoint t4 selects t1.
        long[] expected = {0, 2, 3, 4};
        assertEquals(expected.length, times.getPositionCount());
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], times.getLong(i));
        }
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  @Test
  public void testSparseColumnsReleaseTheirFirstWindowWhenEmitted() throws Exception {
    for (boolean timeWindow : new boolean[] {false, true}) {
      TableFunctionDataProcessor processor = processor(timeWindow, 1, 1, Type.DOUBLE, Type.DOUBLE);
      List<ColumnBuilder> builders = new ArrayList<>(builders(timeWindow));
      builders.add(new LongColumnBuilder(null, 16));
      builders.add(new DoubleColumnBuilder(null, 16));
      try {
        processor.process(record(0, 11.0, null), builders, null);
        processor.process(record(1, null, 22.0), builders, null);
        processor.process(record(2, null, null), builders, null);
        assertEquals(2, builders.get(0).getPositionCount());
        Field field = processor.getClass().getSuperclass().getDeclaredField("firstEligibleWindows");
        field.setAccessible(true);
        for (Object firstWindow : (Object[]) field.get(processor)) {
          assertNull(firstWindow);
        }
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  @Test
  public void testFiniteLookaheadBeyondNonFiniteBuckets() {
    for (boolean timeWindow : new boolean[] {false, true}) {
      BatchedTableFunctionDataProcessor processor =
          (BatchedTableFunctionDataProcessor) processor(timeWindow, 3, 3, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(timeWindow);
      try {
        processor.process(record(0, 0.0), builders, null);
        processor.process(record(1, 0.0), builders, null);
        processor.process(record(2, 100.0), builders, null);
        for (int i = 3; i < 900; i++) {
          processor.process(record(i, Double.NaN), builders, null);
        }
        assertEquals(0, builders.get(0).getPositionCount());
        processor.process(record(900, 45_000.0), builders, null);
        processor.process(record(901, (Number) null), builders, null);
        processor.process(record(902, (Number) null), builders, null);
        processor.processInput(record(903, 0.0), builders, null);
        assertEquals(0, builders.get(0).getPositionCount());
        boolean drained = false;
        while (!drained) {
          int previousRows = builders.get(0).getPositionCount();
          drained = processor.drainOutput(builders, () -> true);
          int added = builders.get(0).getPositionCount() - previousRows;
          // A final call may only consume the pending input after the last output block filled.
          assertTrue(added == 1 || (drained && added == 0));
        }
        ColumnBuilder times = builders.get(timeWindow ? 2 : 1);
        // C lies on the line through t=0 and t=2, so t=1 wins once its lookahead closes.
        assertEquals(302, times.getPositionCount());
        assertEquals(1, times.build().getLong(1));
        assertEquals(897, times.build().getLong(300));
        processor.finish(builders, null);
        assertEquals(303, times.getPositionCount());
        assertEquals(900, times.build().getLong(301));
        assertEquals(903, times.build().getLong(302));
      } finally {
        processor.beforeDestroy();
      }
    }
  }

  @Test
  public void testFinishSplitsThreeRowsOfOneWindow() {
    for (boolean timeWindow : new boolean[] {false, true}) {
      BatchedTableFunctionDataProcessor processor =
          (BatchedTableFunctionDataProcessor) processor(timeWindow, 3, 3, Type.DOUBLE);
      List<ColumnBuilder> builders = builders(timeWindow);
      try {
        for (int time = 0; time < 3; time++) {
          processor.process(record(time, 5.0), builders, null);
        }
        for (int time = 0; time < 3; time++) {
          assertEquals(time == 2, processor.finishBatch(builders, () -> true));
          ColumnBuilder times = builders.get(timeWindow ? 2 : 1);
          assertEquals(1, times.getPositionCount());
          assertEquals(time, times.build().getLong(0));
          builders = builders(timeWindow);
        }
      } finally {
        processor.beforeDestroy();
      }
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
        ColumnBuilder valuesBuilder = builders.get(timeWindow ? 3 : 2);
        // W0 reports F(t0) and its pick(t1); W1 reports its pick(t2); W2 reports its pick(t4) and
        // L(t5).
        assertEquals(5, times.getPositionCount());
        assertEquals(0, times.build().getLong(0));
        assertEquals(1, times.build().getLong(1));
        assertEquals(2, times.build().getLong(2));
        assertEquals(4, times.build().getLong(3));
        assertEquals(5, times.build().getLong(4));
        assertEquals(0.0, valuesBuilder.build().getDouble(0), 0.0);
        assertEquals(1.0, valuesBuilder.build().getDouble(1), 0.0);
        assertEquals(8.0, valuesBuilder.build().getDouble(2), 0.0);
        assertEquals(0.0, valuesBuilder.build().getDouble(3), 0.0);
        assertEquals(0.0, valuesBuilder.build().getDouble(4), 0.0);
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
          String.format(
              CommonMessages
                  .EXCEPTION_LTTB_BUFFERS_EXCEED_LTTB_BUFFER_SIZE_IN_BYTES_ARG_BYTES_26BDE62D,
              limit),
          e.getMessage());
    }
  }
}
