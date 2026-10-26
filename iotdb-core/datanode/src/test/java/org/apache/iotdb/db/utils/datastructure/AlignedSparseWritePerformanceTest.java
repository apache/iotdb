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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

public class AlignedSparseWritePerformanceTest {

  private static final String PREFIX = "iotdb.aligned.sparse-write.perf.";
  private static final String ENABLED_PROPERTY = PREFIX + "enabled";
  private static final String COLUMNS_PROPERTY = PREFIX + "columns";
  private static final String WRITTEN_COLUMNS_PROPERTY = PREFIX + "written-columns";
  private static final String ROWS_PROPERTY = PREFIX + "rows";
  private static final String HISTORICAL_ROWS_PROPERTY = PREFIX + "historical-rows";
  private static final String WARMUPS_PROPERTY = PREFIX + "warmup.iterations";
  private static final String ITERATIONS_PROPERTY = PREFIX + "iterations";
  private static final String ROUNDS_PROPERTY = PREFIX + "rounds";

  private static volatile long benchmarkBlackhole;

  @Test
  public void sparseAlignedWriteBenchmark() {
    Assume.assumeTrue(
        String.format(
            Locale.ROOT,
            "Manual performance UT. Enable with -D%s=true and tune properties under -D%s*.",
            ENABLED_PROPERTY,
            PREFIX),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    int columns = Integer.getInteger(COLUMNS_PROPERTY, 64);
    int writtenColumns = Integer.getInteger(WRITTEN_COLUMNS_PROPERTY, 1);
    int rows = Integer.getInteger(ROWS_PROPERTY, ARRAY_SIZE);
    int historicalRows = Integer.getInteger(HISTORICAL_ROWS_PROPERTY, ARRAY_SIZE * 1024);
    int warmups = Integer.getInteger(WARMUPS_PROPERTY, 200);
    int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 5000);
    int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    Assert.assertTrue(columns > 0);
    Assert.assertTrue(writtenColumns > 0 && writtenColumns <= columns);
    Assert.assertTrue(rows > 0);
    Assert.assertTrue(historicalRows > 0);
    Assert.assertTrue(warmups > 0);
    Assert.assertTrue(iterations > 0);
    Assert.assertTrue(rounds > 0);

    List<TSDataType> dataTypes = createDataTypes(columns);
    Summary dense =
        runWriteScenario(
            "dense",
            dataTypes,
            Scenario.batch(columns, columns, rows),
            warmups,
            iterations,
            rounds);
    Summary sparse =
        runWriteScenario(
            "sparse",
            dataTypes,
            Scenario.batch(columns, writtenColumns, rows),
            warmups,
            iterations,
            rounds);
    Summary allNull =
        runWriteScenario(
            "all-null", dataTypes, Scenario.batch(columns, 0, rows), warmups, iterations, rounds);
    Summary nullPrefix =
        runWriteScenario(
            "null-prefix-first-non-null",
            dataTypes,
            Scenario.nullPrefix(columns, writtenColumns, rows),
            warmups,
            iterations,
            rounds);
    runExtensionScenario(historicalRows, warmups, iterations, rounds);

    printComparison("sparse/dense", sparse, dense);
    printComparison("all-null/dense", allNull, dense);
    printComparison("null-prefix/sparse", nullPrefix, sparse);
    printAvoidedBitmapAccounting(columns, writtenColumns, rows, historicalRows);
  }

  private static Summary runWriteScenario(
      String label,
      List<TSDataType> dataTypes,
      Scenario scenario,
      int warmups,
      int iterations,
      int rounds) {
    runWrite(AlignedTVList.newAlignedList(new ArrayList<>(dataTypes)), scenario, warmups);
    Measurement[] measurements = new Measurement[rounds];
    for (int round = 0; round < rounds; round++) {
      AlignedTVList target = AlignedTVList.newAlignedList(new ArrayList<>(dataTypes));
      measurements[round] =
          ManualPerformanceTestUtils.measure(1, () -> runWrite(target, scenario, iterations));
    }
    Summary summary = ManualPerformanceTestUtils.summarize(measurements, iterations);
    System.out.printf(
        Locale.ROOT,
        "Aligned sparse-write benchmark (%s): columns=%d, written columns=%d, rows=%d, batches/round=%d, rounds=%d%n",
        label,
        dataTypes.size(),
        scenario.writtenColumnCount,
        scenario.rowCount,
        iterations,
        rounds);
    printSummary(summary, "batch");
    return summary;
  }

  private static void runWrite(AlignedTVList target, Scenario scenario, int iterations) {
    for (int i = 0; i < iterations; i++) {
      scenario.writeTo(target);
    }
    benchmarkBlackhole = target.getRamSize() + target.rowCount();
  }

  private static List<TSDataType> createDataTypes(int columnCount) {
    List<TSDataType> dataTypes = new ArrayList<>(columnCount);
    for (int column = 0; column < columnCount; column++) {
      dataTypes.add(TSDataType.INT32);
    }
    return dataTypes;
  }

  private static long[] createTimes(int rowCount, int startTime) {
    long[] times = new long[rowCount];
    for (int row = 0; row < rowCount; row++) {
      times[row] = startTime + row;
    }
    return times;
  }

  private static Object[] createColumns(int columnCount, int writtenColumnCount, int rowCount) {
    Object[] columns = new Object[columnCount];
    for (int column = 0; column < writtenColumnCount; column++) {
      int[] columnValues = new int[rowCount];
      for (int row = 0; row < rowCount; row++) {
        columnValues[row] = row;
      }
      columns[column] = columnValues;
    }
    return columns;
  }

  private static void runExtensionScenario(
      int historicalRows, int warmups, int iterations, int rounds) {
    runExtensions(createHistoricalTarget(historicalRows), warmups);
    Measurement[] measurements = new Measurement[rounds];
    for (int round = 0; round < rounds; round++) {
      AlignedTVList target = createHistoricalTarget(historicalRows);
      measurements[round] =
          ManualPerformanceTestUtils.measure(1, () -> runExtensions(target, iterations));
    }
    Summary summary = ManualPerformanceTestUtils.summarize(measurements, iterations);
    System.out.printf(
        Locale.ROOT,
        "Aligned sparse-write benchmark (extend-column): historical rows=%d, historical blocks=%d, extensions/round=%d, rounds=%d%n",
        historicalRows,
        (historicalRows + ARRAY_SIZE - 1) / ARRAY_SIZE,
        iterations,
        rounds);
    printSummary(summary, "column");
  }

  private static AlignedTVList createHistoricalTarget(int historicalRows) {
    long[] times = new long[historicalRows];
    int[] values = new int[historicalRows];
    for (int row = 0; row < historicalRows; row++) {
      times[row] = row;
      values[row] = row;
    }
    AlignedTVList target = AlignedTVList.newAlignedList(new ArrayList<>(List.of(TSDataType.INT32)));
    target.putAlignedValues(times, new Object[] {values}, null, 0, historicalRows, null);
    return target;
  }

  private static void runExtensions(AlignedTVList target, int count) {
    for (int i = 0; i < count; i++) {
      target.extendColumn(TSDataType.INT32);
    }
    benchmarkBlackhole = target.getRamSize() + target.getTsDataTypes().size();
  }

  private static void printSummary(Summary summary, String operation) {
    System.out.printf(
        Locale.ROOT,
        "  CPU=%.3f us/%s, allocated=%.1f bytes/%s, peak heap delta=%.3f MiB/round%n",
        summary.getCpuNanosPerOperation() / 1_000.0,
        operation,
        summary.getAllocatedBytesPerOperation(),
        operation,
        summary.getPeakHeapDeltaBytes() / 1024.0 / 1024.0);
    if (summary.getCpuNanosPerOperation() == 0) {
      System.out.printf(
          "  CPU sample is below the platform timer resolution; increase -D%s.%n",
          ITERATIONS_PROPERTY);
    }
  }

  private static void printComparison(String label, Summary numerator, Summary denominator) {
    System.out.printf(
        Locale.ROOT,
        "  %s CPU ratio=%.2f%%, allocation ratio=%.2f%%%n",
        label,
        percentage(numerator.getCpuNanosPerOperation(), denominator.getCpuNanosPerOperation()),
        percentage(
            numerator.getAllocatedBytesPerOperation(),
            denominator.getAllocatedBytesPerOperation()));
  }

  private static double percentage(double numerator, double denominator) {
    return denominator == 0 ? 0 : numerator * 100.0 / denominator;
  }

  private static void printAvoidedBitmapAccounting(
      int columns, int writtenColumns, int rows, int historicalRows) {
    long bitmapCost = AlignedTVList.bitmapReferenceRamCost() + AlignedTVList.bitmapRamCost();
    long blocks = (rows + ARRAY_SIZE - 1L) / ARRAY_SIZE;
    long sparseBytes = (columns - writtenColumns) * blocks * bitmapCost;
    long allNullBytes = columns * blocks * bitmapCost;
    long historicalBlocks = (historicalRows + ARRAY_SIZE - 1L) / ARRAY_SIZE;
    System.out.printf(
        Locale.ROOT,
        "  avoided bitmap RAM accounting: sparse=%d bytes/batch, all-null=%d bytes/batch, extension=%d bytes/column%n",
        sparseBytes,
        allNullBytes,
        historicalBlocks * bitmapCost);
  }

  private static final class Scenario {

    private final long[] firstTimes;
    private final Object[] firstColumns;
    private final long[] secondTimes;
    private final Object[] secondColumns;
    private final int rowCount;
    private final int writtenColumnCount;

    private Scenario(
        long[] firstTimes,
        Object[] firstColumns,
        long[] secondTimes,
        Object[] secondColumns,
        int writtenColumnCount) {
      this.firstTimes = firstTimes;
      this.firstColumns = firstColumns;
      this.secondTimes = secondTimes;
      this.secondColumns = secondColumns;
      rowCount = firstTimes.length + (secondTimes == null ? 0 : secondTimes.length);
      this.writtenColumnCount = writtenColumnCount;
    }

    private static Scenario batch(int columns, int writtenColumns, int rows) {
      return new Scenario(
          createTimes(rows, 0),
          createColumns(columns, writtenColumns, rows),
          null,
          null,
          writtenColumns);
    }

    private static Scenario nullPrefix(int columns, int writtenColumns, int rows) {
      int prefixRows = rows - 1;
      return new Scenario(
          createTimes(prefixRows, 0),
          new Object[columns],
          createTimes(1, prefixRows),
          createColumns(columns, writtenColumns, 1),
          writtenColumns);
    }

    private void writeTo(AlignedTVList target) {
      if (firstTimes.length > 0) {
        target.putAlignedValues(firstTimes, firstColumns, null, 0, firstTimes.length, null);
      }
      if (secondTimes != null) {
        target.putAlignedValues(secondTimes, secondColumns, null, 0, secondTimes.length, null);
      }
    }
  }
}
