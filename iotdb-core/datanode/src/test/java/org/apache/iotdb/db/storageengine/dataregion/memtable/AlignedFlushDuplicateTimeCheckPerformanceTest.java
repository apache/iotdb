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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class AlignedFlushDuplicateTimeCheckPerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.aligned.flush.duplicate-check.perf.enabled";
  private static final String ROUNDS_PROPERTY = "iotdb.aligned.flush.duplicate-check.perf.rounds";
  private static final String SCANS_PROPERTY = "iotdb.aligned.flush.duplicate-check.perf.scans";
  private static final int ROW_COUNT = 16_384;
  private static final int COLUMN_COUNT = 8;
  private static final int WARMUP_ROUNDS = 3;

  private static volatile long benchmarkBlackhole;

  @Test
  public void duplicateTimeCheckBenchmark() {
    Assume.assumeTrue(
        String.format(
            Locale.ROOT,
            "Manual performance UT. Enable with -D%s=true; optionally tune -D%s and -D%s.",
            ENABLED_PROPERTY,
            ROUNDS_PROPERTY,
            SCANS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    int scans = Integer.getInteger(SCANS_PROPERTY, 128);
    Assert.assertTrue(rounds > 0);
    Assert.assertTrue(scans > 0);

    BenchmarkData data = createBenchmarkData();
    long legacyResult = runLegacy(data);
    long optimizedResult = runOptimized(data);
    Assert.assertEquals(legacyResult, optimizedResult);

    for (int i = 0; i < WARMUP_ROUNDS; i++) {
      runLegacy(data);
      runOptimized(data);
    }

    Measurement[] legacyMeasurements = new Measurement[rounds];
    Measurement[] optimizedMeasurements = new Measurement[rounds];
    for (int i = 0; i < rounds; i++) {
      if ((i & 1) == 0) {
        legacyMeasurements[i] = measureLegacy(data, scans);
        optimizedMeasurements[i] = measureOptimized(data, scans);
      } else {
        optimizedMeasurements[i] = measureOptimized(data, scans);
        legacyMeasurements[i] = measureLegacy(data, scans);
      }
    }

    Summary legacySummary = ManualPerformanceTestUtils.summarize(legacyMeasurements, scans);
    Summary optimizedSummary = ManualPerformanceTestUtils.summarize(optimizedMeasurements, scans);
    printResult(rounds, scans, legacySummary, optimizedSummary);
  }

  private static Measurement measureLegacy(BenchmarkData data, int scans) {
    return ManualPerformanceTestUtils.measure(scans, () -> runLegacy(data));
  }

  private static Measurement measureOptimized(BenchmarkData data, int scans) {
    return ManualPerformanceTestUtils.measure(scans, () -> runOptimized(data));
  }

  private static long runLegacy(BenchmarkData data) {
    long nullCount = 0;
    Pair<Long, Integer>[] lastValidPointForTimeDupCheck = new Pair[COLUMN_COUNT];
    for (int columnIndex = 0; columnIndex < COLUMN_COUNT; columnIndex++) {
      for (int rowIndex = 0; rowIndex < ROW_COUNT; rowIndex++) {
        if (lastValidPointForTimeDupCheck[columnIndex] == null) {
          lastValidPointForTimeDupCheck[columnIndex] = new Pair<>(Long.MIN_VALUE, null);
        }
        int valueIndex = data.list.getValueIndex(rowIndex);
        long time = data.list.getTime(rowIndex);
        if (!data.list.isNullValue(valueIndex, columnIndex)) {
          lastValidPointForTimeDupCheck[columnIndex].left = time;
          lastValidPointForTimeDupCheck[columnIndex].right = valueIndex;
        }
        if (data.timeDuplicateInfo[rowIndex]) {
          continue;
        }

        int originRowIndex;
        if (time == lastValidPointForTimeDupCheck[columnIndex].left) {
          originRowIndex = lastValidPointForTimeDupCheck[columnIndex].right;
        } else {
          originRowIndex = valueIndex;
        }
        if (data.list.isNullValue(originRowIndex, columnIndex)) {
          nullCount++;
        }
      }
    }
    benchmarkBlackhole = nullCount;
    return nullCount;
  }

  private static long runOptimized(BenchmarkData data) {
    long nullCount = 0;
    long[] lastValidPointTimeForTimeDupCheck = new long[COLUMN_COUNT];
    int[] lastValidPointIndexForTimeDupCheck = new int[COLUMN_COUNT];
    Arrays.fill(lastValidPointIndexForTimeDupCheck, -1);
    for (int columnIndex = 0; columnIndex < COLUMN_COUNT; columnIndex++) {
      for (int rowIndex = 0; rowIndex < ROW_COUNT; rowIndex++) {
        int valueIndex = data.list.getValueIndex(rowIndex);
        long time = data.list.getTime(rowIndex);
        boolean currentValueIsNull = data.list.isNullValue(valueIndex, columnIndex);
        if (!currentValueIsNull) {
          lastValidPointTimeForTimeDupCheck[columnIndex] = time;
          lastValidPointIndexForTimeDupCheck[columnIndex] = valueIndex;
        }
        if (data.timeDuplicateInfo[rowIndex]) {
          continue;
        }

        int originRowIndex;
        if (lastValidPointIndexForTimeDupCheck[columnIndex] >= 0
            && time == lastValidPointTimeForTimeDupCheck[columnIndex]) {
          originRowIndex = lastValidPointIndexForTimeDupCheck[columnIndex];
        } else {
          originRowIndex = valueIndex;
        }
        boolean isNull = originRowIndex == valueIndex && currentValueIsNull;
        if (isNull) {
          nullCount++;
        }
      }
    }
    benchmarkBlackhole = nullCount;
    return nullCount;
  }

  private static BenchmarkData createBenchmarkData() {
    List<TSDataType> dataTypes = new ArrayList<>(COLUMN_COUNT);
    for (int columnIndex = 0; columnIndex < COLUMN_COUNT; columnIndex++) {
      dataTypes.add(TSDataType.INT64);
    }
    AlignedTVList list = AlignedTVList.newAlignedList(dataTypes);
    for (int rowIndex = 0; rowIndex < ROW_COUNT; rowIndex++) {
      Object[] values = new Object[COLUMN_COUNT];
      for (int columnIndex = 0; columnIndex < COLUMN_COUNT; columnIndex++) {
        if ((rowIndex + columnIndex) % 4 != 0) {
          values[columnIndex] = (long) rowIndex + columnIndex;
        }
      }
      list.putAlignedValue(rowIndex / 4, values);
    }
    list.sort();

    boolean[] timeDuplicateInfo = new boolean[ROW_COUNT];
    for (int rowIndex = 0; rowIndex < ROW_COUNT - 1; rowIndex++) {
      timeDuplicateInfo[rowIndex] = list.getTime(rowIndex) == list.getTime(rowIndex + 1);
    }
    return new BenchmarkData(list, timeDuplicateInfo);
  }

  private static void printResult(
      int rounds, int scans, Summary legacySummary, Summary optimizedSummary) {
    System.out.printf(
        Locale.ROOT,
        "Aligned flush duplicate-time check benchmark: rows=%d, columns=%d, value checks/scan=%d, scans/round=%d, rounds=%d%n",
        ROW_COUNT,
        COLUMN_COUNT,
        ROW_COUNT * COLUMN_COUNT,
        scans,
        rounds);
    printSummary("legacy", legacySummary);
    printSummary("optimized", optimizedSummary);
    System.out.printf(
        Locale.ROOT,
        "  optimized/legacy CPU ratio=%.2f%%, allocation ratio=%.2f%%%n",
        percentage(
            optimizedSummary.getCpuNanosPerOperation(), legacySummary.getCpuNanosPerOperation()),
        percentage(
            optimizedSummary.getAllocatedBytesPerOperation(),
            legacySummary.getAllocatedBytesPerOperation()));
  }

  private static void printSummary(String label, Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "  %-10s CPU=%.3f ms/scan, allocated=%.2f bytes/scan%n",
        label,
        summary.getCpuNanosPerOperation() / 1_000_000.0,
        summary.getAllocatedBytesPerOperation());
  }

  private static double percentage(double numerator, double denominator) {
    return denominator == 0 ? 0 : numerator * 100.0 / denominator;
  }

  private static final class BenchmarkData {
    private final AlignedTVList list;
    private final boolean[] timeDuplicateInfo;

    private BenchmarkData(AlignedTVList list, boolean[] timeDuplicateInfo) {
      this.list = list;
      this.timeDuplicateInfo = timeDuplicateInfo;
    }
  }
}
