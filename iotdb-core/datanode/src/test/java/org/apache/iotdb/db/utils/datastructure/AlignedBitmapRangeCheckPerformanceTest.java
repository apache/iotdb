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

import org.apache.tsfile.utils.BitMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Locale;

public class AlignedBitmapRangeCheckPerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.aligned.bitmap.range-check.perf.enabled";
  private static final String ITERATIONS_PROPERTY =
      "iotdb.aligned.bitmap.range-check.perf.iterations";
  private static final String ROUNDS_PROPERTY = "iotdb.aligned.bitmap.range-check.perf.rounds";
  private static final int REPETITIONS = 2048;
  private static final int BITMAP_COUNT = 64;
  private static final int BITMAP_MASK = BITMAP_COUNT - 1;

  private static volatile long benchmarkBlackhole;

  @Test
  public void bitmapRangeCheckBenchmark() {
    Assume.assumeTrue(
        String.format(
            Locale.ROOT,
            "Manual performance UT. Enable with -D%s=true; optionally tune -D%s and -D%s.",
            ENABLED_PROPERTY,
            ITERATIONS_PROPERTY,
            ROUNDS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 4000);
    int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    Assert.assertTrue(iterations > 0);
    Assert.assertTrue(rounds > 0);

    runScenario("prefix", createBitMaps(), 0, Long.SIZE, iterations, rounds);
    runScenario("partial", createBitMaps(), 1, Long.SIZE - 1, iterations, rounds);
  }

  private static void runScenario(
      String label, BitMap[] bitMaps, int start, int length, int iterations, int rounds) {
    int operations = iterations * REPETITIONS;
    runLegacy(bitMaps, start, length, REPETITIONS);
    runOptimized(bitMaps, start, length, REPETITIONS);

    Measurement[] legacyMeasurements = new Measurement[rounds];
    Measurement[] optimizedMeasurements = new Measurement[rounds];
    for (int i = 0; i < rounds; i++) {
      if ((i & 1) == 0) {
        legacyMeasurements[i] = measureLegacy(bitMaps, start, length, operations);
        optimizedMeasurements[i] = measureOptimized(bitMaps, start, length, operations);
      } else {
        optimizedMeasurements[i] = measureOptimized(bitMaps, start, length, operations);
        legacyMeasurements[i] = measureLegacy(bitMaps, start, length, operations);
      }
    }

    Summary legacySummary = ManualPerformanceTestUtils.summarize(legacyMeasurements, operations);
    Summary optimizedSummary =
        ManualPerformanceTestUtils.summarize(optimizedMeasurements, operations);
    printResult(label, start, length, operations, rounds, legacySummary, optimizedSummary);
  }

  private static Measurement measureLegacy(
      BitMap[] bitMaps, int start, int length, int operations) {
    return ManualPerformanceTestUtils.measure(
        1, () -> runLegacy(bitMaps, start, length, operations));
  }

  private static Measurement measureOptimized(
      BitMap[] bitMaps, int start, int length, int operations) {
    return ManualPerformanceTestUtils.measure(
        1, () -> runOptimized(bitMaps, start, length, operations));
  }

  private static void runLegacy(BitMap[] bitMaps, int start, int length, int operations) {
    long markedCount = 0;
    for (int i = 0; i < operations; i++) {
      if (legacyContainsMarkedBit(bitMaps[i & BITMAP_MASK], start, length)) {
        markedCount++;
      }
    }
    benchmarkBlackhole = markedCount;
  }

  private static void runOptimized(BitMap[] bitMaps, int start, int length, int operations) {
    long markedCount = 0;
    for (int i = 0; i < operations; i++) {
      if (bitMaps[i & BITMAP_MASK].isRangeAnyMarked(start, length)) {
        markedCount++;
      }
    }
    benchmarkBlackhole = markedCount;
  }

  private static boolean legacyContainsMarkedBit(BitMap bitMap, int start, int length) {
    byte[] bytes = bitMap.getByteArray();
    int end = start + length - 1;
    int firstByteIndex = start >>> 3;
    int lastByteIndex = end >>> 3;
    if (firstByteIndex == lastByteIndex) {
      int mask = (0xFF << (start & 7)) & (0xFF >>> (7 - (end & 7)));
      return (bytes[firstByteIndex] & mask) != 0;
    }
    if ((bytes[firstByteIndex] & (0xFF << (start & 7))) != 0) {
      return true;
    }
    for (int i = firstByteIndex + 1; i < lastByteIndex; i++) {
      if (bytes[i] != 0) {
        return true;
      }
    }
    return (bytes[lastByteIndex] & (0xFF >>> (7 - (end & 7)))) != 0;
  }

  private static BitMap[] createBitMaps() {
    BitMap[] bitMaps = new BitMap[BITMAP_COUNT];
    for (int i = 0; i < BITMAP_COUNT; i++) {
      bitMaps[i] = BitMap.createBitMapDynamically(Long.SIZE);
      if ((i & 1) != 0) {
        bitMaps[i].mark(Long.SIZE - 1);
      }
    }
    return bitMaps;
  }

  private static void printResult(
      String label,
      int start,
      int length,
      int operations,
      int rounds,
      Summary legacySummary,
      Summary optimizedSummary) {
    System.out.printf(
        Locale.ROOT,
        "Aligned bitmap range-check benchmark (%s): start=%d, length=%d, operations/round=%d, rounds=%d%n",
        label,
        start,
        length,
        operations,
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
    System.out.printf(
        Locale.ROOT,
        "  optimized-legacy CPU delta=%+.3f ns/check, allocation delta=%+.1f bytes/check%n",
        optimizedSummary.getCpuNanosPerOperation() - legacySummary.getCpuNanosPerOperation(),
        optimizedSummary.getAllocatedBytesPerOperation()
            - legacySummary.getAllocatedBytesPerOperation());
  }

  private static void printSummary(String label, Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "  %-10s CPU=%.3f ns/check, allocated=%.1f bytes/check%n",
        label,
        summary.getCpuNanosPerOperation(),
        summary.getAllocatedBytesPerOperation());
  }

  private static double percentage(double numerator, double denominator) {
    return denominator == 0 ? 0 : numerator * 100.0 / denominator;
  }
}
