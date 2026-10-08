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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

public class TabletDeviceRangeReusePerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.tablet.device.ranges.perf.enabled";
  private static final String ROWS_PROPERTY = "iotdb.tablet.device.ranges.perf.rows";
  private static final String DEVICES_PROPERTY = "iotdb.tablet.device.ranges.perf.devices";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.tablet.device.ranges.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY = "iotdb.tablet.device.ranges.perf.iterations";
  private static final String ROUNDS_PROPERTY = "iotdb.tablet.device.ranges.perf.rounds";
  private static volatile int benchmarkBlackhole;

  @Test
  public void tabletDeviceRangeReuseBenchmark() throws IllegalPathException {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s, -D%s, -D%s, -D%s and -D%s.",
            ENABLED_PROPERTY,
            ROWS_PROPERTY,
            DEVICES_PROPERTY,
            WARMUP_ITERATIONS_PROPERTY,
            ITERATIONS_PROPERTY,
            ROUNDS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    final int rowCount = Integer.getInteger(ROWS_PROPERTY, 4096);
    final int deviceCount = Integer.getInteger(DEVICES_PROPERTY, 64);
    final int warmupIterations = Integer.getInteger(WARMUP_ITERATIONS_PROPERTY, 1000);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 10_000);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 7);
    Assert.assertTrue(rowCount > 0);
    Assert.assertTrue(deviceCount > 0);
    Assert.assertTrue(rowCount >= deviceCount);
    Assert.assertTrue(warmupIterations > 0);
    Assert.assertTrue(iterations > 0);
    Assert.assertTrue(rounds > 0);

    final BenchmarkNode node = new BenchmarkNode(rowCount, deviceCount);
    final List<int[]> ranges = Collections.singletonList(new int[] {0, rowCount});
    runLegacy(node, ranges, warmupIterations);
    runCached(node, ranges, warmupIterations);
    node.resetCalls();

    final Measurement[] legacyMeasurements = new Measurement[rounds];
    final Measurement[] cachedMeasurements = new Measurement[rounds];
    for (int round = 0; round < rounds; ++round) {
      if ((round & 1) == 0) {
        legacyMeasurements[round] = measureLegacy(node, ranges, iterations);
        cachedMeasurements[round] = measureCached(node, ranges, iterations);
      } else {
        cachedMeasurements[round] = measureCached(node, ranges, iterations);
        legacyMeasurements[round] = measureLegacy(node, ranges, iterations);
      }
    }

    final Summary legacySummary =
        ManualPerformanceTestUtils.summarize(legacyMeasurements, iterations);
    final Summary cachedSummary =
        ManualPerformanceTestUtils.summarize(cachedMeasurements, iterations);
    final long expectedLegacySplits = (long) iterations * rounds * 4;
    final long expectedCachedSplits = iterations * rounds;
    Assert.assertEquals(expectedLegacySplits, node.legacySplitCalls.get());
    Assert.assertEquals(expectedCachedSplits, node.cachedSplitCalls.get());
    Assert.assertEquals(node.cachedDeviceIdCalls.get() * 4, node.legacyDeviceIdCalls.get());
    printResult(
        rowCount,
        deviceCount,
        warmupIterations,
        iterations,
        rounds,
        legacySummary,
        cachedSummary,
        node.legacySplitCalls.get(),
        node.cachedSplitCalls.get(),
        node.legacyDeviceIdCalls.get(),
        node.cachedDeviceIdCalls.get());
  }

  private static Measurement measureLegacy(BenchmarkNode node, List<int[]> ranges, int iterations) {
    return ManualPerformanceTestUtils.measure(iterations, () -> runLegacyOnce(node, ranges));
  }

  private static Measurement measureCached(BenchmarkNode node, List<int[]> ranges, int iterations) {
    return ManualPerformanceTestUtils.measure(iterations, () -> runCachedOnce(node, ranges));
  }

  private static void runLegacy(BenchmarkNode node, List<int[]> ranges, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      runLegacyOnce(node, ranges);
    }
  }

  private static void runCached(BenchmarkNode node, List<int[]> ranges, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      runCachedOnce(node, ranges);
    }
  }

  private static void runLegacyOnce(BenchmarkNode node, List<int[]> ranges) {
    long checksum = 0;
    for (int[] range : ranges) {
      List<Pair<IDeviceID, Integer>> ramSnapshotRanges =
          node.legacySplitByDevice(range[0], range[1]);
      checksum += ramSnapshotRanges.size();
    }
    for (int[] range : ranges) {
      List<Pair<IDeviceID, Integer>> memoryCheckRanges =
          node.legacySplitByDevice(range[0], range[1]);
      checksum += memoryCheckRanges.size();
    }
    for (int[] range : ranges) {
      List<Pair<IDeviceID, Integer>> memTableWriteRanges =
          node.legacySplitByDevice(range[0], range[1]);
      checksum += memTableWriteRanges.size();
    }
    for (int[] range : ranges) {
      List<Pair<IDeviceID, Integer>> resourceUpdateRanges =
          node.legacySplitByDevice(range[0], range[1]);
      checksum += resourceUpdateRanges.size();
    }
    benchmarkBlackhole = (int) checksum;
  }

  private static void runCachedOnce(BenchmarkNode node, List<int[]> ranges) {
    List<List<Pair<IDeviceID, Integer>>> deviceRanges = new ArrayList<>(ranges.size());
    for (int[] range : ranges) {
      deviceRanges.add(node.cachedSplitByDevice(range[0], range[1]));
    }
    long checksum = 0;
    for (List<Pair<IDeviceID, Integer>> deviceEndOffsetPairs : deviceRanges) {
      checksum += deviceEndOffsetPairs.size() * 4L;
    }
    benchmarkBlackhole = (int) checksum;
  }

  private static void printResult(
      int rowCount,
      int deviceCount,
      int warmupIterations,
      int iterations,
      int rounds,
      Summary legacySummary,
      Summary cachedSummary,
      long legacySplitCalls,
      long cachedSplitCalls,
      long legacyDeviceIdCalls,
      long cachedDeviceIdCalls) {
    System.out.printf(
        Locale.ROOT,
        "Tablet device range reuse benchmark: rows=%d, devices=%d, warmup=%d, iterations=%d, rounds=%d%n"
            + "legacy repeated split: cpu=%.2f ns/op, allocation=%.2f bytes/op, splitCalls=%d, getDeviceIDCalls=%d%n"
            + "cached ranges: cpu=%.2f ns/op, allocation=%.2f bytes/op, splitCalls=%d, getDeviceIDCalls=%d%n"
            + "reduction: cpu=%.2f%%, allocation=%.2f%%%n",
        rowCount,
        deviceCount,
        warmupIterations,
        iterations,
        rounds,
        legacySummary.getCpuNanosPerOperation(),
        legacySummary.getAllocatedBytesPerOperation(),
        legacySplitCalls,
        legacyDeviceIdCalls,
        cachedSummary.getCpuNanosPerOperation(),
        cachedSummary.getAllocatedBytesPerOperation(),
        cachedSplitCalls,
        cachedDeviceIdCalls,
        reduction(legacySummary.getCpuNanosPerOperation(), cachedSummary.getCpuNanosPerOperation()),
        reduction(
            legacySummary.getAllocatedBytesPerOperation(),
            cachedSummary.getAllocatedBytesPerOperation()));
  }

  private static double reduction(double before, double after) {
    return before == 0 ? 0 : (before - after) * 100 / before;
  }

  private static final class BenchmarkNode extends RelationalInsertTabletNode {

    private final IDeviceID[] deviceIds;
    private final int rowsPerDevice;
    private final AtomicLong legacySplitCalls = new AtomicLong();
    private final AtomicLong cachedSplitCalls = new AtomicLong();
    private final AtomicLong legacyDeviceIdCalls = new AtomicLong();
    private final AtomicLong cachedDeviceIdCalls = new AtomicLong();

    private BenchmarkNode(int rowCount, int deviceCount) throws IllegalPathException {
      super(new PlanNodeId("tablet-device-range-benchmark"));
      deviceIds = new IDeviceID[deviceCount];
      for (int i = 0; i < deviceCount; ++i) {
        deviceIds[i] = IDeviceID.Factory.DEFAULT_FACTORY.create("root.benchmark.d" + i);
      }
      rowsPerDevice = Math.max(1, rowCount / deviceCount);
      setAligned(true);
    }

    @Override
    public IDeviceID getDeviceID(int rowIdx) {
      return deviceIds[Math.min(rowIdx / rowsPerDevice, deviceIds.length - 1)];
    }

    @Override
    public List<Pair<IDeviceID, Integer>> splitByDevice(int start, int end) {
      return split(start, end, legacySplitCalls, legacyDeviceIdCalls);
    }

    private List<Pair<IDeviceID, Integer>> legacySplitByDevice(int start, int end) {
      return split(start, end, legacySplitCalls, legacyDeviceIdCalls);
    }

    private List<Pair<IDeviceID, Integer>> cachedSplitByDevice(int start, int end) {
      return split(start, end, cachedSplitCalls, cachedDeviceIdCalls);
    }

    private List<Pair<IDeviceID, Integer>> split(
        int start, int end, AtomicLong splitCounter, AtomicLong deviceIdCounter) {
      splitCounter.incrementAndGet();
      final List<Pair<IDeviceID, Integer>> result = new ArrayList<>();
      IDeviceID previous = getDeviceID(start, deviceIdCounter);
      for (int i = start + 1; i < end; ++i) {
        IDeviceID current = getDeviceID(i, deviceIdCounter);
        if (!current.equals(previous)) {
          result.add(new Pair<>(previous, i));
          previous = getDeviceID(i, deviceIdCounter);
        }
      }
      result.add(new Pair<>(previous, end));
      return result;
    }

    private IDeviceID getDeviceID(int rowIdx, AtomicLong counter) {
      counter.incrementAndGet();
      return deviceIds[Math.min(rowIdx / rowsPerDevice, deviceIds.length - 1)];
    }

    private void resetCalls() {
      legacySplitCalls.set(0);
      cachedSplitCalls.set(0);
      legacyDeviceIdCalls.set(0);
      cachedDeviceIdCalls.set(0);
    }
  }
}
