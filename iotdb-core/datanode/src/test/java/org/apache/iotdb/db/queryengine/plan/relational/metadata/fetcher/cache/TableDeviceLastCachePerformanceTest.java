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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Locale;

public class TableDeviceLastCachePerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.last.cache.write.perf.enabled";
  private static final String MEASUREMENTS_PROPERTY = "iotdb.last.cache.write.perf.measurements";
  private static final String CACHED_INTERVAL_PROPERTY =
      "iotdb.last.cache.write.perf.cached.interval";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.last.cache.write.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY = "iotdb.last.cache.write.perf.iterations";
  private static final String ROUNDS_PROPERTY = "iotdb.last.cache.write.perf.rounds";

  private static volatile int benchmarkBlackhole;

  @Test
  public void sparseInitializedMeasurementsBenchmark() {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s, -D%s, -D%s, -D%s and -D%s.",
            ENABLED_PROPERTY,
            MEASUREMENTS_PROPERTY,
            CACHED_INTERVAL_PROPERTY,
            WARMUP_ITERATIONS_PROPERTY,
            ITERATIONS_PROPERTY,
            ROUNDS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    final int measurementCount = Integer.getInteger(MEASUREMENTS_PROPERTY, 1_000);
    final int cachedInterval = Integer.getInteger(CACHED_INTERVAL_PROPERTY, 10);
    final int warmupIterations = Integer.getInteger(WARMUP_ITERATIONS_PROPERTY, 1_000);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 20_000);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    Assert.assertTrue(measurementCount > 0);
    Assert.assertTrue(cachedInterval > 0 && cachedInterval <= measurementCount);
    Assert.assertTrue(warmupIterations > 0);
    Assert.assertTrue(iterations > 0);
    Assert.assertTrue(rounds > 0);

    final Scenario legacy = createScenario(measurementCount, cachedInterval);
    final Scenario optimized = createScenario(measurementCount, cachedInterval);
    runLegacyUpdate(legacy);
    runOptimizedUpdate(optimized);
    assertCacheEquals(legacy, optimized);

    for (int i = 0; i < warmupIterations; ++i) {
      if ((i & 1) == 0) {
        runLegacyUpdate(legacy);
        runOptimizedUpdate(optimized);
      } else {
        runOptimizedUpdate(optimized);
        runLegacyUpdate(legacy);
      }
    }

    final Measurement[] legacyMeasurements = new Measurement[rounds];
    final Measurement[] optimizedMeasurements = new Measurement[rounds];
    for (int i = 0; i < rounds; ++i) {
      if ((i & 1) == 0) {
        legacyMeasurements[i] =
            ManualPerformanceTestUtils.measure(iterations, () -> runLegacyUpdate(legacy));
        optimizedMeasurements[i] =
            ManualPerformanceTestUtils.measure(iterations, () -> runOptimizedUpdate(optimized));
      } else {
        optimizedMeasurements[i] =
            ManualPerformanceTestUtils.measure(iterations, () -> runOptimizedUpdate(optimized));
        legacyMeasurements[i] =
            ManualPerformanceTestUtils.measure(iterations, () -> runLegacyUpdate(legacy));
      }
    }

    assertCacheEquals(legacy, optimized);
    final Summary legacySummary =
        ManualPerformanceTestUtils.summarize(legacyMeasurements, iterations);
    final Summary optimizedSummary =
        ManualPerformanceTestUtils.summarize(optimizedMeasurements, iterations);
    printResult(
        measurementCount,
        legacy.cachedMeasurementCount,
        warmupIterations,
        iterations,
        rounds,
        legacySummary,
        optimizedSummary);
  }

  private static Scenario createScenario(final int measurementCount, final int cachedInterval) {
    final String[] measurements = new String[measurementCount];
    final MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurementCount];
    final int[] values = new int[measurementCount];
    for (int i = 0; i < measurementCount; ++i) {
      measurements[i] = "s" + i;
      measurementSchemas[i] = new MeasurementSchema(measurements[i], TSDataType.INT32);
      values[i] = i;
    }

    final int cachedMeasurementCount = (measurementCount + cachedInterval - 1) / cachedInterval;
    final String[] cachedMeasurements = new String[cachedMeasurementCount];
    for (int i = 0; i < cachedMeasurementCount; ++i) {
      cachedMeasurements[i] = measurements[i * cachedInterval];
    }

    final TableDeviceLastCache cache = new TableDeviceLastCache(false);
    cache.initOrInvalidate(null, null, cachedMeasurements, false);
    return new Scenario(
        cache,
        measurements,
        measurementSchemas,
        new RowUpdateSource(values),
        cachedMeasurementCount);
  }

  private static void runLegacyUpdate(final Scenario scenario) {
    // Keep the eager implementation from origin/master for comparison.
    final TimeValuePair[] timeValuePairs = new TimeValuePair[scenario.measurements.length];
    for (int i = 0; i < scenario.measurements.length; ++i) {
      timeValuePairs[i] = scenario.updateSource.getLastCacheValue(i);
    }
    benchmarkBlackhole =
        scenario.cache.tryUpdate(
            scenario.measurements, scenario.measurementSchemas, timeValuePairs, false);
  }

  private static void runOptimizedUpdate(final Scenario scenario) {
    benchmarkBlackhole =
        scenario.cache.tryUpdate(
            scenario.measurements, scenario.measurementSchemas, scenario.updateSource);
  }

  private static void assertCacheEquals(
      final Scenario expectedScenario, final Scenario actualScenario) {
    for (final String measurement : expectedScenario.measurements) {
      Assert.assertEquals(
          expectedScenario.cache.getTimeValuePair(measurement),
          actualScenario.cache.getTimeValuePair(measurement));
    }
  }

  private static void printResult(
      final int measurementCount,
      final int cachedMeasurementCount,
      final int warmupIterations,
      final int iterations,
      final int rounds,
      final Summary legacy,
      final Summary optimized) {
    System.out.printf(
        Locale.ROOT,
        "Last-cache row-update benchmark: measurements=%d, initialized=%d, warmups=%d, iterations/round=%d, rounds=%d%n",
        measurementCount,
        cachedMeasurementCount,
        warmupIterations,
        iterations,
        rounds);
    printSummary("legacy", legacy);
    printSummary("optimized", optimized);
    System.out.printf(
        Locale.ROOT,
        "  change: CPU speedup=%.2fx, allocation reduction=%.1f%%, peak-heap reduction=%.1f%%%n",
        ratio(legacy.getCpuNanosPerOperation(), optimized.getCpuNanosPerOperation()),
        reduction(
            legacy.getAllocatedBytesPerOperation(), optimized.getAllocatedBytesPerOperation()),
        reduction(legacy.getPeakHeapDeltaBytes(), optimized.getPeakHeapDeltaBytes()));
  }

  private static void printSummary(final String label, final Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "  %-9s CPU=%.3f us/op, allocated=%.1f bytes/op, peak heap delta=%.3f MiB%n",
        label,
        summary.getCpuNanosPerOperation() / 1_000.0,
        summary.getAllocatedBytesPerOperation(),
        summary.getPeakHeapDeltaBytes() / 1024.0 / 1024.0);
  }

  private static double ratio(final double baseline, final double optimized) {
    return optimized == 0 ? Double.POSITIVE_INFINITY : baseline / optimized;
  }

  private static double reduction(final double baseline, final double optimized) {
    return baseline == 0 ? 0 : (baseline - optimized) * 100.0 / baseline;
  }

  private static final class Scenario {

    private final TableDeviceLastCache cache;
    private final String[] measurements;
    private final MeasurementSchema[] measurementSchemas;
    private final RowUpdateSource updateSource;
    private final int cachedMeasurementCount;

    private Scenario(
        final TableDeviceLastCache cache,
        final String[] measurements,
        final MeasurementSchema[] measurementSchemas,
        final RowUpdateSource updateSource,
        final int cachedMeasurementCount) {
      this.cache = cache;
      this.measurements = measurements;
      this.measurementSchemas = measurementSchemas;
      this.updateSource = updateSource;
      this.cachedMeasurementCount = cachedMeasurementCount;
    }
  }

  private static final class RowUpdateSource implements LastCacheUpdateSource {

    private final int[] values;

    private RowUpdateSource(final int[] values) {
      this.values = values;
    }

    @Override
    public long getLastCacheTimestamp() {
      return 1L;
    }

    @Override
    public boolean hasLastCacheValue(final int index) {
      return true;
    }

    @Override
    public TimeValuePair getLastCacheValue(final int index) {
      return new TimeValuePair(1L, new TsPrimitiveType.TsInt(values[index]));
    }
  }
}
