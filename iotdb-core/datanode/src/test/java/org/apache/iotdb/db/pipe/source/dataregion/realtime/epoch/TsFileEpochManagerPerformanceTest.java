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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TsFileEpochManagerPerformanceTest {

  private static final String ENABLED_PROPERTY =
      "iotdb.pipe.epoch.insert.rows.aggregation.perf.enabled";
  private static final String ROWS_PROPERTY = "iotdb.pipe.epoch.insert.rows.aggregation.perf.rows";
  private static final String MEASUREMENTS_PROPERTY =
      "iotdb.pipe.epoch.insert.rows.aggregation.perf.measurements";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.pipe.epoch.insert.rows.aggregation.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY =
      "iotdb.pipe.epoch.insert.rows.aggregation.perf.iterations";

  private static volatile Map<IDeviceID, String[]> benchmarkBlackhole;

  @Test
  public void sameDeviceSameSchemaAggregationBenchmark() {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s, -D%s, -D%s and -D%s.",
            ENABLED_PROPERTY,
            ROWS_PROPERTY,
            MEASUREMENTS_PROPERTY,
            WARMUP_ITERATIONS_PROPERTY,
            ITERATIONS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));

    final int rowCount = Integer.getInteger(ROWS_PROPERTY, 10_000);
    final int measurementCount = Integer.getInteger(MEASUREMENTS_PROPERTY, 100);
    final int warmupIterations = Integer.getInteger(WARMUP_ITERATIONS_PROPERTY, 5);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 10);
    Assert.assertTrue(rowCount > 1);
    Assert.assertTrue(measurementCount > 0);
    Assert.assertTrue(warmupIterations > 0);
    Assert.assertTrue(iterations > 0);

    final InsertRowsNode insertRowsNode = createInsertRowsNode(rowCount, measurementCount);
    assertSchemaInfoEquals(
        getLegacyDevice2MeasurementsMap(insertRowsNode),
        TsFileEpochManager.getDevice2MeasurementsMapFromInsertRowsNode(insertRowsNode));

    for (int i = 0; i < warmupIterations; ++i) {
      if ((i & 1) == 0) {
        benchmarkBlackhole = getLegacyDevice2MeasurementsMap(insertRowsNode);
        benchmarkBlackhole =
            TsFileEpochManager.getDevice2MeasurementsMapFromInsertRowsNode(insertRowsNode);
      } else {
        benchmarkBlackhole =
            TsFileEpochManager.getDevice2MeasurementsMapFromInsertRowsNode(insertRowsNode);
        benchmarkBlackhole = getLegacyDevice2MeasurementsMap(insertRowsNode);
      }
    }

    final BenchmarkResult result = runBenchmark(insertRowsNode, iterations);
    System.out.printf(
        Locale.ROOT,
        "InsertRows schema aggregation benchmark: rows=%d, measurements=%d, warmups=%d, iterations=%d, legacy median=%.3f ms/op, optimized median=%.3f ms/op, speedup=%.2fx%n",
        rowCount,
        measurementCount,
        warmupIterations,
        iterations,
        toMillis(result.getLegacyMedianNanos()),
        toMillis(result.getOptimizedMedianNanos()),
        result.getLegacyMedianNanos() / result.getOptimizedMedianNanos());
  }

  private static BenchmarkResult runBenchmark(
      final InsertRowsNode insertRowsNode, final int iterations) {
    final long[] legacyElapsedNanos = new long[iterations];
    final long[] optimizedElapsedNanos = new long[iterations];

    for (int i = 0; i < iterations; ++i) {
      if ((i & 1) == 0) {
        legacyElapsedNanos[i] = measureLegacyAggregation(insertRowsNode);
        optimizedElapsedNanos[i] = measureOptimizedAggregation(insertRowsNode);
      } else {
        optimizedElapsedNanos[i] = measureOptimizedAggregation(insertRowsNode);
        legacyElapsedNanos[i] = measureLegacyAggregation(insertRowsNode);
      }
    }

    return new BenchmarkResult(median(legacyElapsedNanos), median(optimizedElapsedNanos));
  }

  private static long measureLegacyAggregation(final InsertRowsNode insertRowsNode) {
    final long startTime = System.nanoTime();
    benchmarkBlackhole = getLegacyDevice2MeasurementsMap(insertRowsNode);
    return System.nanoTime() - startTime;
  }

  private static long measureOptimizedAggregation(final InsertRowsNode insertRowsNode) {
    final long startTime = System.nanoTime();
    benchmarkBlackhole =
        TsFileEpochManager.getDevice2MeasurementsMapFromInsertRowsNode(insertRowsNode);
    return System.nanoTime() - startTime;
  }

  private static Map<IDeviceID, String[]> getLegacyDevice2MeasurementsMap(
      final InsertRowsNode insertRowsNode) {
    // Keep the pre-optimization implementation from base commit 7ffb1364b84 for comparison.
    return insertRowsNode.getInsertRowNodeList().stream()
        .collect(
            Collectors.toMap(
                InsertNode::getDeviceID,
                InsertNode::getMeasurements,
                (oldMeasurements, newMeasurements) ->
                    Stream.of(Arrays.asList(oldMeasurements), Arrays.asList(newMeasurements))
                        .flatMap(Collection::stream)
                        .distinct()
                        .toArray(String[]::new)));
  }

  private static InsertRowsNode createInsertRowsNode(
      final int rowCount, final int measurementCount) {
    final String[] measurements = new String[measurementCount];
    for (int i = 0; i < measurementCount; ++i) {
      measurements[i] = "s" + i;
    }

    final PlanNodeId planNodeId = new PlanNodeId("schema-aggregation-performance");
    final IDeviceID deviceID =
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.schema_aggregation_performance.d1");
    final InsertRowsNode insertRowsNode = new InsertRowsNode(planNodeId);
    for (int i = 0; i < rowCount; ++i) {
      final InsertRowNode insertRowNode = new InsertRowNode(planNodeId);
      insertRowNode.setDeviceID(deviceID);
      // insertRecords creates a separate measurement array and decoded strings for every row.
      final String[] rowMeasurements = new String[measurementCount];
      for (int j = 0; j < measurementCount; ++j) {
        rowMeasurements[j] = new String(measurements[j].toCharArray());
      }
      insertRowNode.setMeasurements(rowMeasurements);
      insertRowsNode.addOneInsertRowNode(insertRowNode, i);
    }
    return insertRowsNode;
  }

  private static void assertSchemaInfoEquals(
      final Map<IDeviceID, String[]> expected, final Map<IDeviceID, String[]> actual) {
    Assert.assertEquals(expected.keySet(), actual.keySet());
    expected.forEach(
        (deviceID, measurements) -> Assert.assertArrayEquals(measurements, actual.get(deviceID)));
  }

  private static double median(final long[] values) {
    Arrays.sort(values);
    final int middle = values.length / 2;
    return (values.length & 1) == 1
        ? values[middle]
        : values[middle - 1] + (values[middle] - values[middle - 1]) / 2.0;
  }

  private static double toMillis(final double nanos) {
    return nanos / 1_000_000.0;
  }

  private static class BenchmarkResult {

    private final double legacyMedianNanos;
    private final double optimizedMedianNanos;

    private BenchmarkResult(final double legacyMedianNanos, final double optimizedMedianNanos) {
      this.legacyMedianNanos = legacyMedianNanos;
      this.optimizedMedianNanos = optimizedMedianNanos;
    }

    private double getLegacyMedianNanos() {
      return legacyMedianNanos;
    }

    private double getOptimizedMedianNanos() {
      return optimizedMedianNanos;
    }
  }
}
