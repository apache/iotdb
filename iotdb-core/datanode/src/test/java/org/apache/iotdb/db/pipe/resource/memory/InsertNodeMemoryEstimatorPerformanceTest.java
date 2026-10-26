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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Locale;

public class InsertNodeMemoryEstimatorPerformanceTest {

  private static final String ENABLED_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.perf.enabled";
  private static final String COLUMNS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.perf.columns";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.perf.iterations";
  private static final String REUSE_ENABLED_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.reuse.perf.enabled";
  private static final String REUSE_ROWS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.reuse.perf.rows";
  private static final String REUSE_MEASUREMENTS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.reuse.perf.measurements";
  private static final String REUSE_WARMUP_ITERATIONS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.reuse.perf.warmup.iterations";
  private static final String REUSE_ITERATIONS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.reuse.perf.iterations";
  private static final String REUSE_ROUNDS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.reuse.perf.rounds";

  private static volatile long benchmarkBlackhole;

  @Test
  public void wideTabletSizeOfBenchmark() throws IllegalPathException {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s, -D%s and -D%s.",
            ENABLED_PROPERTY, COLUMNS_PROPERTY, WARMUP_ITERATIONS_PROPERTY, ITERATIONS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));

    final int columnCount = Integer.getInteger(COLUMNS_PROPERTY, 100_000);
    final int warmupIterations = Integer.getInteger(WARMUP_ITERATIONS_PROPERTY, 3);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 10);
    Assert.assertTrue(columnCount > 0);
    Assert.assertTrue(warmupIterations > 0);
    Assert.assertTrue(iterations > 0);

    final InsertTabletNode insertTabletNode = createWideInsertTabletNode(columnCount);
    runBenchmark(insertTabletNode, warmupIterations);
    final long elapsedNanos = runBenchmark(insertTabletNode, iterations);

    Assert.assertTrue(benchmarkBlackhole > 0);
    System.out.printf(
        "InsertNode memory estimate benchmark: columns=%d, warmups=%d, iterations=%d, %.3f ms/op%n",
        columnCount,
        warmupIterations,
        iterations,
        elapsedNanos / (double) iterations / 1_000_000.0);
  }

  @Test
  public void compositeInsertRowsReusableSetBenchmark() throws IllegalPathException {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s, -D%s, -D%s, -D%s and -D%s.",
            REUSE_ENABLED_PROPERTY,
            REUSE_ROWS_PROPERTY,
            REUSE_MEASUREMENTS_PROPERTY,
            REUSE_WARMUP_ITERATIONS_PROPERTY,
            REUSE_ITERATIONS_PROPERTY,
            REUSE_ROUNDS_PROPERTY),
        Boolean.getBoolean(REUSE_ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    final int rowCount = Integer.getInteger(REUSE_ROWS_PROPERTY, 10);
    final int measurementCount = Integer.getInteger(REUSE_MEASUREMENTS_PROPERTY, 500);
    final int warmupIterations = Integer.getInteger(REUSE_WARMUP_ITERATIONS_PROPERTY, 1_000);
    final int iterations = Integer.getInteger(REUSE_ITERATIONS_PROPERTY, 20_000);
    final int rounds = Integer.getInteger(REUSE_ROUNDS_PROPERTY, 5);
    Assert.assertTrue(rowCount > 1);
    Assert.assertTrue(measurementCount > 0);
    Assert.assertTrue(warmupIterations > 0);
    Assert.assertTrue(iterations > 0);
    Assert.assertTrue(rounds > 0);

    final InsertRowsNode insertRowsNode = createCompositeInsertRowsNode(rowCount, measurementCount);
    InsertNodeMemoryEstimator.clearReusableDeduplicatedObjectsForTest();
    final long freshSetEstimate = InsertNodeMemoryEstimator.sizeOf(insertRowsNode);
    final long reusedSetEstimate = InsertNodeMemoryEstimator.sizeOf(insertRowsNode);
    Assert.assertEquals(freshSetEstimate, reusedSetEstimate);

    for (int i = 0; i < warmupIterations; ++i) {
      if ((i & 1) == 0) {
        runFreshSetEstimate(insertRowsNode);
        runEstimate(insertRowsNode);
      } else {
        runEstimate(insertRowsNode);
        runFreshSetEstimate(insertRowsNode);
      }
    }

    final Measurement[] freshSetMeasurements = new Measurement[rounds];
    final Measurement[] reusedSetMeasurements = new Measurement[rounds];
    for (int i = 0; i < rounds; ++i) {
      if ((i & 1) == 0) {
        freshSetMeasurements[i] =
            ManualPerformanceTestUtils.measure(
                iterations,
                InsertNodeMemoryEstimator::clearReusableDeduplicatedObjectsForTest,
                () -> runEstimate(insertRowsNode));
        reusedSetMeasurements[i] =
            ManualPerformanceTestUtils.measure(iterations, () -> runEstimate(insertRowsNode));
      } else {
        reusedSetMeasurements[i] =
            ManualPerformanceTestUtils.measure(iterations, () -> runEstimate(insertRowsNode));
        freshSetMeasurements[i] =
            ManualPerformanceTestUtils.measure(
                iterations,
                InsertNodeMemoryEstimator::clearReusableDeduplicatedObjectsForTest,
                () -> runEstimate(insertRowsNode));
      }
    }
    InsertNodeMemoryEstimator.clearReusableDeduplicatedObjectsForTest();

    final Summary freshSetSummary =
        ManualPerformanceTestUtils.summarize(freshSetMeasurements, iterations);
    final Summary reusedSetSummary =
        ManualPerformanceTestUtils.summarize(reusedSetMeasurements, iterations);
    System.out.printf(
        Locale.ROOT,
        "InsertRows memory-estimator set-reuse benchmark: rows=%d, measurements=%d, warmups=%d, iterations/round=%d, rounds=%d%n",
        rowCount,
        measurementCount,
        warmupIterations,
        iterations,
        rounds);
    printSummary("legacy", freshSetSummary);
    printSummary("optimized", reusedSetSummary);
    System.out.printf(
        Locale.ROOT,
        "  change: CPU speedup=%.2fx, allocation reduction=%.1f%%, peak-heap reduction=%.1f%%%n",
        ratio(
            freshSetSummary.getCpuNanosPerOperation(), reusedSetSummary.getCpuNanosPerOperation()),
        reduction(
            freshSetSummary.getAllocatedBytesPerOperation(),
            reusedSetSummary.getAllocatedBytesPerOperation()),
        reduction(
            freshSetSummary.getPeakHeapDeltaBytes(), reusedSetSummary.getPeakHeapDeltaBytes()));
  }

  private static long runBenchmark(final InsertTabletNode insertTabletNode, final int iterations) {
    final long startTime = System.nanoTime();
    for (int i = 0; i < iterations; ++i) {
      benchmarkBlackhole = InsertNodeMemoryEstimator.sizeOf(insertTabletNode);
    }
    return System.nanoTime() - startTime;
  }

  private static void runFreshSetEstimate(final InsertRowsNode insertRowsNode) {
    InsertNodeMemoryEstimator.clearReusableDeduplicatedObjectsForTest();
    runEstimate(insertRowsNode);
  }

  private static void runEstimate(final InsertRowsNode insertRowsNode) {
    benchmarkBlackhole = InsertNodeMemoryEstimator.sizeOf(insertRowsNode);
  }

  private static InsertRowsNode createCompositeInsertRowsNode(
      final int rowCount, final int measurementCount) throws IllegalPathException {
    final String[] measurements = new String[measurementCount];
    final TSDataType[] dataTypes = new TSDataType[measurementCount];
    final MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurementCount];
    final Object[] values = new Object[measurementCount];
    for (int i = 0; i < measurementCount; ++i) {
      measurements[i] = "s" + i;
      dataTypes[i] = TSDataType.INT32;
      measurementSchemas[i] = new MeasurementSchema(measurements[i], TSDataType.INT32);
      values[i] = i;
    }

    final PlanNodeId planNodeId = new PlanNodeId("composite-memory-estimator");
    final PartialPath devicePath = new PartialPath("root.memory_estimator.d1");
    final InsertRowsNode insertRowsNode = new InsertRowsNode(planNodeId);
    for (int i = 0; i < rowCount; ++i) {
      insertRowsNode.addOneInsertRowNode(
          new InsertRowNode(
              planNodeId,
              devicePath,
              false,
              measurements,
              dataTypes,
              measurementSchemas,
              i,
              values,
              false),
          i);
    }
    insertRowsNode.setTargetPath(devicePath);
    insertRowsNode.setMeasurements(measurements);
    insertRowsNode.setDataTypes(dataTypes);
    insertRowsNode.setMeasurementSchemas(measurementSchemas);
    return insertRowsNode;
  }

  private static void printSummary(final String label, final Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "  %-10s CPU=%.3f us/op, allocated=%.1f bytes/op, peak heap delta=%.3f MiB%n",
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

  private static InsertTabletNode createWideInsertTabletNode(final int columnCount)
      throws IllegalPathException {
    final String[] measurements = new String[columnCount];
    final TSDataType[] dataTypes = new TSDataType[columnCount];
    final MeasurementSchema[] measurementSchemas = new MeasurementSchema[columnCount];
    for (int i = 0; i < columnCount; ++i) {
      measurements[i] = "s" + i;
      dataTypes[i] = TSDataType.INT32;
      measurementSchemas[i] = new MeasurementSchema(measurements[i], TSDataType.INT32);
    }
    return new InsertTabletNode(
        new PlanNodeId("wide-memory-estimator"),
        new PartialPath("root.sg.d1"),
        false,
        measurements,
        dataTypes,
        measurementSchemas,
        new long[] {0L},
        null,
        new Object[columnCount],
        1);
  }
}
