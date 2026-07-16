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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class InsertNodeMemoryEstimatorPerformanceTest {

  private static final String ENABLED_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.perf.enabled";
  private static final String COLUMNS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.perf.columns";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY =
      "iotdb.pipe.insert.node.memory.estimator.perf.iterations";

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

  private static long runBenchmark(final InsertTabletNode insertTabletNode, final int iterations) {
    final long startTime = System.nanoTime();
    for (int i = 0; i < iterations; ++i) {
      benchmarkBlackhole = InsertNodeMemoryEstimator.sizeOf(insertTabletNode);
    }
    return System.nanoTime() - startTime;
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
