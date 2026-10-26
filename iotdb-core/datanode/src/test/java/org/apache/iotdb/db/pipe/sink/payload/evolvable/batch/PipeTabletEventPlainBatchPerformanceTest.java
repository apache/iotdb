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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeTabletEventPlainBatchPerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.pipe.plain.batch.perf.enabled";
  private static final String COLUMNS_PROPERTY = "iotdb.pipe.plain.batch.perf.columns";
  private static final String ITERATIONS_PROPERTY = "iotdb.pipe.plain.batch.perf.iterations";

  @Test
  public void wideTableMayAppendTabletBenchmark() {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s and -D%s.",
            ENABLED_PROPERTY, COLUMNS_PROPERTY, ITERATIONS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));

    final int columnCount = Integer.getInteger(COLUMNS_PROPERTY, 100_000);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 1_000);
    final int warmupIterations = Math.max(10, Math.min(iterations, 200));

    final Tablet target = createWideTableModelTablet("table1", columnCount);
    final Tablet source = createWideTableModelTablet("table1", columnCount);

    runLegacyPrecheck(target, source, warmupIterations);
    runOptimizedPrecheck(target, source, warmupIterations);

    final BenchmarkResult legacyResult = runLegacyPrecheck(target, source, iterations);
    final BenchmarkResult optimizedResult = runOptimizedPrecheck(target, source, iterations);

    Assert.assertEquals(iterations, legacyResult.getSuccessCount());
    Assert.assertEquals(iterations, optimizedResult.getSuccessCount());

    System.out.printf(
        "Wide-table tablet append precheck benchmark: columns=%d, iterations=%d, legacy=%.3f ms, optimized=%.3f ms, speedup=%.2fx%n",
        columnCount,
        iterations,
        toMillis(legacyResult.getElapsedNanos()),
        toMillis(optimizedResult.getElapsedNanos()),
        (double) legacyResult.getElapsedNanos() / optimizedResult.getElapsedNanos());
  }

  private static BenchmarkResult runLegacyPrecheck(
      final Tablet target, final Tablet source, final int iterations) {
    int successCount = 0;
    final long startTime = System.nanoTime();
    for (int i = 0; i < iterations; ++i) {
      if (legacyCanAppendTablet(target, source)) {
        ++successCount;
      }
    }
    return new BenchmarkResult(System.nanoTime() - startTime, successCount);
  }

  private static BenchmarkResult runOptimizedPrecheck(
      final Tablet target, final Tablet source, final int iterations) {
    int successCount = 0;
    final long startTime = System.nanoTime();
    for (int i = 0; i < iterations; ++i) {
      if (PipeTabletEventPlainBatch.mayAppendTablet(target, source)) {
        ++successCount;
      }
    }
    return new BenchmarkResult(System.nanoTime() - startTime, successCount);
  }

  private static boolean legacyCanAppendTablet(final Tablet target, final Tablet source) {
    return Objects.equals(target.getDeviceId(), source.getDeviceId())
        && Objects.equals(target.getSchemas(), source.getSchemas())
        && Objects.equals(target.getColumnTypes(), source.getColumnTypes());
  }

  private static Tablet createWideTableModelTablet(final String tableName, final int columnCount) {
    final List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    final List<ColumnCategory> columnCategories = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; ++i) {
      schemas.add(new MeasurementSchema("s" + i, TSDataType.INT32));
      columnCategories.add(ColumnCategory.FIELD);
    }
    return new Tablet(
        tableName, schemas, columnCategories, new long[] {0L}, new Object[columnCount], null, 1);
  }

  private static double toMillis(final long nanos) {
    return nanos / 1_000_000.0;
  }

  private static class BenchmarkResult {

    private final long elapsedNanos;
    private final int successCount;

    private BenchmarkResult(final long elapsedNanos, final int successCount) {
      this.elapsedNanos = elapsedNanos;
      this.successCount = successCount;
    }

    private long getElapsedNanos() {
      return elapsedNanos;
    }

    private int getSuccessCount() {
      return successCount;
    }
  }
}
