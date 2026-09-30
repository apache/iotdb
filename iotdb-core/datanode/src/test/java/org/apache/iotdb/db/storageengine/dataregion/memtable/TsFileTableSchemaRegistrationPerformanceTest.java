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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionInfo;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.file.metadata.TableSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class TsFileTableSchemaRegistrationPerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.tsfile.table.schema.perf.enabled";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.tsfile.table.schema.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY = "iotdb.tsfile.table.schema.perf.iterations";
  private static final String ROUNDS_PROPERTY = "iotdb.tsfile.table.schema.perf.rounds";
  private static volatile int benchmarkBlackhole;

  @Test
  public void tableSchemaRegistrationBenchmark() throws Exception {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s, -D%s and -D%s.",
            ENABLED_PROPERTY, WARMUP_ITERATIONS_PROPERTY, ITERATIONS_PROPERTY, ROUNDS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    final int warmupIterations = Integer.getInteger(WARMUP_ITERATIONS_PROPERTY, 20_000);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 100_000);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 7);
    Assert.assertTrue(warmupIterations > 0);
    Assert.assertTrue(iterations > 0);
    Assert.assertTrue(rounds > 0);

    final String storageGroup = "root.vehicle";
    final String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
    final String filePath = TestConstant.getTestTsFilePath(storageGroup, 0, 0, 0);
    TsFileProcessor processor = null;
    EnvironmentUtils.envSetUp();
    try {
      final File file = SystemFileFactory.INSTANCE.getFile(filePath);
      if (!file.getParentFile().exists()) {
        Assert.assertTrue(file.getParentFile().mkdirs());
      }
      final DataRegionInfo regionInfo =
          new DataRegionInfo(new DataRegionTest.DummyDataRegion(systemDir, storageGroup));
      processor =
          new TsFileProcessor(
              storageGroup,
              file,
              regionInfo,
              tsFileProcessor -> {},
              (tsFileProcessor, updateMap, systemFlushTime) -> {},
              true);
      final TsFileProcessorInfo processorInfo = new TsFileProcessorInfo(regionInfo);
      processor.setTsFileProcessorInfo(processorInfo);
      regionInfo.initTsFileProcessorInfo(processor);
      SystemInfo.getInstance().reportStorageGroupStatus(regionInfo, processor);

      final AtomicLong legacySupplierCalls = new AtomicLong();
      final AtomicLong cachedSupplierCalls = new AtomicLong();
      final Function<String, TableSchema> legacySupplier =
          tableName -> {
            legacySupplierCalls.incrementAndGet();
            return new TableSchema(tableName);
          };
      final Function<String, TableSchema> cachedSupplier =
          tableName -> {
            cachedSupplierCalls.incrementAndGet();
            return new TableSchema(tableName);
          };

      runLegacy(processor, warmupIterations, legacySupplier);
      runCached(processor, warmupIterations, cachedSupplier);
      legacySupplierCalls.set(0);
      cachedSupplierCalls.set(0);

      final Measurement[] legacyMeasurements = new Measurement[rounds];
      final Measurement[] cachedMeasurements = new Measurement[rounds];
      for (int round = 0; round < rounds; ++round) {
        if ((round & 1) == 0) {
          legacyMeasurements[round] = measureLegacy(processor, iterations, legacySupplier);
          cachedMeasurements[round] = measureCached(processor, iterations, cachedSupplier);
        } else {
          cachedMeasurements[round] = measureCached(processor, iterations, cachedSupplier);
          legacyMeasurements[round] = measureLegacy(processor, iterations, legacySupplier);
        }
      }

      final Summary legacySummary =
          ManualPerformanceTestUtils.summarize(legacyMeasurements, iterations);
      final Summary cachedSummary =
          ManualPerformanceTestUtils.summarize(cachedMeasurements, iterations);
      Assert.assertEquals((long) iterations * rounds, legacySupplierCalls.get());
      Assert.assertEquals(0, cachedSupplierCalls.get());
      printResult(
          warmupIterations,
          iterations,
          rounds,
          legacySummary,
          cachedSummary,
          legacySupplierCalls.get(),
          cachedSupplierCalls.get());
    } finally {
      if (processor != null) {
        processor.syncClose();
      }
      EnvironmentUtils.cleanEnv();
      EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    }
  }

  private static Measurement measureLegacy(
      TsFileProcessor processor,
      int iterations,
      Function<String, TableSchema> tableSchemaFunction) {
    return ManualPerformanceTestUtils.measure(
        iterations, () -> legacyRegister(processor, tableSchemaFunction));
  }

  private static Measurement measureCached(
      TsFileProcessor processor,
      int iterations,
      Function<String, TableSchema> tableSchemaFunction) {
    return ManualPerformanceTestUtils.measure(
        iterations, () -> cachedRegister(processor, tableSchemaFunction));
  }

  private static void runLegacy(
      TsFileProcessor processor,
      int iterations,
      Function<String, TableSchema> tableSchemaFunction) {
    for (int i = 0; i < iterations; ++i) {
      legacyRegister(processor, tableSchemaFunction);
    }
  }

  private static void runCached(
      TsFileProcessor processor,
      int iterations,
      Function<String, TableSchema> tableSchemaFunction) {
    for (int i = 0; i < iterations; ++i) {
      cachedRegister(processor, tableSchemaFunction);
    }
  }

  private static void legacyRegister(
      TsFileProcessor processor, Function<String, TableSchema> tableSchemaFunction) {
    final String tableName = "legacy_table";
    processor
        .getWriter()
        .getSchema()
        .getTableSchemaMap()
        .put(tableName, tableSchemaFunction.apply(tableName));
    benchmarkBlackhole = processor.getWriter().getSchema().getTableSchemaMap().size();
  }

  private static void cachedRegister(
      TsFileProcessor processor, Function<String, TableSchema> tableSchemaFunction) {
    processor.registerToTsFile("cached_table", 1, tableSchemaFunction);
    benchmarkBlackhole = processor.getWriter().getSchema().getTableSchemaMap().size();
  }

  private static void printResult(
      int warmupIterations,
      int iterations,
      int rounds,
      Summary legacySummary,
      Summary cachedSummary,
      long legacySupplierCalls,
      long cachedSupplierCalls) {
    System.out.printf(
        Locale.ROOT,
        "TsFile table schema registration benchmark: warmup=%d, iterations=%d, rounds=%d%n"
            + "legacy: cpu=%.2f ns/op, allocation=%.2f bytes/op, supplierCalls=%d%n"
            + "cached: cpu=%.2f ns/op, allocation=%.2f bytes/op, supplierCalls=%d%n"
            + "reduction: cpu=%.2f%%, allocation=%.2f%%%n",
        warmupIterations,
        iterations,
        rounds,
        legacySummary.getCpuNanosPerOperation(),
        legacySummary.getAllocatedBytesPerOperation(),
        legacySupplierCalls,
        cachedSummary.getCpuNanosPerOperation(),
        cachedSummary.getAllocatedBytesPerOperation(),
        cachedSupplierCalls,
        reduction(legacySummary.getCpuNanosPerOperation(), cachedSummary.getCpuNanosPerOperation()),
        reduction(
            legacySummary.getAllocatedBytesPerOperation(),
            cachedSummary.getAllocatedBytesPerOperation()));
  }

  private static double reduction(double before, double after) {
    return before == 0 ? 0 : (before - after) * 100 / before;
  }
}
