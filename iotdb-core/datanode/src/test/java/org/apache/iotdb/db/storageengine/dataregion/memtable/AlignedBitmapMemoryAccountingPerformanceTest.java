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

import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class AlignedBitmapMemoryAccountingPerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.aligned.bitmap.accounting.perf.enabled";
  private static final String COLUMNS_PROPERTY = "iotdb.aligned.bitmap.accounting.perf.columns";
  private static final String ROWS_PROPERTY = "iotdb.aligned.bitmap.accounting.perf.rows";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.aligned.bitmap.accounting.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY =
      "iotdb.aligned.bitmap.accounting.perf.iterations";
  private static final String ROUNDS_PROPERTY = "iotdb.aligned.bitmap.accounting.perf.rounds";
  private static final int RECONCILIATION_REPETITIONS = 1024;

  private static volatile long benchmarkBlackhole;

  @Test
  public void alignedTabletBitmapAccountingBenchmark() {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s, -D%s, -D%s, -D%s and -D%s.",
            ENABLED_PROPERTY,
            COLUMNS_PROPERTY,
            ROWS_PROPERTY,
            WARMUP_ITERATIONS_PROPERTY,
            ITERATIONS_PROPERTY,
            ROUNDS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    int columnCount = Integer.getInteger(COLUMNS_PROPERTY, 64);
    int rowCount = Integer.getInteger(ROWS_PROPERTY, 256);
    int warmupIterations = Integer.getInteger(WARMUP_ITERATIONS_PROPERTY, 200);
    int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 2000);
    int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    Assert.assertTrue(columnCount > 0);
    Assert.assertTrue(rowCount > 0);
    Assert.assertTrue(warmupIterations > 0);
    Assert.assertTrue(iterations > 0);
    Assert.assertTrue(rounds > 0);

    runScenario(
        "dense",
        createScenario(columnCount, rowCount, false),
        warmupIterations,
        iterations,
        rounds);
    runScenario(
        "null-heavy",
        createScenario(columnCount, rowCount, true),
        warmupIterations,
        iterations,
        rounds);
  }

  private static void runScenario(
      String label, Scenario scenario, int warmupIterations, int iterations, int rounds) {
    runReconciliation(
        createAccountingTarget(scenario), warmupIterations * RECONCILIATION_REPETITIONS);
    runWrite(scenario, createMemChunks(scenario, warmupIterations));

    Measurement[] reconciliationMeasurements = new Measurement[rounds];
    Measurement[] writeMeasurements = new Measurement[rounds];
    for (int i = 0; i < rounds; i++) {
      if ((i & 1) == 0) {
        reconciliationMeasurements[i] = measureReconciliation(scenario, iterations);
        writeMeasurements[i] = measureWrite(scenario, iterations);
      } else {
        writeMeasurements[i] = measureWrite(scenario, iterations);
        reconciliationMeasurements[i] = measureReconciliation(scenario, iterations);
      }
    }

    Summary reconciliationSummary =
        ManualPerformanceTestUtils.summarize(
            reconciliationMeasurements, iterations * RECONCILIATION_REPETITIONS);
    Summary writeSummary = ManualPerformanceTestUtils.summarize(writeMeasurements, iterations);
    printResult(
        label, scenario, warmupIterations, iterations, rounds, reconciliationSummary, writeSummary);
  }

  private static Measurement measureReconciliation(Scenario scenario, int iterations) {
    AccountingTarget target = createAccountingTarget(scenario);
    return ManualPerformanceTestUtils.measure(
        1, () -> runReconciliation(target, iterations * RECONCILIATION_REPETITIONS));
  }

  private static Measurement measureWrite(Scenario scenario, int iterations) {
    AlignedWritableMemChunk[] memChunks = createMemChunks(scenario, iterations);
    return ManualPerformanceTestUtils.measure(1, () -> runWrite(scenario, memChunks));
  }

  private static void runReconciliation(AccountingTarget target, int iterations) {
    long correction = 0;
    for (int i = 0; i < iterations; i++) {
      TsFileProcessor.AlignedTVListRamCostSnapshot snapshot =
          new TsFileProcessor.AlignedTVListRamCostSnapshot(target.memTable, target.deviceId);
      correction += snapshot.getMemoryCorrection(0);
    }
    benchmarkBlackhole = correction + iterations;
  }

  private static void runWrite(Scenario scenario, AlignedWritableMemChunk[] memChunks) {
    for (AlignedWritableMemChunk memChunk : memChunks) {
      memChunk.writeAlignedTablet(
          scenario.times,
          scenario.columns,
          scenario.bitMaps,
          scenario.schemas,
          0,
          scenario.times.length,
          null);
    }
    benchmarkBlackhole = memChunks[memChunks.length - 1].rowCount();
  }

  private static AlignedWritableMemChunk[] createMemChunks(Scenario scenario, int count) {
    AlignedWritableMemChunk[] memChunks = new AlignedWritableMemChunk[count];
    for (int i = 0; i < count; i++) {
      memChunks[i] = new AlignedWritableMemChunk(new ArrayList<>(scenario.schemas), false);
    }
    return memChunks;
  }

  private static AccountingTarget createAccountingTarget(Scenario scenario) {
    AlignedWritableMemChunk memChunk =
        new AlignedWritableMemChunk(new ArrayList<>(scenario.schemas), false);
    memChunk.writeAlignedTablet(
        scenario.times,
        scenario.columns,
        scenario.bitMaps,
        scenario.schemas,
        0,
        scenario.times.length,
        null);
    IDeviceID deviceId = IDeviceID.Factory.DEFAULT_FACTORY.create("root.accounting.d0");
    IMemTable memTable =
        new PrimitiveMemTable(
            "root.accounting",
            "0",
            Collections.singletonMap(
                deviceId,
                new AlignedWritableMemChunkGroup(
                    memChunk, new ArrayList<>(scenario.schemas), false)));
    return new AccountingTarget(memTable, deviceId);
  }

  private static Scenario createScenario(int columnCount, int rowCount, boolean nullHeavy) {
    String[] measurements = new String[columnCount];
    TSDataType[] dataTypes = new TSDataType[columnCount];
    Object[] columns = new Object[columnCount];
    BitMap[] bitMaps = nullHeavy ? new BitMap[columnCount] : null;
    List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    for (int column = 0; column < columnCount; column++) {
      measurements[column] = "s" + column;
      dataTypes[column] = TSDataType.INT32;
      schemas.add(new MeasurementSchema(measurements[column], TSDataType.INT32));
      int[] values = new int[rowCount];
      for (int row = 0; row < rowCount; row++) {
        values[row] = row;
      }
      columns[column] = values;
      if (nullHeavy) {
        bitMaps[column] = BitMap.createBitMapDynamically(rowCount);
        for (int row = column & 1; row < rowCount; row += 2) {
          bitMaps[column].mark(row);
        }
      }
    }
    long[] times = new long[rowCount];
    for (int row = 0; row < rowCount; row++) {
      times[row] = row;
    }
    return new Scenario(measurements, dataTypes, columns, bitMaps, schemas, times);
  }

  private static void printResult(
      String label,
      Scenario scenario,
      int warmupIterations,
      int iterations,
      int rounds,
      Summary reconciliationSummary,
      Summary writeSummary) {
    System.out.printf(
        Locale.ROOT,
        "Aligned bitmap accounting benchmark (%s): columns=%d, rows=%d, warmups=%d, iterations/round=%d, rounds=%d%n",
        label,
        scenario.measurements.length,
        scenario.times.length,
        warmupIterations,
        iterations,
        rounds);
    printSummary("reconcile", reconciliationSummary);
    printSummary("write", writeSummary);
    System.out.printf(
        Locale.ROOT,
        "  reconciliation/write CPU ratio=%.2f%%, allocation ratio=%.2f%%%n",
        percentage(
            reconciliationSummary.getCpuNanosPerOperation(),
            writeSummary.getCpuNanosPerOperation()),
        percentage(
            reconciliationSummary.getAllocatedBytesPerOperation(),
            writeSummary.getAllocatedBytesPerOperation()));
  }

  private static void printSummary(String label, Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "  %-10s CPU=%.3f us/batch, allocated=%.1f bytes/batch, peak heap delta=%.3f MiB%n",
        label,
        summary.getCpuNanosPerOperation() / 1_000.0,
        summary.getAllocatedBytesPerOperation(),
        summary.getPeakHeapDeltaBytes() / 1024.0 / 1024.0);
  }

  private static double percentage(double numerator, double denominator) {
    return denominator == 0 ? 0 : numerator * 100.0 / denominator;
  }

  private static final class AccountingTarget {

    private final IMemTable memTable;
    private final IDeviceID deviceId;

    private AccountingTarget(IMemTable memTable, IDeviceID deviceId) {
      this.memTable = memTable;
      this.deviceId = deviceId;
    }
  }

  private static final class Scenario {

    private final String[] measurements;
    private final TSDataType[] dataTypes;
    private final Object[] columns;
    private final BitMap[] bitMaps;
    private final List<IMeasurementSchema> schemas;
    private final long[] times;

    private Scenario(
        String[] measurements,
        TSDataType[] dataTypes,
        Object[] columns,
        BitMap[] bitMaps,
        List<IMeasurementSchema> schemas,
        long[] times) {
      this.measurements = measurements;
      this.dataTypes = dataTypes;
      this.columns = columns;
      this.bitMaps = bitMaps;
      this.schemas = schemas;
      this.times = times;
    }
  }
}
