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

package org.apache.iotdb.db.it.performance;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBAlignedTVListPrimitiveArrayPerformanceIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBAlignedTVListPrimitiveArrayPerformanceIT.class);

  private static final String PROPERTY_PREFIX = "iotdb.aligned.lazy-allocation.perf.";
  private static final String ENABLED_PROPERTY = PROPERTY_PREFIX + "enabled";
  private static final String IMPLEMENTATION_PROPERTY = PROPERTY_PREFIX + "implementation";
  private static final String REVISION_PROPERTY = PROPERTY_PREFIX + "revision";
  private static final String HISTORICAL_ROW_COUNT_PROPERTY =
      PROPERTY_PREFIX + "historical.row.count";
  private static final String NEW_COLUMN_COUNT_PROPERTY = PROPERTY_PREFIX + "new.column.count";
  private static final String NO_NULL_BATCH_COUNT_PROPERTY =
      PROPERTY_PREFIX + "no-null.batch.count";
  private static final String WARMUP_ROUND_COUNT_PROPERTY = PROPERTY_PREFIX + "warmup.round.count";
  private static final String ROUND_COUNT_PROPERTY = PROPERTY_PREFIX + "round.count";
  private static final String RESULT_PATH_PROPERTY = PROPERTY_PREFIX + "result.path";
  private static final String BASELINE_RESULT_PATH_PROPERTY =
      PROPERTY_PREFIX + "baseline.result.path";
  private static final String REPORT_PATH_PROPERTY = PROPERTY_PREFIX + "report.path";

  private static final int DATANODE_MAX_HEAP_SIZE_IN_MB = 1536;
  private static final int PRIMITIVE_ARRAY_SIZE = 64;
  private static final int HISTORICAL_ROW_COUNT =
      Integer.getInteger(HISTORICAL_ROW_COUNT_PROPERTY, 10_000);
  private static final int NEW_COLUMN_COUNT = Integer.getInteger(NEW_COLUMN_COUNT_PROPERTY, 512);
  private static final int NO_NULL_ROWS_PER_BATCH = PRIMITIVE_ARRAY_SIZE;
  private static final int NO_NULL_BATCH_COUNT =
      Integer.getInteger(NO_NULL_BATCH_COUNT_PROPERTY, 50);
  private static final int WARMUP_ROUND_COUNT = Integer.getInteger(WARMUP_ROUND_COUNT_PROPERTY, 1);
  private static final int ROUND_COUNT = Integer.getInteger(ROUND_COUNT_PROPERTY, 5);
  private static final String IMPLEMENTATION =
      System.getProperty(IMPLEMENTATION_PROPERTY, "candidate");
  private static final String REVISION = System.getProperty(REVISION_PROPERTY, "unknown");
  private static final String DEVICE_PREFIX = "root.aligned_lazy_allocation_performance.d";
  private static final String NO_NULL_DEVICE = "root.aligned_lazy_allocation_performance.no_null";

  @BeforeClass
  public static void setUp() throws Exception {
    Assume.assumeTrue(Boolean.getBoolean(ENABLED_PROPERTY));
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeJVMConfig()
        .setMaxHeapSize(DATANODE_MAX_HEAP_SIZE_IN_MB);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setEnableMemControl(true)
        .setPrimitiveArraySize(PRIMITIVE_ARRAY_SIZE)
        .setMemtableSizeThreshold(768L * 1024 * 1024)
        .setDatanodeMemoryProportion("6:1:1:1:1:1")
        .setWriteMemoryProportion("100:1");
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAlignedWritePerformance() throws Exception {
    Assert.assertTrue(HISTORICAL_ROW_COUNT > 0);
    Assert.assertTrue(NEW_COLUMN_COUNT > 0);
    Assert.assertTrue(NO_NULL_BATCH_COUNT > 0);
    Assert.assertTrue(WARMUP_ROUND_COUNT >= 0);
    Assert.assertTrue(ROUND_COUNT > 0);

    List<String> newMeasurements = new ArrayList<>(NEW_COLUMN_COUNT);
    List<TSDataType> newDataTypes = new ArrayList<>(NEW_COLUMN_COUNT);
    List<Object> newValues = new ArrayList<>(NEW_COLUMN_COUNT);
    for (int i = 1; i <= NEW_COLUMN_COUNT; i++) {
      newMeasurements.add("s" + i);
      newDataTypes.add(TSDataType.INT64);
      newValues.add((long) i);
    }

    long[] lazyAllocationElapsedNanos;
    long[] noNullElapsedNanos;
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      lazyAllocationElapsedNanos =
          runLazyAllocationWorkload(session, newMeasurements, newDataTypes, newValues);
      noNullElapsedNanos = runNoNullWorkload(session);
    }

    BenchmarkResult result =
        new BenchmarkResult(
            IMPLEMENTATION,
            REVISION,
            HISTORICAL_ROW_COUNT,
            NEW_COLUMN_COUNT,
            WARMUP_ROUND_COUNT,
            NO_NULL_BATCH_COUNT,
            lazyAllocationElapsedNanos,
            noNullElapsedNanos);
    Path resultPath = getResultPath();
    writeResult(resultPath, result);
    LOGGER.info("AlignedTVList lazy-allocation performance result: {}", result.toSummary());
    LOGGER.info("Performance result written to {}", resultPath.toAbsolutePath());

    String baselineResultPath = System.getProperty(BASELINE_RESULT_PATH_PROPERTY);
    if (baselineResultPath != null && !baselineResultPath.isEmpty()) {
      BenchmarkResult baseline = readResult(Paths.get(baselineResultPath));
      assertComparable(baseline, result);
      Path reportPath =
          Paths.get(
              System.getProperty(
                  REPORT_PATH_PROPERTY,
                  "target/aligned-tvlist-lazy-allocation-performance-report.md"));
      writeFile(reportPath, buildReport(baseline, result));
      LOGGER.info("Comparison report written to {}", reportPath.toAbsolutePath());
    }
  }

  private static long[] runLazyAllocationWorkload(
      ISession session,
      List<String> newMeasurements,
      List<TSDataType> newDataTypes,
      List<Object> newValues)
      throws Exception {
    long[] elapsedNanos = new long[ROUND_COUNT];
    int totalRoundCount = WARMUP_ROUND_COUNT + ROUND_COUNT;
    for (int round = 0; round < totalRoundCount; round++) {
      String device = DEVICE_PREFIX + round;
      createAlignedTimeseries(session, device, NEW_COLUMN_COUNT + 1);
      insertHistoricalRows(session, device);

      long startNanos = System.nanoTime();
      session.insertAlignedRecord(
          device, HISTORICAL_ROW_COUNT, newMeasurements, newDataTypes, newValues);
      long roundElapsedNanos = System.nanoTime() - startNanos;
      if (round >= WARMUP_ROUND_COUNT) {
        elapsedNanos[round - WARMUP_ROUND_COUNT] = roundElapsedNanos;
      }

      assertLazyAllocationDataWritten(session, device);
    }
    return elapsedNanos;
  }

  private static long[] runNoNullWorkload(ISession session) throws Exception {
    createAlignedTimeseries(session, NO_NULL_DEVICE, NEW_COLUMN_COUNT);
    List<IMeasurementSchema> schemas = new ArrayList<>(NEW_COLUMN_COUNT);
    for (int column = 0; column < NEW_COLUMN_COUNT; column++) {
      schemas.add(new MeasurementSchema("s" + column, TSDataType.INT64));
    }
    Tablet tablet = new Tablet(NO_NULL_DEVICE, schemas, NO_NULL_ROWS_PER_BATCH);
    for (int column = 0; column < NEW_COLUMN_COUNT; column++) {
      long[] columnValues = (long[]) tablet.getValues()[column];
      for (int row = 0; row < NO_NULL_ROWS_PER_BATCH; row++) {
        columnValues[row] = column + row;
      }
    }
    tablet.setRowSize(NO_NULL_ROWS_PER_BATCH);

    long nextTimestamp = 0;
    long[] elapsedNanos = new long[ROUND_COUNT];
    int totalRoundCount = WARMUP_ROUND_COUNT + ROUND_COUNT;
    for (int round = 0; round < totalRoundCount; round++) {
      long startNanos = System.nanoTime();
      for (int batch = 0; batch < NO_NULL_BATCH_COUNT; batch++) {
        for (int row = 0; row < NO_NULL_ROWS_PER_BATCH; row++) {
          tablet.getTimestamps()[row] = nextTimestamp++;
        }
        session.insertAlignedTablet(tablet);
      }
      long roundElapsedNanos = System.nanoTime() - startNanos;
      if (round >= WARMUP_ROUND_COUNT) {
        elapsedNanos[round - WARMUP_ROUND_COUNT] = roundElapsedNanos;
      }
    }

    assertNoNullDataWritten(session, nextTimestamp);
    return elapsedNanos;
  }

  private static void createAlignedTimeseries(ISession session, String device, int measurementCount)
      throws Exception {
    StringBuilder statement = new StringBuilder("CREATE ALIGNED TIMESERIES ");
    statement.append(device).append("(s0 INT64");
    for (int i = 1; i < measurementCount; i++) {
      statement.append(",s").append(i).append(" INT64");
    }
    statement.append(')');
    session.executeNonQueryStatement(statement.toString());
  }

  private static void insertHistoricalRows(ISession session, String device) throws Exception {
    List<IMeasurementSchema> schemas =
        Collections.singletonList(new MeasurementSchema("s0", TSDataType.INT64));
    Tablet tablet = new Tablet(device, schemas, Math.min(HISTORICAL_ROW_COUNT, 1024));
    for (int row = 0; row < HISTORICAL_ROW_COUNT; row++) {
      int rowIndex = tablet.getRowSize();
      if (rowIndex == tablet.getMaxRowNumber()) {
        session.insertAlignedTablet(tablet);
        tablet.reset();
        rowIndex = 0;
      }
      tablet.addTimestamp(rowIndex, row);
      tablet.addValue("s0", rowIndex, (long) row);
    }
    if (tablet.getRowSize() > 0) {
      session.insertAlignedTablet(tablet);
    }
  }

  private static void assertLazyAllocationDataWritten(ISession session, String device)
      throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT COUNT(s0), COUNT(s" + NEW_COLUMN_COUNT + ") FROM " + device)) {
      Assert.assertTrue(dataSet.hasNext());
      List<org.apache.tsfile.read.common.Field> fields = dataSet.next().getFields();
      Assert.assertEquals(HISTORICAL_ROW_COUNT, fields.get(0).getLongV());
      Assert.assertEquals(1, fields.get(1).getLongV());
      Assert.assertFalse(dataSet.hasNext());
    }
  }

  private static void assertNoNullDataWritten(ISession session, long expectedRowCount)
      throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT COUNT(s0), COUNT(s" + (NEW_COLUMN_COUNT - 1) + ") FROM " + NO_NULL_DEVICE)) {
      Assert.assertTrue(dataSet.hasNext());
      List<org.apache.tsfile.read.common.Field> fields = dataSet.next().getFields();
      Assert.assertEquals(expectedRowCount, fields.get(0).getLongV());
      Assert.assertEquals(expectedRowCount, fields.get(1).getLongV());
      Assert.assertFalse(dataSet.hasNext());
    }
  }

  private static Path getResultPath() {
    String defaultFileName =
        "target/aligned-tvlist-lazy-allocation-"
            + IMPLEMENTATION.replaceAll("[^A-Za-z0-9_.-]", "_")
            + ".properties";
    return Paths.get(System.getProperty(RESULT_PATH_PROPERTY, defaultFileName));
  }

  private static void writeResult(Path path, BenchmarkResult result) throws IOException {
    Properties properties = new Properties();
    properties.setProperty("format.version", "2");
    properties.setProperty("implementation", result.implementation);
    properties.setProperty("revision", result.revision);
    properties.setProperty("historical.row.count", Integer.toString(result.historicalRowCount));
    properties.setProperty("new.column.count", Integer.toString(result.newColumnCount));
    properties.setProperty("warmup.round.count", Integer.toString(result.warmupRoundCount));
    properties.setProperty(
        "round.count", Integer.toString(result.lazyAllocationElapsedNanos.length));
    properties.setProperty("no-null.batch.count", Integer.toString(result.noNullBatchCount));
    properties.setProperty("no-null.rows.per.batch", Integer.toString(NO_NULL_ROWS_PER_BATCH));
    properties.setProperty(
        "lazy-allocation.latencies.nanos", join(result.lazyAllocationElapsedNanos));
    properties.setProperty("no-null.latencies.nanos", join(result.noNullElapsedNanos));
    createParentDirectories(path);
    try (OutputStream outputStream = Files.newOutputStream(path)) {
      properties.store(outputStream, "AlignedTVList lazy-allocation integration benchmark");
    }
  }

  private static BenchmarkResult readResult(Path path) throws IOException {
    Properties properties = new Properties();
    try (InputStream inputStream = Files.newInputStream(path)) {
      properties.load(inputStream);
    }
    return new BenchmarkResult(
        properties.getProperty("implementation"),
        properties.getProperty("revision"),
        Integer.parseInt(properties.getProperty("historical.row.count")),
        Integer.parseInt(properties.getProperty("new.column.count")),
        Integer.parseInt(properties.getProperty("warmup.round.count")),
        Integer.parseInt(properties.getProperty("no-null.batch.count")),
        readLongArray(properties, "lazy-allocation.latencies.nanos"),
        readLongArray(properties, "no-null.latencies.nanos"));
  }

  private static long[] readLongArray(Properties properties, String key) {
    String[] values = properties.getProperty(key).split(",");
    long[] result = new long[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = Long.parseLong(values[i]);
    }
    return result;
  }

  private static void assertComparable(BenchmarkResult baseline, BenchmarkResult candidate) {
    Assert.assertEquals(baseline.historicalRowCount, candidate.historicalRowCount);
    Assert.assertEquals(baseline.newColumnCount, candidate.newColumnCount);
    Assert.assertEquals(baseline.warmupRoundCount, candidate.warmupRoundCount);
    Assert.assertEquals(baseline.noNullBatchCount, candidate.noNullBatchCount);
    Assert.assertEquals(
        baseline.lazyAllocationElapsedNanos.length, candidate.lazyAllocationElapsedNanos.length);
    Assert.assertEquals(baseline.noNullElapsedNanos.length, candidate.noNullElapsedNanos.length);
  }

  private static String buildReport(BenchmarkResult baseline, BenchmarkResult candidate) {
    StringBuilder report = new StringBuilder();
    report.append("# AlignedTVList integration performance report\n\n");
    report.append(
        "Both revisions were built in separate Git worktrees and ran this same "
            + "`LocalStandaloneIT` against a real 1C1D IoTDB environment.\n\n");
    report.append(
        String.format(
            Locale.ROOT,
            "- Lazy-allocation workload: write %,d historical rows only to `s0`, then write "
                + "one row to %,d newly used columns.%n"
                + "- No-null workload: write %d aligned tablets per round, with %,d columns and "
                + "%d rows per tablet; every value is non-null.%n"
                + "- Primitive array size: %d%n"
                + "- DataNode maximum heap: %,d MiB%n"
                + "- Warm-up rounds: %d%n"
                + "- Measured rounds: %d%n%n",
            candidate.historicalRowCount,
            candidate.newColumnCount,
            candidate.noNullBatchCount,
            candidate.newColumnCount,
            NO_NULL_ROWS_PER_BATCH,
            PRIMITIVE_ARRAY_SIZE,
            DATANODE_MAX_HEAP_SIZE_IN_MB,
            candidate.warmupRoundCount,
            candidate.lazyAllocationElapsedNanos.length));
    report.append("| Workload | Revision | Commit | Median ms | P95 ms | Values/s |\n");
    report.append("| --- | --- | --- | ---: | ---: | ---: |\n");
    appendReportRow(report, "Lazy allocation", baseline, false);
    appendReportRow(report, "Lazy allocation", candidate, false);
    appendReportRow(report, "No nulls", baseline, true);
    appendReportRow(report, "No nulls", candidate, true);
    report.append('\n');
    report.append(
        String.format(
            Locale.ROOT,
            "- Lazy-allocation median latency improvement: %.2f%%%n"
                + "- Lazy-allocation throughput improvement: %.2f%%%n"
                + "- No-null median latency improvement: %.2f%%%n"
                + "- No-null throughput improvement: %.2f%%%n%n",
            latencyImprovement(baseline.lazyMedianNanos(), candidate.lazyMedianNanos()),
            throughputImprovement(baseline.lazyValuesPerSecond(), candidate.lazyValuesPerSecond()),
            latencyImprovement(baseline.noNullMedianNanos(), candidate.noNullMedianNanos()),
            throughputImprovement(
                baseline.noNullValuesPerSecond(), candidate.noNullValuesPerSecond())));
    report.append(
        String.format(
            Locale.ROOT,
            "- %s lazy-allocation latencies (ms): `%s`%n"
                + "- %s lazy-allocation latencies (ms): `%s`%n"
                + "- %s no-null latencies (ms): `%s`%n"
                + "- %s no-null latencies (ms): `%s`%n%n",
            baseline.implementation,
            latenciesMillis(baseline.lazyAllocationElapsedNanos),
            candidate.implementation,
            latenciesMillis(candidate.lazyAllocationElapsedNanos),
            baseline.implementation,
            latenciesMillis(baseline.noNullElapsedNanos),
            candidate.implementation,
            latenciesMillis(candidate.noNullElapsedNanos)));
    report.append(
        "The timed regions contain the client calls and complete server write path; schema "
            + "creation, workload preparation, and result verification are outside them. Raw "
            + "per-round latencies are retained in the generated result property files.\n\n");
    report.append("## Reproduction\n\n");
    report.append(
        "1. Create the baseline worktree: `git worktree add --detach <master-worktree> master`.\n"
            + "2. Copy this test source to the identical path in the baseline worktree.\n"
            + "3. In each worktree, build with "
            + "`mvn -Pwith-integration-tests -pl integration-test -am package -DskipTests`.\n"
            + String.format(
                Locale.ROOT,
                "4. Run the baseline with `-D%s=true`, `-D%s=master`, "
                    + "`-D%s=<master-commit>`, and `-D%s=<baseline-result>`.%n"
                    + "5. Run the candidate with the corresponding candidate values plus "
                    + "`-D%s=<baseline-result>` and `-D%s=<report-path>`. For both runs, select "
                    + "this class with `-Dit.test=IoTDBAlignedTVListPrimitiveArrayPerformanceIT` "
                    + "and execute `failsafe:integration-test@integration-test "
                    + "failsafe:verify@verify`.%n",
                ENABLED_PROPERTY,
                IMPLEMENTATION_PROPERTY,
                REVISION_PROPERTY,
                RESULT_PATH_PROPERTY,
                BASELINE_RESULT_PATH_PROPERTY,
                REPORT_PATH_PROPERTY));
    return report.toString();
  }

  private static void appendReportRow(
      StringBuilder report, String workload, BenchmarkResult result, boolean noNull) {
    long medianNanos = noNull ? result.noNullMedianNanos() : result.lazyMedianNanos();
    long p95Nanos = noNull ? result.noNullP95Nanos() : result.lazyP95Nanos();
    double valuesPerSecond = noNull ? result.noNullValuesPerSecond() : result.lazyValuesPerSecond();
    report.append(
        String.format(
            Locale.ROOT,
            "| %s | %s | `%s` | %.3f | %.3f | %.0f |%n",
            workload,
            result.implementation,
            result.revision,
            medianNanos / 1_000_000.0,
            p95Nanos / 1_000_000.0,
            valuesPerSecond));
  }

  private static double latencyImprovement(double baseline, double candidate) {
    return (baseline - candidate) * 100.0 / baseline;
  }

  private static double throughputImprovement(double baseline, double candidate) {
    return (candidate - baseline) * 100.0 / baseline;
  }

  private static String latenciesMillis(long[] elapsedNanos) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < elapsedNanos.length; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(String.format(Locale.ROOT, "%.3f", elapsedNanos[i] / 1_000_000.0));
    }
    return builder.toString();
  }

  private static void writeFile(Path path, String content) throws IOException {
    createParentDirectories(path);
    Files.writeString(path, content, StandardCharsets.UTF_8);
  }

  private static void createParentDirectories(Path path) throws IOException {
    Path parent = path.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
  }

  private static String join(long[] values) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append(values[i]);
    }
    return builder.toString();
  }

  private static class BenchmarkResult {
    private final String implementation;
    private final String revision;
    private final int historicalRowCount;
    private final int newColumnCount;
    private final int warmupRoundCount;
    private final int noNullBatchCount;
    private final long[] lazyAllocationElapsedNanos;
    private final long[] noNullElapsedNanos;

    private BenchmarkResult(
        String implementation,
        String revision,
        int historicalRowCount,
        int newColumnCount,
        int warmupRoundCount,
        int noNullBatchCount,
        long[] lazyAllocationElapsedNanos,
        long[] noNullElapsedNanos) {
      this.implementation = implementation;
      this.revision = revision;
      this.historicalRowCount = historicalRowCount;
      this.newColumnCount = newColumnCount;
      this.warmupRoundCount = warmupRoundCount;
      this.noNullBatchCount = noNullBatchCount;
      this.lazyAllocationElapsedNanos = lazyAllocationElapsedNanos;
      this.noNullElapsedNanos = noNullElapsedNanos;
    }

    private static long medianNanos(long[] elapsedNanos) {
      long[] sorted = Arrays.copyOf(elapsedNanos, elapsedNanos.length);
      Arrays.sort(sorted);
      return sorted[sorted.length / 2];
    }

    private static long p95Nanos(long[] elapsedNanos) {
      long[] sorted = Arrays.copyOf(elapsedNanos, elapsedNanos.length);
      Arrays.sort(sorted);
      return sorted[(int) Math.ceil(sorted.length * 0.95) - 1];
    }

    private long lazyMedianNanos() {
      return medianNanos(lazyAllocationElapsedNanos);
    }

    private long lazyP95Nanos() {
      return p95Nanos(lazyAllocationElapsedNanos);
    }

    private long noNullMedianNanos() {
      return medianNanos(noNullElapsedNanos);
    }

    private long noNullP95Nanos() {
      return p95Nanos(noNullElapsedNanos);
    }

    private double lazyValuesPerSecond() {
      return newColumnCount * 1_000_000_000.0 / lazyMedianNanos();
    }

    private double noNullValuesPerSecond() {
      return (double) newColumnCount
          * NO_NULL_ROWS_PER_BATCH
          * noNullBatchCount
          * 1_000_000_000.0
          / noNullMedianNanos();
    }

    private String toSummary() {
      return String.format(
          Locale.ROOT,
          "implementation=%s, revision=%s, lazy median=%.3f ms, lazy values/s=%.0f, "
              + "no-null median=%.3f ms, no-null values/s=%.0f",
          implementation,
          revision,
          lazyMedianNanos() / 1_000_000.0,
          lazyValuesPerSecond(),
          noNullMedianNanos() / 1_000_000.0,
          noNullValuesPerSecond());
    }
  }
}
