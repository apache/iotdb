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

package org.apache.iotdb.relational.it.performance;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.NumericRow;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Manual end-to-end benchmark for the numeric table-row API and relational {@link Tablet} writes.
 *
 * <p>Enable this test with {@code -Diotdb.numeric.row.insert.perf.enabled=true}. The timed region
 * includes the concurrent Session calls and server write confirmations. Schema creation and
 * client-side input preparation are outside the timed region. By default 16 client sessions write
 * one row per request; set {@code iotdb.numeric.row.insert.perf.client.count} or {@code
 * iotdb.numeric.row.insert.perf.tablet.row.count} to vary concurrency or Tablet batching.
 *
 * <p>Results are written to {@code target/numeric-row-insert-performance.properties}. Percentiles
 * describe elapsed time per complete round, not individual RPC latency. See the adjacent README for
 * reproducible commands and interpretation.
 */
@RunWith(IoTDBTestRunner.class)
@Category(TableLocalStandaloneIT.class)
public class IoTDBNumericRowInsertPerformanceIT {

  private static final String PROPERTY_PREFIX = "iotdb.numeric.row.insert.perf.";
  private static final String ENABLED_PROPERTY = PROPERTY_PREFIX + "enabled";
  private static final String ROW_COUNT_PROPERTY = PROPERTY_PREFIX + "row.count";
  private static final String COLUMN_COUNT_PROPERTY = PROPERTY_PREFIX + "column.count";
  private static final String CLIENT_COUNT_PROPERTY = PROPERTY_PREFIX + "client.count";
  private static final String TABLET_ROW_COUNT_PROPERTY = PROPERTY_PREFIX + "tablet.row.count";
  private static final String WARMUP_ROUND_COUNT_PROPERTY = PROPERTY_PREFIX + "warmup.round.count";
  private static final String ROUND_COUNT_PROPERTY = PROPERTY_PREFIX + "round.count";
  private static final String RESULT_PATH_PROPERTY = PROPERTY_PREFIX + "result.path";

  private static final int ROW_COUNT = Integer.getInteger(ROW_COUNT_PROPERTY, 10_000);
  private static final int COLUMN_COUNT = Integer.getInteger(COLUMN_COUNT_PROPERTY, 100);
  private static final int CLIENT_COUNT = Integer.getInteger(CLIENT_COUNT_PROPERTY, 16);
  private static final int TABLET_ROW_COUNT = Integer.getInteger(TABLET_ROW_COUNT_PROPERTY, 1);
  private static final int WARMUP_ROUND_COUNT = Integer.getInteger(WARMUP_ROUND_COUNT_PROPERTY, 1);
  private static final int ROUND_COUNT = Integer.getInteger(ROUND_COUNT_PROPERTY, 5);

  private static final String DATABASE = "numeric_row_insert_performance";
  private static final List<TSDataType> TYPE_SEQUENCE =
      Arrays.asList(
          TSDataType.BOOLEAN,
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE);
  private static List<String> columnNames;
  private static List<TSDataType> dataTypes;
  private static List<ColumnCategory> columnCategories;

  private static boolean environmentInitialized;

  @BeforeClass
  public static void setUp() throws Exception {
    Assume.assumeTrue(Boolean.getBoolean(ENABLED_PROPERTY));
    // Validate before allocating inputs or starting nodes, including when a count is negative.
    assertValidConfiguration();
    columnNames = createColumnNames();
    dataTypes = createDataTypes();
    columnCategories = Collections.nCopies(COLUMN_COUNT, ColumnCategory.FIELD);
    environmentInitialized = true;
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (environmentInitialized) {
      EnvFactory.getEnv().cleanClusterEnvironment();
    }
  }

  @Test
  // Compare identical, prebuilt numeric data; verify every field count outside the timed calls.
  public void testNumericRowInsertPerformanceAgainstTablet() throws Exception {
    final long[] numericRowElapsedNanos = new long[ROUND_COUNT];
    final long[] tabletElapsedNanos = new long[ROUND_COUNT];

    final List<ITableSession> sessions = new ArrayList<>(CLIENT_COUNT);
    final ExecutorService executor = Executors.newFixedThreadPool(CLIENT_COUNT);
    try {
      final ITableSession setupSession = EnvFactory.getEnv().getTableSessionConnection();
      setupSession.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      setupSession.executeNonQueryStatement("USE " + DATABASE);
      sessions.add(setupSession);
      for (int client = 1; client < CLIENT_COUNT; client++) {
        final ITableSession session = EnvFactory.getEnv().getTableSessionConnection();
        session.executeNonQueryStatement("USE " + DATABASE);
        sessions.add(session);
      }

      final int totalRoundCount = WARMUP_ROUND_COUNT + ROUND_COUNT;
      for (int round = 0; round < totalRoundCount; round++) {
        final List<NumericRowWorkload> numericRowWorkloads =
            prepareNumericRowWorkloads(setupSession, round);
        final List<TabletWorkload> tabletWorkloads = prepareTabletWorkloads(setupSession, round);

        final long numericRowNanos;
        final long tabletNanos;
        // Alternate execution order so one path does not consistently receive a warmer server.
        if ((round & 1) == 0) {
          numericRowNanos = executeNumericRowWorkload(sessions, numericRowWorkloads, executor);
          tabletNanos = executeTabletWorkload(sessions, tabletWorkloads, executor);
        } else {
          tabletNanos = executeTabletWorkload(sessions, tabletWorkloads, executor);
          numericRowNanos = executeNumericRowWorkload(sessions, numericRowWorkloads, executor);
        }
        for (NumericRowWorkload workload : numericRowWorkloads) {
          assertRowCount(setupSession, workload.tableName);
        }
        for (TabletWorkload workload : tabletWorkloads) {
          assertRowCount(setupSession, workload.tableName);
        }

        if (round >= WARMUP_ROUND_COUNT) {
          final int measuredRound = round - WARMUP_ROUND_COUNT;
          numericRowElapsedNanos[measuredRound] = numericRowNanos;
          tabletElapsedNanos[measuredRound] = tabletNanos;
        }
      }
    } finally {
      executor.shutdownNow();
      for (ITableSession session : sessions) {
        session.close();
      }
    }

    final BenchmarkResult result = new BenchmarkResult(numericRowElapsedNanos, tabletElapsedNanos);
    writeResult(result);
  }

  private static List<NumericRowWorkload> prepareNumericRowWorkloads(
      ITableSession session, int round) throws Exception {
    final List<NumericRowWorkload> workloads = new ArrayList<>(CLIENT_COUNT);
    for (int client = 0; client < CLIENT_COUNT; client++) {
      final String tableName = "numeric_row_" + round + "_" + client;
      createTable(session, tableName);
      final long startTimestamp = ((long) round * CLIENT_COUNT + client) * ROW_COUNT;
      final NumericRow[] rows = new NumericRow[ROW_COUNT];
      final String[] tagValues = new String[0];
      for (int row = 0; row < ROW_COUNT; row++) {
        rows[row] =
            new NumericRow(
                tableName,
                startTimestamp + row,
                columnNames,
                dataTypes,
                columnCategories,
                createRawValues(startTimestamp + row),
                tagValues);
      }
      workloads.add(new NumericRowWorkload(tableName, rows));
    }
    return workloads;
  }

  private static List<TabletWorkload> prepareTabletWorkloads(ITableSession session, int round)
      throws Exception {
    final List<TabletWorkload> workloads = new ArrayList<>(CLIENT_COUNT);
    for (int client = 0; client < CLIENT_COUNT; client++) {
      final String tableName = "tablet_" + round + "_" + client;
      createTable(session, tableName);
      final long startTimestamp = ((long) round * CLIENT_COUNT + client) * ROW_COUNT;
      final List<Tablet> tablets = new ArrayList<>((ROW_COUNT - 1) / TABLET_ROW_COUNT + 1);
      for (int rowStart = 0; rowStart < ROW_COUNT; ) {
        final int rowCount = Math.min(TABLET_ROW_COUNT, ROW_COUNT - rowStart);
        final Tablet tablet =
            new Tablet(tableName, columnNames, dataTypes, columnCategories, rowCount);
        for (int row = 0; row < rowCount; row++) {
          final int rowIndex = tablet.getRowSize();
          final long timestamp = startTimestamp + rowStart + row;
          final long[] values = createRawValues(timestamp);
          tablet.addTimestamp(rowIndex, timestamp);
          for (int column = 0; column < COLUMN_COUNT; column++) {
            tablet.addValue(
                columnNames.get(column),
                rowIndex,
                toTabletValue(dataTypes.get(column), values[column]));
          }
        }
        tablets.add(tablet);
        rowStart += rowCount;
      }
      workloads.add(new TabletWorkload(tableName, tablets));
    }
    return workloads;
  }

  private static long executeNumericRowWorkload(
      List<ITableSession> sessions, List<NumericRowWorkload> workloads, ExecutorService executor)
      throws Exception {
    return executeConcurrently(
        sessions,
        workloads,
        executor,
        (session, workload) -> {
          for (NumericRow row : workload.rows) {
            session.insert(row);
          }
        });
  }

  private static long executeTabletWorkload(
      List<ITableSession> sessions, List<TabletWorkload> workloads, ExecutorService executor)
      throws Exception {
    return executeConcurrently(
        sessions,
        workloads,
        executor,
        (session, workload) -> {
          for (Tablet tablet : workload.tablets) {
            session.insert(tablet);
          }
        });
  }

  private static <T> long executeConcurrently(
      List<ITableSession> sessions,
      List<T> workloads,
      ExecutorService executor,
      WorkloadWriter<T> writer)
      throws Exception {
    final CountDownLatch ready = new CountDownLatch(CLIENT_COUNT);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<?>> futures = new ArrayList<>(CLIENT_COUNT);
    for (int client = 0; client < CLIENT_COUNT; client++) {
      final ITableSession session = sessions.get(client);
      final T workload = workloads.get(client);
      futures.add(
          executor.submit(
              () -> {
                ready.countDown();
                start.await();
                writer.write(session, workload);
                return null;
              }));
    }
    ready.await();
    final long startNanos = System.nanoTime();
    start.countDown();
    for (Future<?> future : futures) {
      future.get();
    }
    return System.nanoTime() - startNanos;
  }

  @FunctionalInterface
  private interface WorkloadWriter<T> {

    void write(ITableSession session, T workload) throws Exception;
  }

  private static void createTable(ITableSession session, String tableName) throws Exception {
    final StringBuilder statement = new StringBuilder("CREATE TABLE ");
    statement.append(tableName).append('(');
    for (int column = 0; column < COLUMN_COUNT; column++) {
      if (column > 0) {
        statement.append(',');
      }
      statement
          .append(columnNames.get(column))
          .append(' ')
          .append(dataTypes.get(column))
          .append(" FIELD");
    }
    statement.append(')');
    session.executeNonQueryStatement(statement.toString());
  }

  private static void assertRowCount(ITableSession session, String tableName) throws Exception {
    final List<String> counts = new ArrayList<>(COLUMN_COUNT);
    for (String columnName : columnNames) {
      counts.add("COUNT(" + columnName + ")");
    }
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT " + String.join(",", counts) + " FROM " + tableName)) {
      Assert.assertTrue(dataSet.hasNext());
      for (var field : dataSet.next().getFields()) {
        Assert.assertEquals(ROW_COUNT, field.getLongV());
      }
      Assert.assertFalse(dataSet.hasNext());
    }
  }

  private static long[] createRawValues(long timestamp) {
    final long[] values = new long[COLUMN_COUNT];
    for (int column = 0; column < COLUMN_COUNT; column++) {
      final TSDataType type = dataTypes.get(column);
      // Floating-point values occupy raw IEEE 754 bits in the numeric-row API.
      values[column] =
          switch (type) {
            case BOOLEAN -> (timestamp + column) & 1;
            case INT32 -> (int) (timestamp + column);
            case INT64 -> timestamp + column;
            case FLOAT -> Float.floatToRawIntBits((float) timestamp + column / 10.0F);
            case DOUBLE -> Double.doubleToRawLongBits(timestamp + column / 10.0D);
            default -> throw new AssertionError(type);
          };
    }
    return values;
  }

  private static Object toTabletValue(TSDataType type, long value) {
    return switch (type) {
      case BOOLEAN -> value != 0;
      case INT32 -> (int) value;
      case INT64 -> value;
      case FLOAT -> Float.intBitsToFloat((int) value);
      case DOUBLE -> Double.longBitsToDouble(value);
      default -> throw new AssertionError(type);
    };
  }

  private static List<String> createColumnNames() {
    final List<String> columnNames = new ArrayList<>(COLUMN_COUNT);
    for (int column = 0; column < COLUMN_COUNT; column++) {
      columnNames.add("v" + column);
    }
    return Collections.unmodifiableList(columnNames);
  }

  private static List<TSDataType> createDataTypes() {
    final List<TSDataType> dataTypes = new ArrayList<>(COLUMN_COUNT);
    for (int column = 0; column < COLUMN_COUNT; column++) {
      dataTypes.add(TYPE_SEQUENCE.get(column % TYPE_SEQUENCE.size()));
    }
    return Collections.unmodifiableList(dataTypes);
  }

  private static void assertValidConfiguration() {
    Assert.assertTrue(ROW_COUNT > 0);
    Assert.assertTrue(COLUMN_COUNT > 0);
    Assert.assertTrue(CLIENT_COUNT > 0);
    Assert.assertTrue(TABLET_ROW_COUNT > 0);
    Assert.assertTrue(WARMUP_ROUND_COUNT >= 0);
    Assert.assertTrue(ROUND_COUNT > 0);
    Assert.assertTrue((long) WARMUP_ROUND_COUNT + ROUND_COUNT <= Integer.MAX_VALUE);
  }

  private static void writeResult(BenchmarkResult result) throws IOException {
    final Properties properties = new Properties();
    properties.setProperty("format.version", "2");
    properties.setProperty("java.version", System.getProperty("java.version"));
    properties.setProperty("os.name", System.getProperty("os.name"));
    properties.setProperty("os.arch", System.getProperty("os.arch"));
    properties.setProperty(
        "client.available.processors",
        Integer.toString(Runtime.getRuntime().availableProcessors()));
    properties.setProperty(
        "client.max.heap.bytes", Long.toString(Runtime.getRuntime().maxMemory()));
    properties.setProperty("client.count", Integer.toString(CLIENT_COUNT));
    properties.setProperty("row.count", Integer.toString(ROW_COUNT));
    properties.setProperty("column.count", Integer.toString(COLUMN_COUNT));
    properties.setProperty("tablet.row.count", Integer.toString(TABLET_ROW_COUNT));
    properties.setProperty("warmup.round.count", Integer.toString(WARMUP_ROUND_COUNT));
    properties.setProperty("round.count", Integer.toString(ROUND_COUNT));
    properties.setProperty("column.types", join(dataTypes));
    properties.setProperty(
        "numeric.row.requests.per.round", Integer.toString(CLIENT_COUNT * ROW_COUNT));
    properties.setProperty(
        "insert.tablet.requests.per.round",
        Integer.toString(CLIENT_COUNT * ((ROW_COUNT - 1) / TABLET_ROW_COUNT + 1)));
    properties.setProperty("numeric.row.round.elapsed.nanos", join(result.numericRowElapsedNanos));
    properties.setProperty("insert.tablet.round.elapsed.nanos", join(result.tabletElapsedNanos));
    properties.setProperty(
        "numeric.row.round.p50.nanos", Long.toString(result.numericRowP50Nanos()));
    properties.setProperty(
        "numeric.row.round.p95.nanos", Long.toString(result.numericRowP95Nanos()));
    properties.setProperty("numeric.row.rows.per.second", format(result.numericRowRowsPerSecond()));
    properties.setProperty(
        "numeric.row.values.per.second", format(result.numericRowValuesPerSecond()));
    properties.setProperty("insert.tablet.round.p50.nanos", Long.toString(result.tabletP50Nanos()));
    properties.setProperty("insert.tablet.round.p95.nanos", Long.toString(result.tabletP95Nanos()));
    properties.setProperty("insert.tablet.rows.per.second", format(result.tabletRowsPerSecond()));
    properties.setProperty(
        "insert.tablet.values.per.second", format(result.tabletValuesPerSecond()));

    final Path resultPath =
        Paths.get(
            System.getProperty(
                RESULT_PATH_PROPERTY, "target/numeric-row-insert-performance.properties"));
    final Path parent = resultPath.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    try (OutputStream outputStream = Files.newOutputStream(resultPath)) {
      properties.store(outputStream, null);
    }
  }

  private static String join(List<TSDataType> values) {
    final StringBuilder builder = new StringBuilder();
    for (int index = 0; index < values.size(); index++) {
      if (index > 0) {
        builder.append(',');
      }
      builder.append(values.get(index));
    }
    return builder.toString();
  }

  private static String join(long[] values) {
    final StringBuilder builder = new StringBuilder();
    for (int index = 0; index < values.length; index++) {
      if (index > 0) {
        builder.append(',');
      }
      builder.append(values[index]);
    }
    return builder.toString();
  }

  private static String format(double value) {
    return String.format(Locale.ROOT, "%.2f", value);
  }

  private static long percentile(long[] latencies, double percentile) {
    final long[] sorted = latencies.clone();
    Arrays.sort(sorted);
    return sorted[(int) Math.ceil(percentile * sorted.length) - 1];
  }

  private static final class NumericRowWorkload {

    private final String tableName;
    private final NumericRow[] rows;

    private NumericRowWorkload(String tableName, NumericRow[] rows) {
      this.tableName = tableName;
      this.rows = rows;
    }
  }

  private static final class TabletWorkload {

    private final String tableName;
    private final List<Tablet> tablets;

    private TabletWorkload(String tableName, List<Tablet> tablets) {
      this.tableName = tableName;
      this.tablets = tablets;
    }
  }

  private static final class BenchmarkResult {

    private final long[] numericRowElapsedNanos;
    private final long[] tabletElapsedNanos;

    private BenchmarkResult(long[] numericRowElapsedNanos, long[] tabletElapsedNanos) {
      this.numericRowElapsedNanos = numericRowElapsedNanos;
      this.tabletElapsedNanos = tabletElapsedNanos;
    }

    private long numericRowP50Nanos() {
      return percentile(numericRowElapsedNanos, 0.5D);
    }

    private long numericRowP95Nanos() {
      return percentile(numericRowElapsedNanos, 0.95D);
    }

    private double numericRowRowsPerSecond() {
      return CLIENT_COUNT * ROW_COUNT * 1_000_000_000.0D / numericRowP50Nanos();
    }

    private double numericRowValuesPerSecond() {
      return numericRowRowsPerSecond() * COLUMN_COUNT;
    }

    private long tabletP50Nanos() {
      return percentile(tabletElapsedNanos, 0.5D);
    }

    private long tabletP95Nanos() {
      return percentile(tabletElapsedNanos, 0.95D);
    }

    private double tabletRowsPerSecond() {
      return CLIENT_COUNT * ROW_COUNT * 1_000_000_000.0D / tabletP50Nanos();
    }

    private double tabletValuesPerSecond() {
      return tabletRowsPerSecond() * COLUMN_COUNT;
    }
  }
}
