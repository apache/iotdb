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

package org.apache.iotdb.relational.it.session;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.NumericRow;
import org.apache.iotdb.isession.NumericRowPreparedStatement;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTablesColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableModelSessionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNumericRowInsert() throws Exception {
    // Exercise direct and pooled table sessions, schema reordering and missing STRING columns.
    // The same values must remain readable from MemTable and after flushing to TsFile.
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_rows");
      session.executeNonQueryStatement("USE numeric_rows");
      session.executeNonQueryStatement(
          "CREATE TABLE readings(tag STRING TAG, attr STRING ATTRIBUTE, "
              + "event_time TIMESTAMP FIELD, event_date DATE FIELD, d DOUBLE FIELD, "
              + "f FLOAT FIELD, l INT64 FIELD, i INT32 FIELD, b BOOLEAN FIELD, missing STRING FIELD)");
      List<String> names = Arrays.asList("b", "i", "l", "f", "d", "event_date", "event_time");
      List<TSDataType> types =
          Arrays.asList(
              TSDataType.BOOLEAN,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.DATE,
              TSDataType.TIMESTAMP);
      long[] values = {
        -1,
        Integer.MIN_VALUE,
        Long.MIN_VALUE,
        Float.floatToRawIntBits(-1.25f),
        Double.doubleToRawLongBits(2.5),
        20260910,
        1234
      };
      session.insert(
          new NumericRow(
              "readings",
              1,
              names,
              types,
              Collections.nCopies(types.size(), ColumnCategory.FIELD),
              values,
              new String[0]));
      try (ITableSessionPool pool = EnvFactory.getEnv().getTableSessionPool(1, "numeric_rows");
          ITableSession pooledSession = pool.getSession()) {
        pooledSession.insert(
            new NumericRow(
                "readings",
                2,
                names,
                types,
                Collections.nCopies(types.size(), ColumnCategory.FIELD),
                values,
                new String[0]));
      }
      assertNumericRows(session);
      session.executeNonQueryStatement("FLUSH");
      assertNumericRows(session);
    }
  }

  @Test
  public void testNumericRowInsertWithTags() throws Exception {
    // Different TAGs at the same timestamp must stay separate, with identical behavior after flush.
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_tag_rows");
      session.executeNonQueryStatement("USE numeric_tag_rows");
      session.executeNonQueryStatement(
          "CREATE TABLE readings(region STRING TAG, device STRING TAG, "
              + "missing STRING TAG, v INT64 FIELD, enabled BOOLEAN FIELD)");
      List<String> names = Arrays.asList("enabled", "device", "v", "region");
      List<TSDataType> types =
          Arrays.asList(TSDataType.BOOLEAN, TSDataType.STRING, TSDataType.INT64, TSDataType.STRING);
      List<ColumnCategory> categories =
          Arrays.asList(
              ColumnCategory.FIELD, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.TAG);
      session.insert(
          new NumericRow(
              "readings",
              1,
              names,
              types,
              categories,
              new long[] {1, 10},
              new String[] {"设备甲😀", "华北"}));
      try (ITableSessionPool pool = EnvFactory.getEnv().getTableSessionPool(1, "numeric_tag_rows");
          ITableSession pooled = pool.getSession()) {
        pooled.insert(
            new NumericRow(
                "readings",
                1,
                names,
                types,
                categories,
                new long[] {0, 20},
                new String[] {"设备乙", "华北"}));
      }
      session.insert(
          new NumericRow(
              "readings",
              2,
              names,
              types,
              categories,
              new long[] {1, 30},
              new String[] {"", null}));
      session.insert(
          new NumericRow(
              "readings",
              2,
              names,
              types,
              categories,
              new long[] {0, 40},
              new String[] {null, null}));
      assertNumericTagRows(session);
      session.executeNonQueryStatement("FLUSH");
      assertNumericTagRows(session);
    }
  }

  @Test
  public void testPreparedNumericRowSchemaVersionChange() throws Exception {
    // Reuse a prepared row before and after adding a TAG, including a flush/read round trip.
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_version");
      session.executeNonQueryStatement("USE numeric_version");
      session.executeNonQueryStatement("CREATE TABLE readings (tag STRING TAG, v DOUBLE FIELD)");
      NumericRowPreparedStatement prepared =
          session.prepareInsertNumericRow(
              "readings",
              Arrays.asList("tag", "v"),
              Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
      for (int time = 1; time <= 3; time++) {
        if (time == 3) {
          session.executeNonQueryStatement("ALTER TABLE readings ADD COLUMN tag2 STRING TAG");
        }
        session.executePreparedInsertNumericRow(
            prepared,
            time,
            new long[] {Double.doubleToRawLongBits(time + 0.5)},
            new String[] {"device"});
      }
      session.executeNonQueryStatement("FLUSH");
      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT time,tag,tag2,v FROM readings ORDER BY time")) {
        for (int time = 1; time <= 3; time++) {
          assertTrue(dataSet.hasNext());
          RowRecord row = dataSet.next();
          assertEquals(time, row.getFields().get(0).getLongV());
          assertEquals("device", row.getFields().get(1).getStringValue());
          assertNull(row.getFields().get(2).getDataType());
          assertEquals(time + 0.5, row.getFields().get(3).getDoubleV(), 0);
        }
        assertFalse(dataSet.hasNext());
      }
    }
  }

  @Test
  public void testPreparedNumericRowAndTabletWritePerformance() throws Exception {
    Assume.assumeTrue(
        "Performance benchmark disabled; set iotdb.numeric.row.insert.perf.enabled=true to run",
        Boolean.getBoolean("iotdb.numeric.row.insert.perf.enabled"));
    final int concurrency = 16;
    // Compare sustained single-row writes after warming both paths; verify all acknowledged rows.
    final int warmupRounds = 2;
    final int measuredRounds = 6;
    final int fieldCount = 99;
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_perf");
      session.executeNonQueryStatement("USE numeric_perf");
      StringBuilder ddl = new StringBuilder("CREATE TABLE prepared_perf (tag STRING TAG");
      for (int i = 0; i < fieldCount; i++) {
        ddl.append(", f").append(i).append(" DOUBLE FIELD");
      }
      ddl.append(")");
      session.executeNonQueryStatement(ddl.toString());
      session.executeNonQueryStatement(ddl.toString().replace("prepared_perf", "tablet_perf"));
    }

    List<String> names = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    names.add("tag");
    types.add(TSDataType.STRING);
    categories.add(ColumnCategory.TAG);
    for (int i = 0; i < fieldCount; i++) {
      names.add("f" + i);
      types.add(TSDataType.DOUBLE);
      categories.add(ColumnCategory.FIELD);
    }
    ITableSession[] sessions = new ITableSession[concurrency];
    NumericRowPreparedStatement[] prepared = new NumericRowPreparedStatement[concurrency];
    Tablet[] tablets = new Tablet[concurrency];
    String[][] tags = new String[concurrency][];
    long[][] numericValues = new long[concurrency][fieldCount];
    long[] preparedRows = new long[concurrency];
    long[] tabletRows = new long[concurrency];
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    try {
      // Connections, USE, prepare and reusable input allocation are outside every timed round.
      for (int i = 0; i < concurrency; i++) {
        sessions[i] = EnvFactory.getEnv().getTableSessionConnection();
        sessions[i].executeNonQueryStatement("USE numeric_perf");
        prepared[i] =
            sessions[i].prepareInsertNumericRow("prepared_perf", names, types, categories);
        tablets[i] = new Tablet("tablet_perf", names, types, categories, 1);
        tags[i] = new String[] {"device-" + i};
        Arrays.fill(numericValues[i], Double.doubleToRawLongBits(1.0));
      }
      IntConsumer preparedWrite =
          threadId -> {
            try {
              sessions[threadId].executePreparedInsertNumericRow(
                  prepared[threadId],
                  preparedRows[threadId],
                  numericValues[threadId],
                  tags[threadId]);
              preparedRows[threadId]++;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };
      IntConsumer tabletWrite =
          threadId -> {
            try {
              Tablet tablet = tablets[threadId];
              tablet.addTimestamp(0, tabletRows[threadId]);
              tablet.addValue("tag", 0, tags[threadId][0]);
              for (int i = 0; i < fieldCount; i++) {
                tablet.addValue(names.get(i + 1), 0, 1.0D);
              }
              sessions[threadId].insert(tablet);
              tablet.reset();
              tabletRows[threadId]++;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };
      double[][] rates = new double[2][measuredRounds];
      long[] totalRows = new long[2];
      long[] totalNanos = new long[2];
      for (int round = 0; round < warmupRounds + measuredRounds; round++) {
        boolean warmup = round < warmupRounds;
        int seconds = warmup ? 3 : 5;
        // Alternate AB/BA to distribute shared JVM warmup and background storage work.
        for (int position = 0; position < 2; position++) {
          int path = (round + position) % 2;
          WritePerformance result =
              runConcurrentWrites(
                  executor, concurrency, seconds, path == 0 ? preparedWrite : tabletWrite);
          System.out.printf(
              "%s round %d position %d %s: %d rows, %.3f s, %.1f rows/s%n",
              warmup ? "warmup" : "measured",
              warmup ? round + 1 : round - warmupRounds + 1,
              position + 1,
              path == 0 ? "prepared numeric row" : "insertTablet batch=1",
              result.rows(),
              result.nanos() / 1_000_000_000.0,
              result.rowsPerSecond());
          if (!warmup) {
            rates[path][round - warmupRounds] = result.rowsPerSecond();
            totalRows[path] += result.rows();
            totalNanos[path] += result.nanos();
          }
        }
      }
      for (int path = 0; path < 2; path++) {
        Arrays.sort(rates[path]);
        System.out.printf(
            "summary %s: median %.1f rows/s, aggregate %.1f rows/s, min %.1f, max %.1f%n",
            path == 0 ? "prepared numeric row" : "insertTablet batch=1",
            (rates[path][measuredRounds / 2 - 1] + rates[path][measuredRounds / 2]) / 2,
            totalRows[path] * 1_000_000_000.0 / totalNanos[path],
            rates[path][0],
            rates[path][measuredRounds - 1]);
      }
      // Per-device monotonic timestamps span warmup and measurement, so no rows overwrite others.
      for (int path = 0; path < 2; path++) {
        String table = path == 0 ? "prepared_perf" : "tablet_perf";
        long expectedRows = Arrays.stream(path == 0 ? preparedRows : tabletRows).sum();
        try (SessionDataSet dataSet =
            sessions[0].executeQueryStatement("SELECT count(*) FROM " + table)) {
          assertTrue(dataSet.hasNext());
          assertEquals(expectedRows, dataSet.next().getFields().get(0).getLongV());
          assertFalse(dataSet.hasNext());
        }
        System.out.printf("verified %s: %d rows%n", table, expectedRows);
      }
    } finally {
      executor.shutdownNow();
      try {
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
      } finally {
        // Closing each session also releases its server-side prepared metadata.
        Exception closeFailure = null;
        for (ITableSession session : sessions) {
          if (session != null) {
            try {
              session.close();
            } catch (Exception e) {
              if (closeFailure == null) closeFailure = e;
              else closeFailure.addSuppressed(e);
            }
          }
        }
        if (closeFailure != null) throw closeFailure;
      }
    }
  }

  private record WritePerformance(long rows, long nanos) {
    double rowsPerSecond() {
      return rows * 1_000_000_000.0 / nanos;
    }
  }

  private static WritePerformance runConcurrentWrites(
      ExecutorService executor, int concurrency, int seconds, IntConsumer write) throws Exception {
    CountDownLatch ready = new CountDownLatch(concurrency);
    CountDownLatch start = new CountDownLatch(1);
    AtomicLong deadline = new AtomicLong();
    List<Future<WritePerformance>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < concurrency; i++) {
        final int threadId = i;
        futures.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  long rows = 0;
                  while (!Thread.currentThread().isInterrupted()
                      && System.nanoTime() < deadline.get()) {
                    write.accept(threadId);
                    rows++;
                  }
                  return new WritePerformance(rows, System.nanoTime());
                }));
      }
      assertTrue(ready.await(30, TimeUnit.SECONDS));
      long startNanos = System.nanoTime();
      deadline.set(startNanos + TimeUnit.SECONDS.toNanos(seconds));
      start.countDown();
      long rows = 0;
      long endNanos = startNanos;
      for (Future<WritePerformance> future : futures) {
        WritePerformance result = future.get(seconds + 60, TimeUnit.SECONDS);
        rows += result.rows();
        endNanos = Math.max(endNanos, result.nanos());
      }
      // Include completion of in-flight RPCs, but exclude main-thread collection/printing time.
      return new WritePerformance(rows, endNanos - startNanos);
    } finally {
      start.countDown();
      for (Future<WritePerformance> future : futures) {
        if (!future.isDone()) future.cancel(true);
      }
    }
  }

  private void assertNumericTagRows(ITableSession session) throws Exception {
    String[] devices = {"设备甲😀", "设备乙", "", null};
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT region,device,missing,v,enabled FROM readings ORDER BY v")) {
      for (int i = 0; i < devices.length; i++) {
        assertTrue(dataSet.hasNext());
        RowRecord row = dataSet.next();
        if (i < 2) {
          assertEquals("华北", row.getFields().get(0).getStringValue());
        } else {
          assertNull(row.getFields().get(0).getDataType());
        }
        if (devices[i] == null) {
          assertNull(row.getFields().get(1).getDataType());
        } else {
          assertEquals(devices[i], row.getFields().get(1).getStringValue());
        }
        assertNull(row.getFields().get(2).getDataType());
        assertEquals((i + 1) * 10, row.getFields().get(3).getLongV());
        assertEquals(i % 2 == 0, row.getFields().get(4).getBoolV());
      }
      assertFalse(dataSet.hasNext());
    }
  }

  private void assertNumericRows(ITableSession session) throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT time,b,i,l,f,d,event_date,event_time,tag,attr,missing FROM readings ORDER BY time")) {
      for (long time = 1; time <= 2; time++) {
        assertTrue(dataSet.hasNext());
        RowRecord row = dataSet.next();
        assertEquals(time, row.getFields().get(0).getLongV());
        assertTrue(row.getFields().get(1).getBoolV());
        assertEquals(Integer.MIN_VALUE, row.getFields().get(2).getIntV());
        assertEquals(Long.MIN_VALUE, row.getFields().get(3).getLongV());
        assertEquals(-1.25f, row.getFields().get(4).getFloatV(), 0);
        assertEquals(2.5, row.getFields().get(5).getDoubleV(), 0);
        assertEquals(TSDataType.DATE, row.getFields().get(6).getDataType());
        assertEquals(LocalDate.of(2026, 9, 10), row.getFields().get(6).getDateV());
        assertEquals(1234, row.getFields().get(7).getLongV());
        for (int column = 8; column <= 10; column++) {
          assertNull(row.getFields().get(column).getDataType());
        }
      }
      assertFalse(dataSet.hasNext());
    }
  }

  @Test
  public void testUseDatabase() {
    final String[] table1Names = new String[] {"table1"};
    final String[] table1ttls = new String[] {"3600000"};

    final String[] table2Names = new String[] {"table2"};
    final String[] table2ttls = new String[] {"6600000"};

    try (final ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB("test2")) {

      session.executeNonQueryStatement("CREATE DATABASE test1");
      session.executeNonQueryStatement("CREATE DATABASE test2");

      // or use full qualified table name
      session.executeNonQueryStatement(
          "create table test1.table1(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)");

      session.executeNonQueryStatement(
          "create table table2(region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD, speed DOUBLE FIELD) with (TTL=6600000)");

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          final RowRecord rowRecord = dataSet.next();
          assertEquals(table2Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table2ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table2Names.length, cnt);
      }

      session.executeNonQueryStatement("use test1");

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          final RowRecord rowRecord = dataSet.next();
          assertEquals(table1Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table1ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table1Names.length, cnt);
      }

    } catch (final IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateSessionWithCapitalDB() {
    final String[] table2Names = new String[] {"table2"};
    final String[] table2ttls = new String[] {"6600000"};

    try (final ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB("TEST2")) {

      session.executeNonQueryStatement("CREATE DATABASE test1");
      session.executeNonQueryStatement("CREATE DATABASE test2");

      // or use full qualified table name
      session.executeNonQueryStatement(
          "create table test1.table1(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)");

      session.executeNonQueryStatement(
          "create table table2(region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD, speed DOUBLE FIELD) with (TTL=6600000)");

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          final RowRecord rowRecord = dataSet.next();
          assertEquals(table2Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table2ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table2Names.length, cnt);
      }

    } catch (final IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    }
  }
}
