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
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableModelSessionIT {

  @Before
  public void setUp() throws Exception {
    if (Boolean.getBoolean("iotdb.numeric.row.insert.perf.enabled")
        && Boolean.getBoolean("iotdb.numeric.row.insert.perf.io.enabled")) {
      // Isolate per-round storage accounting from compaction of earlier rounds.
      EnvFactory.getEnv()
          .getConfig()
          .getCommonConfig()
          .setEnableSeqSpaceCompaction(false)
          .setEnableUnseqSpaceCompaction(false)
          .setEnableCrossSpaceCompaction(false);
    }
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
    // Refresh compatible FIELD additions, validate TAG padding, then verify a flush/read round
    // trip.
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
      for (int time = 1; time <= 5; time++) {
        if (time == 2) {
          session.executeNonQueryStatement("ALTER TABLE readings ADD COLUMN extra INT64 FIELD");
        }
        if (time == 4) {
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
        for (int time = 1; time <= 5; time++) {
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
  public void testNumericRowNullBitmap() throws Exception {
    // Exercise bits 0/7/8, interleaved TAG metadata, prepared fast/fallback paths and pooled
    // sessions.
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_nulls");
      session.executeNonQueryStatement("USE numeric_nulls");
      StringBuilder ddl = new StringBuilder("CREATE TABLE readings(tag STRING TAG");
      List<String> names = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      List<ColumnCategory> categories = new ArrayList<>();
      long[] values = new long[9];
      for (int i = 0; i < 9; i++) {
        ddl.append(", f").append(i).append(" DOUBLE FIELD");
        names.add("f" + i);
        types.add(TSDataType.DOUBLE);
        categories.add(ColumnCategory.FIELD);
        values[i] = Double.doubleToRawLongBits(i + 0.25);
      }
      session.executeNonQueryStatement(ddl.append(")").toString());
      names.add(1, "tag");
      types.add(1, TSDataType.STRING);
      categories.add(1, ColumnCategory.TAG);
      BitMap nulls = new BitMap(9);
      nulls.mark(0);
      nulls.mark(7);
      nulls.mark(8);
      String[] tags = {"device"};
      session.insert(new NumericRow("readings", 1, names, types, categories, values, tags, nulls));
      try (NumericRowPreparedStatement prepared =
          session.prepareInsertNumericRow("readings", names, types, categories)) {
        session.executePreparedInsertNumericRow(prepared, 2, values, tags, nulls);
        session.executePreparedInsertNumericRow(prepared, 3, values, tags);
        // All-null table rows retain their timestamp/TAGs; null slots must never become zeros.
        BitMap allNulls = new BitMap(9);
        allNulls.markAll();
        session.executePreparedInsertNumericRow(prepared, 3, values, tags, allNulls);
        session.insert(
            new NumericRow("readings", 10, names, types, categories, values, tags, allNulls));
        session.executePreparedInsertNumericRow(prepared, 11, values, tags, allNulls);
        session.executeNonQueryStatement("ALTER TABLE readings ADD COLUMN tag2 STRING TAG");
        session.executePreparedInsertNumericRow(prepared, 4, values, tags, nulls);
      }
      try (ITableSessionPool pool = EnvFactory.getEnv().getTableSessionPool(1, "numeric_nulls");
          ITableSession pooled = pool.getSession();
          NumericRowPreparedStatement prepared =
              pooled.prepareInsertNumericRow("readings", names, types, categories)) {
        pooled.insert(new NumericRow("readings", 5, names, types, categories, values, tags, nulls));
        pooled.executePreparedInsertNumericRow(prepared, 6, values, tags, nulls);
      }
      assertNumericNullRows(session);
      session.executeNonQueryStatement("FLUSH");
      assertNumericNullRows(session);
    }
  }

  // Failed schema validation closes only its handle; fixing the table still requires a new prepare.
  @Test
  public void testPreparedNumericRowClosesOnSchemaFailure() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_invalid");
      session.executeNonQueryStatement("USE numeric_invalid");
      session.executeNonQueryStatement("CREATE TABLE readings(tag STRING TAG, v DOUBLE FIELD)");
      List<String> names = Arrays.asList("tag", "v");
      List<TSDataType> types = Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE);
      List<ColumnCategory> categories = Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);
      long[] values = {Double.doubleToRawLongBits(2.5)};
      String[] tags = {"device"};
      try (NumericRowPreparedStatement prepared =
              session.prepareInsertNumericRow("readings", names, types, categories);
          NumericRowPreparedStatement other =
              session.prepareInsertNumericRow("readings", names, types, categories)) {
        session.executeNonQueryStatement("DROP TABLE readings");
        session.executeNonQueryStatement("CREATE TABLE readings(tag STRING TAG, v BOOLEAN FIELD)");
        StatementExecutionException schemaError =
            assertThrows(
                StatementExecutionException.class,
                () -> session.executePreparedInsertNumericRow(prepared, 1, values, tags));
        assertTrue(schemaError.getMessage().contains("v"));
        session.executeNonQueryStatement("DROP TABLE readings");
        session.executeNonQueryStatement("CREATE TABLE readings(tag STRING TAG, v DOUBLE FIELD)");
        IoTDBConnectionException closedError =
            assertThrows(
                IoTDBConnectionException.class,
                () -> session.executePreparedInsertNumericRow(prepared, 2, values, tags));
        assertTrue(prepared.isClosed());
        assertTrue(closedError.getMessage().contains("closed"));
        session.executePreparedInsertNumericRow(other, 3, values, tags);
        try (NumericRowPreparedStatement replacement =
            session.prepareInsertNumericRow("readings", names, types, categories)) {
          session.executePreparedInsertNumericRow(replacement, 4, values, tags);
        }
        try (SessionDataSet dataSet =
            session.executeQueryStatement("SELECT v FROM readings ORDER BY time")) {
          for (int i = 0; i < 2; i++) {
            assertTrue(dataSet.hasNext());
            assertEquals(2.5, dataSet.next().getFields().get(0).getDoubleV(), 0);
          }
          assertFalse(dataSet.hasNext());
        }
      }
    }
  }

  private void assertNumericNullRows(ITableSession session) throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT time,tag,tag2,f0,f1,f2,f3,f4,f5,f6,f7,f8 FROM readings ORDER BY time")) {
      for (int time : new int[] {1, 2, 3, 4, 5, 6, 10, 11}) {
        assertTrue(dataSet.hasNext());
        RowRecord row = dataSet.next();
        assertEquals(time, row.getFields().get(0).getLongV());
        assertEquals("device", row.getFields().get(1).getStringValue());
        assertNull(row.getFields().get(2).getDataType());
        for (int field = 0; field < 9; field++) {
          if (time >= 10 || (time != 3 && (field == 0 || field == 7 || field == 8))) {
            assertNull(row.getFields().get(field + 3).getDataType());
          } else {
            assertEquals(field + 0.25, row.getFields().get(field + 3).getDoubleV(), 0);
          }
        }
      }
      assertFalse(dataSet.hasNext());
    }
  }

  // Pin clients to every DataNode so at least one must learn a remote leader in a cluster.
  @Test
  public void testPreparedNumericRowRedirection() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_redirect");
      session.executeNonQueryStatement("USE numeric_redirect");
      session.executeNonQueryStatement(
          "CREATE TABLE readings(tag STRING TAG, v INT64 FIELD, n INT64 FIELD)");
      int clientIndex = 0;
      boolean redirected = false;
      for (var node : EnvFactory.getEnv().getDataNodeWrapperList()) {
        try (ITableSession pinned =
            EnvFactory.getEnv().getTableSessionConnection(List.of(node.getIpAndPortString()))) {
          pinned.executeNonQueryStatement("USE numeric_redirect");
          redirected |= writeRedirectedNumericRows(pinned, clientIndex++ * 1000L);
        }
      }
      try (ITableSessionPool pool = EnvFactory.getEnv().getTableSessionPool(1, "numeric_redirect");
          ITableSession pooled = pool.getSession()) {
        redirected |= writeRedirectedNumericRows(pooled, clientIndex++ * 1000L);
      }
      if (EnvFactory.getEnv().getDataNodeWrapperList().size() > 1) {
        assertTrue("Expected a prepared handle on a remote leader", redirected);
      }
      session.executeNonQueryStatement("FLUSH");
      try (SessionDataSet result =
          session.executeQueryStatement("SELECT count(v),count(n) FROM readings")) {
        assertTrue(result.hasNext());
        RowRecord counts = result.next();
        assertEquals(clientIndex * 32L, counts.getFields().get(0).getLongV());
        assertEquals(clientIndex * 16L, counts.getFields().get(1).getLongV());
        assertFalse(result.hasNext());
      }
    }
  }

  private boolean writeRedirectedNumericRows(ITableSession session, long start) throws Exception {
    try (NumericRowPreparedStatement prepared =
        session.prepareInsertNumericRow(
            "readings",
            List.of("tag", "v", "n"),
            List.of(TSDataType.STRING, TSDataType.INT64, TSDataType.INT64),
            List.of(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD))) {
      BitMap nulls = new BitMap(2);
      nulls.mark(1);
      for (int round = 0; round < 4; round++) {
        for (int device = 0; device < 8; device++) {
          long time = start + round * 8 + device;
          session.executePreparedInsertNumericRow(
              prepared,
              time,
              new long[] {time, time},
              new String[] {"d" + device},
              round % 2 == 0 ? nulls : null);
        }
      }
      // Count actual physical bindings: row counts alone cannot prove the client followed a hint.
      Field bindings = NumericRowPreparedStatement.class.getDeclaredField("connectionBindings");
      bindings.setAccessible(true);
      return ((Map<?, ?>) bindings.get(prepared)).size() > 1;
    }
  }

  @Test
  public void testPreparedNumericRowAndTabletWritePerformance() throws Exception {
    // Compare synchronous single-row paths and optionally account for their Linux TCP/storage I/O.
    Assume.assumeTrue(
        "Performance benchmark disabled; set iotdb.numeric.row.insert.perf.enabled=true to run",
        Boolean.getBoolean("iotdb.numeric.row.insert.perf.enabled"));
    final int concurrency = Integer.getInteger("iotdb.numeric.row.insert.perf.concurrency", 16);
    // Compare sustained single-row writes after warming all three paths; verify all acknowledged
    // rows.
    final int warmupRounds = 2;
    final int measuredRounds = 6;
    final int fieldCount = 99;
    final String databasePrefix = "numeric_perf_";
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      StringBuilder ddl = new StringBuilder("CREATE TABLE prepared_perf (tag STRING TAG");
      for (int i = 0; i < fieldCount; i++) {
        ddl.append(", f").append(i).append(" DOUBLE FIELD");
      }
      ddl.append(")");
      // Give each writer its own database so concurrent writes use independent DataRegions.
      for (int i = 0; i < concurrency; i++) {
        session.executeNonQueryStatement("CREATE DATABASE " + databasePrefix + i);
        session.executeNonQueryStatement("USE " + databasePrefix + i);
        session.executeNonQueryStatement(ddl.toString());
        session.executeNonQueryStatement(ddl.toString().replace("prepared_perf", "tablet_perf"));
        session.executeNonQueryStatement(ddl.toString().replace("prepared_perf", "row_perf"));
      }
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
    NumericRow[] rows = new NumericRow[concurrency];
    String[][] tags = new String[concurrency][];
    long[][] numericValues = new long[concurrency][fieldCount];
    long[][] pathRows = new long[3][concurrency];
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    try {
      // Connections, USE, prepare and reusable input allocation are outside every timed round.
      for (int i = 0; i < concurrency; i++) {
        sessions[i] = EnvFactory.getEnv().getTableSessionConnection();
        sessions[i].executeNonQueryStatement("USE " + databasePrefix + i);
        prepared[i] =
            sessions[i].prepareInsertNumericRow("prepared_perf", names, types, categories);
        tablets[i] = new Tablet("tablet_perf", names, types, categories, 1);
        tags[i] = new String[] {"device-" + i};
        Arrays.fill(numericValues[i], Double.doubleToRawLongBits(1.0));
        rows[i] =
            new NumericRow("row_perf", 0, names, types, categories, numericValues[i], tags[i]);
      }
      IntConsumer preparedWrite =
          threadId -> {
            try {
              sessions[threadId].executePreparedInsertNumericRow(
                  prepared[threadId],
                  pathRows[0][threadId],
                  numericValues[threadId],
                  tags[threadId]);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };
      IntConsumer tabletPrepare =
          threadId -> {
            Tablet tablet = tablets[threadId];
            tablet.addTimestamp(0, pathRows[2][threadId]);
            tablet.addValue("tag", 0, tags[threadId][0]);
            for (int i = 0; i < fieldCount; i++) {
              tablet.addValue(names.get(i + 1), 0, 1.0D);
            }
          };
      IntConsumer rowPrepare =
          threadId ->
              rows[threadId] =
                  new NumericRow(
                      "row_perf",
                      pathRows[1][threadId],
                      names,
                      types,
                      categories,
                      numericValues[threadId],
                      tags[threadId]);
      IntConsumer[] paths = {
        preparedWrite,
        threadId -> {
          try {
            sessions[threadId].insert(rows[threadId]);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        threadId -> {
          try {
            sessions[threadId].insert(tablets[threadId]);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      IntConsumer[] prepareWrites = {null, rowPrepare, tabletPrepare};
      IntConsumer[] completeWrites = {
        threadId -> pathRows[0][threadId]++,
        threadId -> pathRows[1][threadId]++,
        threadId -> {
          tablets[threadId].reset();
          pathRows[2][threadId]++;
        }
      };
      String[] pathNames = {"prepared numeric row", "insert NumericRow", "insertTablet batch=1"};
      System.out.printf("configuration: concurrency=%d, tabletBatch=1%n", concurrency);
      WritePerformanceIoTracker ioTracker = null;
      if (Boolean.getBoolean("iotdb.numeric.row.insert.perf.io.enabled")) {
        assertEquals(
            "I/O measurement requires one local DataNode",
            1,
            EnvFactory.getEnv().getDataNodeWrapperList().size());
        assertEquals(
            "I/O measurement requires one local ConfigNode",
            1,
            EnvFactory.getEnv().getConfigNodeWrapperList().size());
        ioTracker =
            new WritePerformanceIoTracker(
                EnvFactory.getEnv().getDataNodeWrapperList().get(0).getPid(),
                EnvFactory.getEnv().getConfigNodeWrapperList().get(0).getPid(),
                EnvFactory.getEnv().getDataNodeWrapperList().get(0).getPort());
      }
      double[][] rates = new double[paths.length][measuredRounds];
      long[] totalRows = new long[paths.length];
      long[] totalNanos = new long[paths.length];
      LongSamples[] requestLatencies = new LongSamples[paths.length];
      for (int path = 0; path < paths.length; path++) {
        requestLatencies[path] = new LongSamples();
      }
      for (int round = 0; round < warmupRounds + measuredRounds; round++) {
        boolean warmup = round < warmupRounds;
        int seconds = warmup ? 3 : 5;
        // Rotate all three paths so each occupies every position during measured rounds.
        for (int position = 0; position < paths.length; position++) {
          int path = (round + position) % paths.length;
          WritePerformanceIoTracker.Snapshot ioBefore =
              ioTracker == null ? null : ioTracker.beforeRound(sessions[0]);
          WritePerformance result =
              runConcurrentWrites(
                  executor,
                  concurrency,
                  seconds,
                  prepareWrites[path],
                  paths[path],
                  completeWrites[path]);
          if (ioTracker != null) {
            System.out.printf(
                "io %s round %d position %d %s: rows=%d, %s%n",
                warmup ? "warmup" : "measured",
                warmup ? round + 1 : round - warmupRounds + 1,
                position + 1,
                pathNames[path],
                result.rows(),
                ioTracker.afterRound(ioBefore, sessions[0]));
          }
          System.out.printf(
              "%s round %d position %d %s: %d rows, %.3f s, %.1f rows/s, API latency %s%n",
              warmup ? "warmup" : "measured",
              warmup ? round + 1 : round - warmupRounds + 1,
              position + 1,
              pathNames[path],
              result.rows(),
              result.nanos() / 1_000_000_000.0,
              result.rowsPerSecond(),
              summarizeRequestLatencies(result.requestNanos()));
          if (!warmup) {
            rates[path][round - warmupRounds] = result.rowsPerSecond();
            totalRows[path] += result.rows();
            totalNanos[path] += result.nanos();
            requestLatencies[path].appendAll(result.requestNanos());
          }
        }
      }
      for (int path = 0; path < paths.length; path++) {
        Arrays.sort(rates[path]);
        System.out.printf(
            "summary %s: median %.1f rows/s, aggregate %.1f rows/s, min %.1f, max %.1f, "
                + "measured API latency %s%n",
            pathNames[path],
            (rates[path][measuredRounds / 2 - 1] + rates[path][measuredRounds / 2]) / 2,
            totalRows[path] * 1_000_000_000.0 / totalNanos[path],
            rates[path][0],
            rates[path][measuredRounds - 1],
            summarizeRequestLatencies(requestLatencies[path].toArray()));
      }
      // Per-device monotonic timestamps span warmup and measurement, so no rows overwrite others.
      for (int path = 0; path < paths.length; path++) {
        String table =
            switch (path) {
              case 0 -> "prepared_perf";
              case 1 -> "row_perf";
              default -> "tablet_perf";
            };
        long[] rowsByThread = pathRows[path];
        long expectedRows = Arrays.stream(rowsByThread).sum();
        for (int i = 0; i < concurrency; i++) {
          try (SessionDataSet dataSet =
              sessions[i].executeQueryStatement("SELECT count(*) FROM " + table)) {
            assertTrue(dataSet.hasNext());
            // Each worker writes to its own database, including warm-up rows.
            assertEquals(rowsByThread[i], dataSet.next().getFields().get(0).getLongV());
            assertFalse(dataSet.hasNext());
          }
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

  private record WritePerformance(long rows, long nanos, long[] requestNanos) {
    double rowsPerSecond() {
      return rows * 1_000_000_000.0 / nanos;
    }
  }

  private record RequestLatency(
      long count, double averageNanos, long p50Nanos, long p95Nanos, long p99Nanos) {
    @Override
    public String toString() {
      return String.format(
          "n=%d, avg=%.3f us, p50=%.3f us, p95=%.3f us, p99=%.3f us",
          count,
          averageNanos / 1_000.0,
          p50Nanos / 1_000.0,
          p95Nanos / 1_000.0,
          p99Nanos / 1_000.0);
    }
  }

  private static class LongSamples {
    private long[] values = new long[1024];
    private int size;

    void add(long value) {
      if (size == values.length) {
        values = Arrays.copyOf(values, values.length * 2);
      }
      values[size++] = value;
    }

    void appendAll(long[] samples) {
      while (values.length - size < samples.length) {
        values = Arrays.copyOf(values, values.length * 2);
      }
      System.arraycopy(samples, 0, values, size, samples.length);
      size += samples.length;
    }

    long[] toArray() {
      return Arrays.copyOf(values, size);
    }
  }

  private static RequestLatency summarizeRequestLatencies(long[] requestNanos) {
    if (requestNanos.length == 0) {
      return new RequestLatency(0, 0, 0, 0, 0);
    }
    long[] sorted = Arrays.copyOf(requestNanos, requestNanos.length);
    Arrays.sort(sorted);
    double sum = 0;
    for (long latency : sorted) {
      sum += latency;
    }
    return new RequestLatency(
        sorted.length,
        sum / sorted.length,
        percentile(sorted, 0.50),
        percentile(sorted, 0.95),
        percentile(sorted, 0.99));
  }

  private static long percentile(long[] sorted, double quantile) {
    return sorted[Math.max(0, (int) Math.ceil(quantile * sorted.length) - 1)];
  }

  private static WritePerformance runConcurrentWrites(
      ExecutorService executor,
      int concurrency,
      int seconds,
      IntConsumer prepare,
      IntConsumer write,
      IntConsumer complete)
      throws Exception {
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
                  LongSamples requestNanos = new LongSamples();
                  while (!Thread.currentThread().isInterrupted()
                      && System.nanoTime() < deadline.get()) {
                    if (prepare != null) {
                      prepare.accept(threadId);
                    }
                    long requestStart = System.nanoTime();
                    write.accept(threadId);
                    requestNanos.add(System.nanoTime() - requestStart);
                    complete.accept(threadId);
                    rows++;
                  }
                  return new WritePerformance(rows, System.nanoTime(), requestNanos.toArray());
                }));
      }
      assertTrue(ready.await(30, TimeUnit.SECONDS));
      long startNanos = System.nanoTime();
      deadline.set(startNanos + TimeUnit.SECONDS.toNanos(seconds));
      start.countDown();
      long rows = 0;
      long endNanos = startNanos;
      LongSamples requestNanos = new LongSamples();
      for (Future<WritePerformance> future : futures) {
        WritePerformance result = future.get(seconds + 60, TimeUnit.SECONDS);
        rows += result.rows();
        endNanos = Math.max(endNanos, result.nanos());
        requestNanos.appendAll(result.requestNanos());
      }
      // Include completion of in-flight RPCs, but exclude main-thread collection/printing time.
      return new WritePerformance(rows, endNanos - startNanos, requestNanos.toArray());
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
