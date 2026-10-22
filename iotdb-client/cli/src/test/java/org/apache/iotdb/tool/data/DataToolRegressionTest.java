/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tool.data;

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.common.ImportTsFileOperation;
import org.apache.iotdb.tool.tsfile.ImportTsFileBase;
import org.apache.iotdb.tool.tsfile.ImportTsFileScanTool;

import org.apache.commons.csv.CSVParser;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataToolRegressionTest extends AbstractDataTool {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Map<Field, Object> originalFields = new HashMap<>();
  private Path directory;

  @Before
  public void setUp() throws Exception {
    for (Class<?> owner :
        Arrays.asList(
            AbstractDataTool.class,
            ImportDataTree.class,
            ImportDataTable.class,
            ImportTsFileBase.class,
            ImportTsFileScanTool.class)) {
      for (Field field : owner.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) && !Modifier.isFinal(field.getModifiers())) {
          field.setAccessible(true);
          originalFields.put(field, field.get(null));
        }
      }
    }
    directory = temporaryFolder.getRoot().toPath();
    fileType = "csv";
    timeColumn = "Time";
    timestampPrecision = "ms";
    database = "db";
    table = "t";
    aligned = false;
    batchPointSize = 100;
    threadNum = 1;
  }

  @After
  public void tearDown() throws Exception {
    for (Map.Entry<Field, Object> entry : originalFields.entrySet()) {
      entry.getKey().set(null, entry.getValue());
    }
  }

  @Test
  public void testMultiDeviceSqlPreservesColumnMappingAndSplitsFiles() throws Exception {
    Session session =
        mockTree(
            dataSet(
                List.of("Time", "root.a.d1.s1", "root.a.d2.s2", "root.a.d1.s3"),
                List.of("INT64", "INT32", "INT32", "INT32"),
                new Object[] {1L, 11, 22, 33},
                new Object[] {2L, null, 44, null}));
    exportTree(session, "select * from root.a.*", 0, "-ft", "sql", "-lpf", "1");
    // -lpf counts data rows, so the statements of one row share a file.
    assertEquals(
        "INSERT INTO root.a.d1(TIMESTAMP,s1,s3) VALUES (1,11,33);\n"
            + "INSERT INTO root.a.d2(TIMESTAMP,s2) VALUES (1,22);\n",
        read("dump0_0.sql"));
    assertEquals("INSERT INTO root.a.d2(TIMESTAMP,s2) VALUES (2,44);\n", read("dump0_1.sql"));
    assertFalse(Files.exists(directory.resolve("dump0_2.sql")));
  }

  @Test
  public void testCustomTableQueriesUseDistinctFilesAndActualTables() throws Exception {
    ITableSession session = mockTable("table_a", "table_b");
    describe(session, "table_a", "v", "INT32", "FIELD");
    describe(session, "table_b", "v", "INT32", "FIELD");
    String qualified = "SELECT * FROM \"db\".\"table_b\" WHERE tag = 'from table_a'";
    SessionDataSet first =
        dataSet(List.of("time", "v"), List.of("TIMESTAMP", "INT32"), new Object[] {1L, 11});
    when(session.executeQueryStatement(eq("select * from table_a"), anyLong())).thenReturn(first);
    SessionDataSet second =
        dataSet(List.of("time", "v"), List.of("TIMESTAMP", "INT32"), new Object[] {2L, 22});
    when(session.executeQueryStatement(eq(qualified), anyLong())).thenReturn(second);
    exportTable(session, "select * from table_a", 0, "-ft", "sql");
    exportTable(session, qualified, 1, "-ft", "sql");
    assertEquals("insert into \"table_a\"(\"time\",\"v\") values(1,11);\n", read("dump0_0.sql"));
    assertEquals("insert into \"table_b\"(\"time\",\"v\") values(2,22);\n", read("dump1_0.sql"));
  }

  @Test
  public void testCustomTableQueryOverridesTableFilter() throws Exception {
    ITableSession session = mockTable("table_a", "table_b");
    describe(session, "table_b", "v", "INT32", "FIELD");
    SessionDataSet result =
        dataSet(List.of("time", "v"), List.of("TIMESTAMP", "INT32"), new Object[] {1L, 11});
    when(session.executeQueryStatement(eq("select * from table_b"), anyLong())).thenReturn(result);

    exportTable(session, "select * from table_b", 0, "-ft", "sql", "-table", "table_a");
    assertEquals("insert into \"table_b\"(\"time\",\"v\") values(1,11);\n", read("dump0_0.sql"));
  }

  @Test
  public void testDefaultTableExportStillUsesTableFilter() throws Exception {
    ITableSession session = mockTable("table_a", "table_b");
    describe(session, "table_a", "v", "INT32", "FIELD");
    SessionDataSet result =
        dataSet(List.of("time", "v"), List.of("TIMESTAMP", "INT32"), new Object[] {1L, 11});
    when(session.executeQueryStatement(eq("SELECT * FROM \"table_a\""), anyLong()))
        .thenReturn(result);

    exportTable(session, null, 0, "-ft", "sql", "-table", "table_a");
    verify(session).executeQueryStatement(eq("SELECT * FROM \"table_a\""), anyLong());
    assertEquals("insert into \"table_a\"(\"time\",\"v\") values(1,11);\n", read("dump0_0.sql"));
    assertFalse(Files.exists(directory.resolve("dump1_0.sql")));
  }

  @Test
  public void testQuotedTableAndColumnNames() throws Exception {
    ITableSession session = mockTable("odd\"name");
    describe(session, "odd\"name", "select", "INT32", "FIELD");
    SessionDataSet result =
        dataSet(List.of("time", "select"), List.of("TIMESTAMP", "INT32"), new Object[] {1L, 11});
    when(session.executeQueryStatement(eq("select * from \"ODD\"\"NAME\""), anyLong()))
        .thenReturn(result);
    exportTable(session, "select * from \"ODD\"\"NAME\"", 0, "-ft", "sql");
    assertEquals(
        "insert into \"odd\"\"name\"(\"time\",\"select\") values(1,11);\n", read("dump0_0.sql"));
  }

  @Test
  public void testAmbiguousAndCrossDatabaseTableQueriesFailBeforeWriting() throws Exception {
    ITableSession session = mockTable("table_a", "table_b");
    String message =
        String.format(
            CliMessages
                .EXCEPTION_SQL_TSFILE_EXPORT_REQUIRES_A_QUERY_FROM_A_SINGLE_TABLE_IN_DATABASE_ARG_C9EB3768,
            "db");
    for (String query :
        List.of(
            "select * from table_a join table_b on table_a.time = table_b.time",
            "select * from other_db.table_b",
            "select * from",
            "select * from table_b #invalid_character",
            "explain select * from table_a",
            "with a as (select * from table_a) select * from a",
            "select * from table_a where v > (select 1)",
            "select * from table_a union all select * from table_a",
            // Column aliases rename the columns the rows are written to.
            "select * from table_a as a(time, w)",
            // All queries are checked before the first one runs.
            "select * from table_a; select * from table_a, table_b")) {
      ArgsErrorException e =
          assertThrows(
              query,
              ArgsErrorException.class,
              () -> exportTable(session, null, 0, "-ft", "sql", "-q", query));
      assertEquals(query, message, e.getMessage());
    }
    verify(session, never()).executeQueryStatement(not(eq("show tables")), anyLong());
    assertFalse(Files.exists(directory.resolve("dump0_0.sql")));
  }

  @Test
  public void testTableQueriesCannotWriteComputedValuesBack() throws Exception {
    ITableSession session = mockTable("t");
    describe(session, "t", "tag", "STRING", "TAG", "v", "INT32", "FIELD", "w", "INT32", "FIELD");
    for (String query :
        List.of(
            // Aliases, expressions and FILL can store values that the table does not hold.
            "select time, tag, v + 1 as v from t",
            "select time, tag, w as v, v as w from t",
            "select time as time, tag, v from t",
            "select time, tag, abs(v) from t",
            "select time, tag, v from t fill method previous")) {
      for (String format : List.of("sql", "tsfile")) {
        ArgsErrorException e =
            assertThrows(
                query,
                ArgsErrorException.class,
                () -> exportTable(session, query, 0, "-ft", format));
        assertEquals(
            query,
            CliMessages
                .EXCEPTION_SQL_TSFILE_EXPORT_CAN_ONLY_SELECT_OR_COLUMN_NAMES_WITHOUT_FILL_EC83126D,
            e.getMessage());
      }
    }
    verify(session, never()).executeQueryStatement(not(eq("show tables")), anyLong());
  }

  @Test
  public void testTableQuerySelectingColumnsIsExported() throws Exception {
    ITableSession session = mockTable("t");
    describe(session, "t", "tag", "STRING", "TAG", "v", "INT32", "FIELD", "w", "INT32", "FIELD");
    String query = "SELECT \"TIME\", Tag, v FROM t AS x WHERE x.w > 1 ORDER BY time LIMIT 5";
    SessionDataSet result =
        dataSet(
            List.of("time", "tag", "v"),
            List.of("TIMESTAMP", "STRING", "INT32"),
            new Object[] {2L, "d1", 5});
    when(session.executeQueryStatement(eq(query), anyLong())).thenReturn(result);
    exportTable(session, query, 0, "-ft", "sql");
    assertEquals(
        "insert into \"t\"(\"time\",\"tag\",\"v\") values(2,'d1',5);\n", read("dump0_0.sql"));
  }

  @Test
  public void testTableQueriesNeedAFieldColumnBeforeAnyQueryRuns() throws Exception {
    ITableSession session = mockTable("t");
    describe(session, "t", "tag", "STRING", "TAG", "v", "INT32", "FIELD");
    // IoTDB rejects an INSERT without a FIELD column, so this SQL could not be imported.
    ArgsErrorException e =
        assertThrows(
            ArgsErrorException.class,
            () ->
                exportTable(
                    session,
                    null,
                    0,
                    "-ft",
                    "sql",
                    "-q",
                    "select * from t; select time, tag from t"));
    assertEquals(
        String.format(
            CliMessages.EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_A_FIELD_COLUMN_FROM_TABLE_ARG_1C08B044,
            "t"),
        e.getMessage());
    verify(session, never()).executeQueryStatement(startsWith("select"), anyLong());
    assertFalse(Files.exists(directory.resolve("dump0_0.sql")));
  }

  @Test
  public void testTableQueryColumnsAreCheckedAgainstTheTable() throws Exception {
    ITableSession session = mockTable("t");
    describe(session, "t", "tag", "STRING", "TAG", "v", "INT32", "FIELD");
    String message =
        CliMessages
            .EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_THE_TIME_COLUMN_AND_COLUMNS_OF_TABLE_ARG_14B7FB67;
    String invalid = String.format(message, "t");
    for (String query :
        List.of("select tag, v from t", "select *, v from t", "select time, tag, missing from t")) {
      ArgsErrorException e =
          assertThrows(
              query, ArgsErrorException.class, () -> exportTable(session, query, 0, "-ft", "sql"));
      assertEquals(query, invalid, e.getMessage());
    }
    // Without its TAG columns, a TsFile would merge the rows of every device.
    ArgsErrorException e =
        assertThrows(
            ArgsErrorException.class,
            () -> exportTable(session, "select time, v from t", 0, "-ft", "tsfile"));
    assertEquals(
        String.format(
            CliMessages.EXCEPTION_TSFILE_OUTPUT_REQUIRES_EVERY_TAG_COLUMN_FROM_TABLE_ARG_901F420F,
            "t"),
        e.getMessage());
    verify(session, never()).executeQueryStatement(startsWith("select"), anyLong());
  }

  @Test
  public void testExportChecksTheColumnsTheQueryReturns() throws Exception {
    ITableSession session = mockTable("t");
    describe(session, "t", "tag", "STRING", "TAG", "v", "INT32", "FIELD");
    // The table can change between planning and the query, so the returned columns are checked.
    SessionDataSet result =
        dataSet(List.of("time", "tag"), List.of("TIMESTAMP", "STRING"), new Object[] {1L, "d1"});
    when(session.executeQueryStatement(eq("select * from t"), anyLong())).thenReturn(result);
    ArgsErrorException e =
        assertThrows(
            ArgsErrorException.class,
            () -> exportTable(session, "select * from t", 0, "-ft", "sql"));
    assertEquals(
        String.format(
            CliMessages.EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_A_FIELD_COLUMN_FROM_TABLE_ARG_1C08B044,
            "t"),
        e.getMessage());
    assertFalse(Files.exists(directory.resolve("dump0_0.sql")));
  }

  @Test
  public void testTableSqlEscapesStringsAndPreservesNull() throws Exception {
    ITableSession session = mockTable("t");
    describe(session, "t", "tag", "STRING", "TAG", "value", "TEXT", "FIELD");
    SessionDataSet result =
        dataSet(
            List.of("time", "tag", "value"),
            List.of("TIMESTAMP", "STRING", "TEXT"),
            new Object[] {1L, "O'Brien\\path", null});
    when(session.executeQueryStatement(eq("select * from t"), anyLong())).thenReturn(result);
    exportTable(session, "select * from t", 0, "-ft", "sql");
    assertEquals(
        "insert into \"t\"(\"time\",\"tag\",\"value\") values(1,'O''Brien\\path',NULL);\n",
        read("dump0_0.sql"));
  }

  @Test
  public void testAlignedTreeSqlEscapesStringsAndPreservesNull() throws Exception {
    Session session =
        mockTree(
            dataSet(
                List.of("Time", "Device", "s1", "s2"),
                List.of("INT64", "TEXT", "TEXT", "STRING"),
                new Object[] {1L, "root.a.d1", "a'b\"\\c", null}));
    exportTree(session, "select * from root.a.* align by device", 0, "-ft", "sql");
    assertEquals(
        "INSERT INTO root.a.d1(TIMESTAMP,s1,s2) ALIGNED VALUES (1,'a''b\"\\c',NULL);\n",
        read("dump0_0.sql"));
  }

  @Test
  public void testTreeCsvTypeHeadersInEverySplitFile() throws Exception {
    Session typed =
        mockTree(
            dataSet(
                List.of("Time", "Device", "s1"),
                List.of("INT64", "TEXT", "DOUBLE"),
                new Object[] {1L, "root.a.d1", 1.5},
                new Object[] {2L, "root.a.d1", 2.5}));
    exportTree(
        typed,
        "select * from root.a.* align by device",
        0,
        "-ft",
        "csv",
        "-lpf",
        "1",
        "-dt",
        "true");
    assertEquals("Time,Device,s1(DOUBLE)", firstLine("dump0_0.csv"));
    assertEquals("Time,Device,s1(DOUBLE)", firstLine("dump0_1.csv"));
    Session plain =
        mockTree(
            dataSet(
                List.of("Time", "root.a.d1.s1"),
                List.of("INT64", "DOUBLE"),
                new Object[] {1L, 1.5}));
    exportTree(plain, "select * from root.a.d1", 1, "-ft", "csv");
    assertEquals("Time,root.a.d1.s1", firstLine("dump1_0.csv"));
  }

  @Test
  public void testTableCsvTypeHeadersAndMultipleQueryFiles() throws Exception {
    ITableSession session = mockTable("t");
    SessionDataSet first =
        dataSet(
            List.of("time", "device", "v"),
            List.of("TIMESTAMP", "STRING", "DOUBLE"),
            new Object[] {1L, "sensor", 1.5});
    when(session.executeQueryStatement(eq("select * from t"), anyLong())).thenReturn(first);
    SessionDataSet second =
        dataSet(List.of("time", "v"), List.of("TIMESTAMP", "DOUBLE"), new Object[] {2L, 2.5});
    when(session.executeQueryStatement(eq("select time,v from t"), anyLong())).thenReturn(second);
    exportTable(session, "select * from t", 0, "-ft", "csv", "-dt", "true");
    exportTable(session, "select time,v from t", 1, "-ft", "csv");
    assertEquals("time,device(STRING),v(DOUBLE)", firstLine("dump0_0.csv"));
    assertEquals("time,v", firstLine("dump1_0.csv"));
  }

  @Test
  public void testExportQueryFailureIsReported() throws Exception {
    Session session = mock(Session.class);
    when(session.executeQueryStatement(anyString(), anyLong()))
        .thenThrow(new IoTDBConnectionException("disconnect"));
    // ExportData.run reports the failure: FINISH is FAILED and the exit code is 1.
    IoTDBConnectionException e =
        assertThrows(
            IoTDBConnectionException.class,
            () -> exportTree(session, "select * from root.a.d1", 0, "-ft", "sql"));
    assertEquals("disconnect", e.getMessage());
  }

  @Test
  public void testOneDeviceRetriesRetainFailedBatch() throws Exception {
    SessionPool pool = mock(SessionPool.class);
    set(ImportDataTree.class, "sessionPool", pool);
    List<Long> timestamps = new ArrayList<>(List.of(1L));
    List<List<TSDataType>> types = new ArrayList<>(List.of(List.of(TSDataType.INT32)));
    List<List<Object>> values = new ArrayList<>(List.of(List.of(11)));
    List<List<String>> measurements = new ArrayList<>(List.of(List.of("s1")));
    IoTDBConnectionException failure = new IoTDBConnectionException("disconnect");
    doThrow(failure)
        .when(pool)
        .insertRecordsOfOneDevice("root.a.d1", timestamps, measurements, types, values);
    try {
      invoke(
          ImportDataTree.class,
          "writeAndEmptyDataSet",
          null,
          new Class<?>[] {String.class, List.class, List.class, List.class, List.class, int.class},
          "root.a.d1",
          timestamps,
          types,
          values,
          measurements,
          3);
      fail("Expected exhausted retries to fail");
    } catch (IllegalStateException e) {
      assertSame(failure, e.getCause());
    }
    verify(pool, times(4))
        .insertRecordsOfOneDevice("root.a.d1", timestamps, measurements, types, values);
    assertEquals(List.of(1L), timestamps);
    assertEquals(List.of(List.of(11)), values);
    assertFalse(types.isEmpty());
    assertFalse(measurements.isEmpty());
  }

  @Test
  public void testOneDeviceSuccessfulRetryClearsBatchOnlyAfterInsert() throws Exception {
    SessionPool pool = mock(SessionPool.class);
    set(ImportDataTree.class, "sessionPool", pool);
    List<Long> timestamps = new ArrayList<>(List.of(1L));
    List<List<TSDataType>> types = new ArrayList<>(List.of(List.of(TSDataType.INT32)));
    List<List<Object>> values = new ArrayList<>(List.of(List.of(11)));
    List<List<String>> measurements = new ArrayList<>(List.of(List.of("s1")));
    AtomicInteger attempts = new AtomicInteger();
    doAnswer(
            invocation -> {
              assertEquals(List.of(1L), timestamps);
              assertEquals(List.of(List.of(11)), values);
              if (attempts.incrementAndGet() < 3) {
                throw new IoTDBConnectionException("disconnect");
              }
              return null;
            })
        .when(pool)
        .insertRecordsOfOneDevice("root.a.d1", timestamps, measurements, types, values);
    invoke(
        ImportDataTree.class,
        "writeAndEmptyDataSet",
        null,
        new Class<?>[] {String.class, List.class, List.class, List.class, List.class, int.class},
        "root.a.d1",
        timestamps,
        types,
        values,
        measurements,
        3);
    assertEquals(3, attempts.get());
    assertTrue(timestamps.isEmpty());
    assertTrue(values.isEmpty());
    assertTrue(types.isEmpty());
    assertTrue(measurements.isEmpty());
  }

  @Test
  public void testAlignedMultiDeviceRetriesRetainFailedBatch() throws Exception {
    aligned = true;
    SessionPool pool = mock(SessionPool.class);
    set(ImportDataTree.class, "sessionPool", pool);
    List<String> devices = new ArrayList<>(List.of("root.a.d1"));
    List<Long> timestamps = new ArrayList<>(List.of(1L));
    List<List<TSDataType>> types = new ArrayList<>(List.of(List.of(TSDataType.INT32)));
    List<List<Object>> values = new ArrayList<>(List.of(List.of(11)));
    List<List<String>> measurements = new ArrayList<>(List.of(List.of("s1")));
    doThrow(new IoTDBConnectionException("disconnect"))
        .when(pool)
        .insertAlignedRecords(devices, timestamps, measurements, types, values);
    try {
      invoke(
          ImportDataTree.class,
          "writeAndEmptyDataSet",
          null,
          new Class<?>[] {List.class, List.class, List.class, List.class, List.class, int.class},
          devices,
          timestamps,
          types,
          values,
          measurements,
          3);
      fail("Expected exhausted retries to fail");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getCause() instanceof IoTDBConnectionException);
    }
    verify(pool, times(4)).insertAlignedRecords(devices, timestamps, measurements, types, values);
    assertEquals(List.of("root.a.d1"), devices);
    assertEquals(List.of(1L), timestamps);
    assertEquals(List.of(List.of(11)), values);
  }

  @Test
  public void testTableRetriesRetainTabletAndCloseSessions() throws Exception {
    ITableSessionPool pool = mock(ITableSessionPool.class);
    ITableSession session = mock(ITableSession.class);
    when(pool.getSession()).thenReturn(session);
    set(ImportDataTable.class, "sessionPool", pool);
    Tablet tablet =
        new Tablet(
            "t", List.of("v"), List.of(TSDataType.DOUBLE), List.of(ColumnCategory.FIELD), 100);
    tablet.addTimestamp(0, 1L);
    tablet.addValue("v", 0, 1.5);
    doThrow(new IoTDBConnectionException("disconnect")).when(session).insert(tablet);
    try {
      invoke(
          ImportDataTable.class,
          "writeAndEmptyDataSet",
          null,
          new Class<?>[] {Tablet.class, int.class},
          tablet,
          3);
      fail("Expected exhausted retries to fail");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getCause() instanceof IoTDBConnectionException);
    }
    assertEquals(1, tablet.getRowSize());
    verify(session, times(4)).insert(tablet);
    verify(session, times(4)).close();
  }

  @Test
  public void testWorkerFailurePreventsSuccessAndResetsForNextImport() throws Exception {
    Path csv = directory.resolve("input.csv");
    Files.writeString(csv, "Time,root.a.d1.s1(INT32)\n1,11\n");
    SessionPool pool = mock(SessionPool.class);
    set(ImportDataTree.class, "sessionPool", pool);
    doThrow(new IoTDBConnectionException("disconnect"))
        .when(pool)
        .insertRecords(any(), any(), any(), any(), any());
    long successes = loadFileSuccessfulNum.sum();
    ImportDataScanTool.putToQueue(csv.toString());
    assertFalse(AbstractImportData.init(new ImportDataTree()));
    assertEquals(successes, loadFileSuccessfulNum.sum());
    doNothing().when(pool).insertRecords(any(), any(), any(), any(), any());
    ImportDataScanTool.putToQueue(csv.toString());
    assertTrue(AbstractImportData.init(new ImportDataTree()));
    assertEquals(successes + 1, loadFileSuccessfulNum.sum());
  }

  @Test
  public void testFailedCsvBatchDoesNotSkipNextFile() throws Exception {
    Path bad = directory.resolve("a_bad.csv");
    Path good = directory.resolve("b_good.csv");
    Files.writeString(bad, "Time,root.a.d1.s1(INT32)\n1,11\n");
    Files.writeString(good, "Time,root.a.d2.s1(INT32)\n1,22\n");
    SessionPool pool = mock(SessionPool.class);
    set(ImportDataTree.class, "sessionPool", pool);
    AtomicInteger goodInserted = new AtomicInteger();
    doAnswer(
            invocation -> {
              if (((List<?>) invocation.getArgument(0)).contains("root.a.d1")) {
                throw new StatementExecutionException("type mismatch");
              }
              if (((List<?>) invocation.getArgument(0)).contains("root.a.d2")) {
                goodInserted.incrementAndGet();
              }
              return null;
            })
        .when(pool)
        .insertRecords(any(), any(), any(), any(), any());
    ImportDataScanTool.putToQueue(bad.toString());
    ImportDataScanTool.putToQueue(good.toString());

    assertFalse(AbstractImportData.init(new ImportDataTree()));

    assertEquals(1, goodInserted.get());
    assertNull(ImportDataScanTool.pollFromQueue());
  }

  @Test
  public void testRejectedCsvAndFailedSqlAffectImportExitStatus() throws Exception {
    Path badCsv = directory.resolve("bad_header.csv");
    Files.writeString(badCsv, "ts,root.a.d1.s1(INT32)\n1,11\n");
    ImportDataScanTool.putToQueue(badCsv.toString());
    assertFalse(AbstractImportData.init(new ImportDataTree()));

    Path badTableCsv = directory.resolve("bad_table_header.csv");
    Files.writeString(badTableCsv, "ts,v\n1,11\n");
    ImportDataScanTool.putToQueue(badTableCsv.toString());
    assertFalse(AbstractImportData.init(new ImportDataTable()));

    Path failedTreeSql = directory.resolve("failed_tree.sql");
    Files.writeString(failedTreeSql, "insert into root.a.d1(time,s1) values(1,11)\n");
    SessionPool treePool = mock(SessionPool.class);
    set(ImportDataTree.class, "sessionPool", treePool);
    doThrow(new StatementExecutionException("statement failed"))
        .when(treePool)
        .executeNonQueryStatement(anyString());
    ImportDataScanTool.putToQueue(failedTreeSql.toString());
    assertFalse(AbstractImportData.init(new ImportDataTree()));

    Path failedTableSql = directory.resolve("failed_table.sql");
    Files.writeString(failedTableSql, "insert into db.t(time,v) values(1,11)\n");
    ITableSessionPool tablePool = mock(ITableSessionPool.class);
    ITableSession tableSession = mock(ITableSession.class);
    when(tablePool.getSession()).thenReturn(tableSession);
    doThrow(new StatementExecutionException("statement failed"))
        .when(tableSession)
        .executeNonQueryStatement(anyString());
    set(ImportDataTable.class, "sessionPool", tablePool);
    ImportDataScanTool.putToQueue(failedTableSql.toString());
    assertFalse(AbstractImportData.init(new ImportDataTable()));
  }

  @Test
  public void testRejectedFilesAreCountedOnceInBothModels() throws Exception {
    Path empty = directory.resolve("empty.csv");
    Path header = directory.resolve("invalid.csv");
    Path extension = directory.resolve("input.other");
    Files.writeString(empty, "");
    Files.writeString(header, "ts,v\n1,11\n");
    Files.writeString(extension, "Time,v\n1,11\n");
    for (AbstractImportData importer : List.of(new ImportDataTree(), new ImportDataTable())) {
      for (Path source :
          List.of(
              empty,
              header,
              extension,
              directory.resolve("missing.csv"),
              directory.resolve("missing.sql"))) {
        long failures = loadFileFailedNum.sum();
        long successes = loadFileSuccessfulNum.sum();
        ImportDataScanTool.putToQueue(source.toString());
        assertFalse(AbstractImportData.init(importer));
        assertEquals(failures + 1, loadFileFailedNum.sum());
        assertEquals(successes, loadFileSuccessfulNum.sum());
      }
    }
  }

  @Test
  public void testFailedDeviceCsvDoesNotSkipNextFile() throws Exception {
    Path bad = directory.resolve("a_bad.csv");
    Path good = directory.resolve("b_good.csv");
    Files.writeString(bad, "Time,Device,s1(INT32)\n1,root.a.d1,11\n");
    Files.writeString(good, "Time,Device,s1(INT32)\n1,root.a.d2,22\n");
    SessionPool pool = mock(SessionPool.class);
    set(ImportDataTree.class, "sessionPool", pool);
    AtomicInteger goodInserted = new AtomicInteger();
    doAnswer(
            invocation -> {
              if ("root.a.d1".equals(invocation.getArgument(0))) {
                throw new StatementExecutionException("type mismatch");
              }
              goodInserted.incrementAndGet();
              return null;
            })
        .when(pool)
        .insertRecordsOfOneDevice(anyString(), any(), any(), any(), any());
    ImportDataScanTool.putToQueue(bad.toString());
    ImportDataScanTool.putToQueue(good.toString());
    assertFalse(AbstractImportData.init(new ImportDataTree()));
    assertEquals(1, goodInserted.get());
    assertNull(ImportDataScanTool.pollFromQueue());
  }

  @Test
  public void testFailedTableCsvDoesNotSkipNextFile() throws Exception {
    Path bad = directory.resolve("a_bad.csv");
    Path good = directory.resolve("b_good.csv");
    Files.writeString(bad, "Time,v(INT32)\n1,11\n");
    Files.writeString(good, "Time,v(INT32)\n2,22\n");
    set(ImportDataTable.class, "dataTypes", Map.of("v", TSDataType.INT32));
    set(ImportDataTable.class, "columnCategory", Map.of("v", ColumnCategory.FIELD));
    ITableSessionPool pool = mock(ITableSessionPool.class);
    ITableSession session = mock(ITableSession.class);
    when(pool.getSession()).thenReturn(session);
    set(ImportDataTable.class, "sessionPool", pool);
    AtomicInteger goodInserted = new AtomicInteger();
    doAnswer(
            invocation -> {
              Tablet tablet = invocation.getArgument(0);
              if (tablet.getTimestamps()[0] == 1L) {
                throw new StatementExecutionException("type mismatch");
              }
              goodInserted.incrementAndGet();
              return null;
            })
        .when(session)
        .insert(any(Tablet.class));
    ImportDataScanTool.putToQueue(bad.toString());
    ImportDataScanTool.putToQueue(good.toString());
    assertFalse(AbstractImportData.init(new ImportDataTable()));
    assertEquals(1, goodInserted.get());
    assertNull(ImportDataScanTool.pollFromQueue());
  }

  @Test
  public void testTypedTableCsvUsesOriginalHeaderSpelling() throws Exception {
    Map<String, TSDataType> types = new LinkedHashMap<>();
    types.put("id", TSDataType.STRING);
    types.put("v", TSDataType.DOUBLE);
    Map<String, ColumnCategory> categories = new LinkedHashMap<>();
    categories.put("id", ColumnCategory.TAG);
    categories.put("v", ColumnCategory.FIELD);
    set(ImportDataTable.class, "dataTypes", types);
    set(ImportDataTable.class, "columnCategory", categories);
    ITableSessionPool pool = mock(ITableSessionPool.class);
    ITableSession session = mock(ITableSession.class);
    when(pool.getSession()).thenReturn(session);
    set(ImportDataTable.class, "sessionPool", pool);
    Path csv = directory.resolve("typed.csv");
    Files.writeString(csv, "TIME,ID(STRING),v(double)\n1,sensor,1.5\n");
    try (CSVParser records = readCsvFile(csv.toString())) {
      new ImportDataTable()
          .writeData(
              records.getHeaderNames(),
              records.getRecords(),
              directory.resolve("typed.failed").toString());
    }
    verify(session).insert(any(Tablet.class));
    assertFalse(Files.exists(directory.resolve("typed.failed")));
  }

  @Test
  public void testTableCsvRoundTripPreservesWhitespaceAndParenthesesInColumnNames()
      throws Exception {
    assertTableCsvRoundTrip("value(double) with space");
  }

  @Test
  public void testTableCsvRoundTripPreservesTrailingParenthesesInColumnNames() throws Exception {
    for (String column : List.of("value(foo)", "value(double)", "value(foo)(double)")) {
      assertTableCsvRoundTrip(column);
    }
  }

  @Test
  public void testTableCsvPrefersCompleteColumnNameOverTypeSuffix() throws Exception {
    Map<String, TSDataType> types = new LinkedHashMap<>();
    types.put("value", TSDataType.DOUBLE);
    types.put("value(double)", TSDataType.DOUBLE);
    Map<String, ColumnCategory> categories = new LinkedHashMap<>();
    categories.put("value", ColumnCategory.FIELD);
    categories.put("value(double)", ColumnCategory.FIELD);
    set(ImportDataTable.class, "dataTypes", types);
    set(ImportDataTable.class, "columnCategory", categories);
    ITableSessionPool pool = mock(ITableSessionPool.class);
    ITableSession session = mock(ITableSession.class);
    when(pool.getSession()).thenReturn(session);
    set(ImportDataTable.class, "sessionPool", pool);
    doAnswer(
            invocation -> {
              Tablet tablet = invocation.getArgument(0);
              assertEquals(2, tablet.getSchemas().size());
              assertEquals("value(double)", tablet.getSchemas().get(1).getMeasurementName());
              assertEquals(1, tablet.getRowSize());
              assertEquals(1L, tablet.getTimestamps()[0]);
              assertEquals(1.5, ((double[]) tablet.getValues()[1])[0], 0);
              assertEquals(0, ((double[]) tablet.getValues()[0])[0], 0);
              return null;
            })
        .when(session)
        .insert(any(Tablet.class));
    Path csv = directory.resolve("parentheses.csv");
    Files.writeString(csv, "TIME,VALUE(DOUBLE)\n1,1.5\n");
    try (CSVParser records = readCsvFile(csv.toString())) {
      new ImportDataTable()
          .writeData(
              records.getHeaderNames(),
              records.getRecords(),
              directory.resolve("parentheses.failed").toString());
    }
    verify(session).insert(any(Tablet.class));
    assertFalse(Files.exists(directory.resolve("parentheses.failed")));
  }

  @Test
  public void testTypedTableCsvRejectsAmbiguousTwoColumnRoundTrip() throws Exception {
    assertTableCsvTwoColumnRoundTrip(true);
  }

  @Test
  public void testUntypedTableCsvPreservesTwoColumnRoundTrip() throws Exception {
    assertTableCsvTwoColumnRoundTrip(false);
  }

  private void assertTableCsvTwoColumnRoundTrip(boolean typed) throws Exception {
    Map<String, TSDataType> types = new LinkedHashMap<>();
    types.put("value", TSDataType.DOUBLE);
    types.put("value(double)", TSDataType.DOUBLE);
    Map<String, ColumnCategory> categories = new LinkedHashMap<>();
    categories.put("value", ColumnCategory.FIELD);
    categories.put("value(double)", ColumnCategory.FIELD);
    set(ImportDataTable.class, "dataTypes", types);
    set(ImportDataTable.class, "columnCategory", categories);
    ITableSessionPool pool = mock(ITableSessionPool.class);
    ITableSession session = mockTable("t");
    when(pool.getSession()).thenReturn(session);
    set(ImportDataTable.class, "sessionPool", pool);
    doAnswer(
            invocation -> {
              Tablet tablet = invocation.getArgument(0);
              assertEquals(2, tablet.getSchemas().size());
              assertEquals("value", tablet.getSchemas().get(0).getMeasurementName());
              assertEquals("value(double)", tablet.getSchemas().get(1).getMeasurementName());
              assertEquals(1, tablet.getRowSize());
              assertEquals(1L, tablet.getTimestamps()[0]);
              assertEquals(1.5, ((double[]) tablet.getValues()[0])[0], 0);
              assertEquals(2.5, ((double[]) tablet.getValues()[1])[0], 0);
              return null;
            })
        .when(session)
        .insert(any(Tablet.class));
    for (int i = 0; i < 2; i++) {
      List<String> columns =
          i == 0
              ? List.of("time", "value", "value(double)")
              : List.of("time", "value(double)", "value");
      Object[] row = i == 0 ? new Object[] {1L, 1.5, 2.5} : new Object[] {1L, 2.5, 1.5};
      SessionDataSet result = dataSet(columns, List.of("TIMESTAMP", "DOUBLE", "DOUBLE"), row);
      when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(result);
      exportTable(session, "select * from t", i, "-ft", "csv", "-dt", String.valueOf(typed));
      long failedFiles = loadFileFailedNum.sum();
      long successfulFiles = loadFileSuccessfulNum.sum();
      ImportDataScanTool.putToQueue(directory.resolve("dump" + i + "_0.csv").toString());
      assertEquals(!typed, AbstractImportData.init(new ImportDataTable()));
      assertEquals(failedFiles + (typed ? 1 : 0), loadFileFailedNum.sum());
      assertEquals(successfulFiles + (typed ? 0 : 1), loadFileSuccessfulNum.sum());
      assertNull(ImportDataScanTool.pollFromQueue());
    }
    verify(session, times(typed ? 0 : 2)).insert(any(Tablet.class));
  }

  private void assertTableCsvRoundTrip(String column) throws Exception {
    set(ImportDataTable.class, "dataTypes", Map.of(column, TSDataType.DOUBLE));
    set(ImportDataTable.class, "columnCategory", Map.of(column, ColumnCategory.FIELD));
    ITableSessionPool pool = mock(ITableSessionPool.class);
    ITableSession session = mockTable("t");
    when(pool.getSession()).thenReturn(session);
    set(ImportDataTable.class, "sessionPool", pool);
    doAnswer(
            invocation -> {
              Tablet tablet = invocation.getArgument(0);
              assertEquals(1, tablet.getSchemas().size());
              assertEquals(column, tablet.getSchemas().get(0).getMeasurementName());
              assertEquals(1, tablet.getRowSize());
              assertEquals(1L, tablet.getTimestamps()[0]);
              assertEquals(1.5, ((double[]) tablet.getValues()[0])[0], 0);
              return null;
            })
        .when(session)
        .insert(any(Tablet.class));
    for (boolean typed : List.of(false, true)) {
      SessionDataSet result =
          dataSet(List.of("time", column), List.of("TIMESTAMP", "DOUBLE"), new Object[] {1L, 1.5});
      when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(result);
      int index = typed ? 1 : 0;
      exportTable(session, "select * from t", index, "-ft", "csv", "-dt", String.valueOf(typed));
      String csv = "dump" + index + "_0.csv";
      assertEquals("time," + column + (typed ? "(DOUBLE)" : ""), firstLine(csv));
      try (CSVParser records = readCsvFile(directory.resolve(csv).toString())) {
        new ImportDataTable()
            .writeData(
                records.getHeaderNames(),
                records.getRecords(),
                directory.resolve("roundtrip.failed").toString());
      }
    }
    verify(session, times(2)).insert(any(Tablet.class));
    assertFalse(Files.exists(directory.resolve("roundtrip.failed")));
  }

  @Test
  public void testSuccessArchivalPreservesFilesAndSidecars() throws Exception {
    for (ImportTsFileOperation operation :
        List.of(
            ImportTsFileOperation.CP, ImportTsFileOperation.HARDLINK, ImportTsFileOperation.MV)) {
      Path input = Files.createDirectories(directory.resolve(operation.name()).resolve("input"));
      Path nested = Files.createDirectories(input.resolve("nested"));
      Path source = nested.resolve("one.tsfile");
      Path archive =
          Files.createDirectories(directory.resolve(operation.name()).resolve("success"));
      for (String suffix : List.of("", ".resource", ".mods")) {
        Files.writeString(Path.of(source + suffix), "content" + suffix);
      }
      successDir = archive.toString();
      successOperation = operation;
      ImportTsFileScanTool.setSourceFullPath(input.toString());
      new ImportDataTree().processingFile(source.toString(), true);
      assertTrue(Files.isDirectory(archive));
      for (String suffix : List.of("", ".resource", ".mods")) {
        assertEquals(
            "content" + suffix, Files.readString(archive.resolve("nested/one.tsfile" + suffix)));
        assertEquals(operation != ImportTsFileOperation.MV, Files.exists(Path.of(source + suffix)));
      }
      Path other = input.resolve("other.tsfile");
      Files.writeString(other, "other");
      new ImportDataTree().processingFile(other.toString(), true);
      assertEquals("other", Files.readString(archive.resolve("other.tsfile")));
      assertEquals("content", Files.readString(archive.resolve("nested/one.tsfile")));
    }
  }

  @Test
  public void testArchivalPreservesCollidingFlattenedNames() throws Exception {
    for (boolean isSuccess : List.of(false, true)) {
      for (ImportTsFileOperation operation :
          List.of(
              ImportTsFileOperation.CP, ImportTsFileOperation.HARDLINK, ImportTsFileOperation.MV)) {
        Path root = Files.createDirectories(directory.resolve(isSuccess + "-" + operation));
        Path input = Files.createDirectories(root.resolve("input"));
        Path archive = root.resolve("archive");
        Files.createDirectories(input.resolve("a"));
        successDir = failDir = archive.toString();
        successOperation = failOperation = operation;
        ImportTsFileScanTool.setSourceFullPath(input.toString());
        for (String name : List.of("a/b.tsfile", "a_b.tsfile")) {
          Path source = input.resolve(name);
          for (String suffix : List.of("", ".resource", ".mods")) {
            Files.writeString(Path.of(source + suffix), name + suffix);
          }
          new ImportDataTree().processingFile(source.toString(), isSuccess);
        }
        for (String name : List.of("a/b.tsfile", "a_b.tsfile")) {
          for (String suffix : List.of("", ".resource", ".mods")) {
            assertEquals(name + suffix, Files.readString(archive.resolve(name + suffix)));
            assertEquals(
                operation != ImportTsFileOperation.MV, Files.exists(input.resolve(name + suffix)));
          }
        }
      }
    }
  }

  @Test
  public void testExistingArchiveFilesAreNotOverwritten() throws Exception {
    for (ImportTsFileOperation operation :
        List.of(
            ImportTsFileOperation.CP, ImportTsFileOperation.HARDLINK, ImportTsFileOperation.MV)) {
      for (String conflict : List.of("", ".resource", ".mods")) {
        Path root = Files.createDirectories(directory.resolve(operation + "-" + conflict));
        Path input = Files.createDirectories(root.resolve("input"));
        Path archive = Files.createDirectories(root.resolve("archive"));
        Path source = input.resolve("one.tsfile");
        for (String suffix : List.of("", ".resource", ".mods")) {
          Files.writeString(Path.of(source + suffix), "source" + suffix);
        }
        Path occupied = archive.resolve("one.tsfile" + conflict);
        Files.writeString(occupied, "existing");
        successDir = archive.toString();
        successOperation = operation;
        ImportTsFileScanTool.setSourceFullPath(input.toString());
        try {
          new ImportDataTree().processingFile(source.toString(), true);
          fail("Expected existing archive file to be rejected");
        } catch (FileAlreadyExistsException expected) {
          assertEquals("existing", Files.readString(occupied));
          for (String suffix : List.of("", ".resource", ".mods")) {
            assertEquals("source" + suffix, Files.readString(Path.of(source + suffix)));
            if (!suffix.equals(conflict)) {
              assertFalse(Files.exists(archive.resolve("one.tsfile" + suffix)));
            }
          }
        }
      }
    }
  }

  @Test
  public void testArchivalErrorsArePropagated() throws Exception {
    Path source = directory.resolve("one.tsfile");
    Files.writeString(source, "source");
    Path archive = Files.createDirectories(directory.resolve("success"));
    Files.createDirectories(archive.resolve("one.tsfile"));
    Files.writeString(archive.resolve("one.tsfile/child"), "occupied");
    successDir = archive.toString();
    successOperation = ImportTsFileOperation.CP;
    ImportTsFileScanTool.setSourceFullPath(directory.toString());
    try {
      new ImportDataTree().processingFile(source.toString(), true);
      fail("Expected archival error to propagate");
    } catch (IOException expected) {
      assertEquals("source", Files.readString(source));
    }
  }

  @Test
  public void testImportTsFileArchivalPreservesRelativePathsAndSidecars() throws Exception {
    for (boolean isSuccess : List.of(false, true)) {
      for (ImportTsFileOperation operation :
          List.of(
              ImportTsFileOperation.CP, ImportTsFileOperation.HARDLINK, ImportTsFileOperation.MV)) {
        Path root = Files.createDirectories(directory.resolve("tsfile-" + isSuccess + operation));
        Path input = Files.createDirectories(root.resolve("input"));
        Files.createDirectories(input.resolve("a"));
        Path archive = Files.createDirectories(root.resolve("archive"));
        ImportTsFileBase.setSuccessAndFailDirAndOperation(
            archive.resolve("success").toString(),
            operation,
            archive.resolve("fail").toString(),
            operation);
        ImportTsFileScanTool.setSourceFullPath(input.toString());

        for (String name : List.of("a/b.tsfile", "a_b.tsfile")) {
          Path source = input.resolve(name);
          for (String suffix : List.of("", ".resource", ".mods")) {
            Files.writeString(Path.of(source + suffix), name + suffix);
          }
          ImportTsFileBase.processingFile(source.toString(), isSuccess);
        }

        Path destination = archive.resolve(isSuccess ? "success" : "fail");
        for (String name : List.of("a/b.tsfile", "a_b.tsfile")) {
          for (String suffix : List.of("", ".resource", ".mods")) {
            assertEquals(name + suffix, Files.readString(destination.resolve(name + suffix)));
            assertEquals(
                operation != ImportTsFileOperation.MV, Files.exists(input.resolve(name + suffix)));
          }
        }
      }
    }
  }

  @Test
  public void testImportTsFileArchiveConflictsKeepSourceFiles() throws Exception {
    for (boolean isSuccess : List.of(false, true)) {
      for (String conflict : List.of("", ".resource", ".mods")) {
        Path root = Files.createDirectories(directory.resolve("conflict-" + isSuccess + conflict));
        Path input = Files.createDirectories(root.resolve("input"));
        Path archive = Files.createDirectories(root.resolve("archive"));
        Path source = input.resolve("one.tsfile");
        for (String suffix : List.of("", ".resource", ".mods")) {
          Files.writeString(Path.of(source + suffix), "source" + suffix);
        }
        Path occupied = archive.resolve("one.tsfile" + conflict);
        Files.writeString(occupied, "existing");
        ImportTsFileScanTool.setSourceFullPath(input.toString());
        ImportTsFileBase.setSuccessAndFailDirAndOperation(
            archive.toString(),
            ImportTsFileOperation.MV,
            archive.toString(),
            ImportTsFileOperation.MV);
        try {
          ImportTsFileBase.processingFile(source.toString(), isSuccess);
          fail("Expected existing archive file to be rejected");
        } catch (FileAlreadyExistsException expected) {
          assertEquals("existing", Files.readString(occupied));
          for (String suffix : List.of("", ".resource", ".mods")) {
            assertEquals("source" + suffix, Files.readString(Path.of(source + suffix)));
            if (!suffix.equals(conflict)) {
              assertFalse(Files.exists(archive.resolve("one.tsfile" + suffix)));
            }
          }
        }
      }
    }
  }

  @Test
  public void testSingleFileRelativeSourceArchival() throws Exception {
    Path workingDirectory = Path.of("").toAbsolutePath();
    Path source = Files.createTempFile(workingDirectory, "one", ".tsfile");
    try {
      Files.writeString(source, "source");
      Path archive = Files.createDirectories(directory.resolve("success"));
      successDir = archive.toString();
      successOperation = ImportTsFileOperation.CP;
      ImportTsFileScanTool.setSourceFullPath(source.getFileName().toString());
      new ImportDataTree().processingFile(source.toString(), true);
      assertEquals("source", Files.readString(archive.resolve(source.getFileName())));
    } finally {
      Files.deleteIfExists(source);
    }
  }

  private String read(String name) throws IOException {
    return Files.readString(directory.resolve(name));
  }

  private String firstLine(String name) throws IOException {
    return Files.readAllLines(directory.resolve(name)).get(0);
  }

  private static void set(Class<?> owner, String name, Object value) throws Exception {
    Field field = owner.getDeclaredField(name);
    field.setAccessible(true);
    field.set(null, value);
  }

  private static Object invoke(
      Class<?> owner, String name, Object target, Class<?>[] parameterTypes, Object... arguments)
      throws Exception {
    Method method = owner.getDeclaredMethod(name, parameterTypes);
    method.setAccessible(true);
    try {
      return method.invoke(target, arguments);
    } catch (InvocationTargetException e) {
      throw (Exception) e.getCause();
    }
  }

  /** Exports one query to dump<index> files, as ExportData.run does for one -q statement. */
  private void exportTree(Session session, String sql, int index, String... options)
      throws Exception {
    ExportConfig config = exportConfig(false, options);
    try (ExportProgressReporter progress = progress();
        ExportDataTree exporter = new ExportDataTree(config, progress, session)) {
      exporter.export(
          new ExportTask(sql, config.targetDirectory.resolve(config.prefix + index), null, null));
    }
  }

  /**
   * Plans and exports like ExportData.run: the given query is written to dump<index> files, a null
   * query exports the -q option or, without one, the tables of the database.
   */
  private void exportTable(ITableSession session, String sql, int index, String... options)
      throws Exception {
    ExportConfig config = exportConfig(true, options);
    try (ExportProgressReporter progress = progress();
        ExportDataTable exporter = new ExportDataTable(config, progress, session)) {
      List<ExportTask> tasks;
      if (sql != null) {
        tasks =
            exporter.planQueries(
                List.of(
                    new ExportTask(
                        sql, config.targetDirectory.resolve(config.prefix + index), null, null)));
      } else if (config.query != null) {
        tasks = exporter.planQueries(ExportData.queryTasks(config, config.query));
      } else {
        tasks = exporter.planTables(exporter.timeRange());
      }
      for (ExportTask task : tasks) {
        exporter.export(task);
      }
    }
  }

  private ExportConfig exportConfig(boolean table, String... options) throws Exception {
    List<String> args = new ArrayList<>(List.of("-t", directory.toString(), "-tf", "timestamp"));
    if (table) {
      args.addAll(List.of("-sql_dialect", "table", "-db", "db"));
    }
    args.addAll(Arrays.asList(options));
    return ExportConfig.parse(args.toArray(new String[0]), ZonedDateTime.now());
  }

  private static ExportProgressReporter progress() {
    return new ExportProgressReporter(
        new StringWriter(),
        "tree",
        System.nanoTime(),
        new PrintStream(OutputStream.nullOutputStream()));
  }

  private static Session mockTree(SessionDataSet dataSet) throws Exception {
    Session session = mock(Session.class);
    when(session.executeQueryStatement(anyString(), anyLong())).thenReturn(dataSet);
    return session;
  }

  /** A table session whose SHOW TABLES lists the given tables. */
  private static ITableSession mockTable(String... names) throws Exception {
    ITableSession session = mock(ITableSession.class);
    String[][] rows = new String[names.length][];
    for (int i = 0; i < names.length; i++) {
      rows[i] = new String[] {names[i]};
    }
    when(session.executeQueryStatement(eq("show tables"), anyLong()))
        .thenAnswer(invocation -> rows(rows));
    return session;
  }

  /** Answers DESCRIBE for a table: its time column, then name, type and category per column. */
  private static void describe(ITableSession session, String table, String... columns)
      throws Exception {
    List<String[]> rows = new ArrayList<>();
    rows.add(new String[] {"time", "TIMESTAMP", "TIME"});
    for (int i = 0; i < columns.length; i += 3) {
      rows.add(Arrays.copyOfRange(columns, i, i + 3));
    }
    when(session.executeQueryStatement(
            eq("DESCRIBE \"" + table.replace("\"", "\"\"") + "\""), anyLong()))
        .thenAnswer(invocation -> rows(rows.toArray(new String[0][])));
  }

  /** A metadata result whose rows hold the given strings. */
  private static SessionDataSet rows(String[]... rows) throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    AtomicInteger row = new AtomicInteger();
    when(dataSet.hasNext()).thenAnswer(invocation -> row.get() < rows.length);
    when(dataSet.next())
        .thenAnswer(
            invocation -> {
              RowRecord record = new RowRecord(0);
              for (String value : rows[row.getAndIncrement()]) {
                record.addField(new Binary(value, StandardCharsets.UTF_8), TSDataType.TEXT);
              }
              return record;
            });
    return dataSet;
  }

  private static SessionDataSet dataSet(List<String> names, List<String> types, Object[]... rows)
      throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    AtomicInteger row = new AtomicInteger(-1);
    when(dataSet.getColumnNames()).thenAnswer(invocation -> new ArrayList<>(names));
    when(dataSet.getColumnTypes()).thenReturn(types);
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList()).thenReturn(types);
    when(iterator.next()).thenAnswer(invocation -> row.incrementAndGet() < rows.length);
    when(iterator.getString(anyInt()))
        .thenAnswer(
            invocation -> {
              Object value = rows[row.get()][(Integer) invocation.getArgument(0) - 1];
              return value == null ? null : value.toString();
            });
    when(iterator.getLong(anyInt()))
        .thenAnswer(
            invocation ->
                ((Number) rows[row.get()][(Integer) invocation.getArgument(0) - 1]).longValue());
    when(iterator.isNull(anyInt()))
        .thenAnswer(invocation -> rows[row.get()][(Integer) invocation.getArgument(0) - 1] == null);
    return dataSet;
  }
}
