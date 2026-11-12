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

package org.apache.iotdb.pipe.it.dual.tablemodel;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

/**
 * This class is specifically designed for table model testing and is used for data insertion.
 * Please note that the Table pattern defined by this class is fixed, and it is recommended to use
 * all methods together to ensure data consistency and integrity.
 *
 * <p>This class provides a structured approach to inserting data, suitable for scenarios where
 * batch data insertion is required. Due to the fixed pattern, users should ensure that the data
 * format matches the pattern defined by the class.
 */
public class TableModelUtils {

  // These helpers prepare data for replication assertions, rather than testing individual inserts.
  // Keep every row and the caller's flush boundaries while reducing write RPCs to about one tenth.
  private static final int ROWS_PER_INSERT = 10;

  private static final class SqlInsertBatch {
    private final String prefix;
    private final int rowsPerInsert;
    private final List<String> statements = new ArrayList<>();
    private final List<String> values;

    private SqlInsertBatch(final String tableName) {
      this(tableName, ROWS_PER_INSERT);
    }

    private SqlInsertBatch(final String tableName, final int rowsPerInsert) {
      this.rowsPerInsert = rowsPerInsert;
      values = new ArrayList<>(rowsPerInsert);
      prefix =
          "insert into "
              + tableName
              + " (s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, time) values ";
    }

    private void addValues(final String row) {
      values.add(row);
      if (values.size() == rowsPerInsert) {
        appendStatement();
      }
    }

    private void appendStatement() {
      if (!values.isEmpty()) {
        statements.add(prefix + String.join(",", values));
        values.clear();
      }
    }

    private List<String> finish() {
      // Retain the final partial batch, including ranges smaller than the requested batch size.
      appendStatement();
      return statements;
    }
  }

  public static void createDataBaseAndTable(
      final BaseEnv baseEnv, final String table, final String database) {
    try (Connection connection = baseEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database if not exists " + database);
      statement.execute("use " + database);
      statement.execute(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s(s0 string tag, s1 string tag, s2 string tag, s3 string tag,s4 int64 field, s5 float field, s6 string field, s7 timestamp  field, s8 int32  field, s9 double  field, s10 date  field, s11 text  field )",
              table));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  public static void createDatabase(final BaseEnv baseEnv, final String database) {
    createDatabase(baseEnv, database, Long.MAX_VALUE);
  }

  public static void createDatabase(final BaseEnv baseEnv, final String database, final long ttl) {
    try (final Connection connection = baseEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create database if not exists "
              + database
              + (ttl < Long.MAX_VALUE ? " with (ttl=" + ttl + ")" : ""));
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  public static void insertData(
      final String dataBaseName,
      final String tableName,
      final int startInclusive,
      final int endExclusive,
      final BaseEnv baseEnv) {
    final SqlInsertBatch batch = new SqlInsertBatch(tableName);
    for (int i = startInclusive; i < endExclusive; ++i) {
      batch.addValues(
          String.format(
              "('t%s','t%s','t%s','t%s','%s', %s.0, %s, %s, %d, %d.0, '%s', '%s', %s)",
              i, i, i, i, i, i, i, i, i, i, getDateStr(i), i, i));
    }
    final List<String> list = batch.finish();
    list.add("flush");
    TestUtils.executeNonQueries(dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list, null);
  }

  public static void insertData(
      final String dataBaseName,
      final String tableName,
      final int deviceStartIndex,
      final int deviceEndIndex,
      final int startInclusive,
      final int endExclusive,
      final BaseEnv baseEnv) {
    TestUtils.executeNonQueries(
        dataBaseName,
        BaseEnv.TABLE_SQL_DIALECT,
        baseEnv,
        generateInsertStatements(
            tableName,
            deviceStartIndex,
            deviceEndIndex,
            startInclusive,
            endExclusive,
            ROWS_PER_INSERT),
        null);
  }

  /** Reuses a caller-owned Session for SQL inserts, leaving all flush boundaries to the caller. */
  public static void insertData(
      final String dataBaseName,
      final String tableName,
      final int deviceStartIndex,
      final int deviceEndIndex,
      final int startInclusive,
      final int endExclusive,
      final ITableSession session,
      final int rowsPerInsert)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("use " + dataBaseName);
    for (final String statement :
        generateInsertStatements(
            tableName,
            deviceStartIndex,
            deviceEndIndex,
            startInclusive,
            endExclusive,
            rowsPerInsert)) {
      session.executeNonQueryStatement(statement);
    }
  }

  private static List<String> generateInsertStatements(
      final String tableName,
      final int deviceStartIndex,
      final int deviceEndIndex,
      final int startInclusive,
      final int endExclusive,
      final int rowsPerInsert) {
    final SqlInsertBatch batch = new SqlInsertBatch(tableName, rowsPerInsert);
    for (int deviceIndex = deviceStartIndex; deviceIndex < deviceEndIndex; ++deviceIndex) {
      for (int i = startInclusive; i < endExclusive; ++i) {
        batch.addValues(
            String.format(
                "('t%s','t%s','t%s','t%s','%s', %s.0, %s, %s, %d, %d.0, '%s', '%s', %s)",
                deviceIndex,
                deviceIndex,
                deviceIndex,
                deviceIndex,
                i,
                i,
                i,
                i,
                i,
                i,
                getDateStr(i),
                i,
                i));
      }
    }
    return batch.finish();
  }

  public static void insertData(
      final String dataBaseName,
      final String tableName,
      final int start,
      final int end,
      final BaseEnv baseEnv,
      final boolean allowNullValue) {
    final SqlInsertBatch batch = new SqlInsertBatch(tableName);
    Object[] values = new Object[12];
    Random random = new Random();
    // s0 string, s1 string, s2 string, s3 string, s4 int64, s5 float, s6 string s7 timestamp, s8
    // int32, s9 double, s10 date, s11 text
    for (int i = start; i < end; ++i) {
      Arrays.fill(values, i);
      values[0] = String.format("'t%s'", i);
      values[1] = String.format("'t%s'", i);
      values[2] = String.format("'t%s'", i);
      values[3] = String.format("'t%s'", i);
      values[4] = String.format("%s", i);
      values[5] = String.format("%s.0", i);
      values[6] = String.format("%s", i);
      values[7] = String.format("%s", i);
      values[8] = String.format("%s", i);
      values[9] = String.format("%s.0", i);
      values[10] = String.format("'%s'", getDateStr(i));
      values[11] = String.format("'%s'", i);
      if (allowNullValue) {
        values[random.nextInt(9)] = "null";
      }
      batch.addValues(
          String.format(
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
              values[0],
              values[1],
              values[2],
              values[3],
              values[4],
              values[5],
              values[6],
              values[7],
              values[8],
              values[9],
              values[10],
              values[11],
              i));
    }
    TestUtils.executeNonQueries(
        dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, batch.finish(), null);
  }

  public static boolean insertDataNotThrowError(
      final String dataBaseName,
      final String tableName,
      final int start,
      final int end,
      final BaseEnv baseEnv) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (s0, s1, s2, s3,  s4, s5, s6, s7, s8, s9, s10, s11, time) values ('t%s','t%s','t%s','t%s','%s', %s.0, %s, %s, %d, %d.0, '%s', '%s', %s)",
              tableName, i, i, i, i, i, i, i, i, i, i, getDateStr(i), i, i));
    }
    try {
      TestUtils.executeNonQueries(dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list, null);
      return true;
    } catch (final Throwable e) {
      return false;
    }
  }

  public static boolean insertData(
      final String dataBaseName,
      final String tableName,
      final int start,
      final int end,
      final BaseEnv baseEnv,
      final DataNodeWrapper wrapper) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (s0, s1, s2, s3,  s4, s5, s6, s7, s8, s9, s10, s11, time) values ('t%s','t%s','t%s','t%s','%s', %s.0, %s, %s, %d, %d.0, '%s', '%s', %s)",
              tableName, i, i, i, i, i, i, i, i, i, i, getDateStr(i), i, i));
    }
    list.add("flush");
    return TestUtils.tryExecuteNonQueriesOnSpecifiedDataNodeWithRetry(
        baseEnv, wrapper, list, dataBaseName, BaseEnv.TABLE_SQL_DIALECT);
  }

  public static boolean insertTablet(
      final String dataBaseName,
      final String tableName,
      final int start,
      final int end,
      final BaseEnv baseEnv,
      final boolean allowNullValue) {
    try (final ITableSessionPool tableSessionPool = baseEnv.getTableSessionPool(1);
        final ITableSession session = tableSessionPool.getSession()) {
      insertTablet(dataBaseName, tableName, start, end, session, allowNullValue);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  /** Reuses a caller-owned Session while retaining one flush for each Tablet. */
  public static void insertTablet(
      final String dataBaseName,
      final String tableName,
      final int start,
      final int end,
      final ITableSession session,
      final boolean allowNullValue)
      throws Exception {
    final Tablet tablet = generateTablet(tableName, start, end, allowNullValue, true);
    session.executeNonQueryStatement("use " + dataBaseName);
    session.insert(tablet);
    session.executeNonQueryStatement("flush");
  }

  public static boolean insertTablet(
      final String dataBaseName, final Tablet tablet, final BaseEnv baseEnv) {
    return insertTablet(dataBaseName, tablet, baseEnv, null);
  }

  public enum TabletInsertPhase {
    OPEN_SESSION,
    USE_DATABASE,
    INSERT,
    FLUSH,
    CLOSE_SESSION
  }

  /**
   * Records client-side step durations in nanoseconds without changing the per-Tablet connection or
   * flush boundaries. If a step fails, its duration includes automatic resource cleanup.
   */
  public static boolean insertTablet(
      final String dataBaseName,
      final Tablet tablet,
      final BaseEnv baseEnv,
      final BiConsumer<TabletInsertPhase, Long> recordTiming) {
    long stepStart = System.nanoTime();
    TabletInsertPhase phase = TabletInsertPhase.OPEN_SESSION;
    try (ITableSessionPool tableSessionPool = baseEnv.getTableSessionPool(20);
        final ITableSession session = tableSessionPool.getSession()) {
      stepStart = recordTabletInsertTime(recordTiming, phase, stepStart);
      phase = TabletInsertPhase.USE_DATABASE;
      session.executeNonQueryStatement("use " + dataBaseName);
      stepStart = recordTabletInsertTime(recordTiming, phase, stepStart);
      phase = TabletInsertPhase.INSERT;
      session.insert(tablet);
      stepStart = recordTabletInsertTime(recordTiming, phase, stepStart);
      phase = TabletInsertPhase.FLUSH;
      session.executeNonQueryStatement("flush");
      stepStart = recordTabletInsertTime(recordTiming, phase, stepStart);
      phase = TabletInsertPhase.CLOSE_SESSION;
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    } finally {
      recordTabletInsertTime(recordTiming, phase, stepStart);
    }
  }

  private static long recordTabletInsertTime(
      final BiConsumer<TabletInsertPhase, Long> recordTiming,
      final TabletInsertPhase phase,
      final long start) {
    final long end = System.nanoTime();
    if (recordTiming != null) {
      recordTiming.accept(phase, end - start);
    }
    return end;
  }

  public static void deleteData(
      final String dataBaseName,
      final String tableName,
      final int start,
      final int end,
      final BaseEnv baseEnv) {
    List<String> list = new ArrayList<>(end - start + 1);
    list.add(
        String.format("delete from %s where time >= %s and time <= %s", tableName, start, end));
    TestUtils.executeNonQueries(dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list, null);
  }

  // s0 string, s1 string, s2 string, s3 string, s4 int64, s5 float, s6 string s7 timestamp, s8
  // int32, s9 double, s10 date, s11 text
  public static Set<String> generateExpectedResults(final int start, final int end) {
    Set<String> expectedResSet = new HashSet<>();
    for (int i = start; i < end; ++i) {
      final String time = RpcUtils.formatDatetime("default", "ms", i, ZoneOffset.UTC);
      expectedResSet.add(
          String.format(
              "t%s,t%s,t%s,t%s,%s,%s.0,%s,%s,%d,%d.0,%s,%s,%s,",
              i, i, i, i, i, i, i, time, i, i, getDateStr(i), i, time));
    }
    return expectedResSet;
  }

  public static Set<String> generateExpectedResults(final Tablet tablet) {
    Set<String> expectedResSet = new HashSet<>();
    List<IMeasurementSchema> schemas = tablet.getSchemas();
    for (int i = 0; i < tablet.getRowSize(); i++) {
      StringBuilder stringBuffer = new StringBuilder();
      for (int j = 0; j < tablet.getSchemas().size(); j++) {
        BitMap bitMap = tablet.getBitMaps()[j];
        if (bitMap.isMarked(i)) {
          stringBuffer.append("null,");
          continue;
        }
        switch (schemas.get(j).getType()) {
          case TIMESTAMP:
            final String time =
                RpcUtils.formatDatetime(
                    "default", "ms", (long) tablet.getValue(i, j), ZoneOffset.UTC);
            stringBuffer.append(time);
            stringBuffer.append(",");
            break;
          case DATE:
            stringBuffer.append(tablet.getValue(i, j).toString());
            stringBuffer.append(",");
            break;
          case BLOB:
            stringBuffer.append(
                BytesUtils.parseBlobByteArrayToString(
                    ((Binary) tablet.getValue(i, j)).getValues()));
            stringBuffer.append(",");
            break;
          case TEXT:
          case STRING:
            stringBuffer.append(
                new String(((Binary) tablet.getValue(i, j)).getValues(), StandardCharsets.UTF_8));
            stringBuffer.append(",");
            break;
          case DOUBLE:
            stringBuffer.append((double) tablet.getValue(i, j));
            stringBuffer.append(",");
            break;
          case FLOAT:
            stringBuffer.append((float) tablet.getValue(i, j));
            stringBuffer.append(",");
            break;
          case INT32:
            stringBuffer.append((int) tablet.getValue(i, j));
            stringBuffer.append(",");
            break;
          case INT64:
            stringBuffer.append((long) tablet.getValue(i, j));
            stringBuffer.append(",");
            break;
        }
      }
      String time =
          RpcUtils.formatDatetime("default", "ms", tablet.getTimestamp(i), ZoneOffset.UTC);
      stringBuffer.append(time);
      stringBuffer.append(",");
      expectedResSet.add(stringBuffer.toString());
    }

    return expectedResSet;
  }

  public static String generateHeaderResults() {
    return "s0,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,time,";
  }

  public static String getQuerySql(final String table) {
    return "select s0,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,time from " + table;
  }

  public static String getQueryCountSql(final String table) {
    return "select count(*) from " + table;
  }

  public static void assertData(
      final String database,
      final String table,
      final int start,
      final int end,
      final BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        TableModelUtils.getQuerySql(table),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(start, end),
        database);
  }

  public static void assertData(
      final String database,
      final String table,
      final Set<String> expectedResults,
      final BaseEnv baseEnv,
      final Consumer<String> handleFailure) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        TableModelUtils.getQuerySql(table),
        TableModelUtils.generateHeaderResults(),
        expectedResults,
        database,
        handleFailure);
  }

  public static void assertData(
      final String database,
      final String table,
      final int start,
      final int end,
      final BaseEnv baseEnv,
      final Consumer<String> handleFailure) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        TableModelUtils.getQuerySql(table),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(start, end),
        database,
        handleFailure);
  }

  public static void assertData(
      final String database, final String table, final Tablet tablet, final BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        TableModelUtils.getQuerySql(table),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(tablet),
        database);
  }

  public static boolean hasDataBase(final String database, final BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(baseEnv, "", "", Collections.emptySet(), database);
    return true;
  }

  public static void assertCountDataAlwaysOnEnv(
      final String database, final String table, final int count, final BaseEnv baseEnv) {
    TestUtils.assertDataAlwaysOnEnv(
        baseEnv, getQueryCountSql(table), "_col0,", Collections.singleton(count + ","), database);
  }

  public static void assertCountData(
      final String database, final String table, final int count, final BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv, getQueryCountSql(table), "_col0,", Collections.singleton(count + ","), database);
  }

  public static void assertCountData(
      final String database,
      final String table,
      final int count,
      final BaseEnv baseEnv,
      final Consumer<String> handleFailure) {
    TestUtils.executeNonQueryWithRetry(baseEnv, "flush");
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        getQueryCountSql(table),
        "_col0,",
        Collections.singleton(count + ","),
        database,
        handleFailure);
  }

  public static void assertCountData(
      final String database,
      final String table,
      final int count,
      final BaseEnv baseEnv,
      final DataNodeWrapper wrapper) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        wrapper,
        getQueryCountSql(table),
        "_col0,",
        Collections.singleton(count + ","),
        database);
  }

  public static String getDateStr(final int value) {
    Date date = new Date(value);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    try {
      return dateFormat.format(date);
    } catch (Exception e) {
      return "1970-01-01";
    }
  }

  public static LocalDate getDate(final int value) {
    Date date = new Date(value);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    try {
      return DateUtils.parseIntToLocalDate(
          DateUtils.parseDateExpressionToInt(dateFormat.format(date)));
    } catch (Exception e) {
      return DateUtils.parseIntToLocalDate(DateUtils.parseDateExpressionToInt("1970-01-01"));
    }
  }

  public static Tablet generateTablet(
      final String tableName,
      final int start,
      final int end,
      final boolean allowNullValue,
      final boolean allowNullDeviceColumn) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s0", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s1", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s2", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s3", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s4", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s5", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s6", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s7", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s8", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s9", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s10", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s11", TSDataType.TEXT));

    final List<ColumnCategory> columnTypes =
        Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD);
    Tablet tablet =
        new Tablet(
            tableName,
            IMeasurementSchema.getMeasurementNameList(schemaList),
            IMeasurementSchema.getDataTypeList(schemaList),
            columnTypes,
            end - start);
    tablet.initBitMaps();
    Random random = new Random();
    int nullDeviceIndex = allowNullDeviceColumn ? random.nextInt(4) : 4;

    for (long row = 0; row < end - start; row++) {
      int randomNumber = allowNullValue ? random.nextInt(12) : 12;
      long value = start + row;
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, value);
      tablet.addValue(
          "s0", rowIndex, new Binary(String.format("t%s", value).getBytes(StandardCharsets.UTF_8)));
      tablet.addValue(
          "s1", rowIndex, new Binary(String.format("t%s", value).getBytes(StandardCharsets.UTF_8)));
      tablet.addValue(
          "s2", rowIndex, new Binary(String.format("t%s", value).getBytes(StandardCharsets.UTF_8)));
      tablet.addValue(
          "s3", rowIndex, new Binary(String.format("t%s", value).getBytes(StandardCharsets.UTF_8)));
      tablet.addValue("s4", rowIndex, value);
      tablet.addValue("s5", rowIndex, (value * 1.0f));
      tablet.addValue(
          "s6", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
      tablet.addValue("s7", rowIndex, value);
      tablet.addValue("s8", rowIndex, (int) value);
      tablet.addValue("s9", rowIndex, value * 0.1);
      tablet.addValue("s10", rowIndex, getDate((int) value));
      tablet.addValue(
          "s11", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
      if (randomNumber < 11) {
        tablet.addValue("s" + randomNumber, rowIndex, null);
      }
      if (nullDeviceIndex < 4) {
        tablet.addValue("s" + nullDeviceIndex, rowIndex, null);
      }
      tablet.setRowSize(rowIndex + 1);
    }

    return tablet;
  }

  public static Tablet generateTablet(
      final String tableName,
      final int deviceStartIndex,
      final int deviceEndIndex,
      final int start,
      final int end,
      final boolean allowNullValue,
      final boolean allowNullDeviceColumn) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s0", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s1", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s2", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s3", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s4", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s5", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s6", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s7", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s8", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s9", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s10", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s11", TSDataType.TEXT));

    final List<ColumnCategory> columnTypes =
        Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD);
    Tablet tablet =
        new Tablet(
            tableName,
            IMeasurementSchema.getMeasurementNameList(schemaList),
            IMeasurementSchema.getDataTypeList(schemaList),
            columnTypes,
            (deviceEndIndex - deviceStartIndex) * (end - start));
    tablet.initBitMaps();
    final Random random = new Random();
    int nullDeviceIndex = allowNullDeviceColumn ? random.nextInt(4) : 4;

    for (int deviceIndex = deviceStartIndex; deviceIndex < deviceEndIndex; deviceIndex++) {
      for (long row = start; row < end; row++) {
        int randomNumber = allowNullValue ? random.nextInt(12) : 12;
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, row);
        tablet.addValue(
            "s0",
            rowIndex,
            new Binary(String.format("t%s", deviceIndex).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue(
            "s1",
            rowIndex,
            new Binary(String.format("t%s", deviceIndex).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue(
            "s2",
            rowIndex,
            new Binary(String.format("t%s", deviceIndex).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue(
            "s3",
            rowIndex,
            new Binary(String.format("t%s", deviceIndex).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue("s4", rowIndex, row);
        tablet.addValue("s5", rowIndex, (row * 1.0f));
        tablet.addValue(
            "s6", rowIndex, new Binary(String.valueOf(row).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue("s7", rowIndex, row);
        tablet.addValue("s8", rowIndex, (int) row);
        tablet.addValue("s9", rowIndex, row * 0.1);
        tablet.addValue("s10", rowIndex, getDate((int) row));
        tablet.addValue(
            "s11", rowIndex, new Binary(String.valueOf(row).getBytes(StandardCharsets.UTF_8)));
        if (randomNumber < 12) {
          tablet.addValue("s" + randomNumber, rowIndex, null);
        }
        if (nullDeviceIndex < 4) {
          tablet.addValue("s" + nullDeviceIndex, rowIndex, null);
        }
        tablet.setRowSize(rowIndex + 1);
      }
    }

    return tablet;
  }

  public static Tablet generateTablet(
      final String tableName,
      final int deviceStartIndex,
      final int deviceEndIndex,
      final int deviceDataSize,
      final boolean allowNullValue,
      final boolean allowNullDeviceColumn) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s0", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s1", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s2", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s3", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s4", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s5", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s6", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s7", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s8", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s9", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s10", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s11", TSDataType.TEXT));

    final List<ColumnCategory> columnTypes =
        Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD);
    Tablet tablet =
        new Tablet(
            tableName,
            IMeasurementSchema.getMeasurementNameList(schemaList),
            IMeasurementSchema.getDataTypeList(schemaList),
            columnTypes,
            (deviceEndIndex - deviceStartIndex) * deviceDataSize);
    tablet.initBitMaps();
    final Random random = new Random();
    int nullDeviceIndex = allowNullDeviceColumn ? random.nextInt(4) : 4;

    for (int deviceIndex = deviceStartIndex; deviceIndex < deviceEndIndex; deviceIndex++) {
      // s2 float, s3 string, s4 timestamp, s5 int32, s6 double, s7 date, s8 text
      long value = random.nextInt(1 << 16);
      for (long row = 0; row < deviceDataSize; row++) {
        int randomNumber = allowNullValue ? random.nextInt(12) : 12;
        int rowIndex = tablet.getRowSize();
        value += random.nextInt(100);
        tablet.addTimestamp(rowIndex, value);
        tablet.addValue(
            "s0",
            rowIndex,
            new Binary(String.format("t%s", deviceIndex).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue(
            "s1",
            rowIndex,
            new Binary(String.format("t%s", deviceIndex).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue(
            "s2",
            rowIndex,
            new Binary(String.format("t%s", deviceIndex).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue(
            "s3",
            rowIndex,
            new Binary(String.format("t%s", deviceIndex).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue("s4", rowIndex, value);
        tablet.addValue("s5", rowIndex, (value * 1.0f));
        tablet.addValue(
            "s6", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue("s7", rowIndex, value);
        tablet.addValue("s8", rowIndex, (int) value);
        tablet.addValue("s9", rowIndex, value * 0.1);
        tablet.addValue("s10", rowIndex, getDate((int) value));
        tablet.addValue(
            "s11", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
        if (randomNumber < 12) {
          tablet.addValue("s" + randomNumber, rowIndex, null);
        }
        if (nullDeviceIndex < 4) {
          tablet.addValue("s" + nullDeviceIndex, rowIndex, null);
        }
        tablet.setRowSize(rowIndex + 1);
      }
    }

    return tablet;
  }

  public static Tablet generateTabletDeviceIDAllIsNull(
      final String tableName,
      final int deviceStartIndex,
      final int deviceEndIndex,
      final int deviceDataSize,
      final boolean allowNullValue) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s0", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s1", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s2", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s3", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s4", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s5", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s6", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s7", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s8", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s9", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s10", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s11", TSDataType.TEXT));

    final List<ColumnCategory> columnTypes =
        Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD);
    Tablet tablet =
        new Tablet(
            tableName,
            IMeasurementSchema.getMeasurementNameList(schemaList),
            IMeasurementSchema.getDataTypeList(schemaList),
            columnTypes,
            (deviceEndIndex - deviceStartIndex) * deviceDataSize);
    tablet.initBitMaps();
    final Random random = new Random();

    for (int deviceIndex = deviceStartIndex; deviceIndex < deviceEndIndex; deviceIndex++) {
      // s2 float, s3 string, s4 timestamp, s5 int32, s6 double, s7 date, s8 text
      long value = random.nextInt(1 << 16);
      for (long row = 0; row < deviceDataSize; row++) {
        int randomNumber = allowNullValue ? random.nextInt(12) : 12;
        int rowIndex = tablet.getRowSize();
        value += random.nextInt(100);
        tablet.addTimestamp(rowIndex, value);
        tablet.addValue("s0", rowIndex, null);
        tablet.addValue("s1", rowIndex, null);
        tablet.addValue("s2", rowIndex, null);
        tablet.addValue("s3", rowIndex, null);
        tablet.addValue("s4", rowIndex, value);
        tablet.addValue("s5", rowIndex, (value * 1.0f));
        tablet.addValue(
            "s6", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
        tablet.addValue("s7", rowIndex, value);
        tablet.addValue("s8", rowIndex, (int) value);
        tablet.addValue("s9", rowIndex, value * 0.1);
        tablet.addValue("s10", rowIndex, getDate((int) value));
        tablet.addValue(
            "s11", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
        if (randomNumber < 12) {
          tablet.addValue("s" + randomNumber, rowIndex, null);
        }
        tablet.setRowSize(rowIndex + 1);
      }
    }

    return tablet;
  }

  public static int showPipesCount(final BaseEnv baseEnv, final String sqlDialect) {
    try (final Connection connection = baseEnv.getConnection(sqlDialect);
        final Statement statement = connection.createStatement()) {
      return showPipesCount(statement);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }
    return 0;
  }

  /** Count visible pipes without closing the caller's dialect-specific Statement or connection. */
  public static int showPipesCount(final Statement statement) throws SQLException {
    try (final ResultSet resultSet = statement.executeQuery("show pipes")) {
      int count = 0;
      while (resultSet.next()) {
        if (resultSet.getString("ID").startsWith("__consensus")) {
          continue;
        }
        count++;
      }
      return count;
    }
  }
}
