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

package org.apache.iotdb.pipe.it.tablemodel;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.RpcUtils;

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

  public static void createDataBaseAndTable(BaseEnv baseEnv, String table, String database) {
    try (Connection connection = baseEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database if not exists " + database);
      statement.execute("use " + database);
      statement.execute(
          "CREATE TABLE "
              + table
              + "(s0 string id, s1 int64 measurement, s2 float measurement, s3 string measurement, s4 timestamp  measurement, s5 int32  measurement, s6 double  measurement, s7 date  measurement, s8 text  measurement )");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  public static void createDataBase(BaseEnv baseEnv, String database) {
    try (Connection connection = baseEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database if not exists " + database);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  public static boolean insertData(
      String dataBaseName, String tableName, int start, int end, BaseEnv baseEnv) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (s0, s3, s2, s1, s4, s5, s6, s7, s8, time) values ('t%s','%s', %s.0, %s, %s, %d, %d.0, '%s', '%s', %s)",
              tableName, i, i, i, i, i, i, i, getDateStr(i), i, i));
    }
    list.add("flush");
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list)) {
      return false;
    }
    return true;
  }

  public static boolean insertData(
      String dataBaseName,
      String tableName,
      int start,
      int end,
      BaseEnv baseEnv,
      boolean allowNullValue) {
    List<String> list = new ArrayList<>(end - start + 1);
    Object[] values = new Object[9];
    Random random = new Random();
    // s0 string, s1 int64, s2 float, s3 string, s4 timestamp, s5 int32, s6 double, s7 date, s8 text
    for (int i = start; i < end; ++i) {
      Arrays.fill(values, i);
      values[0] = String.format("'t%s'", i);
      values[2] = String.format("%s.0", i);
      values[3] = String.format("%s", i);
      values[6] = String.format("%s.0", i);
      values[7] = String.format("'%s'", getDateStr(i));
      values[8] = String.format("'%s'", i);
      if (allowNullValue) {
        values[random.nextInt(9)] = "null";
      }
      list.add(
          String.format(
              "insert into %s (s0, s1, s2, s3, s4, s5, s6, s7, s8, time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
              tableName, values[0], values[1], values[2], values[3], values[4], values[5],
              values[6], values[7], values[8], i));
    }
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list)) {
      return false;
    }
    return true;
  }

  public static boolean insertDataNotThrowError(
      String dataBaseName, String tableName, int start, int end, BaseEnv baseEnv) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (s0, s3, s2, s1, s4, s5, s6, s7, s8, time) values ('t%s','%s', %s.0, %s, %s, %d, %d.0, '%s', '%s', %s)",
              tableName, i, i, i, i, i, i, i, getDateStr(i), i, i));
    }
    return TestUtils.tryExecuteNonQueriesWithRetry(
        dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list);
  }

  public static boolean insertData(
      String dataBaseName,
      String tableName,
      int start,
      int end,
      BaseEnv baseEnv,
      DataNodeWrapper wrapper) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (s0, s3, s2, s1, s4, s5, s6, s7, s8, time) values ('t%s','%s', %s.0, %s, %s, %d, %d.0, '%s', '%s', %s)",
              tableName, i, i, i, i, i, i, i, getDateStr(i), i, i));
    }
    list.add("flush");
    if (!TestUtils.tryExecuteNonQueriesOnSpecifiedDataNodeWithRetry(
        baseEnv, wrapper, list, dataBaseName, BaseEnv.TABLE_SQL_DIALECT)) {
      return false;
    }

    return true;
  }

  public static boolean insertDataByTablet(
      String dataBaseName,
      String tableName,
      int start,
      int end,
      BaseEnv baseEnv,
      boolean allowNullValue) {
    final Tablet tablet = generateTablet(tableName, start, end, allowNullValue);
    ITableSessionPool tableSessionPool = baseEnv.getTableSessionPool(1);
    try (final ITableSession session = tableSessionPool.getSession()) {
      session.executeNonQueryStatement("use " + dataBaseName);
      session.insert(tablet);
      session.executeNonQueryStatement("flush");
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public static void deleteData(
      String dataBaseName, String tableName, int start, int end, BaseEnv baseEnv) {
    List<String> list = new ArrayList<>(end - start + 1);
    list.add(
        String.format("delete from %s where time >= %s and time <= %s", tableName, start, end));
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list)) {
      fail();
    }
  }

  public static Set<String> generateExpectedResults(int start, int end) {
    Set<String> expectedResSet = new HashSet<>();
    for (int i = start; i < end; ++i) {
      final String time = RpcUtils.formatDatetime("default", "ms", i, ZoneOffset.UTC);
      expectedResSet.add(
          String.format(
              "t%d,%d,%d.0,%d,%s,%d,%d.0,%s,%s,%s,",
              i, i, i, i, time, i, i, getDateStr(i), i, time));
    }
    return expectedResSet;
  }

  public static Set<String> generateExpectedResults(Tablet tablet) {
    Set<String> expectedResSet = new HashSet<>();
    List<IMeasurementSchema> schemas = tablet.getSchemas();
    for (int i = 0; i < tablet.getRowSize(); i++) {
      StringBuilder stringBuffer = new StringBuilder();
      for (int j = 0; j < tablet.getSchemas().size(); j++) {
        BitMap bitMap = tablet.bitMaps[j];
        if (bitMap.isMarked(i)) {
          stringBuffer.append("null,");
          continue;
        }
        switch (schemas.get(j).getType()) {
          case TIMESTAMP:
            final String time =
                RpcUtils.formatDatetime(
                    "default", "ms", ((long[]) tablet.values[j])[i], ZoneOffset.UTC);
            stringBuffer.append(time);
            stringBuffer.append(",");
            break;
          case DATE:
            stringBuffer.append(((LocalDate[]) tablet.values[j])[i].toString());
            stringBuffer.append(",");
            break;
          case BLOB:
            stringBuffer.append(
                BytesUtils.parseBlobByteArrayToString(
                    ((Binary[]) tablet.values[j])[i].getValues()));
            stringBuffer.append(",");
            break;
          case TEXT:
          case STRING:
            stringBuffer.append(
                new String(((Binary[]) tablet.values[j])[i].getValues(), StandardCharsets.UTF_8));
            stringBuffer.append(",");
            break;
          case DOUBLE:
            stringBuffer.append(((double[]) tablet.values[j])[i]);
            stringBuffer.append(",");
            break;
          case FLOAT:
            stringBuffer.append(((float[]) tablet.values[j])[i]);
            stringBuffer.append(",");
          case INT32:
            stringBuffer.append(((int[]) tablet.values[j])[i]);
            stringBuffer.append(",");
          case INT64:
            stringBuffer.append(((long[]) tablet.values[j])[i]);
            stringBuffer.append(",");
        }
      }
      String time = RpcUtils.formatDatetime("default", "ms", tablet.timestamps[i], ZoneOffset.UTC);
      stringBuffer.append(time);
      stringBuffer.append(",");
      expectedResSet.add(stringBuffer.toString());
    }

    return expectedResSet;
  }

  public static String generateHeaderResults() {
    return "s0,s3,s2,s1,s4,s5,s6,s7,s8,time,";
  }

  public static String getQuerySql(String table) {
    return "select s0,s3,s2,s1,s4,s5,s6,s7,s8,time from " + table;
  }

  public static String getQueryCountSql(String table) {
    return "select count(*) from " + table;
  }

  public static void assertData(
      String database, String table, int start, int end, BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        TableModelUtils.getQuerySql(table),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(start, end),
        database);
  }

  public static void assertData(
      String database,
      String table,
      int start,
      int end,
      BaseEnv baseEnv,
      Consumer<String> handleFailure) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        TableModelUtils.getQuerySql(table),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(start, end),
        database,
        handleFailure);
  }

  public static void assertData(String database, String table, Tablet tablet, BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        TableModelUtils.getQuerySql(table),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(tablet),
        database);
  }

  public static boolean hasDataBase(String database, BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(baseEnv, "", "", Collections.emptySet(), database);
    return true;
  }

  public static void assertCountData(String database, String table, int count, BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv, getQueryCountSql(table), "_col0,", Collections.singleton(count + ","), database);
  }

  public static void assertCountData(
      String database, String table, int count, BaseEnv baseEnv, Consumer<String> handleFailure) {
    TestUtils.executeNonQueryWithRetry(baseEnv, "flush");
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        getQueryCountSql(table),
        "_col0,",
        Collections.singleton(count + ","),
        database,
        handleFailure);
  }

  public static String getDateStr(int value) {
    Date date = new Date(value);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    try {
      return dateFormat.format(date);
    } catch (Exception e) {
      return "1970-01-01";
    }
  }

  public static LocalDate getDate(int value) {
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
      String tableName, int start, int end, boolean allowNullValue) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s0", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s3", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s4", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s5", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s6", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s7", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s8", TSDataType.TEXT));

    final List<Tablet.ColumnCategory> columnTypes =
        Arrays.asList(
            Tablet.ColumnCategory.ID,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT);
    Tablet tablet =
        new Tablet(
            tableName,
            IMeasurementSchema.getMeasurementNameList(schemaList),
            IMeasurementSchema.getDataTypeList(schemaList),
            columnTypes,
            end - start);
    tablet.initBitMaps();
    Random random = new Random();

    // s2 float, s3 string, s4 timestamp, s5 int32, s6 double, s7 date, s8 text
    for (long row = 0; row < end - start; row++) {
      int randomNumber = allowNullValue ? random.nextInt(9) : 9;
      long value = start + row;
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, value);
      tablet.addValue(
          "s0", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
      tablet.addValue("s1", rowIndex, value);
      tablet.addValue("s2", rowIndex, (value * 1.0f));
      tablet.addValue(
          "s3", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
      tablet.addValue("s4", rowIndex, value);
      tablet.addValue("s5", rowIndex, (int) value);
      tablet.addValue("s6", rowIndex, value * 0.1);
      tablet.addValue("s7", rowIndex, getDate((int) value));
      tablet.addValue(
          "s8", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
      if (randomNumber < 9) {
        tablet.addValue("s" + randomNumber, rowIndex, null);
      }
    }

    return tablet;
  }
}
