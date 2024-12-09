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

package org.apache.iotdb.db.it.utils;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.iotdb.itbase.constant.TestConstant.DELTA;
import static org.apache.iotdb.itbase.constant.TestConstant.NULL;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  public static final ZoneId DEFAULT_ZONE_ID = ZoneId.ofOffset("UTC", ZoneOffset.of("Z"));

  public static final String TIME_PRECISION_IN_MS = "ms";

  public static final String TIME_PRECISION_IN_US = "us";

  public static final String TIME_PRECISION_IN_NS = "ns";

  public static String defaultFormatDataTime(long time) {
    return RpcUtils.formatDatetime(
        RpcUtils.DEFAULT_TIME_FORMAT, TIME_PRECISION_IN_MS, time, DEFAULT_ZONE_ID);
  }

  public static void prepareData(String[] sqls) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void prepareData(List<String> sqls) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void prepareTableData(String[] sqls) {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void prepareTableData(List<String> sqls) {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void resultSetEqualTest(String sql, double[][] retArray, String[] columnNames) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        // if result has more than one rows, "Time" is included in columnNames
        assertEquals(
            retArray.length > 1 ? columnNames.length + 1 : columnNames.length,
            resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            String result = resultSet.getString(index);
            ans[i] = result == null ? NULL : Double.parseDouble(result);
          }
          assertArrayEquals(retArray[cnt], ans, DELTA);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void resultSetEqualTest(
      String sql, String expectedHeader, Set<String> expectedRetSet) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        assertResultSetEqual(resultSet, expectedHeader, expectedRetSet);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void resultSetEqualTest(
      String sql,
      String expectedHeader,
      String[] expectedRetArray,
      DateFormat df,
      String userName,
      String password,
      TimeUnit currPrecision) {
    try (Connection connection = EnvFactory.getEnv().getConnection(userName, password);
        Statement statement = connection.createStatement()) {
      connection.setClientInfo("time_zone", "+00:00");
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        assertResultSetEqual(resultSet, expectedHeader, expectedRetArray, df, currPrecision);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void resultSetEqualTest(
      String sql, String expectedHeader, String[] expectedRetArray) {
    resultSetEqualTest(
        sql,
        expectedHeader,
        expectedRetArray,
        null,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        TimeUnit.MILLISECONDS);
  }

  public static void resultSetEqualTest(
      String sql, String[] expectedHeader, String[] expectedRetArray) {
    resultSetEqualTest(sql, expectedHeader, expectedRetArray, null);
  }

  public static void tableResultSetEqualTest(
      String sql, String[] expectedHeader, String[] expectedRetArray, String database) {
    tableResultSetEqualTest(
        sql,
        expectedHeader,
        expectedRetArray,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        database);
  }

  public static void tableResultSetEqualTest(
      String sql,
      String[] expectedHeader,
      String[] expectedRetArray,
      String userName,
      String password,
      String database) {
    try (Connection connection =
        EnvFactory.getEnv().getConnection(userName, password, BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + database);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
          }
          assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

          int cnt = 0;
          while (resultSet.next()) {
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= expectedHeader.length; i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            assertEquals(expectedRetArray[cnt], builder.toString());
            // System.out.println(String.format("\"%s\",", builder.toString()));
            cnt++;
          }
          assertEquals(expectedRetArray.length, cnt);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void tableResultSetFuzzyTest(
      String sql, String[] expectedHeader, int expectedCount, String database) {
    tableResultSetFuzzyTest(
        sql,
        expectedHeader,
        expectedCount,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        database);
  }

  public static void tableResultSetFuzzyTest(
      String sql,
      String[] expectedHeader,
      int expectedCount,
      String userName,
      String password,
      String database) {
    try (Connection connection =
        EnvFactory.getEnv().getConnection(userName, password, BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + database);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
          }
          assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

          int cnt = 0;
          while (resultSet.next()) {
            cnt++;
          }
          assertEquals(expectedCount, cnt);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void tableAssertTestFail(String sql, String errMsg, String databaseName) {
    tableAssertTestFail(
        sql, errMsg, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, databaseName);
  }

  public static void tableAssertTestFail(
      String sql, String errMsg, String userName, String password, String databaseName) {
    try (Connection connection =
            EnvFactory.getEnv().getConnection(userName, password, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + databaseName);
      statement.executeQuery(sql);
      fail("No exception!");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    }
  }

  public static void resultSetEqualTest(
      String sql,
      String[] expectedHeader,
      String[] expectedRetArray,
      String userName,
      String password) {
    resultSetEqualTest(sql, expectedHeader, expectedRetArray, null, userName, password);
  }

  public static void resultSetEqualTest(
      String sql, String[] expectedHeader, String[] expectedRetArray, DateFormat df) {
    StringBuilder header = new StringBuilder();
    for (String s : expectedHeader) {
      header.append(s).append(",");
    }
    resultSetEqualTest(
        sql,
        header.toString(),
        expectedRetArray,
        df,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        TimeUnit.MILLISECONDS);
  }

  public static void resultSetEqualTest(
      String sql,
      String[] expectedHeader,
      String[] expectedRetArray,
      DateFormat df,
      TimeUnit currPrecision) {
    StringBuilder header = new StringBuilder();
    for (String s : expectedHeader) {
      header.append(s).append(",");
    }
    resultSetEqualTest(
        sql,
        header.toString(),
        expectedRetArray,
        df,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        currPrecision);
  }

  public static void resultSetEqualTest(
      String sql,
      String[] expectedHeader,
      String[] expectedRetArray,
      DateFormat df,
      String userName,
      String password) {
    StringBuilder header = new StringBuilder();
    for (String s : expectedHeader) {
      header.append(s).append(",");
    }
    resultSetEqualTest(
        sql, header.toString(), expectedRetArray, df, userName, password, TimeUnit.MILLISECONDS);
  }

  public static void resultSetEqualWithDescOrderTest(
      String sql, String[] expectedHeader, String[] expectedRetArray) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }

        cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(expectedHeader[i - 1])).append(",");
          }
          assertEquals(expectedRetArray[cnt], builder.toString());
          cnt++;
        }
        Assert.assertEquals(expectedRetArray.length, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery(sql + " order by time desc")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }

        cnt = expectedRetArray.length;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(expectedHeader[i - 1])).append(",");
          }
          assertEquals(expectedRetArray[cnt - 1], builder.toString());
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void assertTestFail(String sql, String errMsg) {
    assertTestFail(sql, errMsg, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  public static void assertTestFail(String sql, String errMsg, String userName, String password) {
    assertTestFail(EnvFactory.getEnv(), sql, errMsg, userName, password);
  }

  public static void assertTestFail(
      BaseEnv env, String sql, String errMsg, String userName, String password) {
    try (Connection connection = env.getConnection(userName, password);
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail("No exception!");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    }
  }

  public static void assertNonQueryTestFail(String sql, String errMsg) {
    assertNonQueryTestFail(sql, errMsg, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  public static void assertTableNonQueryTestFail(String sql, String errMsg, String dbName) {
    assertTableNonQueryTestFail(
        sql, errMsg, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, dbName);
  }

  public static void assertNonQueryTestFail(
      String sql, String errMsg, String userName, String password) {
    assertNonQueryTestFail(EnvFactory.getEnv(), sql, errMsg, userName, password);
  }

  public static void assertTableNonQueryTestFail(
      String sql, String errMsg, String userName, String password, String dbName) {
    assertTableNonQueryTestFail(EnvFactory.getEnv(), sql, errMsg, userName, password, dbName);
  }

  public static void assertNonQueryTestFail(
      BaseEnv env, String sql, String errMsg, String userName, String password) {
    try (Connection connection = env.getConnection(userName, password);
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      fail("No exception!");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    }
  }

  public static void assertTableNonQueryTestFail(
      BaseEnv env, String sql, String errMsg, String userName, String password, String db) {
    try (Connection connection = env.getConnection(userName, password, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + "\"" + db + "\"");
      statement.execute(sql);
      fail("No exception!");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    }
  }

  public static void assertResultSetSize(final ResultSet actualResultSet, int size) {
    try {
      while (actualResultSet.next()) {
        --size;
      }
      Assert.assertEquals(0, size);
    } catch (final Exception e) {
      e.printStackTrace();
      Assert.fail(String.valueOf(e));
    }
  }

  public static void assertResultSetEqual(
      ResultSet actualResultSet, String expectedHeader, String[] expectedRetArray) {
    assertResultSetEqual(
        actualResultSet, expectedHeader, expectedRetArray, null, TimeUnit.MILLISECONDS);
  }

  public static void assertResultSetEqual(
      ResultSet actualResultSet, String expectedHeader, Set<String> expectedRetSet) {
    try {
      ResultSetMetaData resultSetMetaData = actualResultSet.getMetaData();
      StringBuilder header = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        header.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      assertEquals(expectedHeader, header.toString());

      Set<String> actualRetSet = new HashSet<>();

      while (actualResultSet.next()) {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          builder.append(actualResultSet.getString(i)).append(",");
        }
        actualRetSet.add(builder.toString());
      }
      assertEquals(expectedRetSet, actualRetSet);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(String.valueOf(e));
    }
  }

  public static void assertResultSetEqual(
      ResultSet actualResultSet,
      String expectedHeader,
      Set<String> expectedRetSet,
      Consumer consumer) {
    try {
      ResultSetMetaData resultSetMetaData = actualResultSet.getMetaData();
      StringBuilder header = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        header.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      assertEquals(expectedHeader, header.toString());

      Set<String> actualRetSet = new HashSet<>();

      while (actualResultSet.next()) {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          builder.append(actualResultSet.getString(i)).append(",");
        }
        actualRetSet.add(builder.toString());
      }
      assertEquals(expectedRetSet, actualRetSet);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(String.valueOf(e));
    }
  }

  public static void assertSingleResultSetEqual(
      ResultSet actualResultSet, Map<String, String> expectedHeaderWithResult) {
    try {
      ResultSetMetaData resultSetMetaData = actualResultSet.getMetaData();
      assertTrue(actualResultSet.next());
      Map<String, String> actualHeaderWithResult = new HashMap<>();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        actualHeaderWithResult.put(
            resultSetMetaData.getColumnName(i), actualResultSet.getString(i));
      }
      String expected = new TreeMap<>(expectedHeaderWithResult).toString();
      String actual = new TreeMap<>(actualHeaderWithResult).toString();
      LOGGER.info("[AssertSingleResultSetEqual] expected {}, actual {}", expected, actual);
      assertEquals(expected, actual);
      assertFalse(actualResultSet.next());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(String.valueOf(e));
    }
  }

  public static void assertResultSetEqual(
      ResultSet actualResultSet,
      String expectedHeader,
      String[] expectedRetArray,
      DateFormat df,
      TimeUnit currPrecision) {
    try {
      ResultSetMetaData resultSetMetaData = actualResultSet.getMetaData();
      StringBuilder header = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        header.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      assertEquals(expectedHeader, header.toString());

      int cnt = 0;
      while (actualResultSet.next()) {
        StringBuilder builder = new StringBuilder();
        if (df != null) {
          builder
              .append(
                  df.format(
                      TimeUnit.MILLISECONDS.convert(
                          Long.parseLong(actualResultSet.getString(1)), currPrecision)))
              .append(",");
        } else {
          builder.append(actualResultSet.getString(1)).append(",");
        }
        for (int i = 2; i <= resultSetMetaData.getColumnCount(); i++) {
          builder.append(actualResultSet.getString(i)).append(",");
        }
        assertEquals(expectedRetArray[cnt], builder.toString());
        cnt++;
      }
      assertEquals(expectedRetArray.length, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(String.valueOf(e));
    }
  }

  public static void executeNonQuery(String sql) {
    executeNonQuery(sql, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  public static void executeNonQuery(String sql, String userName, String password) {
    try (Connection connection = EnvFactory.getEnv().getConnection(userName, password);
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void executeNonQueryWithRetry(BaseEnv env, String sql) {
    for (int retryCountLeft = 10; retryCountLeft >= 0; retryCountLeft--) {
      try (Connection connection = env.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute(sql);
        break;
      } catch (SQLException e) {
        if (retryCountLeft > 0) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException ignored) {
          }
        } else {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  public static boolean tryExecuteNonQueryWithRetry(BaseEnv env, String sql) {
    return tryExecuteNonQueryWithRetry(
        env, sql, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  public static boolean tryExecuteNonQueryWithRetry(
      String dataBaseName, String sqlDialect, BaseEnv env, String sql) {
    return tryExecuteNonQueryWithRetry(
        env,
        sql,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        dataBaseName,
        sqlDialect);
  }

  public static boolean tryExecuteNonQueryWithRetry(
      BaseEnv env, String sql, String userName, String password) {
    return tryExecuteNonQueriesWithRetry(env, Collections.singletonList(sql), userName, password);
  }

  public static boolean tryExecuteNonQueryWithRetry(
      BaseEnv env,
      String sql,
      String userName,
      String password,
      String dataBaseName,
      String sqlDialect) {
    return tryExecuteNonQueriesWithRetry(
        env, Collections.singletonList(sql), userName, password, dataBaseName, sqlDialect);
  }

  public static boolean tryExecuteNonQueriesWithRetry(BaseEnv env, List<String> sqlList) {
    return tryExecuteNonQueriesWithRetry(
        env,
        sqlList,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        null,
        BaseEnv.TREE_SQL_DIALECT);
  }

  public static boolean tryExecuteNonQueriesWithRetry(
      String dataBase, String sqlDialect, BaseEnv env, List<String> sqlList) {
    return tryExecuteNonQueriesWithRetry(
        env,
        sqlList,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        dataBase,
        sqlDialect);
  }

  // This method will not throw failure given that a failure is encountered.
  // Instead, it returns a flag to indicate the result of the execution.
  public static boolean tryExecuteNonQueriesWithRetry(
      BaseEnv env, List<String> sqlList, String userName, String password) {
    return tryExecuteNonQueriesWithRetry(
        env, sqlList, userName, password, null, BaseEnv.TREE_SQL_DIALECT);
  }

  public static boolean tryExecuteNonQueriesWithRetry(
      BaseEnv env,
      List<String> sqlList,
      String userName,
      String password,
      String dataBase,
      String sqlDialect) {
    int lastIndex = 0;
    for (int retryCountLeft = 10; retryCountLeft >= 0; retryCountLeft--) {
      try (Connection connection =
              env.getConnection(
                  userName,
                  password,
                  BaseEnv.TABLE_SQL_DIALECT.equals(sqlDialect)
                      ? BaseEnv.TABLE_SQL_DIALECT
                      : BaseEnv.TREE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        if (BaseEnv.TABLE_SQL_DIALECT.equals(sqlDialect) && dataBase != null) {
          statement.execute("use " + dataBase);
        }
        for (int i = lastIndex; i < sqlList.size(); ++i) {
          lastIndex = i;
          statement.execute(sqlList.get(i));
        }
        return true;
      } catch (SQLException e) {
        if (retryCountLeft > 0) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException ignored) {
          }
        } else {
          e.printStackTrace();
          return false;
        }
      }
    }
    return false;
  }

  public static void executeNonQueryOnSpecifiedDataNodeWithRetry(
      BaseEnv env, DataNodeWrapper wrapper, String sql) {
    for (int retryCountLeft = 10; retryCountLeft >= 0; retryCountLeft--) {
      try (Connection connection = env.getWriteOnlyConnectionWithSpecifiedDataNode(wrapper);
          Statement statement = connection.createStatement()) {
        statement.execute(sql);
        break;
      } catch (SQLException e) {
        if (retryCountLeft > 0) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException ignored) {
          }
        } else {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  // This method will not throw failure given that a failure is encountered.
  // Instead, it returns a flag to indicate the result of the execution.
  public static boolean tryExecuteNonQueryOnSpecifiedDataNodeWithRetry(
      BaseEnv env, DataNodeWrapper wrapper, String sql) {
    return tryExecuteNonQueriesOnSpecifiedDataNodeWithRetry(
        env, wrapper, Collections.singletonList(sql));
  }

  public static boolean tryExecuteNonQueriesOnSpecifiedDataNodeWithRetry(
      BaseEnv env, DataNodeWrapper wrapper, List<String> sqlList) {
    int lastIndex = 0;
    for (int retryCountLeft = 10; retryCountLeft >= 0; retryCountLeft--) {
      try (Connection connection = env.getWriteOnlyConnectionWithSpecifiedDataNode(wrapper);
          Statement statement = connection.createStatement()) {
        for (int i = lastIndex; i < sqlList.size(); ++i) {
          statement.execute(sqlList.get(i));
          lastIndex = i;
        }
        return true;
      } catch (SQLException e) {
        if (retryCountLeft > 0) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException ignored) {
          }
        } else {
          e.printStackTrace();
          return false;
        }
      }
    }
    return false;
  }

  public static boolean tryExecuteNonQueriesOnSpecifiedDataNodeWithRetry(
      BaseEnv env,
      DataNodeWrapper wrapper,
      List<String> sqlList,
      String dataBase,
      String sqlDialect) {
    int lastIndex = 0;
    for (int retryCountLeft = 10; retryCountLeft >= 0; retryCountLeft--) {
      try (Connection connection =
              env.getWriteOnlyConnectionWithSpecifiedDataNode(wrapper, sqlDialect);
          Statement statement = connection.createStatement()) {

        if (BaseEnv.TABLE_SQL_DIALECT.equals(sqlDialect) && dataBase != null) {
          statement.execute("use " + dataBase);
        }

        for (int i = lastIndex; i < sqlList.size(); ++i) {
          statement.execute(sqlList.get(i));
          lastIndex = i;
        }
        return true;
      } catch (SQLException e) {
        if (retryCountLeft > 0) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException ignored) {
          }
        } else {
          e.printStackTrace();
          return false;
        }
      }
    }
    return false;
  }

  public static void executeQuery(String sql) {
    executeQuery(sql, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  public static void executeQuery(String sql, String userName, String password) {
    try (Connection connection = EnvFactory.getEnv().getConnection(userName, password);
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void executeQueryWithRetry(
      BaseEnv env, String sql, String userName, String password) {
    try (Connection connection = env.getConnection(userName, password);
        Statement statement = connection.createStatement()) {
      for (int retryCountLeft = 10; retryCountLeft >= 0; retryCountLeft--) {
        try {
          statement.executeQuery(sql);
        } catch (SQLException e) {
          if (retryCountLeft > 0) {
            try {
              Thread.sleep(10000);
            } catch (InterruptedException ignored) {
            }
          } else {
            e.printStackTrace();
            fail(e.getMessage());
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static ResultSet executeQueryWithRetry(Statement statement, String sql) {
    for (int retryCountLeft = 10; retryCountLeft >= 0; retryCountLeft--) {
      try {
        return statement.executeQuery(sql);
      } catch (SQLException e) {
        if (retryCountLeft > 0) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException ignored) {
          }
        } else {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
    return null;
  }

  public static void assertResultSetEqual(
      SessionDataSet actualResultSet,
      List<String> expectedColumnNames,
      String[] expectedRetArray,
      boolean ignoreTimeStamp) {
    try {
      List<String> actualColumnNames = actualResultSet.getColumnNames();
      if (ignoreTimeStamp) {
        assertEquals(expectedColumnNames, actualColumnNames);
      } else {
        assertEquals(TIMESTAMP_STR, actualColumnNames.get(0));
        assertEquals(expectedColumnNames, actualColumnNames.subList(1, actualColumnNames.size()));
      }

      int count = 0;
      while (actualResultSet.hasNext()) {
        RowRecord rowRecord = actualResultSet.next();
        assertEquals(expectedRetArray[count++], rowRecord.toString().replace('\t', ','));
      }
      assertEquals(expectedRetArray.length, count);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void createUser(String userName, String password) {
    createUser(EnvFactory.getEnv(), userName, password);
  }

  public static void createUser(BaseEnv env, String userName, String password) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("create user %s '%s'", userName, password));
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void grantUserSystemPrivileges(String userName, PrivilegeType privilegeType) {
    grantUserSystemPrivileges(EnvFactory.getEnv(), userName, privilegeType);
  }

  public static void grantUserSystemPrivileges(
      BaseEnv env, String userName, PrivilegeType privilegeType) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("grant %s on root.** to user %s", privilegeType, userName));
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void grantUserSeriesPrivilege(
      String userName, PrivilegeType privilegeType, String path) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("grant %s on %s to user %s", privilegeType, path, userName));
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void revokeUserSeriesPrivilege(
      String userName, PrivilegeType privilegeType, String path) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("revoke %s on %s from user %s", privilegeType, path, userName));
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void restartCluster(BaseEnv env) {
    env.shutdownAllDataNodes();
    env.shutdownAllConfigNodes();
    env.startAllConfigNodes();
    env.startAllDataNodes();
    ((AbstractEnv) env).checkClusterStatusWithoutUnknown();
  }

  public static void assertDataEventuallyOnEnv(
      BaseEnv env, String sql, String expectedHeader, Set<String> expectedResSet) {
    assertDataEventuallyOnEnv(env, sql, expectedHeader, expectedResSet, 600);
  }

  public static void assertDataEventuallyOnEnv(
      final BaseEnv env,
      final String sql,
      final String expectedHeader,
      final Set<String> expectedResSet,
      final Consumer<String> handleFailure) {
    assertDataEventuallyOnEnv(env, sql, expectedHeader, expectedResSet, 600, handleFailure);
  }

  public static void assertDataEventuallyOnEnv(
      BaseEnv env,
      String sql,
      String expectedHeader,
      Set<String> expectedResSet,
      long timeoutSeconds) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      // Keep retrying if there are execution failures
      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(timeoutSeconds, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try {
                  TestUtils.assertResultSetEqual(
                      executeQueryWithRetry(statement, sql), expectedHeader, expectedResSet);
                } catch (Exception e) {
                  Assert.fail();
                }
              });
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  public static void assertDataEventuallyOnEnv(
      final BaseEnv env,
      final String sql,
      final String expectedHeader,
      final Set<String> expectedResSet,
      final long timeoutSeconds,
      final Consumer<String> handleFailure) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      // Keep retrying if there are execution failures
      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(timeoutSeconds, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try {
                  TestUtils.assertResultSetEqual(
                      executeQueryWithRetry(statement, sql), expectedHeader, expectedResSet);
                } catch (Exception e) {
                  if (handleFailure != null) {
                    handleFailure.accept(e.getMessage());
                  }
                  Assert.fail();
                } catch (Error e) {
                  if (handleFailure != null) {
                    handleFailure.accept(e.getMessage());
                  }
                  throw e;
                }
              });
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  public static void assertDataEventuallyOnEnv(
      final BaseEnv env,
      final String sql,
      final String expectedHeader,
      final Set<String> expectedResSet,
      final String dataBaseName) {
    assertDataEventuallyOnEnv(env, sql, expectedHeader, expectedResSet, 600, dataBaseName, null);
  }

  public static void assertDataEventuallyOnEnv(
      final BaseEnv env,
      final String sql,
      final String expectedHeader,
      final Set<String> expectedResSet,
      final String dataBaseName,
      final Consumer<String> handleFailure) {
    assertDataEventuallyOnEnv(
        env, sql, expectedHeader, expectedResSet, 600, dataBaseName, handleFailure);
  }

  public static void assertDataEventuallyOnEnv(
      final BaseEnv env,
      final String sql,
      final String expectedHeader,
      final Set<String> expectedResSet,
      final long timeoutSeconds,
      final String dataBaseName,
      final Consumer<String> handleFailure) {
    try (Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      // Keep retrying if there are execution failures
      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(timeoutSeconds, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try {
                  if (dataBaseName != null) {
                    statement.execute("use " + dataBaseName);
                  }
                  if (sql != null && !sql.isEmpty()) {
                    TestUtils.assertResultSetEqual(
                        executeQueryWithRetry(statement, sql), expectedHeader, expectedResSet);
                  }
                } catch (Exception e) {
                  if (handleFailure != null) {
                    handleFailure.accept(e.getMessage());
                  }
                  Assert.fail();
                } catch (Error e) {
                  if (handleFailure != null) {
                    handleFailure.accept(e.getMessage());
                  }
                  throw e;
                }
              });
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  public static void assertDataEventuallyOnEnv(
      BaseEnv env, String sql, Map<String, String> expectedHeaderWithResult) {
    assertDataEventuallyOnEnv(env, sql, expectedHeaderWithResult, 600);
  }

  public static void assertDataEventuallyOnEnv(
      BaseEnv env, String sql, Map<String, String> expectedHeaderWithResult, long timeoutSeconds) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      // Keep retrying if there are execution failures
      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(timeoutSeconds, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try {
                  TestUtils.assertSingleResultSetEqual(
                      executeQueryWithRetry(statement, sql), expectedHeaderWithResult);
                } catch (Exception e) {
                  Assert.fail();
                }
              });
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  public static void assertDataAlwaysOnEnv(
      BaseEnv env, String sql, String expectedHeader, Set<String> expectedResSet) {
    assertDataAlwaysOnEnv(env, sql, expectedHeader, expectedResSet, 10);
  }

  public static void assertDataAlwaysOnEnv(
      BaseEnv env,
      String sql,
      String expectedHeader,
      Set<String> expectedResSet,
      Consumer<String> handleFailure) {
    assertDataAlwaysOnEnv(env, sql, expectedHeader, expectedResSet, 10, handleFailure);
  }

  public static void assertDataAlwaysOnEnv(
      BaseEnv env,
      String sql,
      String expectedHeader,
      Set<String> expectedResSet,
      long consistentSeconds) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      // Keep retrying if there are execution failures
      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(consistentSeconds, TimeUnit.SECONDS)
          .failFast(
              () -> {
                try {
                  TestUtils.assertResultSetEqual(
                      executeQueryWithRetry(statement, sql), expectedHeader, expectedResSet);
                } catch (Exception e) {
                  Assert.fail();
                }
              });
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  public static void assertDataAlwaysOnEnv(
      BaseEnv env,
      String sql,
      String expectedHeader,
      Set<String> expectedResSet,
      long consistentSeconds,
      Consumer<String> handleFailure) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      // Keep retrying if there are execution failures
      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(consistentSeconds, TimeUnit.SECONDS)
          .failFast(
              () -> {
                try {
                  TestUtils.assertResultSetEqual(
                      executeQueryWithRetry(statement, sql), expectedHeader, expectedResSet);
                } catch (Exception e) {
                  if (handleFailure != null) {
                    handleFailure.accept(e.getMessage());
                  }
                  Assert.fail();
                } catch (Error e) {
                  if (handleFailure != null) {
                    handleFailure.accept(e.getMessage());
                  }
                  throw e;
                }
              });
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  public static void restartDataNodes() {
    EnvFactory.getEnv().shutdownAllDataNodes();
    EnvFactory.getEnv().startAllDataNodes();
    long waitStartMS = System.currentTimeMillis();
    long maxWaitMS = 60_000L;
    long retryIntervalMS = 1000;
    while (true) {
      try (Connection connection = EnvFactory.getEnv().getConnection()) {
        break;
      } catch (Exception e) {
        try {
          Thread.sleep(retryIntervalMS);
        } catch (InterruptedException ex) {
          break;
        }
      }
      long waited = System.currentTimeMillis() - waitStartMS;
      if (waited > maxWaitMS) {
        fail("Timeout while waiting for datanodes restart");
      }
    }
  }
}
