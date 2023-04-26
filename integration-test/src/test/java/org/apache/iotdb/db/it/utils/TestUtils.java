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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.itbase.constant.TestConstant.DELTA;
import static org.apache.iotdb.itbase.constant.TestConstant.NULL;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestUtils {

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
      String sql, String expectedHeader, String[] expectedRetArray, DateFormat df) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      if (df != null) {
        connection.setClientInfo("time_zone", "+00:00");
      }
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        assertResultSetEqual(resultSet, expectedHeader, expectedRetArray, df);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void resultSetEqualTest(
      String sql, String expectedHeader, String[] expectedRetArray) {
    resultSetEqualTest(sql, expectedHeader, expectedRetArray, null);
  }

  public static void resultSetEqualTest(
      String sql, String[] expectedHeader, String[] expectedRetArray) {
    resultSetEqualTest(sql, expectedHeader, expectedRetArray, null);
  }

  public static void resultSetEqualTest(
      String sql, String[] expectedHeader, String[] expectedRetArray, DateFormat df) {
    StringBuilder header = new StringBuilder();
    for (String s : expectedHeader) {
      header.append(s).append(",");
    }
    resultSetEqualTest(sql, header.toString(), expectedRetArray, df);
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail("No exception!");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    }
  }

  public static void assertNonQueryTestFail(String sql, String errMsg) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      fail("No exception!");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    }
  }

  public static void assertResultSetEqual(
      ResultSet actualResultSet, String expectedHeader, String[] expectedRetArray)
      throws SQLException {
    assertResultSetEqual(actualResultSet, expectedHeader, expectedRetArray, null);
  }

  public static void assertResultSetEqual(
      ResultSet actualResultSet, String expectedHeader, Set<String> expectedRetSet)
      throws SQLException {
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
  }

  public static void assertResultSetEqual(
      ResultSet actualResultSet, String expectedHeader, String[] expectedRetArray, DateFormat df)
      throws SQLException {
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
        builder.append(df.format(Long.parseLong(actualResultSet.getString(1)))).append(",");
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
  }

  public static void executeNonQuery(String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
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
}
