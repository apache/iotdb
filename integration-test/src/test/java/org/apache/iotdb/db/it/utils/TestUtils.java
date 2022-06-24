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

import org.apache.iotdb.it.env.EnvFactory;

import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestUtils {

  public static void prepareData(String[] SQLs) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void assertTestFail(String sql, String errMsg) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (Exception e) {
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

    while (actualResultSet.next()) {
      StringBuilder builder = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        builder.append(actualResultSet.getString(i)).append(",");
      }
      assertTrue(expectedRetSet.contains(builder.toString()));
      expectedRetSet.remove(builder.toString());
    }
    assertEquals(expectedRetSet.size(), 0);
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
}
