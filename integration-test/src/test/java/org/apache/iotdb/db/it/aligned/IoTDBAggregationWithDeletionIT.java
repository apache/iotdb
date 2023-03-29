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
package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.db.it.utils.AlignedWriteUtil;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAggregationWithDeletionIT {

  private static final double DELTA = 1e-6;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setMaxTsBlockLineNumber(3);
    EnvFactory.getEnv().initClusterEnvironment();
    AlignedWriteUtil.insertData();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete timeseries root.sg1.d1.s2");
      statement.execute("delete from root.sg1.d1.s1 where time <= 21");
    } catch (Exception e) {
      fail(e.getMessage());
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void countAllAlignedWithoutTimeFilterTest() {
    String[] retArray = new String[] {"1", "28", "19", "20"};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s3)",
      "count(root.sg1.d1.s4)",
      "count(root.sg1.d1.s5)"
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select count(*) from root.sg1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }

        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countAllAlignedAndNonAlignedWithoutTimeFilterTest() {
    String[] retArray = new String[] {"1", "28", "19", "20", "19", "29", "28", "18", "19"};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s3)",
      "count(root.sg1.d1.s4)",
      "count(root.sg1.d1.s5)",
      "count(root.sg1.d2.s1)",
      "count(root.sg1.d2.s2)",
      "count(root.sg1.d2.s3)",
      "count(root.sg1.d2.s4)",
      "count(root.sg1.d2.s5)"
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select count(*) from root.sg1.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }

        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countAllAlignedWithTimeFilterTest() {
    String[] retArray = new String[] {"1", "22", "13", "6"};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s3)",
      "count(root.sg1.d1.s4)",
      "count(root.sg1.d1.s5)"
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*) from root.sg1.d1 where time >= 9 and time <= 33")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }

        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** aggregate multi columns of aligned timeseries in one SQL */
  @Test
  public void aggregateSomeAlignedWithoutTimeFilterTest() {
    double[] retArray = new double[] {1, 28, 230000, 390417, 230000, 13943.464285714286};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s3)",
      "sum(root.sg1.d1.s1)",
      "sum(root.sg1.d1.s3)",
      "avg(root.sg1.d1.s1)",
      "avg(root.sg1.d1.s3)",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1),count (s3),sum(s1),sum(s3),avg(s1),avg(s3) from root.sg1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }

        int cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = Double.parseDouble(resultSet.getString(index));
          }
          assertArrayEquals(retArray, ans, DELTA);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** aggregate multi columns of aligned timeseries in one SQL */
  @Test
  public void aggregateSomeAlignedWithTimeFilterTest() {
    double[] retArray = new double[] {1, 15, 230000, 230322, 230000, 15354.8};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s3)",
      "sum(root.sg1.d1.s1)",
      "sum(root.sg1.d1.s3)",
      "avg(root.sg1.d1.s1)",
      "avg(root.sg1.d1.s3)",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1),count (s3),sum(s1),sum(s3),avg(s1),avg(s3) from root.sg1.d1 where time>=16 and time<=34")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }

        int cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = Double.parseDouble(resultSet.getString(index));
          }
          assertArrayEquals(retArray, ans, DELTA);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countSingleAlignedWithTimeFilterTest() {
    String[] retArray = new String[] {"0"};
    String[] columnNames = {"count(root.sg1.d1.s1)"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(s1) from root.sg1.d1 where time>=16 and time<=20")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          // No need to add time column for aggregation query
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            if (builder.length() != 0) {
              builder.append(",");
            }
            builder.append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void sumSingleAlignedWithTimeFilterTest() {
    String[] retArray = new String[] {"null"};
    String[] columnNames = {"sum(root.sg1.d1.s1)"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select sum(s1) from root.sg1.d1 where time>=16 and time<=20")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          // No need to add time column for aggregation query
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            if (builder.length() != 0) {
              builder.append(",");
            }
            builder.append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void avgSingleAlignedWithTimeFilterTest() {
    String[] retArray = new String[] {"null"};
    String[] columnNames = {"avg(root.sg1.d1.s1)"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select avg(s1) from root.sg1.d1 where time>=16 and time<=20")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          // No need to add time column for aggregation query
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            if (builder.length() != 0) {
              builder.append(",");
            }
            builder.append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ----------------------- Aggregation with value filter -----------------------------
  @Test
  public void countAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"11"};
    String[] columnNames = {"count(root.sg1.d1.s4)"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(s4) from root.sg1.d1 where s4 = true")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countAllAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0", "9", "11", "6"};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s3)",
      "count(root.sg1.d1.s4)",
      "count(root.sg1.d1.s5)"
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(*) from root.sg1.d1 where s4 = true")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void aggregationAllAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0", "25", "1"};
    String[] columnNames = {
      "count(root.sg1.d1.s1)", "max_time(root.sg1.d1.s4)", "min_value(root.sg1.d1.s3)",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), max_time(s4), min_value(s3) from root.sg1.d1 where s4 = true")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
