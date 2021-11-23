/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.integration.aligned;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class IoTDBAggregationWithoutValueFilterIT {

  private static final double DELTA = 1e-6;
  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    // TODO When the aligned time series support compaction, we need to set compaction to true
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void countAllAlignedWithoutTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"20", "29", "28", "19", "20"};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s2)",
      "count(root.sg1.d1.s3)",
      "count(root.sg1.d1.s4)",
      "count(root.sg1.d1.s5)"
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select count(*) from root.sg1.d1");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
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
  public void countAllAlignedAndNonAlignedWithoutTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"20", "29", "28", "19", "20", "19", "29", "28", "18", "19"};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s2)",
      "count(root.sg1.d1.s3)",
      "count(root.sg1.d1.s4)",
      "count(root.sg1.d1.s5)",
      "count(root.sg1.d2.s1)",
      "count(root.sg1.d2.s2)",
      "count(root.sg1.d2.s3)",
      "count(root.sg1.d2.s4)",
      "count(root.sg1.d2.s5)"
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select count(*) from root.sg1.*");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
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
  public void countAllAlignedWithTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"12", "15", "22", "13", "6"};
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s2)",
      "count(root.sg1.d1.s3)",
      "count(root.sg1.d1.s4)",
      "count(root.sg1.d1.s5)"
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select count(*) from root.sg1.d1 where time >= 9 and time <= 33");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
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

  /** aggregate multi columns of aligned timeseries in one SQL */
  @Test
  public void aggregateSomeAlignedWithoutTimeFilterTest() throws ClassNotFoundException {
    double[] retArray =
        new double[] {
          20, 29, 28, 390184, 130549, 390417, 19509.2, 4501.689655172413, 13943.464285714286
        };
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s2)",
      "count(root.sg1.d1.s3)",
      "sum(root.sg1.d1.s1)",
      "sum(root.sg1.d1.s2)",
      "sum(root.sg1.d1.s3)",
      "avg(root.sg1.d1.s1)",
      "avg(root.sg1.d1.s2)",
      "avg(root.sg1.d1.s3)",
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select count(s1),count (s2),count (s3),sum(s1),sum(s2),sum(s3),avg(s1),avg(s2),avg(s3) from root.sg1.d1");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
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
  public void aggregateSomeAlignedWithTimeFilterTest() throws ClassNotFoundException {
    double[] retArray =
        new double[] {
          6, 9, 15, 230090, 220, 230322, 38348.333333333336, 24.444444444444443, 15354.8
        };
    String[] columnNames = {
      "count(root.sg1.d1.s1)",
      "count(root.sg1.d1.s2)",
      "count(root.sg1.d1.s3)",
      "sum(root.sg1.d1.s1)",
      "sum(root.sg1.d1.s2)",
      "sum(root.sg1.d1.s3)",
      "avg(root.sg1.d1.s1)",
      "avg(root.sg1.d1.s2)",
      "avg(root.sg1.d1.s3)",
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select count(s1),count (s2),count (s3),sum(s1),sum(s2),sum(s3),avg(s1),avg(s2),avg(s3) from root.sg1.d1 where time>=16 and time<=34");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
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
  public void countSingleAlignedWithTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"9"};
    String[] columnNames = {"count(root.sg1.d1.s2)"};
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select count(s2) from root.sg1.d1 where time>=16 and time<=34");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
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
  public void sumSingleAlignedWithTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"230322.0"};
    String[] columnNames = {"sum(root.sg1.d1.s3)"};
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select sum(s3) from root.sg1.d1 where time>=16 and time<=34");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
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
  public void avgSingleAlignedWithTimeFilterTest() throws ClassNotFoundException {
    double[][] retArray = {{24.444444444444443}};
    String[] columnNames = {"avg(root.sg1.d1.s2)"};
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select avg(s2) from root.sg1.d1 where time>=16 and time<=34");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[columnNames.length];
          StringBuilder builder = new StringBuilder();
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = Double.parseDouble(resultSet.getString(index));
          }
          assertArrayEquals(retArray[cnt], ans, DELTA);
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
