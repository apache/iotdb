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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.udf.example.ExampleUDFConstant;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBNestedQueryIT {

  protected static final int ITERATION_TIMES = 10_000;

  @BeforeClass
  public static void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setUdfCollectorMemoryBudgetInMB(5);
    IoTDBDescriptor.getInstance().getConfig().setUdfTransformerMemoryBudgetInMB(5);
    IoTDBDescriptor.getInstance().getConfig().setUdfReaderMemoryBudgetInMB(5);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.schemaProcessor.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.d2.s2"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.empty"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function adder as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.execute(
          "create function time_window_counter as 'org.apache.iotdb.db.query.udf.example.Counter'");
      statement.execute(
          "create function size_window_counter as 'org.apache.iotdb.db.query.udf.example.Counter'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setUdfCollectorMemoryBudgetInMB(100);
    IoTDBDescriptor.getInstance().getConfig().setUdfTransformerMemoryBudgetInMB(100);
    IoTDBDescriptor.getInstance().getConfig().setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void testNestedArithmeticExpressions() {
    String sqlStr =
        "select d1.s1, d2.s2, d1.s1 + d1.s2 - (d2.s1 + d2.s2), d1.s2 * (d2.s1 / d1.s1), d1.s2 + d1.s2 * d2.s1 - d2.s1, d1.s1 - (d1.s1 - (-d1.s1)), (-d2.s1) * (-d2.s2) / (-d1.s2) from root.vehicle";

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 7, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        ++count;

        assertEquals(count, Integer.parseInt(resultSet.getString(1)));
        assertEquals(count, Double.parseDouble(resultSet.getString(2)), 0);
        assertEquals(count, Double.parseDouble(resultSet.getString(3)), 0);
        assertEquals(0.0, Double.parseDouble(resultSet.getString(4)), 0);
        assertEquals(count, Double.parseDouble(resultSet.getString(5)), 0);
        assertEquals(count * count, Double.parseDouble(resultSet.getString(6)), 0);
        assertEquals(-count, Double.parseDouble(resultSet.getString(7)), 0);
        assertEquals(-count, Double.parseDouble(resultSet.getString(8)), 0);
      }

      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testNestedRowByRowUDFExpressions() {
    String sqlStr =
        "select s1, s2, sin(sin(s1) * sin(s2) + cos(s1) * cos(s1)) + sin(sin(s1 - s1 + s2) * sin(s2) + cos(s1) * cos(s1)), asin(sin(asin(sin(s1 - s2 / (-s1))))) from root.vehicle.d2";

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 4, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        ++count;

        assertEquals(count, Integer.parseInt(resultSet.getString(1)));
        assertEquals(count, Double.parseDouble(resultSet.getString(2)), 0);
        assertEquals(count, Double.parseDouble(resultSet.getString(3)), 0);
        assertEquals(2 * Math.sin(1.0), Double.parseDouble(resultSet.getString(4)), 1e-5);
        assertEquals(
            Math.asin(Math.sin(Math.asin(Math.sin(count - count / (-count))))),
            Double.parseDouble(resultSet.getString(5)),
            1e-5);
      }

      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testUDFTerminateMethodInNestedExpressions() {
    String sqlStr =
        "select bottom_k(top_k(top_k(s1 + s1 / s1 - s2 / s1, 'k'='100'), 'k'='1'), 'k'='1'), top_k(top_k(s1 + s1 / s1 - s2 / s1, 'k'='100'), 'k'='1'), top_k(s1 + s1 / s1 - s2 / s1, 'k'='1'), top_k(s1, 'k'='1'), top_k(s2, 'k'='1') from root.vehicle.d2";

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 5, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        ++count;

        assertEquals(ITERATION_TIMES, Double.parseDouble(resultSet.getString(1)), 1e-5);
        assertEquals(ITERATION_TIMES, Double.parseDouble(resultSet.getString(2)), 1e-5);
        assertEquals(ITERATION_TIMES, Double.parseDouble(resultSet.getString(3)), 1e-5);
        assertEquals(ITERATION_TIMES, Double.parseDouble(resultSet.getString(4)), 1e-5);
        assertEquals(ITERATION_TIMES, Double.parseDouble(resultSet.getString(5)), 1e-5);
      }

      assertEquals(1, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testUDFWithMultiInputsInNestedExpressions() {
    String sqlStr =
        "select adder(d1.s1, d1.s2), -adder(d2.s1, d2.s2), adder(adder(d1.s1, d1.s2), -adder(d2.s1, d2.s2)), adder(adder(d1.s1, d1.s2), adder(d2.s1, d2.s2)), adder(d1.s1, d1.s2) - adder(d1.s1, d1.s2) + adder(adder(d1.s1, d1.s2), -adder(d2.s1, d2.s2)) from root.vehicle";

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 5, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        ++count;

        assertEquals(count, Double.parseDouble(resultSet.getString(1)), 1e-5);
        assertEquals(2 * count, Double.parseDouble(resultSet.getString(2)), 1e-5);
        assertEquals(-2 * count, Double.parseDouble(resultSet.getString(3)), 1e-5);
        assertEquals(0, Double.parseDouble(resultSet.getString(4)), 1e-5);
        assertEquals(4 * count, Double.parseDouble(resultSet.getString(5)), 1e-5);
        assertEquals(0, Double.parseDouble(resultSet.getString(6)), 1e-5);
      }

      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testNestedWindowingFunctionExpressions() {
    final int[] windows =
        new int[] {
          1,
          2,
          3,
          100,
          499,
          ITERATION_TIMES - 1,
          ITERATION_TIMES,
          ITERATION_TIMES + 1,
          ITERATION_TIMES + 13
        };

    for (int window : windows) {
      String sqlStr =
          String.format(
              "select time_window_counter(sin(d1.s1), '%s'='%s', '%s'='%s'), time_window_counter(sin(d1.s1), cos(d2.s2) / sin(d1.s1), d1.s2, '%s'='%s', '%s'='%s'), size_window_counter(cos(d2.s2), '%s'='%s', '%s'='%s'), size_window_counter(cos(d2.s2), cos(d2.s2), '%s'='%s', '%s'='%s') from root.vehicle",
              ExampleUDFConstant.ACCESS_STRATEGY_KEY,
              ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_TIME,
              ExampleUDFConstant.TIME_INTERVAL_KEY,
              window,
              ExampleUDFConstant.ACCESS_STRATEGY_KEY,
              ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_TIME,
              ExampleUDFConstant.TIME_INTERVAL_KEY,
              window,
              ExampleUDFConstant.ACCESS_STRATEGY_KEY,
              ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_SIZE,
              ExampleUDFConstant.WINDOW_SIZE_KEY,
              window,
              ExampleUDFConstant.ACCESS_STRATEGY_KEY,
              ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_SIZE,
              ExampleUDFConstant.WINDOW_SIZE_KEY,
              window);

      try (Connection connection =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
          Statement statement = connection.createStatement()) {
        ResultSet resultSet = statement.executeQuery(sqlStr);

        assertEquals(1 + 4, resultSet.getMetaData().getColumnCount());

        int count = 0;
        while (resultSet.next()) {
          assertEquals((long) count * window + 1, Long.parseLong(resultSet.getString(1)));

          double c2 = Double.parseDouble(resultSet.getString(2));
          double c3 = Double.parseDouble(resultSet.getString(3));
          double c4 = Double.parseDouble(resultSet.getString(4));
          double c5 = Double.parseDouble(resultSet.getString(5));

          ++count;

          assertEquals(
              ((ITERATION_TIMES < count * window)
                  ? window - (count * window - ITERATION_TIMES)
                  : window),
              c2,
              0);
          assertEquals(c2, c3, 0);
          assertEquals(c2, c4, 0);
          assertEquals(c2, c5, 0);
        }

        assertEquals(ITERATION_TIMES / window + (ITERATION_TIMES % window == 0 ? 0 : 1), count);
      } catch (SQLException throwable) {
        fail(throwable.getMessage());
      }
    }
  }

  @Test
  public void testSelectEmptyColumns() {
    final int[] windows =
        new int[] {
          1, 2, 3, 100, 499,
        };

    for (int window : windows) {
      String sqlStr =
          String.format(
              "select time_window_counter(sin(empty), '%s'='%s', '%s'='%s'), "
                  + "time_window_counter(sin(empty), cos(empty) / sin(empty), empty, '%s'='%s', '%s'='%s'), "
                  + "size_window_counter(cos(empty - empty) + empty, '%s'='%s', '%s'='%s'), "
                  + "size_window_counter(cos(empty), cos(empty), '%s'='%s', '%s'='%s'), "
                  + "empty, sin(empty) - bottom_k(top_k(empty, 'k'='111'), 'k'='111'), "
                  + "empty * empty / empty + empty %% empty - empty from root.vehicle",
              ExampleUDFConstant.ACCESS_STRATEGY_KEY,
              ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_TIME,
              ExampleUDFConstant.TIME_INTERVAL_KEY,
              window,
              ExampleUDFConstant.ACCESS_STRATEGY_KEY,
              ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_TIME,
              ExampleUDFConstant.TIME_INTERVAL_KEY,
              window,
              ExampleUDFConstant.ACCESS_STRATEGY_KEY,
              ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_SIZE,
              ExampleUDFConstant.WINDOW_SIZE_KEY,
              window,
              ExampleUDFConstant.ACCESS_STRATEGY_KEY,
              ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_SIZE,
              ExampleUDFConstant.WINDOW_SIZE_KEY,
              window);

      try (Connection connection =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
          Statement statement = connection.createStatement()) {
        ResultSet resultSet = statement.executeQuery(sqlStr);

        assertEquals(1 + 7, resultSet.getMetaData().getColumnCount());
        assertFalse(resultSet.next());
      } catch (SQLException throwable) {
        fail(throwable.getMessage());
      }
    }
  }

  @Test
  public void testInvalidNestedBuiltInAggregation() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String query = "SELECT first_value(abs(s1)) FROM root.vehicle.d1";
      try {
        statement.executeQuery(query);
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("The argument of the aggregation function must be a time series."));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testRawDataQueryWithConstants() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String query = "SELECT 1 + s1 FROM root.vehicle.d1";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1.0D, rs.getDouble(2), 0.01);
        }
        Assert.assertFalse(rs.next());
      }

      query = "SELECT (1 + 4) * 2 / 10 + s1 FROM root.vehicle.d1";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1.0D, rs.getDouble(2), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDuplicatedRawDataQueryWithConstants() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String query = "SELECT 1 + s1, 1 + s1 FROM root.vehicle.d1";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1.0D, rs.getDouble(2), 0.01);
          Assert.assertEquals(i + 1.0D, rs.getDouble(3), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testCommutativeLaws() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String query = "SELECT s1, s1 + 1, 1 + s1, s1 * 2, 2 * s1 FROM root.vehicle.d1";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i, rs.getInt(2));
          Assert.assertEquals(i + 1.0D, rs.getDouble(3), 0.01);
          Assert.assertEquals(i + 1.0D, rs.getDouble(4), 0.01);
          Assert.assertEquals(i * 2.0D, rs.getDouble(5), 0.01);
          Assert.assertEquals(i * 2.0D, rs.getDouble(6), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testAssociativeLaws() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String query =
          "SELECT s1, s1 + 1 + 2, (s1 + 1) + 2, s1 + (1 + 2), s1 * 2 * 3, s1 * (2 * 3), (s1 * 2) * 3 FROM root.vehicle.d1";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i, rs.getInt(2));
          Assert.assertEquals(i + 3.0D, rs.getDouble(3), 0.01);
          Assert.assertEquals(i + 3.0D, rs.getDouble(4), 0.01);
          Assert.assertEquals(i + 3.0D, rs.getDouble(5), 0.01);
          Assert.assertEquals(i * 6.0D, rs.getDouble(6), 0.01);
          Assert.assertEquals(i * 6.0D, rs.getDouble(7), 0.01);
          Assert.assertEquals(i * 6.0D, rs.getDouble(8), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDistributiveLaw() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String query =
          "SELECT s1, (s1 + 1) * 2, s1 * 2 + 1 * 2, (s1 + 1) / 2, s1 / 2 + 1 / 2 FROM root.vehicle.d1";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i, rs.getInt(2));
          Assert.assertEquals(2 * i + 2.0D, rs.getDouble(3), 0.01);
          Assert.assertEquals(2 * i + 2.0D, rs.getDouble(4), 0.01);
          Assert.assertEquals(i / 2.0D + 0.5D, rs.getDouble(5), 0.01);
          Assert.assertEquals(i / 2.0D + 0.5D, rs.getDouble(6), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testOrderOfArithmeticOperations() {
    // Priority from high to low:
    //   1. exponentiation and root extraction (not supported yet)
    //   2. multiplication and division
    //   3. addition and subtraction
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String query =
          "SELECT 1 + s1 * 2 + 1, (1 + s1) * 2 + 1, (1 + s1) * (2 + 1)  FROM root.vehicle.d1";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(2 * i + 2.0D, rs.getDouble(2), 0.01);
          Assert.assertEquals(2 * i + 3.0D, rs.getDouble(3), 0.01);
          Assert.assertEquals(3 * i + 3.0D, rs.getDouble(4), 0.01);
        }
        Assert.assertFalse(rs.next());
      }

      query = "SELECT 1 - s1 / 2 + 1, (1 - s1) / 2 + 1, (1 - s1) / (2 + 1)  FROM root.vehicle.d1";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(2.0D - i / 2.0D, rs.getDouble(2), 0.01);
          Assert.assertEquals(1.5 - i / 2.0D, rs.getDouble(3), 0.01);
          Assert.assertEquals((1.0D / 3.0D) * (1.0D - i), rs.getDouble(4), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
