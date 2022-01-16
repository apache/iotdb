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

package org.apache.iotdb.library.series;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SeriesTest {
  protected static final int ITERATION_TIMES = 10_000;

  protected static final long TIMESTAMP_INTERVAL = 60; // gap = 60ms
  
  protected static final long START_TIMESTAMP = 0;
  
  protected static final long END_TIMESTAMP = START_TIMESTAMP + ITERATION_TIMES * ITERATION_TIMES;

  @BeforeClass
  public static void setUp() throws Exception {
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(5)
        .setUdfTransformerMemoryBudgetInMB(5)
        .setUdfReaderMemoryBudgetInMB(5);
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
    registerUDF();
  }
  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s2"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }
  
  private static void generateData() {
    double x = -100d, y = 100d; // borders of random value
    long t = START_TIMESTAMP;
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        t = t + TIMESTAMP_INTERVAL;
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
                t,
                (int) Math.floor(x + Math.random() * y % (y - x + 1)),
                (int) Math.floor(x + Math.random() * y % (y - x + 1))));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%f,%f)",
                t,
                x + Math.random() * y % (y - x + 1),
                x + Math.random() * y % (y - x + 1))));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
          session.execute(
            "create function ConsecutiveSequences as 'org.apache.iotdb.library.series.UDTFConsecutiveSequences'");
          session.execute(
            "create function ConsecutiveWindows as 'org.apache.iotdb.library.series.UDTFConsecutiveWindows'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(100)
        .setUdfTransformerMemoryBudgetInMB(100)
        .setUdfReaderMemoryBudgetInMB(100);
  }
  
  @Test
  public void testConsecutiveSequences1() {
    String sqlStr = "select ConsecutiveSequences(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      assert resultSetLength > 0;
      while (resultSet.next()) {
        long timeStamp = Long.parseLong(resultSet.getString(0));
        int value = Integer.parseInt(resultSet.getString(1));
        assert timeStamp >= START_TIMESTAMP;
        assert timeStamp <= END_TIMESTAMP;
        assert value <= ITERATION_TIMES;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveSequences2() {
    String sqlStr = "select ConsecutiveSequences(d2.s1,d2.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      assert resultSetLength > 0;
      while (resultSet.next()) {
        long timeStamp = Long.parseLong(resultSet.getString(0));
        int value = Integer.parseInt(resultSet.getString(1));
        assert timeStamp >= START_TIMESTAMP;
        assert timeStamp <= END_TIMESTAMP;
        assert value <= ITERATION_TIMES;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveSequences3() {
    String sqlStr = "select ConsecutiveSequences(d1.s1,d1.s2,\"gap\"=\"60ms\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      assert resultSetLength > 0;
      while (resultSet.next()) {
        long timeStamp = Long.parseLong(resultSet.getString(0));
        int value = Integer.parseInt(resultSet.getString(1));
        assert timeStamp >= START_TIMESTAMP;
        assert timeStamp <= END_TIMESTAMP;
        assert value <= ITERATION_TIMES;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveSequences4() {
    String sqlStr = "select ConsecutiveSequences(d2.s1,d2.s2,\"gap\"=\"60ms\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      assert resultSetLength > 0;
      while (resultSet.next()) {
        long timeStamp = Long.parseLong(resultSet.getString(0));
        int value = Integer.parseInt(resultSet.getString(1));
        assert timeStamp >= START_TIMESTAMP;
        assert timeStamp <= END_TIMESTAMP;
        assert value <= ITERATION_TIMES;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveWindows1() {
    String sqlStr = "select ConsecutiveWindows(d1.s1,d1.s2,\"length\"=\"180ms\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      assert resultSetLength > 0;
      while (resultSet.next()) {
        long timeStamp = Long.parseLong(resultSet.getString(0));
        int value = Integer.parseInt(resultSet.getString(1));
        assert timeStamp >= START_TIMESTAMP;
        assert timeStamp <= END_TIMESTAMP;
        assert value <= ITERATION_TIMES;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveWindows2() {
    String sqlStr = "select ConsecutiveWindows(d2.s1,d2.s2,\"length\"=\"180ms\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      assert resultSetLength > 0;
      while (resultSet.next()) {
        long timeStamp = Long.parseLong(resultSet.getString(0));
        int value = Integer.parseInt(resultSet.getString(1));
        assert timeStamp >= START_TIMESTAMP;
        assert timeStamp <= END_TIMESTAMP;
        assert value <= ITERATION_TIMES;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveWindows3() {
    String sqlStr = "select ConsecutiveWindows(d1.s1,d1.s2,\"length\"=\"180ms\",\"gap\"=\"60ms\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      assert resultSetLength > 0;
      while (resultSet.next()) {
        long timeStamp = Long.parseLong(resultSet.getString(0));
        int value = Integer.parseInt(resultSet.getString(1));
        assert timeStamp >= START_TIMESTAMP;
        assert timeStamp <= END_TIMESTAMP;
        assert value <= ITERATION_TIMES;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveWindows4() {
    String sqlStr = "select ConsecutiveWindows(d2.s1,d2.s2,\"length\"=\"180ms\",\"gap\"=\"60ms\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      assert resultSetLength > 0;
      while (resultSet.next()) {
        long timeStamp = Long.parseLong(resultSet.getString(0));
        int value = Integer.parseInt(resultSet.getString(1));
        assert timeStamp >= START_TIMESTAMP;
        assert timeStamp <= END_TIMESTAMP;
        assert value <= ITERATION_TIMES;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}