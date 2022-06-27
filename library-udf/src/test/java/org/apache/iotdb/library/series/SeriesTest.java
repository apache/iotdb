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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class SeriesTest {
  protected static final int ITERATION_TIMES = 10_000;

  protected static final long TIMESTAMP_INTERVAL = 60; // gap = 60ms

  protected static final long START_TIMESTAMP = 0;

  protected static final long END_TIMESTAMP = START_TIMESTAMP + ITERATION_TIMES * ITERATION_TIMES;

  private static final float oldUdfCollectorMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();
  private static final float oldUdfTransformerMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  private static final float oldUdfReaderMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();

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
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // d1
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577808000, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577808300, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577808600, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577809200, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577809500, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577809800, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577810100, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%d)",
              1577810400, 1)); // s2 == null
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577810700, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", 1577811000, 1, 1));
      // d2
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577808000, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577808300, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577808600, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577809200, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577809500, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577809800, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577810100, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%d)",
              1577810400, 1)); // s2 == null
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577810700, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", 1577811000, 1, 1));

    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function ConsecutiveSequences as 'org.apache.iotdb.library.series.UDTFConsecutiveSequences'");
      statement.execute(
          "create function ConsecutiveWindows as 'org.apache.iotdb.library.series.UDTFConsecutiveWindows'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(oldUdfCollectorMemoryBudgetInMB)
        .setUdfTransformerMemoryBudgetInMB(oldUdfTransformerMemoryBudgetInMB)
        .setUdfReaderMemoryBudgetInMB(oldUdfReaderMemoryBudgetInMB);
  }

  @Test
  public void testConsecutiveSequences1() {
    String sqlStr = "select ConsecutiveSequences(d1.s1,d1.s2) from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 3);

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577808000);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809200);
      Assert.assertEquals(value, 4);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577810700);
      Assert.assertEquals(value, 2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveSequences2() {
    String sqlStr = "select ConsecutiveSequences(d2.s1,d2.s2) from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 3);

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577808000);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809200);
      Assert.assertEquals(value, 4);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577810700);
      Assert.assertEquals(value, 2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveSequences3() {
    String sqlStr = "select ConsecutiveSequences(d1.s1,d1.s2,'gap'='5m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 3);

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577808000);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809200);
      Assert.assertEquals(value, 4);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577810700);
      Assert.assertEquals(value, 2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveSequences4() {
    String sqlStr = "select ConsecutiveSequences(d2.s1,d2.s2,'gap'='5m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 3);

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577808000);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809200);
      Assert.assertEquals(value, 4);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577810700);
      Assert.assertEquals(value, 2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveWindows1() {
    String sqlStr = "select ConsecutiveWindows(d1.s1,d1.s2,'length'='10m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 3);

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577808000);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809200);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809500);
      Assert.assertEquals(value, 3);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveWindows2() {
    String sqlStr = "select ConsecutiveWindows(d2.s1,d2.s2,'length'='10m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 3);

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577808000);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809200);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809500);
      Assert.assertEquals(value, 3);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveWindows3() {
    String sqlStr =
        "select ConsecutiveWindows(d1.s1,d1.s2,'length'='10m','gap'='5m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 3);

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577808000);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809200);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809500);
      Assert.assertEquals(value, 3);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsecutiveWindows4() {
    String sqlStr =
        "select ConsecutiveWindows(d2.s1,d2.s2,'length'='10m','gap'='5m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 3);

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577808000);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809200);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = Long.parseLong(resultSet.getString(0));
      value = Integer.parseInt(resultSet.getString(1));
      Assert.assertEquals(timeStamp, 1577809500);
      Assert.assertEquals(value, 3);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
