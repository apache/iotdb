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

package org.apache.iotdb.libudf.it.series;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class SeriesIT {
  protected static final int ITERATION_TIMES = 10_000;

  protected static final long TIMESTAMP_INTERVAL = 60; // gap = 60ms

  protected static final long START_TIMESTAMP = 0;

  protected static final long END_TIMESTAMP = START_TIMESTAMP + ITERATION_TIMES * ITERATION_TIMES;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch("create database root.vehicle");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s1 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s2 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // d1
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577808000000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577808300000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577808600000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577809200000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577809500000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577809800000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577810100000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%d)",
              1577810400000L, 1)); // s2 == null
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577810700000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
              1577811000000L, 1, 1));
      // d2
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577808000000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577808300000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577808600000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577809200000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577809500000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577809800000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577810100000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%d)",
              1577810400000L, 1)); // s2 == null
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577810700000L, 1, 1));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",
              1577811000000L, 1, 1));

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
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testConsecutiveSequences1() {
    String sqlStr = "select ConsecutiveSequences(d1.s1,d1.s2) from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      resultSet.next();
      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577808000000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809200000L);
      Assert.assertEquals(value, 4);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577810700000L);
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
      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577808000000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809200000L);
      Assert.assertEquals(value, 4);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577810700000L);
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
      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577808000000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809200000L);
      Assert.assertEquals(value, 4);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577810700000L);
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
      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577808000000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809200000L);
      Assert.assertEquals(value, 4);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577810700000L);
      Assert.assertEquals(value, 2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testConsecutiveWindows1() {
    String sqlStr = "select ConsecutiveWindows(d1.s1,d1.s2,'length'='10m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577808000000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809200000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809500000L);
      Assert.assertEquals(value, 3);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testConsecutiveWindows2() {
    String sqlStr = "select ConsecutiveWindows(d2.s1,d2.s2,'length'='10m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577808000000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809200000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809500000L);
      Assert.assertEquals(value, 3);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testConsecutiveWindows3() {
    String sqlStr =
        "select ConsecutiveWindows(d1.s1,d1.s2,'length'='10m','gap'='5m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577808000000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809200000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809500000L);
      Assert.assertEquals(value, 3);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testConsecutiveWindows4() {
    String sqlStr =
        "select ConsecutiveWindows(d2.s1,d2.s2,'length'='10m','gap'='5m') from root.vehicle";
    long timeStamp = 0;
    int value = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577808000000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809200000L);
      Assert.assertEquals(value, 3);

      resultSet.next();

      timeStamp = resultSet.getLong(1);
      value = resultSet.getInt(2);
      Assert.assertEquals(timeStamp, 1577809500000L);
      Assert.assertEquals(value, 3);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
