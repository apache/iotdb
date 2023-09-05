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

package org.apache.iotdb.libudf.it.dlearn;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
public class DLearnIT {

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
          "create timeseries root.vehicle.d1.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s4 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s4 with "
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
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, -4, -4, -4, -4));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, -3, -3, -3, -3));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, -2, -2, -2, -2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 3, 3, 3, 3));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 4, 4, 4, 4));

      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, -4, -4, -4, -4));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, -3, -3, -3, -3));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, -2, -2, -2, -2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 3, 3, 3, 3));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 4, 4, 4, 4));
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function iqr as 'org.apache.iotdb.library.anomaly.UDTFIQR'");
      statement.execute("create function ar as 'org.apache.iotdb.library.dlearn.UDTFAR'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAR1() {
    String sqlStr = "select ar(d1.s1, \"p\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(0.943, result1, 0.01);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(-0.257, result2, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    sqlStr = "select ar(d2.s1, \"p\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      Assert.assertEquals(0.943, result3, 0.01);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      Assert.assertEquals(-0.257, result4, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testAR2() {
    String sqlStr = "select ar(d1.s2, \"p\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(0.943, result1, 0.01);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(-0.257, result2, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    sqlStr = "select ar(d2.s2, \"p\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      Assert.assertEquals(0.943, result3, 0.01);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      Assert.assertEquals(-0.257, result4, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testAR3() {
    String sqlStr = "select ar(d1.s3, \"p\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(0.943, result1, 0.01);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(-0.257, result2, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    sqlStr = "select ar(d2.s3, \"p\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      Assert.assertEquals(0.943, result3, 0.01);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      Assert.assertEquals(-0.257, result4, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testAR4() {
    String sqlStr = "select ar(d1.s4, \"p\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(0.943, result1, 0.01);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(-0.257, result2, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    sqlStr = "select ar(d2.s4, \"p\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      Assert.assertEquals(0.943, result3, 0.01);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      Assert.assertEquals(-0.257, result4, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
