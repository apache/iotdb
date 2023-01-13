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

package org.apache.iotdb.libudf.it.dmatch;

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
public class DMatchIT {
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
          "create timeseries root.vehicle.d1.s5 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s6 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s7 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s8 with "
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
      statement.addBatch(
          "create timeseries root.vehicle.d2.s5 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s6 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s7 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s8 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s4 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s4 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s5 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s6 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s7 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s8 with "
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
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              100, 100, 100, 100, 100, 101, 101, 101, 101));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              100, 100, 100, 100, 100, 101, 101, 101, 101));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              200, 102, 102, 102, 102, 101, 101, 101, 101));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              300, 104, 104, 104, 104, 102, 102, 102, 102));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              400, 126, 126, 126, 126, 102, 102, 102, 102));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              500, 108, 108, 108, 108, 103, 103, 103, 103));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              600, 112, 112, 112, 112, 104, 104, 104, 104));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              700, 114, 114, 114, 114, 104, 104, 104, 104));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              800, 116, 116, 116, 116, 105, 105, 105, 105));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              900, 118, 118, 118, 118, 105, 105, 105, 105));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              1000, 100, 100, 100, 100, 106, 106, 106, 106));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              1100, 124, 124, 124, 124, 108, 108, 108, 108));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              1200, 126, 126, 126, 126, 108, 108, 108, 108));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              1300, 116, 116, 116, 116, 105, 105, 105, 105));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              100, 1, 1, 1, 1, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              200, 1, 1, 1, 1, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              300, 1, 1, 1, 1, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              400, 1, 1, 1, 1, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              500, 1, 1, 1, 1, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              600, 1, 1, 1, 1, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              700, 1, 1, 1, 1, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              800, 1, 1, 1, 1, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, 3, 3, 3, 3));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 3, 3, 3, 3));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1100, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              100, 0, 0, 0, 0, 6, 6, 6, 6));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              200, 2, 2, 2, 2, 7, 7, 7, 7));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              300, 3, 3, 3, 3, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              400, 4, 4, 4, 4, 9, 9, 9, 9));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              500, 5, 5, 5, 5, 10, 10, 10, 10));
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function cov as 'org.apache.iotdb.library.dmatch.UDAFCov'");
      statement.execute("create function dtw as 'org.apache.iotdb.library.dmatch.UDAFDtw'");
      statement.execute("create function pearson as 'org.apache.iotdb.library.dmatch.UDAFPearson'");
      statement.execute("create function ptnsym as 'org.apache.iotdb.library.dmatch.UDTFPtnSym'");
      statement.execute("create function xcorr as 'org.apache.iotdb.library.dmatch.UDTFXCorr'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testCov1() {
    String sqlStr = "select cov(d1.s1,d1.s5) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(12.291666666666666, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testCov2() {
    String sqlStr = "select cov(d1.s2,d1.s6) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(12.291666666666666, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testCov3() {
    String sqlStr = "select cov(d1.s3,d1.s7) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(12.291666666666666, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testCov4() {
    String sqlStr = "select cov(d1.s4,d1.s8) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(12.291666666666666, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw1() {
    String sqlStr = "select dtw(d2.s1,d2.s5) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(8.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw2() {
    String sqlStr = "select dtw(d2.s2,d2.s6) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(8.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw3() {
    String sqlStr = "select dtw(d2.s3,d2.s7) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(8.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw4() {
    String sqlStr = "select dtw(d2.s4,d2.s8) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(8.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson1() {
    String sqlStr = "select pearson(d1.s1,d1.s5) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(0.5630881927754872, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson2() {
    String sqlStr = "select pearson(d1.s2,d1.s6) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(0.5630881927754872, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson3() {
    String sqlStr = "select pearson(d1.s3,d1.s7) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(0.5630881927754872, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson4() {
    String sqlStr = "select pearson(d1.s4,d1.s8) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(0.5630881927754872, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testPthSym1() {
    String sqlStr = "select ptnsym(d1.s1, 'window'='3', 'threshold'='0') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(0.0, result2, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testPtnSym2() {
    String sqlStr = "select ptnsym(d1.s2, 'window'='3', 'threshold'='0') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(0.0, result2, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testPtnSym3() {
    String sqlStr = "select ptnsym(d2.s1, 'window'='3', 'threshold'='0') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(0.0, result2, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testPtnSym4() {
    String sqlStr = "select ptnsym(d2.s2, 'window'='3', 'threshold'='0') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(0.0, result2, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testXCorr1() {
    String sqlStr = "select xcorr(d4.s1,d4.s5) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      resultSet.next();
      double result9 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(4.0, result2, 0.01);
      Assert.assertEquals(9.6, result3, 0.01);
      Assert.assertEquals(13.4, result4, 0.01);
      Assert.assertEquals(20.0, result5, 0.01);
      Assert.assertEquals(15.6, result6, 0.01);
      Assert.assertEquals(9.2, result7, 0.01);
      Assert.assertEquals(11.8, result8, 0.01);
      Assert.assertEquals(6.0, result9, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testXCorr2() {
    String sqlStr = "select xcorr(d4.s2, d4.s6) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      resultSet.next();
      double result9 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(4.0, result2, 0.01);
      Assert.assertEquals(9.6, result3, 0.01);
      Assert.assertEquals(13.4, result4, 0.01);
      Assert.assertEquals(20.0, result5, 0.01);
      Assert.assertEquals(15.6, result6, 0.01);
      Assert.assertEquals(9.2, result7, 0.01);
      Assert.assertEquals(11.8, result8, 0.01);
      Assert.assertEquals(6.0, result9, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testXCorr3() {
    String sqlStr = "select xcorr(d4.s3, d4.s7) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      resultSet.next();
      double result9 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(4.0, result2, 0.01);
      Assert.assertEquals(9.6, result3, 0.01);
      Assert.assertEquals(13.4, result4, 0.01);
      Assert.assertEquals(20.0, result5, 0.01);
      Assert.assertEquals(15.6, result6, 0.01);
      Assert.assertEquals(9.2, result7, 0.01);
      Assert.assertEquals(11.8, result8, 0.01);
      Assert.assertEquals(6.0, result9, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testXCorr4() {
    String sqlStr = "select xcorr(d4.s4,d4.s8) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      resultSet.next();
      double result9 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(4.0, result2, 0.01);
      Assert.assertEquals(9.6, result3, 0.01);
      Assert.assertEquals(13.4, result4, 0.01);
      Assert.assertEquals(20.0, result5, 0.01);
      Assert.assertEquals(15.6, result6, 0.01);
      Assert.assertEquals(9.2, result7, 0.01);
      Assert.assertEquals(11.8, result8, 0.01);
      Assert.assertEquals(6.0, result9, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
