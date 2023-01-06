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

package org.apache.iotdb.libudf.it.string;

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
public class StringIT {

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
              + "datatype=text, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s1 with "
              + "datatype=text, "
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
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              100, "\"[192.168.0.1] [SUCCESS]\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              200, "\"[192.168.0.24] [SUCCESS]\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              300, "\"[192.168.0.2] [FAIL]\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              400, "\"[192.168.0.5] [SUCCESS]\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              500, "\"[192.168.0.124] [SUCCESS]\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 100, "\"A,B,A+,B-\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 200, "\"A,A+,A,B+\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 300, "\"B+,B,B\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 400, "\"A+,A,A+,A\""));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 500, "\"A,B-,B,B\""));
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function regexmatch as 'org.apache.iotdb.library.string.UDTFRegexMatch'");
      statement.execute(
          "create function regexreplace as 'org.apache.iotdb.library.string.UDTFRegexReplace'");
      statement.execute(
          "create function regexsplit as 'org.apache.iotdb.library.string.UDTFRegexSplit'");
      statement.execute(
          "create function strreplace as 'org.apache.iotdb.library.string.UDTFStrReplace'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRegexMatch1() {
    String sqlStr =
        "select regexmatch(d1.s1,\"regex\"=\"\\d+\\.\\d+\\.\\d+\\.\\d+\", \"group\"=\"0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(2);
      resultSet.next();
      String result2 = resultSet.getString(2);
      resultSet.next();
      String result3 = resultSet.getString(2);
      resultSet.next();
      String result4 = resultSet.getString(2);
      resultSet.next();
      String result5 = resultSet.getString(2);
      Assert.assertEquals("192.168.0.1", result1);
      Assert.assertEquals("192.168.0.24", result2);
      Assert.assertEquals("192.168.0.2", result3);
      Assert.assertEquals("192.168.0.5", result4);
      Assert.assertEquals("192.168.0.124", result5);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexReplace1() {
    String sqlStr =
        "select regexreplace(d1.s1,\"regex\"=\"192\\.168\\.0\\.(\\d+)\", \"replace\"=\"cluster-$1\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(2);
      resultSet.next();
      String result2 = resultSet.getString(2);
      resultSet.next();
      String result3 = resultSet.getString(2);
      resultSet.next();
      String result4 = resultSet.getString(2);
      resultSet.next();
      String result5 = resultSet.getString(2);
      Assert.assertEquals("[cluster-1] [SUCCESS]", result1);
      Assert.assertEquals("[cluster-24] [SUCCESS]", result2);
      Assert.assertEquals("[cluster-2] [FAIL]", result3);
      Assert.assertEquals("[cluster-5] [SUCCESS]", result4);
      Assert.assertEquals("[cluster-124] [SUCCESS]", result5);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexSplit1() {
    String sqlStr = "select regexsplit(d2.s1, \"regex\"=\",\", \"index\"=\"-1\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      int result1 = resultSet.getInt(2);
      resultSet.next();
      int result2 = resultSet.getInt(2);
      resultSet.next();
      int result3 = resultSet.getInt(2);
      resultSet.next();
      int result4 = resultSet.getInt(2);
      resultSet.next();
      int result5 = resultSet.getInt(2);
      Assert.assertEquals(4, result1);
      Assert.assertEquals(4, result2);
      Assert.assertEquals(3, result3);
      Assert.assertEquals(4, result4);
      Assert.assertEquals(4, result5);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testStrReplace1() {
    String sqlStr =
        "select strreplace(d2.s1,\"target\"=\",\", \"replace\"=\"/\", \"limit\"=\"2\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(2);
      resultSet.next();
      String result2 = resultSet.getString(2);
      resultSet.next();
      String result3 = resultSet.getString(2);
      resultSet.next();
      String result4 = resultSet.getString(2);
      resultSet.next();
      String result5 = resultSet.getString(2);
      Assert.assertEquals("A/B/A+,B-", result1);
      Assert.assertEquals("A/A+/A,B+", result2);
      Assert.assertEquals("B+/B/B", result3);
      Assert.assertEquals("A+/A/A+,A", result4);
      Assert.assertEquals("A/B-/B,B", result5);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
