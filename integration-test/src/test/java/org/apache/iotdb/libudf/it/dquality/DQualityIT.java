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

package org.apache.iotdb.libudf.it.dquality;

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
public class DQualityIT {
  protected static final int ITERATION_TIMES = 10_000;

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
    double x = -100d, y = 100d; // borders of random value
    long a = 0, b = 1000000000;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
                (int) Math.floor(a + Math.random() * b % (b - a + 1)),
                (int) Math.floor(x + Math.random() * y % (y - x + 1)),
                (int) Math.floor(x + Math.random() * y % (y - x + 1))));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%f,%f)",
                (int) Math.floor(a + Math.random() * b % (b - a + 1)),
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
      statement.execute(
          "create function completeness as 'org.apache.iotdb.library.dquality.UDTFCompleteness'");
      statement.execute(
          "create function timeliness as 'org.apache.iotdb.library.dquality.UDTFTimeliness'");
      statement.execute(
          "create function consistency as 'org.apache.iotdb.library.dquality.UDTFConsistency'");
      statement.execute(
          "create function validity as 'org.apache.iotdb.library.dquality.UDTFValidity'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCompleteness1() {
    String sqlStr = "select completeness(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCompleteness2() {
    String sqlStr = "select completeness(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCompleteness3() {
    String sqlStr = "select completeness(d2.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCompleteness4() {
    String sqlStr = "select completeness(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTimeliness1() {
    String sqlStr = "select timeliness(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTimeliness2() {
    String sqlStr = "select timeliness(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTimeliness3() {
    String sqlStr = "select timeliness(d2.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTimeliness4() {
    String sqlStr = "select timeliness(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsistency1() {
    String sqlStr = "select consistency(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsistency2() {
    String sqlStr = "select consistency(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsistency3() {
    String sqlStr = "select consistency(d2.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConsistency4() {
    String sqlStr = "select consistency(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValidity1() {
    String sqlStr = "select validity(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValidity2() {
    String sqlStr = "select validity(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValidity3() {
    String sqlStr = "select validity(d2.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValidity4() {
    String sqlStr = "select validity(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result = resultSet.getDouble(2);
      Assert.assertTrue(result >= -0.0D && result <= 1.0D);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
