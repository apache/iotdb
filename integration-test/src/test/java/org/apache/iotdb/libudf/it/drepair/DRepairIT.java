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

package org.apache.iotdb.libudf.it.drepair;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
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
public class DRepairIT {
  protected static final int ITERATION_TIMES = 1_000;
  protected static final int DELTA_T = 100;

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
          "create timeseries root.vehicle.d2.s1 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s1 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    double x = -100d, y = 100d; // borders of random value
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        if (Math.random() < 0.99) {
          statement.execute(
              String.format(
                  "insert into root.vehicle.d1(timestamp,s1) values(%d,%d)",
                  (long) i * DELTA_T, (int) Math.floor(x + Math.random() * y % (y - x + 1))));
        } else {
          statement.execute(
              String.format(
                  "insert into root.vehicle.d1(timestamp,s1) values(%d,%d)",
                  (long) i * DELTA_T + (long) Math.floor((Math.random() - 0.5) * DELTA_T),
                  (int) Math.floor(x + Math.random() * y % (y - x + 1))));
        }
      }
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        if (Math.random() < 0.97) {
          statement.execute(
              String.format(
                  "insert into root.vehicle.d2(timestamp,s1) values(%d,%d)",
                  (long) i * DELTA_T, (long) Math.floor(x + Math.random() * y % (y - x + 1))));
        } else {
          statement.execute(
              String.format(
                  "insert into root.vehicle.d2(timestamp,s1) values(%d,%d)",
                  (long) i * DELTA_T, 0));
        }
      }
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d3(timestamp,s1) values(%d,%f)",
                (long) i * DELTA_T,
                i / (double) ITERATION_TIMES * (y - x) + (Math.random() - 0.5) * (y - x)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function timestamprepair as 'org.apache.iotdb.library.drepair.UDTFTimestampRepair'");
      statement.execute(
          "create function valuefill as 'org.apache.iotdb.library.drepair.UDTFValueFill'");
      statement.execute(
          "create function valuerepair as 'org.apache.iotdb.library.drepair.UDTFValueRepair'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTimestampRepair1() {
    String sqlStr =
        String.format("select timestamprepair(d1.s1,'interval'='%d') from root.vehicle", DELTA_T);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTimestampRepair2() {
    String sqlStr = "select timestamprepair(d1.s1,'method'='median') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTimestampRepair3() {
    String sqlStr = "select timestamprepair(d1.s1,'method'='mode') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTimestampRepair4() {
    String sqlStr = "select timestamprepair(d1.s1,'method'='cluster') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValueFill1() {
    String sqlStr = "select valuefill(d2.s1,'method'='previous') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValueFill2() {
    String sqlStr = "select valuefill(d2.s1,'method'='linear') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValueFill3() {
    String sqlStr = "select valuefill(d2.s1,'method'='likelihood') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testValueFill4() {
    String sqlStr = "select valuefill(d2.s1,'method'='ar') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValueFill5() {
    String sqlStr = "select valuefill(d2.s1,'method'='mean') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValueFill6() {
    String sqlStr = "select valuefill(d2.s1,'method'='screen') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValueRepair1() {
    String sqlStr = "select valuerepair(d3.s1,'method'='screen') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testValueRepair2() {
    String sqlStr = "select valuerepair(d3.s1,'method'='lsgreedy') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
