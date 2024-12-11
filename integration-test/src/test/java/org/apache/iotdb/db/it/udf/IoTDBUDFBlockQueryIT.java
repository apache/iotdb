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

package org.apache.iotdb.db.it.udf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDFBlockQueryIT {

  protected static final int ITERATION_TIMES = 10;

  private static final double E = 0.0001;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 WITH datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 WITH datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s1 WITH datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s2 WITH datatype=DOUBLE,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        statement.execute(
            (String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION two_sum AS 'org.apache.iotdb.db.query.udf.example.TwoSum'");
      statement.execute(
          "CREATE FUNCTION two_sum_block AS 'org.apache.iotdb.db.query.udf.example.TwoSumBlock'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testUDFBlockQuery() {
    String sql =
        "select two_sum_block(d1.s1, d1.s2), two_sum_block(d2.s1, d2.s2) from root.vehicle";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(3, columnCount);

      for (int i = 0; i < ITERATION_TIMES; i++) {
        resultSet.next();
        double expected = 2 * i;
        for (int j = 0; j < 2; j++) {
          String actualString = resultSet.getString(j + 2);
          // Regard both INT32 and DOUBLE as double for simplicity
          double actual = Double.parseDouble(actualString);
          assertEquals(expected, actual, E);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testUDFSingleRowQuery() {
    String sql = "select two_sum(d1.s1, d1.s2), two_sum(d2.s1, d2.s2) from root.vehicle";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(3, columnCount);

      for (int i = 0; i < ITERATION_TIMES; i++) {
        resultSet.next();
        double expected = 2 * i;
        for (int j = 0; j < 2; j++) {
          String actualString = resultSet.getString(j + 2);
          // Regard both INT32 and DOUBLE as double for simplicity
          double actual = Double.parseDouble(actualString);
          assertEquals(expected, actual, E);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testUntrustedUri() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION two_sum AS 'org.apache.iotdb.db.query.udf.example.TwoSum' USING URI 'https://alioss.timecho.com/upload/library-udf.jar'");
      fail("should fail");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("701: Untrusted uri "));
    }
  }
}
