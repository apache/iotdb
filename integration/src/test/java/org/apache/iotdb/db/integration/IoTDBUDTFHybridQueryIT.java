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

import org.apache.iotdb.db.query.udf.example.ExampleUDFConstant;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBUDTFHybridQueryIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 10; ++i) {
        statement.execute(
            (String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function counter as 'org.apache.iotdb.db.query.udf.example.Counter'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testUserDefinedBuiltInHybridAggregationQuery() {
    String sql =
        String.format(
            "select count(*), counter(s1, '%s'='%s') from root.vehicle.d1",
            ExampleUDFConstant.ACCESS_STRATEGY_KEY, ExampleUDFConstant.ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("User-defined and built-in hybrid aggregation is not supported together."));
    }
  }

  @Test
  public void testUserDefinedFunctionFillFunctionHybridQuery() {
    String sql =
        String.format(
            "select temperature, counter(temperature, '%s'='%s') from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float [linear, 1m, 1m])",
            ExampleUDFConstant.ACCESS_STRATEGY_KEY, ExampleUDFConstant.ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable.getMessage().contains("Fill functions are not supported in UDF queries."));
    }
  }

  @Test
  public void testLastUserDefinedFunctionQuery() {
    String sql =
        String.format(
            "select last counter(temperature, '%s'='%s') from root.sgcc.wf03.wt01",
            ExampleUDFConstant.ACCESS_STRATEGY_KEY, ExampleUDFConstant.ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable.getMessage().contains("Last queries can only be applied on raw time series."));
    }
  }

  @Test
  public void testUserDefinedFunctionAlignByDeviceQuery() {
    String sql =
        String.format(
            "select adder(temperature), counter(temperature, '%s'='%s') from root.sgcc.wf03.wt01 align by device",
            ExampleUDFConstant.ACCESS_STRATEGY_KEY, ExampleUDFConstant.ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("ALIGN BY DEVICE clause is not supported in UDF queries."));
    }
  }
}
