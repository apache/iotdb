/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.it.env.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.constant.UDFTestConstant;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
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
      statement.execute("SET STORAGE GROUP TO root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s2 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 4; ++i) {
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
            UDFTestConstant.ACCESS_STRATEGY_KEY, UDFTestConstant.ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("Raw data and aggregation hybrid query is not supported."));
    }
  }

  @Test
  public void testUserDefinedFunctionFillFunctionHybridQuery() {
    String sql =
        String.format(
            "select temperature, counter(temperature, '%s'='%s') from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float [linear, 1m, 1m])",
            UDFTestConstant.ACCESS_STRATEGY_KEY, UDFTestConstant.ACCESS_STRATEGY_ROW_BY_ROW);

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
            UDFTestConstant.ACCESS_STRATEGY_KEY, UDFTestConstant.ACCESS_STRATEGY_ROW_BY_ROW);

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
    String[] retArray =
        new String[] {
          "0,root.vehicle.d1,1.0,0.0",
          "1,root.vehicle.d1,2.0,0.8414709848078965",
          "2,root.vehicle.d1,3.0,0.9092974268256817",
          "3,root.vehicle.d1,4.0,0.1411200080598672"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select s1 + 1, sin(s2) from root.vehicle.* align by device")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString("s1 + 1")
                  + ","
                  + resultSet.getString("sin(s2)");
          System.out.println(ans);
          cnt++;
        }
        // assertEquals(retSet.size(), cnt);
      }
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
    }
  }
}
