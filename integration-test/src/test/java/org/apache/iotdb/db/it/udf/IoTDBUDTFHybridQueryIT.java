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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.constant.UDFTestConstant;

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

import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDTFHybridQueryIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
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
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s2 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 5; ++i) {
        statement.execute(
            (String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
      for (int i = 2; i < 4; ++i) {
        statement.execute(
            (String.format("insert into root.vehicle.d1(timestamp,s3) values(%d,%d)", i, i)));
      }
      for (int i = 7; i < 10; ++i) {
        statement.execute(
            (String.format("insert into root.vehicle.d1(timestamp,s3) values(%d,%d)", i, i)));
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
    EnvFactory.getEnv().cleanClusterEnvironment();
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
    } catch (SQLException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("not supported"));
    }
  }

  @Test
  public void testUserDefinedBuiltInHybridAggregationQuery2() {
    String[] retArray = new String[] {"2.0,0.9092974268256817,3.0,-10.0,12.0"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(s1), sin(avg(s2)), avg(s1) + 1, -sum(s2), avg(s1) + sum(s2) from root.vehicle.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString("avg(root.vehicle.d1.s1)")
                  + ","
                  + resultSet.getString("sin(avg(root.vehicle.d1.s2))")
                  + ","
                  + resultSet.getString("avg(root.vehicle.d1.s1) + 1")
                  + ","
                  + resultSet.getString("-sum(root.vehicle.d1.s2)")
                  + ","
                  + resultSet.getString("avg(root.vehicle.d1.s1) + sum(root.vehicle.d1.s2)");
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testUserDefinedFunctionFillFunctionHybridQuery() {
    String[] retArray =
        new String[] {
          "0,0.0,1.0,3.14",
          "1,0.8414709848078965,2.0,3.14",
          "2,0.9092974268256817,3.0,2.0",
          "3,0.1411200080598672,4.0,3.0",
          "4,-0.7568024953079282,5.0,3.14",
          "7,3.14,3.14,7.0",
          "8,3.14,3.14,8.0",
          "9,3.14,3.14,9.0"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select sin(s1), s1 + 1, s3 from root.vehicle.d1 fill(3.14)")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("sin(root.vehicle.d1.s1)")
                  + ","
                  + resultSet.getString("root.vehicle.d1.s1 + 1")
                  + ","
                  + resultSet.getString("root.vehicle.d1.s3");
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
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
  @Ignore // TODO remove after IOTDB-3674
  public void testUserDefinedFunctionAlignByDeviceQuery() {
    String[] retArray =
        new String[] {
          "0,root.vehicle.d1,1.0,0.0",
          "1,root.vehicle.d1,2.0,0.8414709848078965",
          "2,root.vehicle.d1,3.0,0.9092974268256817",
          "3,root.vehicle.d1,4.0,0.1411200080598672",
          "4,root.vehicle.d1,5.0,0.1411200080598672",
          "0,root.vehicle.d2,1.0,0.0",
          "1,root.vehicle.d2,2.0,0.8414709848078965",
          "2,root.vehicle.d2,3.0,0.9092974268256817",
          "3,root.vehicle.d2,4.0,0.1411200080598672",
          "4,root.vehicle.d2,5.0,0.1411200080598672"
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
                  + resultSet.getString(ColumnHeaderConstant.DEVICE)
                  + ","
                  + resultSet.getString("s1 + 1")
                  + ","
                  + resultSet.getString("sin(s2)");
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testNestedMappableAndNonMappableUDF() {
    String[] retArray =
        new String[] {
          "0,0", "1,1", "2,2",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select abs(m4(s1, \"windowSize\" = \"1\")) from root.vehicle.d1 where time <=2")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(resultSet.getMetaData().getColumnName(2));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
    }
  }
}
