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

import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBUDTFAlignByTimeQueryIT {

  protected static final int ITERATION_TIMES = 10_000;

  protected static final int ADDEND = 500_000_000;

  protected static final int LIMIT = (int) (0.1 * ITERATION_TIMES);
  protected static final int OFFSET = (int) (0.1 * ITERATION_TIMES);

  protected static final int SLIMIT = 10;
  protected static final int SOFFSET = 2;

  @BeforeClass
  public static void setUp() throws Exception {
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(5)
        .setUdfTransformerMemoryBudgetInMB(5)
        .setUdfReaderMemoryBudgetInMB(5);
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
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s1 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s2 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d3.s1 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d3.s2 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d4.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d4.s2 with datatype=INT32,encoding=PLAIN");
      // create aligned timeseries
      statement.execute(("CREATE STORAGE GROUP root.sg1"));
      statement.execute("CREATE ALIGNED TIMESERIES root.sg1(s1 INT32, s2 INT32)");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        statement.execute(
            (i % 3 != 0
                ? String.format(
                    "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)
                : i % 2 == 0
                    ? String.format("insert into root.vehicle.d1(timestamp,s1) values(%d,%d)", i, i)
                    : String.format(
                        "insert into root.vehicle.d1(timestamp,s2) values(%d,%d)", i, i)));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d3(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d4(timestamp,s1) values(%d,%d)", 2 * i, 3 * i)));
        statement.execute(
            (String.format(
                "insert into root.sg1(timestamp,s1, s2) aligned values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      statement.execute(
          "create function multiplier as 'org.apache.iotdb.db.query.udf.example.Multiplier'");
      statement.execute("create function max as 'org.apache.iotdb.db.query.udf.example.Max'");
      statement.execute(
          "create function terminate as 'org.apache.iotdb.db.query.udf.example.TerminateTester'");
      statement.execute(
          "create function validate as 'org.apache.iotdb.db.query.udf.example.ValidateTester'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(100)
        .setUdfTransformerMemoryBudgetInMB(100)
        .setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void queryWithValueFilter9() {
    String sqlStr =
        "select max(s1), max(s2) from root.vehicle.d4"
            + String.format(
                " where root.vehicle.d4.s1 >= %d and root.vehicle.d4.s1 < %d ",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 2, resultSet.getMetaData().getColumnCount());

      assertEquals("Time", resultSet.getMetaData().getColumnName(1));

      String columnS1 = "max(root.vehicle.d4.s1)";
      String columnS2 = "max(root.vehicle.d4.s2)";
      assertTrue(
          columnS1.equals(resultSet.getMetaData().getColumnName(2))
              || columnS2.equals(resultSet.getMetaData().getColumnName(2)));
      assertTrue(
          columnS1.equals(resultSet.getMetaData().getColumnName(3))
              || columnS2.equals(resultSet.getMetaData().getColumnName(3)));

      assertTrue(resultSet.next());
      assertEquals(
          (int) (0.7 * ITERATION_TIMES) - 1, Integer.parseInt(resultSet.getString(columnS1)));
      assertNull(resultSet.getString(columnS2));
      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter10() {
    String sqlStr =
        "select terminate(s1), terminate(s2) from root.vehicle.d4"
            + String.format(
                " where root.vehicle.d4.s1 >= %d and root.vehicle.d4.s1 < %d ",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 2, resultSet.getMetaData().getColumnCount());

      assertEquals("Time", resultSet.getMetaData().getColumnName(1));

      String columnS1 = "terminate(root.vehicle.d4.s1)";
      String columnS2 = "terminate(root.vehicle.d4.s2)";
      assertTrue(
          columnS1.equals(resultSet.getMetaData().getColumnName(2))
              || columnS2.equals(resultSet.getMetaData().getColumnName(2)));
      assertTrue(
          columnS1.equals(resultSet.getMetaData().getColumnName(3))
              || columnS2.equals(resultSet.getMetaData().getColumnName(3)));

      for (int i = 0; i < (int) ((0.7 - 0.3) * ITERATION_TIMES) / 3 + 1; ++i) {
        assertTrue(resultSet.next());
        assertEquals(1, Integer.parseInt(resultSet.getString(columnS1)));
        assertNull(resultSet.getString(columnS2));
      }

      assertTrue(resultSet.next());
      assertEquals(
          (int) ((0.7 - 0.3) * ITERATION_TIMES) / 3 + 1,
          Integer.parseInt(resultSet.getString(columnS1)));
      assertNull(resultSet.getString(columnS2));

      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
