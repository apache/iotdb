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

package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

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
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBArithmeticTableIT {

  private static final double E = 0.0001;

  private static final String[] INSERTION_SQLS = {
    "CREATE DATABASE test",
    "USE test",
    "CREATE TABLE table1 (device STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 INT32 MEASUREMENT, s5 BOOLEAN MEASUREMENT, s6 TEXT MEASUREMENT, s7 INT32 MEASUREMENT, s8 INT32 MEASUREMENT)",
    "insert into table1(device, time, s1, s2, s3, s4, s5, s6, s7) values ('d1', 1, 1, 1, 1, 1, false, '1', 1)",
    "insert into table1(device, time, s1, s2, s3, s4, s5, s6, s8) values ('d1', 2, 2, 2, 2, 2, false, '2', 2)",
    "insert into table1(device, time, s1, s2, s3, s4, s5, s6, s7) values ('d1', 3, 3, 3, 3, 3, true, '3', 3)",
    "insert into table1(device, time, s1, s2, s3, s4, s5, s6, s8) values ('d1', 4, 4, 4, 4, 4, true, '4', 4)",
    "insert into table1(device, time, s1, s2, s3, s4, s5, s6, s7, s8) values ('d1', 5, 5, 5, 5, 5, true, '5', 5, 5)",
  };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(INSERTION_SQLS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testArithmeticBinary() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("USE test");
      String[] operands = new String[] {"s1", "s2", "s3", "s4"};
      for (String operator : new String[] {" + ", " - ", " * ", " / ", " % "}) {
        List<String> expressions = new ArrayList<>();
        for (String leftOperand : operands) {
          for (String rightOperand : operands) {
            expressions.add(leftOperand + operator + rightOperand);
          }
        }
        String sql = String.format("select %s from table1", String.join(",", expressions));

        ResultSet resultSet = statement.executeQuery(sql);

        assertEquals(expressions.size(), resultSet.getMetaData().getColumnCount());

        for (int i = 1; i < 6; ++i) {
          resultSet.next();
          for (int j = 0; j < expressions.size(); ++j) {
            double expected = 0;
            switch (operator) {
              case " + ":
                expected = i + i;
                break;
              case " - ":
                expected = i - i;
                break;
              case " * ":
                expected = i * i;
                break;
              case " / ":
                expected = i / i;
                break;
              case " % ":
                expected = i % i;
                break;
            }
            double actual = Double.parseDouble(resultSet.getString(j + 1));
            assertEquals(expected, actual, E);
          }
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testArithmeticUnary() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");
      String[] expressions = new String[] {"- s1", "- s2", "- s3", "- s4"};
      String sql = String.format("select %s from table1", String.join(",", expressions));
      ResultSet resultSet = statement.executeQuery(sql);

      assertEquals(expressions.length, resultSet.getMetaData().getColumnCount());

      for (int i = 1; i < 6; ++i) {
        resultSet.next();
        for (int j = 0; j < expressions.length; ++j) {
          double expected = -i;
          double actual = Double.parseDouble(resultSet.getString(j + 1));
          assertEquals(expected, actual, E);
        }
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  // TODO After support 'sin'
  @Ignore
  @Test
  public void testHybridQuery() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      String[] expressions = new String[] {"s1", "s1 + s2", "sin(s1)"};
      String sql = String.format("select %s from table1", String.join(",", expressions));
      ResultSet resultSet = statement.executeQuery(sql);

      assertEquals(1 + expressions.length, resultSet.getMetaData().getColumnCount());

      for (int i = 1; i < 6; ++i) {
        resultSet.next();
        assertEquals(i, Double.parseDouble(resultSet.getString(2)), E);
        assertEquals(i + i, Double.parseDouble(resultSet.getString(3)), E);
        assertEquals(Math.sin(i), Double.parseDouble(resultSet.getString(4)), E);
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testNonAlign() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");
      ResultSet resultSet = statement.executeQuery("select s7 + s8 from table1");
      assertEquals(1, resultSet.getMetaData().getColumnCount());
      assertTrue(resultSet.next());
      String curr = null;
      while (curr == null) {
        curr = resultSet.getString(1);
        resultSet.next();
      }
      assertEquals(10, Double.parseDouble(resultSet.getString(1)), E);
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select s7 + s8 from table1 where time < 5");
      assertEquals(1, resultSet.getMetaData().getColumnCount());
      curr = null;
      while (curr == null) {
        if (!resultSet.next()) {
          break;
        }
        curr = resultSet.getString(1);
      }
      assertEquals(null, curr);
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testWrongTypeBoolean() {
    tableAssertTestFail("select s1 + s5 from table1", "701: Cannot apply operator", "test");
  }

  @Test
  public void testWrongTypeText() {
    tableAssertTestFail("select s1 + s6 from table1", "701: Cannot apply operator", "test");
  }

  @Test
  public void testNot() {
    String[] header = new String[] {"_col0"};
    String[] retArray = new String[] {"true,", "true,", "false,", "false,", "false,"};
    tableResultSetEqualTest("select not(s5) from table1", header, retArray, "test");
  }
}
