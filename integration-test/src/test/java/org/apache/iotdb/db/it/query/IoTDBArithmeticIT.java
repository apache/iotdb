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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBArithmeticIT {

  private static final double E = 0.0001;

  private static final String[] INSERTION_SQLS = {
    "CREATE DATABASE root.test",
    "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s7 WITH DATATYPE=INT32, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s8 WITH DATATYPE=INT32, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s9 WITH DATATYPE=DATE, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s10 WITH DATATYPE=TIMESTAMP, ENCODING=PLAIN",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7, s9, s10) values (1, 1, 1, 1, 1, false, '1', 1, '2024-01-01', 10)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s8, s9, s10) values (2, 2, 2, 2, 2, false, '2', 2, '2024-02-01', 20)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7, s9, s10) values (3, 3, 3, 3, 3, true, '3', 3, '2024-03-01', 30)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s8, s9, s10) values (4, 4, 4, 4, 4, true, '4', 4, '2024-04-01', 40)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10) values (5, 5, 5, 5, 5, true, '5', 5, 5, '2024-05-01', 50)",
  };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(INSERTION_SQLS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testArithmeticBinary() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] operands = new String[] {"s1", "s2", "s3", "s4"};
      for (String operator : new String[] {" + ", " - ", " * ", " / ", " % "}) {
        List<String> expressions = new ArrayList<>();
        for (String leftOperand : operands) {
          for (String rightOperand : operands) {
            expressions.add(leftOperand + operator + rightOperand);
          }
        }
        String sql = String.format("select %s from root.sg.d1", String.join(",", expressions));

        ResultSet resultSet = statement.executeQuery(sql);

        assertEquals(1 + expressions.size(), resultSet.getMetaData().getColumnCount());

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
            double actual = Double.parseDouble(resultSet.getString(2 + j));
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] expressions = new String[] {"- s1", "- s2", "- s3", "- s4"};
      String sql = String.format("select %s from root.sg.d1", String.join(",", expressions));
      ResultSet resultSet = statement.executeQuery(sql);

      assertEquals(1 + expressions.length, resultSet.getMetaData().getColumnCount());

      for (int i = 1; i < 6; ++i) {
        resultSet.next();
        for (int j = 0; j < expressions.length; ++j) {
          double expected = -i;
          double actual = Double.parseDouble(resultSet.getString(2 + j));
          assertEquals(expected, actual, E);
        }
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTimestampNegation() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("select -s10 from root.sg.d1");

      String[] expected = {
        "1969-12-31T23:59:59.990Z",
        "1969-12-31T23:59:59.980Z",
        "1969-12-31T23:59:59.970Z",
        "1969-12-31T23:59:59.960Z",
        "1969-12-31T23:59:59.950Z"
      };

      for (String expectedValue : expected) {
        resultSet.next();
        assertEquals(expectedValue, resultSet.getString(2));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testUnaryWrongType() {
    assertTestFail("select -s5 from root.sg.d1", "Invalid input expression data type");
    assertTestFail("select -s6 from root.sg.d1", "Invalid input expression data type");
    assertTestFail("select -s9 from root.sg.d1", "Invalid input expression data type");
  }

  @Test
  public void testHybridQuery() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] expressions = new String[] {"s1", "s1 + s2", "sin(s1)"};
      String sql = String.format("select %s from root.sg.d1", String.join(",", expressions));
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("select s7 + s8 from root.sg.d1");
      assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());
      assertTrue(resultSet.next());
      String curr = null;
      while (curr == null) {
        curr = resultSet.getString(2);
        resultSet.next();
      }
      assertEquals(10, Double.parseDouble(resultSet.getString(2)), E);
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select s7 + s8 from root.sg.d1 where time < 5");
      assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());
      curr = null;
      while (curr == null) {
        if (!resultSet.next()) {
          break;
        }
        curr = resultSet.getString(2);
      }
      assertEquals(null, curr);
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testWrongTypeBoolean() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select s1 + s5 from root.sg.d1");
    } catch (Exception throwable) {
      assertTrue(throwable.getMessage().contains("Invalid input expression data type."));
    }
  }

  @Test
  public void testWrongTypeText() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select s1 + s6 from root.sg.d1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Invalid input expression data type."));
    }
  }

  @Test
  public void testNot() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select not(s5), !s5 from root.sg.d1"); ) {
      String[] retArray = new String[] {"true", "true", "false", "false", "false"};
      int cnt = 0;
      while (resultSet.next()) {
        assertEquals(retArray[cnt], resultSet.getString(2));
        assertEquals(retArray[cnt], resultSet.getString(3));
        cnt++;
      }
      assertEquals(retArray.length, cnt);
    } catch (SQLException throwable) {
      fail();
    }
  }

  @Test
  public void testDateAndTimestampArithmetic() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String sql =
          "select s1+s9,s1+s10,s2+s9,s2+s10,s9+s1,s9+s2,s10+s1,s10+s2,s9-s1,s9-s2,s10-s1,s10-s2 from root.sg.d1";
      ResultSet resultSet = statement.executeQuery(sql);

      int[][] expectedResults = {
        {20240101, 11, 20240101, 11, 20240101, 20240101, 11, 11, 20231231, 20231231, 9, 9},
        {20240201, 22, 20240201, 22, 20240201, 20240201, 22, 22, 20240131, 20240131, 18, 18},
        {20240301, 33, 20240301, 33, 20240301, 20240301, 33, 33, 20240229, 20240229, 27, 27},
        {20240401, 44, 20240401, 44, 20240401, 20240401, 44, 44, 20240331, 20240331, 36, 36},
        {20240501, 55, 20240501, 55, 20240501, 20240501, 55, 55, 20240430, 20240430, 45, 45}
      };

      for (int[] expectedResult : expectedResults) {
        resultSet.next();
        for (int i = 0; i < expectedResult.length; i++) {
          assertEquals(expectedResult[i], resultSet.getInt(i + 2));
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDivisionByZero() {
    assertTestFail("select s1/0 from root.sg.d1", "Division by zero");
    assertTestFail("select s2/0 from root.sg.d1", "Division by zero");
    assertTestFail("select s1%0 from root.sg.d1", "Division by zero");
    assertTestFail("select s2%0 from root.sg.d1", "Division by zero");
  }

  @Test
  public void testFloatDivisionByZero() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s3/0.0,0.0/s3,0.0/-s3,-s3/0.0 from root.sg.d1");

      String[] expected = {"Infinity", "0.0", "-0.0", "-Infinity"};

      for (int i = 0; i < 5; i++) {
        resultSet.next();
        for (int j = 0; j < expected.length; j++) {
          assertEquals(expected[j], resultSet.getString(j + 2));
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDoubleModuloByZero() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s4%0.0,0.0%s4,0.0%-s4,-s4%0.0 from root.sg.d1");

      String[] expected = {"NaN", "0.0", "0.0", "NaN"};

      for (int i = 0; i < 5; i++) {
        resultSet.next();
        for (int j = 0; j < expected.length; j++) {
          assertEquals(expected[j], resultSet.getString(j + 2));
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testBinaryWrongType() {
    assertTestFail("select s9 * s1 from root.sg.d1", "Invalid");
    assertTestFail("select s9 / s1 from root.sg.d1", "Invalid");
    assertTestFail("select s9 % s1 from root.sg.d1", "Invalid");
    assertTestFail("select s10 * s1 from root.sg.d1", "Invalid");
    assertTestFail("select s10 / s1 from root.sg.d1", "Invalid");
    assertTestFail("select s10 % s1 from root.sg.d1", "Invalid");
  }

  @Test
  public void testOverflow() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[][] timeseries = {
        {"s1", "INT32"},
        {"s2", "INT64"},
        {"s3", "INT64"},
        {"s7", "INT32"},
        {"s8", "INT32"},
        {"s10", "TIMESTAMP"}
      };
      for (String[] ts : timeseries) {
        statement.execute(
            String.format(
                "CREATE TIMESERIES root.sg.d2.%s WITH DATATYPE=%s, ENCODING=PLAIN", ts[0], ts[1]));
      }

      statement.execute(
          String.format(
              "insert into root.sg.d2(time, s1, s2, s3, s7, s8, s10) values (1, %d, %d, %d, %d, %d, %d)",
              Integer.MAX_VALUE / 2 + 1,
              Long.MAX_VALUE / 2 + 1,
              Long.MIN_VALUE / 2 - 1,
              Integer.MAX_VALUE / 2 + 1,
              Integer.MIN_VALUE / 2 - 1,
              Long.MAX_VALUE / 2 + 1));
      statement.execute(
          String.format(
              "insert into root.sg.d2(time, s1, s2, s7, s8) values (2, %d, %d, %d, %d)",
              Integer.MIN_VALUE / 2 - 1,
              Long.MIN_VALUE / 2 - 1,
              Integer.MIN_VALUE / 2 - 1,
              Integer.MAX_VALUE / 2 + 1));

      assertTestFail("select s1+s7 from root.sg.d2 where time=1", "int Addition overflow");
      assertTestFail("select s1-s8 from root.sg.d2 where time=2", "int Subtraction overflow");
      assertTestFail("select s1*s7 from root.sg.d2 where time=1", "int Multiplication overflow");

      assertTestFail("select s2+s2 from root.sg.d2 where time=1", "long Addition overflow");
      assertTestFail("select s3-s2 from root.sg.d2 where time=1", "long Subtraction overflow");

      assertTestFail(
          String.format("select s10+%d from root.sg.d2 where time=1", Long.MAX_VALUE),
          "long Addition overflow");
      assertTestFail(
          String.format("select s10-(%d) from root.sg.d2 where time=1", Long.MIN_VALUE),
          "long Subtraction overflow");

    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDateOutOfRange() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.d3.date WITH DATATYPE=DATE, ENCODING=PLAIN");
      statement.execute("insert into root.sg.d3(time, date) values (1, '9999-12-31')");
      statement.execute("insert into root.sg.d3(time, date) values (2, '1000-01-01')");

      assertTestFail(
          "select date + 86400000 from root.sg.d3 where time = 1",
          "Year must be between 1000 and 9999");
      assertTestFail(
          "select date - 86400000 from root.sg.d3 where time = 2",
          "Year must be between 1000 and 9999");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
