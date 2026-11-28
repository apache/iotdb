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

package org.apache.iotdb.relational.it.query.view.old.query;

import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class IoTDBArithmeticTableViewBase {

  private static final double E = 0.0001;

  private static final String DATABASE_NAME = "test_view";

  private static final long MILLISECONDS_IN_DAY = 24 * 60 * 60 * 1000;

  private static final double[][] BASE_ANS = {{1, 1, 1.0, 1.0}, {2, 2, 2.0, 2.0}, {3, 3, 3.0, 3.0}};

  private static final Map<String, BiFunction<Double, Double, Double>> OPERATIONS = new HashMap<>();

  static {
    OPERATIONS.put(" + ", (a, b) -> a + b);
    OPERATIONS.put(" - ", (a, b) -> a - b);
    OPERATIONS.put(" * ", (a, b) -> a * b);
    OPERATIONS.put(" / ", (a, b) -> a / b);
    OPERATIONS.put(" % ", (a, b) -> a % b);
  }

  private static final String[] INSERTION_SQLS = {
    "CREATE DATABASE root.test",
    "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s5 WITH DATATYPE=DATE, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s6 WITH DATATYPE=TIMESTAMP, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s7 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s8 WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7, s8) values (1, 1, 1, 1.0, 1.0, '2024-01-01', 10, true, 'test')",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7, s8) values (2, 2, 2, 2.0, 2.0, '2024-02-01', 20, true, 'test')",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7, s8) values (3, 3, 3, 3.0, 3.0, '2024-03-01', 30, true, 'test')",
    "CREATE ALIGNED TIMESERIES root.sg2.d1(date DATE)",
    "insert into root.sg2.d1(time, date) values (1, '9999-12-31')",
    "insert into root.sg2.d1(time, date) values (2, '1000-01-01')"
  };

  private static final String[] CREATE_TABLE_VIEW_SQLS = {
    "CREATE DATABASE " + DATABASE_NAME,
    "USE " + DATABASE_NAME,
    "CREATE VIEW table1 (device STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD, s3 FLOAT FIELD, s4 DOUBLE FIELD, s5 DATE FIELD, s6 TIMESTAMP FIELD, s7 BOOLEAN FIELD, s8 TEXT FIELD) as root.sg.**",
    "CREATE VIEW table2 (device STRING TAG, date DATE FIELD) as root.sg2.**",
  };

  protected static void insertViewData() throws Exception {
    prepareData(INSERTION_SQLS);
    prepareTableData(CREATE_TABLE_VIEW_SQLS);
  }

  public static double[][] calculateExpectedAns(String operator) {
    double[][] expectedAns = new double[BASE_ANS.length][BASE_ANS[0].length * BASE_ANS[0].length];
    BiFunction<Double, Double, Double> operation = OPERATIONS.get(operator);
    if (operation == null) {
      throw new IllegalArgumentException("Unsupported operator: " + operator);
    }

    for (int i = 0; i < BASE_ANS.length; i++) {
      int baseLength = BASE_ANS[i].length;
      for (int j = 0; j < baseLength; j++) {
        for (int k = 0; k < baseLength; k++) {
          expectedAns[i][j * baseLength + k] = operation.apply(BASE_ANS[i][j], BASE_ANS[i][k]);
        }
      }
    }
    return expectedAns;
  }

  // Checks numeric binary operators on view columns and verifies the resulting rows.
  @Test
  public void testViewArithmeticBinaryWithoutDateAndTimestamp() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      // generate sql

      for (String operator : new String[] {" + ", " - ", " * ", " / ", " % "}) {
        List<String> expressions = new ArrayList<>();
        String[] operands = new String[] {"s1", "s2", "s3", "s4"};
        for (String leftOperand : operands) {
          for (String rightOperand : operands) {
            expressions.add(leftOperand + operator + rightOperand);
          }
        }

        String sql = String.format("select %s from table1", String.join(",", expressions));
        ResultSet resultSet = statement.executeQuery(sql);

        // generate answer
        double[][] expectedAns = calculateExpectedAns(operator);

        // Make sure the number of columns in the result set is correct
        assertEquals(expressions.size(), resultSet.getMetaData().getColumnCount());

        // check the result
        for (double[] expectedAn : expectedAns) {
          resultSet.next();
          for (int i = 0; i < expectedAn.length; i++) {
            double result = Double.parseDouble(resultSet.getString(i + 1));
            assertEquals(expectedAn[i], result, E);
          }
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  // Checks binary arithmetic involving DATE and TIMESTAMP view columns.
  @Test
  public void testViewArithmeticBinaryWithDateAndTimestamp() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      String sql =
          "select s1+s5,s1+s6,s2+s5,s2+s6,s5+s1,s5+s2,s6+s1,s6+s2,s5-s1,s5-s2,s6-s1,s6-s2 from table1";
      ResultSet resultSet = statement.executeQuery(sql);

      assertEquals(12, resultSet.getMetaData().getColumnCount());

      int[][] expectedAns = {
        {20240101, 11, 20240101, 11, 20240101, 20240101, 11, 11, 20231231, 20231231, 9, 9},
        {20240201, 22, 20240201, 22, 20240201, 20240201, 22, 22, 20240131, 20240131, 18, 18},
        {20240301, 33, 20240301, 33, 20240301, 20240301, 33, 33, 20240229, 20240229, 27, 27}
      };

      for (int[] expectedAn : expectedAns) {
        resultSet.next();

        for (int i = 0; i < expectedAn.length; i++) {
          int result = resultSet.getInt(i + 1);
          assertEquals(expectedAn[i], result);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  // Checks unary plus and minus over numeric view columns.
  @Test
  public void testViewArithmeticUnary() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      String[] expressions = new String[] {"- s1", "- s2", "- s3", "- s4"};
      String sql = String.format("select %s from table1", String.join(",", expressions));
      ResultSet resultSet = statement.executeQuery(sql);

      assertEquals(expressions.length, resultSet.getMetaData().getColumnCount());

      for (int i = 1; i < 4; ++i) {
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

  // Checks unary negation of TIMESTAMP values exposed through a view.
  @Test
  public void testViewTimestampNegation() {
    String sql = "select -s6 from table1";
    tableResultSetEqualTest(
        sql,
        new String[] {"_col0"},
        new String[] {
          "1969-12-31T23:59:59.990Z,", "1969-12-31T23:59:59.980Z,", "1969-12-31T23:59:59.970Z,"
        },
        DATABASE_NAME);
  }

  // Rejects binary arithmetic when either view operand has an unsupported type.
  @Test
  public void testViewBinaryWrongType() {

    testBinaryDifferentCombinationsFail(
        new String[] {" * ", " / ", " % "},
        new String[] {"s1", "s2", "s3", "s4"},
        new String[] {"s5", "s6"},
        "table1",
        "701: Cannot apply operator",
        DATABASE_NAME);

    testBinaryDifferentCombinationsFail(
        new String[] {" * ", " / ", " % "},
        new String[] {"s5", "s6"},
        new String[] {"s1", "s2", "s3", "s4"},
        "table1",
        "701: Cannot apply operator",
        DATABASE_NAME);
  }

  // Rejects unary arithmetic on view columns that do not support numeric operations.
  @Test
  public void testViewUnaryWrongType() {
    tableAssertTestFail("select -s5 from table1", "701: Cannot negate", DATABASE_NAME);
    tableAssertTestFail("select -s7 from table1", "701: Cannot negate", DATABASE_NAME);
    tableAssertTestFail("select -s8 from table1", "701: Cannot negate", DATABASE_NAME);
  }

  // Checks integer/long overflow, including DATE and TIMESTAMP arithmetic on view fields.
  @Test
  public void testViewOverflow() {
    // int addition overflow
    tableAssertTestFail(
        String.format("select s1+%d from table1", Integer.MAX_VALUE),
        "750: int Addition overflow",
        DATABASE_NAME);

    // int subtraction overflow
    tableAssertTestFail(
        String.format("select s1-(%d) from table1", Integer.MIN_VALUE),
        "750: int Subtraction overflow",
        DATABASE_NAME);

    // int multiplication overflow
    tableAssertTestFail(
        String.format("select (s1+1)*%d from table1", Integer.MAX_VALUE),
        "750: int Multiplication overflow",
        DATABASE_NAME);

    // Date addition overflow
    tableAssertTestFail(
        String.format("select s5+%d from table1", Long.MAX_VALUE),
        "750: long Addition overflow",
        DATABASE_NAME);

    // Date subtraction overflow
    tableAssertTestFail(
        String.format("select s5-(%d) from table1", Long.MIN_VALUE),
        "750: long Subtraction overflow",
        DATABASE_NAME);

    // long addition overflow
    tableAssertTestFail(
        String.format("select s2+%d from table1", Long.MAX_VALUE),
        "750: long Addition overflow",
        DATABASE_NAME);

    // long subtraction overflow
    tableAssertTestFail(
        String.format("select s2-(%d) from table1", Long.MIN_VALUE),
        "750: long Subtraction overflow",
        DATABASE_NAME);

    // Timestamp addition overflow
    tableAssertTestFail(
        String.format("select s6+%d from table1", Long.MAX_VALUE),
        "750: long Addition overflow",
        DATABASE_NAME);

    // Timestamp subtraction overflow
    tableAssertTestFail(
        String.format("select s6-(%d) from table1", Long.MIN_VALUE),
        "750: long Subtraction overflow",
        DATABASE_NAME);
  }

  // Verifies FLOAT division by zero follows the scalar arithmetic special case on a view.
  @Test
  public void testViewFloatDivisionByZeroSpecialCase() {
    String[] expectedHeader = new String[5];
    for (int i = 0; i < expectedHeader.length; i++) {
      expectedHeader[i] = "_col" + i;
    }
    String[] expectedAns = {
      "Infinity,0.0,NaN,-0.0,-Infinity,",
      "Infinity,0.0,NaN,-0.0,-Infinity,",
      "Infinity,0.0,NaN,-0.0,-Infinity,"
    };

    tableResultSetEqualTest(
        "select s3/0.0,0.0/s3,0.0/0.0,0.0/-s3,-s3/0.0 from table1",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  // Verifies DOUBLE division by zero follows the scalar arithmetic special case on a view.
  @Test
  public void testViewDoubleDivisionByZeroSpecialCase() {
    String[] expectedHeader = new String[5];
    for (int i = 0; i < expectedHeader.length; i++) {
      expectedHeader[i] = "_col" + i;
    }
    String[] expectedAns = {"NaN,0.0,NaN,0.0,NaN,", "NaN,0.0,NaN,0.0,NaN,", "NaN,0.0,NaN,0.0,NaN,"};

    tableResultSetEqualTest(
        "select s3%0.0,0.0%s3,0.0%0.0,0.0%-s3,-s3%0.0 from table1",
        expectedHeader, expectedAns, DATABASE_NAME);
  }

  // Checks integer division by zero raises the expected query error through a view.
  @Test
  public void testViewDivisionByZero() {
    tableAssertTestFail("select s1/0 from table1", "751: Division by zero", DATABASE_NAME);
    tableAssertTestFail("select s2/0 from table1", "751: Division by zero", DATABASE_NAME);

    tableAssertTestFail("select s1%0 from table1", "751: Division by zero", DATABASE_NAME);
    tableAssertTestFail("select s2%0 from table1", "751: Division by zero", DATABASE_NAME);
  }

  // Checks DATE arithmetic rejects results outside the supported date range on a view.
  @Test
  public void testViewDateOutOfRange() {
    DateTimeUtils.initTimestampPrecision();
    tableAssertTestFail(
        String.format(
            "select date + %s from table2 where time = 1",
            DateTimeUtils.correctPrecision(MILLISECONDS_IN_DAY)),
        "752: Year must be between 1000 and 9999.",
        DATABASE_NAME);

    tableAssertTestFail(
        String.format(
            "select %s + date from table2 where time = 1",
            DateTimeUtils.correctPrecision(MILLISECONDS_IN_DAY)),
        "752: Year must be between 1000 and 9999.",
        DATABASE_NAME);

    tableAssertTestFail(
        String.format("select %s + date from table2 where time = 1", 86400000),
        "752: Year must be between 1000 and 9999.",
        DATABASE_NAME);

    tableAssertTestFail(
        String.format(
            "select date - %s from table2 where time = 2",
            DateTimeUtils.correctPrecision(MILLISECONDS_IN_DAY)),
        "752: Year must be between 1000 and 9999.",
        DATABASE_NAME);
  }

  private void testBinaryDifferentCombinationsFail(
      String[] operators,
      String[] leftOperands,
      String[] rightOperands,
      String tableName,
      String errMsg,
      String databaseName) {
    for (String operator : operators) {
      for (String leftOperand : leftOperands) {
        for (String rightOperand : rightOperands) {
          tableAssertTestFail(
              String.format(
                  "select %s %s %s from %s", leftOperand, operator, rightOperand, tableName),
              errMsg,
              databaseName);
        }
      }
    }
  }
}
