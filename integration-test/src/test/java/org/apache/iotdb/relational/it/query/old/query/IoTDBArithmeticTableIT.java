/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBArithmeticTableIT {

  private static final double E = 0.0001;

  private static final String DATABASE_NAME = "test";

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
    "CREATE DATABASE " + DATABASE_NAME,
    "USE " + DATABASE_NAME,
    "CREATE TABLE table1 (device STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 DATE MEASUREMENT, s6 TIMESTAMP MEASUREMENT, s7 BOOLEAN MEASUREMENT, s8 TEXT MEASUREMENT)",
    "insert into table1(device, time, s1, s2, s3, s4, s5, s6, s7, s8) values ('d1', 1, 1, 1, 1.0, 1.0, '2024-01-01', 10, true, 'test')",
    "insert into table1(device, time, s1, s2, s3, s4, s5, s6, s7, s8) values ('d1', 2, 2, 2, 2.0, 2.0, '2024-02-01', 20, true, 'test')",
    "insert into table1(device, time, s1, s2, s3, s4, s5, s6, s7, s8) values ('d1', 3, 3, 3, 3.0, 3.0, '2024-03-01', 30, true, 'test')",
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

  @Test
  public void testArithmeticBinaryWithoutDateAndTimestamp() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");

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

  @Test
  public void testArithmeticBinaryWithDateAndTimestamp() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");

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

  @Test
  public void testArithmeticUnary() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");

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

  @Test
  public void testTimestampNegation() {
    String sql = "select -s6 from table1";
    tableResultSetEqualTest(
        sql,
        new String[] {"_col0"},
        new String[] {
          "1969-12-31T23:59:59.990Z,", "1969-12-31T23:59:59.980Z,", "1969-12-31T23:59:59.970Z,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testBinaryWrongType() {

    testBinaryDifferentCombinationsFail(
        new String[] {" * ", " / ", " % "},
        new String[] {"s1", "s2", "s3", "s4"},
        new String[] {"s5", "s6"},
        "table1",
        "701: Cannot apply operator",
        "test");

    testBinaryDifferentCombinationsFail(
        new String[] {" * ", " / ", " % "},
        new String[] {"s5", "s6"},
        new String[] {"s1", "s2", "s3", "s4"},
        "table1",
        "701: Cannot apply operator",
        "test");
  }

  @Test
  public void testUnaryWrongType() {
    tableAssertTestFail("select -s5 from table1", "701: Cannot negate", "test");
    tableAssertTestFail("select -s7 from table1", "701: Cannot negate", "test");
    tableAssertTestFail("select -s8 from table1", "701: Cannot negate", "test");
  }

  @Test
  public void testOverflow() {
    // int addition overflow
    tableAssertTestFail(
        String.format("select s1+%d from table1", Integer.MAX_VALUE),
        "750: int Addition overflow",
        "test");

    // int subtraction overflow
    tableAssertTestFail(
        String.format("select s1-(%d) from table1", Integer.MIN_VALUE),
        "750: int Subtraction overflow",
        "test");

    // int multiplication overflow
    tableAssertTestFail(
        String.format("select (s1+1)*%d from table1", Integer.MAX_VALUE),
        "750: int Multiplication overflow",
        "test");

    // Date addition overflow
    tableAssertTestFail(
        String.format("select s5+%d from table1", Long.MAX_VALUE),
        "750: long Addition overflow",
        "test");

    // Date subtraction overflow
    tableAssertTestFail(
        String.format("select s5-(%d) from table1", Long.MIN_VALUE),
        "750: long Subtraction overflow",
        "test");

    // long addition overflow
    tableAssertTestFail(
        String.format("select s2+%d from table1", Long.MAX_VALUE),
        "750: long Addition overflow",
        "test");

    // long subtraction overflow
    tableAssertTestFail(
        String.format("select s2-(%d) from table1", Long.MIN_VALUE),
        "750: long Subtraction overflow",
        "test");

    // Timestamp addition overflow
    tableAssertTestFail(
        String.format("select s6+%d from table1", Long.MAX_VALUE),
        "750: long Addition overflow",
        "test");

    // Timestamp subtraction overflow
    tableAssertTestFail(
        String.format("select s6-(%d) from table1", Long.MIN_VALUE),
        "750: long Subtraction overflow",
        "test");
  }

  @Test
  public void testFloatDivisionByZeroSpecialCase() {
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
        "test");
  }

  @Test
  public void testDoubleDivisionByZeroSpecialCase() {
    String[] expectedHeader = new String[5];
    for (int i = 0; i < expectedHeader.length; i++) {
      expectedHeader[i] = "_col" + i;
    }
    String[] expectedAns = {"NaN,0.0,NaN,0.0,NaN,", "NaN,0.0,NaN,0.0,NaN,", "NaN,0.0,NaN,0.0,NaN,"};

    tableResultSetEqualTest(
        "select s3%0.0,0.0%s3,0.0%0.0,0.0%-s3,-s3%0.0 from table1",
        expectedHeader, expectedAns, "test");
  }

  @Test
  public void testDivisionByZero() {
    tableAssertTestFail("select s1/0 from table1", "751: Division by zero", "test");
    tableAssertTestFail("select s2/0 from table1", "751: Division by zero", "test");

    tableAssertTestFail("select s1%0 from table1", "751: Division by zero", "test");
    tableAssertTestFail("select s2%0 from table1", "751: Division by zero", "test");
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
