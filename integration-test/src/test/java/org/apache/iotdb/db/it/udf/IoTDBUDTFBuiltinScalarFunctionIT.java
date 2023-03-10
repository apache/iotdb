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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDTFBuiltinScalarFunctionIT {

  private static final String[] INSERTION_SQLS = {
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7, s8) values (0, 0, 0, 0, 0, true, '0', 0, 0)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7) values (2, 1, 1, 1, 1, false, '1', 1)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7) values (4, 2, 2, 2, 2, false, '2', 2)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s8) values (6, 3, 3, 3, 3, true, '3', 3)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s8) values (8, 4, 4, 4, 4, true, '4', 4)",
  };

  private static final double E = 0.0001;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg");
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s3 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s4 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s5 with datatype=BOOLEAN,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s6 with datatype=TEXT,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String dataGenerationSQL : INSERTION_SQLS) {
        statement.execute(dataGenerationSQL);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMathFunctions() {
    testMathFunction("sin", Math::sin);
    testMathFunction("cos", Math::cos);
    testMathFunction("tan", Math::tan);
    testMathFunction("asin", Math::asin);
    testMathFunction("acos", Math::acos);
    testMathFunction("atan", Math::atan);
    testMathFunction("sinh", Math::sinh);
    testMathFunction("cosh", Math::cosh);
    testMathFunction("tanh", Math::tanh);
    testMathFunction("degrees", Math::toDegrees);
    testMathFunction("radians", Math::toRadians);
    testMathFunction("abs", Math::abs);
    testMathFunction("sign", Math::signum);
    testMathFunction("ceil", Math::ceil);
    testMathFunction("floor", Math::floor);
    testMathFunction("round", Math::rint);
    testMathFunction("exp", Math::exp);
    testMathFunction("ln", Math::log);
    testMathFunction("log10", Math::log10);
    testMathFunction("sqrt", Math::sqrt);
  }

  private interface MathFunctionProxy {

    double invoke(double x);
  }

  private void testMathFunction(String functionName, MathFunctionProxy functionProxy) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1), %s(s2), %s(s3), %s(s4) from root.sg.d1",
                  functionName, functionName, functionName, functionName));

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length; ++i) {
        resultSet.next();
        for (int j = 0; j < 4; ++j) {
          double expected = functionProxy.invoke(i);
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
  public void testSelectorFunctions() {
    final String TOP_K = "TOP_K";
    final String BOTTOM_K = "BOTTOM_K";
    final String K = "'k'='2'";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1, %s), %s(s2, %s), %s(s3, %s), %s(s4, %s), %s(s6, %s) from root.sg.d1",
                  TOP_K, K, TOP_K, K, TOP_K, K, TOP_K, K, TOP_K, K));

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 5, columnCount);

      for (int i = INSERTION_SQLS.length - 2; i < INSERTION_SQLS.length; ++i) {
        resultSet.next();
        for (int j = 0; j < 5; ++j) {
          assertEquals(i, Double.parseDouble(resultSet.getString(2 + j)), E);
        }
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1, %s), %s(s2, %s), %s(s3, %s), %s(s4, %s), %s(s6, %s) from root.sg.d1",
                  BOTTOM_K, K, BOTTOM_K, K, BOTTOM_K, K, BOTTOM_K, K, BOTTOM_K, K));

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 5, columnCount);

      for (int i = 0; i < 2; ++i) {
        resultSet.next();
        for (int j = 0; j < 5; ++j) {
          assertEquals(i, Double.parseDouble(resultSet.getString(2 + j)), E);
        }
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testStringProcessingFunctions() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select STRING_CONTAINS(s6, 's'='0'), STRING_MATCHES(s6, 'regex'='\\d') from root.sg.d1");

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 2, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length; ++i) {
        resultSet.next();
        if (i == 0) {
          assertTrue(Boolean.parseBoolean(resultSet.getString(2)));
        } else {
          assertFalse(Boolean.parseBoolean(resultSet.getString(2)));
        }
        assertTrue(Boolean.parseBoolean(resultSet.getString(2 + 1)));
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testVariationTrendCalculationFunctions() {
    testVariationTrendCalculationFunction("TIME_DIFFERENCE", 2);
    testVariationTrendCalculationFunction("DIFFERENCE", 1);
    testVariationTrendCalculationFunction("NON_NEGATIVE_DIFFERENCE", 1);
    testVariationTrendCalculationFunction("DERIVATIVE", 0.5);
    testVariationTrendCalculationFunction("NON_NEGATIVE_DERIVATIVE", 0.5);
  }

  public void testVariationTrendCalculationFunction(String functionName, double expected) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1), %s(s2), %s(s3), %s(s4) from root.sg.d1",
                  functionName, functionName, functionName, functionName));

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length - 1; ++i) {
        resultSet.next();
        for (int j = 0; j < 4; ++j) {
          assertEquals(expected, Double.parseDouble(resultSet.getString(2 + j)), E);
        }
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConstantTimeSeriesGeneratingFunctions() {
    String[] expected = {
      "0, 0.0, 0.0, 1024, 3.141592653589793, 2.718281828459045, ",
      "2, 1.0, null, 1024, 3.141592653589793, 2.718281828459045, ",
      "4, 2.0, null, 1024, 3.141592653589793, 2.718281828459045, ",
      "6, null, 3.0, null, null, 2.718281828459045, ",
      "8, null, 4.0, null, null, 2.718281828459045, ",
    };

    try (Connection connection = EnvFactory.getEnv().getConnection()) {

      try (Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "select s7, s8, const(s7, 'value'='1024', 'type'='INT64'), pi(s7, s7), e(s7, s8, s7, s8) from root.sg.d1")) {
        assertEquals(1 + 5, resultSet.getMetaData().getColumnCount());

        for (int i = 0; i < INSERTION_SQLS.length; ++i) {
          resultSet.next();
          StringBuilder actual = new StringBuilder();
          for (int j = 0; j < 1 + 5; ++j) {
            actual.append(resultSet.getString(1 + j)).append(", ");
          }
          assertEquals(expected[i], actual.toString());
        }

        assertFalse(resultSet.next());
      }

      try (Statement statement = connection.createStatement();
          ResultSet ignored =
              statement.executeQuery("select const(s7, 'value'='1024') from root.sg.d1")) {
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("attribute \"type\" is required but was not provided"));
      }

      try (Statement statement = connection.createStatement();
          ResultSet ignored =
              statement.executeQuery("select const(s8, 'type'='INT64') from root.sg.d1")) {
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("attribute \"value\" is required but was not provided"));
      }

      try (Statement statement = connection.createStatement();
          ResultSet ignored =
              statement.executeQuery(
                  "select const(s8, 'value'='1024', 'type'='long') from root.sg.d1")) {
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("the given value type is not supported"));
      }

      try (Statement statement = connection.createStatement();
          ResultSet ignored =
              statement.executeQuery(
                  "select const(s8, 'value'='1024e', 'type'='INT64') from root.sg.d1")) {
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("java.lang.NumberFormatException"));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testConversionFunction() {
    String[] expected = {
      "0, 0, 0.0, 1, 0.0, ",
      "2, 1, 1.0, 0, 1.0, ",
      "4, 2, 2.0, 0, 2.0, ",
      "6, 3, 3.0, 1, null, ",
      "8, 4, 4.0, 1, null, ",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select cast(s1, 'type'='TEXT'), cast(s3, 'type'='FLOAT'), cast(s5, 'type'='INT32'), cast(s7, 'type'='DOUBLE') from root.sg.d1");

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(5, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length; ++i) {
        resultSet.next();
        StringBuilder actual = new StringBuilder();
        for (int j = 0; j < 1 + 4; ++j) {
          actual.append(resultSet.getString(1 + j)).append(", ");
        }
        assertEquals(expected[i], actual.toString());
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testContinuouslySatisfies() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.d2.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d2.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d2.s3 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d2.s4 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d2.s5 with datatype=BOOLEAN,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    // create timeseries with only 0,1 values
    String[] ZERO_ONE_SQL = {
      "insert into root.sg.d2(time, s1, s2, s3, s4, s5) values (0, 0, 0, 0, 0, false)",
      "insert into root.sg.d2(time, s1, s2, s3, s4, s5) values (1, 1, 1, 1, 1, true)",
      "insert into root.sg.d2(time, s1, s2, s3, s4, s5) values (2, 1, 1, 1, 1, true)",
      "insert into root.sg.d2(time, s1, s2, s3, s4, s5) values (3, 0, 0, 0, 0, false)",
      "insert into root.sg.d2(time, s1, s2, s3, s4, s5) values (4, 1, 1, 1, 1, true)",
      "insert into root.sg.d2(time, s1, s2, s3, s4, s5) values (5, 0, 0, 0, 0, false)",
      "insert into root.sg.d2(time, s1, s2, s3, s4, s5) values (6, 0, 0, 0, 0, false)",
      "insert into root.sg.d2(time, s1, s2, s3, s4, s5) values (7, 1, 1, 1, 1, true)",
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String dataGenerationSQL : ZERO_ONE_SQL) {
        statement.execute(dataGenerationSQL);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    // test ZERO_DURATION
    // result should be (0,0),(3,0),(5,1)
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int[] timestamps = {0, 3, 5};
      int[] durations = {0, 0, 1};
      String functionName = "zero_duration";
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1), %s(s2), %s(s3), %s(s4), %s(s5) from root.sg.d2",
                  functionName, functionName, functionName, functionName, functionName));
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 5, columnCount);

      for (int i = 0; i < timestamps.length; ++i) {
        resultSet.next();
        long expectedTimestamp = timestamps[i];
        long actualTimestamp = Long.parseLong(resultSet.getString(1));
        assertEquals(expectedTimestamp, actualTimestamp);

        long expectedDuration = durations[i];
        for (int j = 0; j < 5; ++j) {
          long actualDuration = Long.parseLong(resultSet.getString(2 + j));
          assertEquals(expectedDuration, actualDuration);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    // test NON_ZERO_DURATION
    // result should be (1,1),(4,0),(7,0)
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String functionName = "non_zero_duration";
      int[] timestamps = {1, 4, 7};
      int[] durations = {1, 0, 0};
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1), %s(s2), %s(s3), %s(s4), %s(s5) from root.sg.d2",
                  functionName, functionName, functionName, functionName, functionName));

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 5, columnCount);

      for (int i = 0; i < timestamps.length; ++i) {
        resultSet.next();
        long expectedTimestamp = timestamps[i];
        long actualTimestamp = Long.parseLong(resultSet.getString(1));
        assertEquals(expectedTimestamp, actualTimestamp);

        long expectedDuration = durations[i];
        for (int j = 0; j < 5; ++j) {
          long actualDuration = Long.parseLong(resultSet.getString(2 + j));
          assertEquals(expectedDuration, actualDuration);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    // test ZERO_COUNT
    // result should be (0,1),(3,1),(5,2)
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String functionName = "zero_count";
      int[] timestamps = {0, 3, 5};
      int[] durations = {1, 1, 2};
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1), %s(s2), %s(s3), %s(s4), %s(s5) from root.sg.d2",
                  functionName, functionName, functionName, functionName, functionName));
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 5, columnCount);

      for (int i = 0; i < timestamps.length; ++i) {
        resultSet.next();
        long expectedTimestamp = timestamps[i];
        long actualTimestamp = Long.parseLong(resultSet.getString(1));
        assertEquals(expectedTimestamp, actualTimestamp);

        long expectedDuration = durations[i];
        for (int j = 0; j < 5; ++j) {
          long actualDuration = Long.parseLong(resultSet.getString(2 + j));
          assertEquals(expectedDuration, actualDuration);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    // test NON_ZERO_COUNT
    // result should be (1,2),(4,1),(7,1)
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String functionName = "non_zero_count";
      int[] timestamps = {1, 4, 7};
      int[] durations = {2, 1, 1};
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1), %s(s2), %s(s3), %s(s4), %s(s5) from root.sg.d2",
                  functionName, functionName, functionName, functionName, functionName));
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 5, columnCount);

      for (int i = 0; i < timestamps.length; ++i) {
        resultSet.next();
        long expectedTimestamp = timestamps[i];
        long actualTimestamp = Long.parseLong(resultSet.getString(1));
        assertEquals(expectedTimestamp, actualTimestamp);

        long expectedDuration = durations[i];
        for (int j = 0; j < 5; ++j) {
          long actualDuration = Long.parseLong(resultSet.getString(2 + j));
          assertEquals(expectedDuration, actualDuration);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testOnOffFunction() {
    Double[] thresholds = {Double.MAX_VALUE, -1.0, 0.0, 1.0, Double.MAX_VALUE};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (Double threshold : thresholds) {
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "select on_off(s1,'threshold'='%f'), on_off(s2,'threshold'='%f'), on_off(s3,'threshold'='%f'), on_off(s4,'threshold'='%f') from root.sg.d1",
                    threshold, threshold, threshold, threshold));

        int columnCount = resultSet.getMetaData().getColumnCount();
        assertEquals(1 + 4, columnCount);

        for (int i = 0; i < INSERTION_SQLS.length; ++i) {
          resultSet.next();
          for (int j = 0; j < 4; ++j) {
            Boolean expected = i >= threshold;
            Boolean actual = Boolean.parseBoolean(resultSet.getString(2 + j));
            assertEquals(expected, actual);
          }
        }
        resultSet.close();
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testInRange() {
    Double[] lowers = {-1.0, 0.0, 1.5, 2.0, 4.0};
    Double[] uppers = {0.0, 2.0, 4.5, 2.0, 1.0};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int k = 0; k < lowers.length; ++k) {
        Double lower = lowers[k];
        Double upper = uppers[k];
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "select in_range(s1,'upper'='%f','lower'='%f'), in_range(s2,'upper'='%f','lower'='%f'), "
                        + "in_range(s3,'upper'='%f','lower'='%f'), in_range(s4,'upper'='%f','lower'='%f') from root.sg.d1",
                    upper, lower, upper, lower, upper, lower, upper, lower));

        int columnCount = resultSet.getMetaData().getColumnCount();
        assertEquals(1 + 4, columnCount);

        for (int i = 0; i < INSERTION_SQLS.length; ++i) {
          resultSet.next();
          for (int j = 0; j < 4; ++j) {
            Boolean expected = (i >= lower && i <= upper);
            Boolean actual = Boolean.parseBoolean(resultSet.getString(2 + j));
            assertEquals(expected, actual);
          }
        }
        resultSet.close();
      }
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Upper can not be smaller than lower."));
    }
  }

  @Test
  public void testEqualBucketSampleForRandom() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.d5.s1 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    String[] SQL_FOR_SAMPLE_S1 = new String[100];
    for (int i = 0; i < 100; i++) {
      SQL_FOR_SAMPLE_S1[i] =
          String.format("insert into root.sg.d5(time, s1) values (%d, %d)", i, i);
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 100; i++) {
        statement.execute(SQL_FOR_SAMPLE_S1[i]);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String functionName = "EQUAL_SIZE_BUCKET_RANDOM_SAMPLE";
      double proportionValue = 0.1;
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select " + "%s(s1, 'proportion'='%f') from root.sg.d5",
                  functionName, proportionValue));
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 1, columnCount);
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(10, count);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testEqualBucketSampleForAgg() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.d4.s1 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d4.s2 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d4.s3 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d4.s4 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d4.s5 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d4.s6 with datatype=DOUBLE,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    String[] SQL_FOR_SAMPLE_S1 = new String[100];
    String[] SQL_FOR_SAMPLE_S2 = new String[100];
    String[] SQL_FOR_SAMPLE_S3 = new String[100];
    String[] SQL_FOR_SAMPLE_S4 = new String[100];
    String[] SQL_FOR_SAMPLE_S5 = new String[100];
    String[] SQL_FOR_SAMPLE_S6 = new String[100];

    for (int i = 0; i < 100; i++) {
      SQL_FOR_SAMPLE_S1[i] =
          String.format("insert into root.sg.d4(time, s1) values (%d, %f)", i, i * 1.0);
      SQL_FOR_SAMPLE_S2[i] =
          String.format("insert into root.sg.d4(time, s2) values (%d, %f)", i, i * 1.0);
      SQL_FOR_SAMPLE_S3[i] =
          String.format("insert into root.sg.d4(time, s3) values (%d, %d)", i, i);
      SQL_FOR_SAMPLE_S4[i] =
          String.format("insert into root.sg.d4(time, s4) values (%d, %d)", i, i);
      SQL_FOR_SAMPLE_S5[i] =
          String.format("insert into root.sg.d4(time, s5) values (%d, %d)", i, -i);
      SQL_FOR_SAMPLE_S6[i] =
          String.format("insert into root.sg.d4(time, s6) values (%d, %f)", i, i * 1.0);
    }
    float[] ANSWER1 =
        new float[] {4.5F, 14.5F, 24.5F, 34.5F, 44.5F, 54.5F, 64.5F, 74.5F, 84.5F, 94.5F};
    double[] ANSWER2 = new double[] {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};
    long[] ANSWER3 = new long[] {9, 19, 29, 39, 49, 59, 69, 79, 89, 99};
    long[] ANSWER4 = new long[] {45, 145, 245, 345, 445, 545, 645, 745, 845, 945};
    int[] ANSWER5 = new int[] {-9, -19, -29, -39, -49, -59, -69, -79, -89, -99};
    double[] ANSWER6 = new double[] {8.25, 8.25, 8.25, 8.25, 8.25, 8.25, 8.25, 8.25, 8.25, 8.25};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 100; i++) {
        statement.execute(SQL_FOR_SAMPLE_S1[i]);
        statement.execute(SQL_FOR_SAMPLE_S2[i]);
        statement.execute(SQL_FOR_SAMPLE_S3[i]);
        statement.execute(SQL_FOR_SAMPLE_S4[i]);
        statement.execute(SQL_FOR_SAMPLE_S5[i]);
        statement.execute(SQL_FOR_SAMPLE_S6[i]);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String functionName = "EQUAL_SIZE_BUCKET_AGG_SAMPLE";
      double proportionValue = 0.1;
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select "
                      + "%s(s1, 'proportion'='%f'), "
                      + "%s(s2, 'type'='%s', 'proportion'='%f'), "
                      + "%s(s3, 'type'='%s', 'proportion'='%f'), "
                      + "%s(s4, 'type'='%s', 'proportion'='%f'), "
                      + "%s(s5, 'type'='%s', 'proportion'='%f'), "
                      + "%s(s6, 'type'='%s', 'proportion'='%f')"
                      + "from root.sg.d4",
                  functionName,
                  proportionValue,
                  functionName,
                  "min",
                  proportionValue,
                  functionName,
                  "max",
                  proportionValue,
                  functionName,
                  "sum",
                  proportionValue,
                  functionName,
                  "extreme",
                  proportionValue,
                  functionName,
                  "variance",
                  proportionValue));
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);
      for (int i = 0; i < 10; i++) {
        resultSet.next();
        assertEquals(ANSWER1[i], resultSet.getDouble(2), 0.01);
        assertEquals(ANSWER2[i], resultSet.getDouble(3), 0.01);
        assertEquals(ANSWER3[i], resultSet.getLong(4));
        assertEquals(ANSWER4[i], resultSet.getLong(5));
        assertEquals(ANSWER5[i], resultSet.getInt(6));
        assertEquals(ANSWER6[i], resultSet.getDouble(7), 0.01);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testEqualBucketSampleForM4() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.d3.s1 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    String[] SQL_FOR_SAMPLE = new String[100];
    for (int i = 0; i < 100; i++) {
      SQL_FOR_SAMPLE[i] =
          String.format("insert into root.sg.d3(time, s1) values (%d, %d)", i, i + 1);
    }
    int[] ANSWER1 = new int[] {1, 2, 39, 40, 41, 42, 79, 80, 81, 82, 99, 100};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String dataGenerationSQL : SQL_FOR_SAMPLE) {
        statement.execute(dataGenerationSQL);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String functionName = "EQUAL_SIZE_BUCKET_M4_SAMPLE";
      String methodName = "m4";
      double proportionValue = 0.1;
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1, 'method'='%s', 'proportion'='%f') from root.sg.d3",
                  functionName, methodName, proportionValue));
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 1, columnCount);
      for (int j : ANSWER1) {
        resultSet.next();
        assertEquals(j, resultSet.getInt(2));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testEqualBucketSampleForOutlier() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.d6.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d6.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d6.s3 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d6.s4 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d6.s5 with datatype=FLOAT,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    String[] SQL_FOR_SAMPLE_S1 = new String[100];
    String[] SQL_FOR_SAMPLE_S2 = new String[100];
    String[] SQL_FOR_SAMPLE_S3 = new String[100];
    String[] SQL_FOR_SAMPLE_S4 = new String[100];
    String[] SQL_FOR_SAMPLE_S5 = new String[20];
    for (int i = 0; i < 100; i++) {
      SQL_FOR_SAMPLE_S1[i] =
          String.format(
              "insert into root.sg.d6(time, s1) values (%d, %d)",
              i, i % 5 == 0 && i % 10 != 0 ? i + 100 : i);
      SQL_FOR_SAMPLE_S2[i] =
          String.format(
              "insert into root.sg.d6(time, s2) values (%d, %d)", i, i % 10 == 6 ? i + 100 : i);
      if (i % 10 == 9 || i % 20 == 0) {
        SQL_FOR_SAMPLE_S2[i] = String.format("insert into root.sg.d6(time, s2) values (%d, 0)", i);
      }
      SQL_FOR_SAMPLE_S3[i] =
          String.format(
              "insert into root.sg.d6(time, s3) values (%d, %d)", i, i % 10 == 7 ? i + 100 : i);
      SQL_FOR_SAMPLE_S4[i] =
          String.format(
              "insert into root.sg.d6(time, s4) values (%d, %d)", i, i % 10 == 8 ? i + 100 : i);
    }
    for (int i = 0; i < 20; i++) {
      SQL_FOR_SAMPLE_S5[i] =
          String.format("insert into root.sg.d6(time, s5) values (%d, %d)", i, i);
    }
    int[] ANSWER1 = new int[] {105, 115, 125, 135, 145, 155, 165, 175, 185, 195};
    long[] ANSWER2 = new long[] {106, 116, 126, 136, 146, 156, 166, 176, 186, 196};
    double[] ANSWER3 = new double[] {107, 117, 127, 137, 147, 157, 167, 177, 187, 197};
    float[] ANSWER4 = new float[] {108, 118, 128, 138, 148, 158, 168, 178, 188, 198};
    float[] ANSWER5 = new float[] {0, 2, 4, 6, 8, 10, 12, 14, 16, 18};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 100; i++) {
        statement.execute(SQL_FOR_SAMPLE_S1[i]);
        statement.execute(SQL_FOR_SAMPLE_S2[i]);
        statement.execute(SQL_FOR_SAMPLE_S3[i]);
        statement.execute(SQL_FOR_SAMPLE_S4[i]);
      }
      for (int i = 0; i < 20; i++) {
        statement.execute(SQL_FOR_SAMPLE_S5[i]);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String functionName = "EQUAL_SIZE_BUCKET_OUTLIER_SAMPLE";
      double proportionValue = 0.1;

      ResultSet resultSet0 =
          statement.executeQuery(
              String.format(
                  "select "
                      + "%s(s1, 'proportion'='%f', 'type'='%s', 'number'='%d') "
                      + "from root.sg.d6",
                  functionName, proportionValue, "avg", 2));
      int columnCount0 = resultSet0.getMetaData().getColumnCount();
      assertEquals(1 + 1, columnCount0);
      for (int i = 0; i < 10; i++) {
        resultSet0.next();
        assertEquals(ANSWER1[i], resultSet0.getInt(2));
      }

      ResultSet resultSet1 =
          statement.executeQuery(
              String.format(
                  "select "
                      + "%s(s2, 'proportion'='%f', 'type'='%s', 'number'='%d') "
                      + "from root.sg.d6",
                  functionName, proportionValue, "stendis", 2));
      int columnCount1 = resultSet1.getMetaData().getColumnCount();
      assertEquals(1 + 1, columnCount1);
      for (int i = 0; i < 10; i++) {
        resultSet1.next();
        assertEquals(ANSWER2[i], resultSet1.getLong(2));
      }

      ResultSet resultSet2 =
          statement.executeQuery(
              String.format(
                  "select "
                      + "%s(s3, 'proportion'='%f', 'type'='%s', 'number'='%d') "
                      + "from root.sg.d6",
                  functionName, proportionValue, "cos", 2));
      int columnCount2 = resultSet2.getMetaData().getColumnCount();
      assertEquals(1 + 1, columnCount2);
      for (int i = 0; i < 10; i++) {
        resultSet2.next();
        assertEquals(ANSWER3[i], resultSet2.getDouble(2), 0.01);
      }

      ResultSet resultSet3 =
          statement.executeQuery(
              String.format(
                  "select "
                      + "%s(s4, 'proportion'='%f', 'type'='%s', 'number'='%d') "
                      + "from root.sg.d6",
                  functionName, proportionValue, "prenextdis", 2));
      int columnCount3 = resultSet3.getMetaData().getColumnCount();
      assertEquals(1 + 1, columnCount3);
      for (int i = 0; i < 10; i++) {
        resultSet3.next();
        assertEquals(ANSWER4[i], resultSet3.getFloat(2), 0.01);
      }

      ResultSet resultSet4 =
          statement.executeQuery(
              String.format(
                  "select "
                      + "%s(s5, 'proportion'='%f', 'type'='%s', 'number'='%d') "
                      + "from root.sg.d6",
                  functionName, 0.5, "cos", 1));
      int columnCount4 = resultSet4.getMetaData().getColumnCount();
      assertEquals(1 + 1, columnCount4);
      for (int i = 0; i < 10; i++) {
        resultSet4.next();
        assertEquals(ANSWER5[i], resultSet4.getFloat(2), 0.01);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testUDTFJexl() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.d7.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d7.s2 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d7.s3 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d7.s4 with datatype=TEXT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d7.s5 with datatype=BOOLEAN,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d7.s6 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d7.s7 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d7.s8 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d7.s9 with datatype=TEXT,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    String[] SQL_FOR_SAMPLE_1 = new String[5];
    String[] SQL_FOR_SAMPLE_2 = new String[5];
    String[] SQL_FOR_SAMPLE_3 = new String[5];
    String[] SQL_FOR_SAMPLE_4 = new String[5];
    String[] SQL_FOR_SAMPLE_5 = new String[5];
    String[] SQL_FOR_SAMPLE_6 = new String[5];
    String[] SQL_FOR_SAMPLE_7 = new String[5];
    String[] SQL_FOR_SAMPLE_8 = new String[5];
    String[] SQL_FOR_SAMPLE_9 = new String[5];
    for (int i = 0; i < 5; i++) {
      SQL_FOR_SAMPLE_1[i] =
          String.format("insert into root.sg.d7(time, s1) values (%d, %d)", i, i + 1);
      SQL_FOR_SAMPLE_2[i] =
          String.format("insert into root.sg.d7(time, s2) values (%d, %f)", i, i + 1.0);
      SQL_FOR_SAMPLE_3[i] =
          String.format("insert into root.sg.d7(time, s3) values (%d, %f)", i, i + 1.0);
      SQL_FOR_SAMPLE_4[i] =
          String.format("insert into root.sg.d7(time, s4) values (%d, '%s')", i, "string");
      SQL_FOR_SAMPLE_5[i] = String.format("insert into root.sg.d7(time, s5) values (%d, true)", i);
      SQL_FOR_SAMPLE_6[i] =
          String.format("insert into root.sg.d7(time, s6) values (%d, %d)", i, i + 8);
      SQL_FOR_SAMPLE_7[i] =
          String.format("insert into root.sg.d7(time, s7) values (%d, %d)", i, i + 1);
      SQL_FOR_SAMPLE_8[i] =
          String.format("insert into root.sg.d7(time, s8) values (%d, %f)", i, i + 1.0);
      SQL_FOR_SAMPLE_9[i] =
          String.format("insert into root.sg.d7(time, s9) values (%d, '%s')", i, "string");
    }
    double[] ANSWER1 = new double[] {2, 4, 6, 8, 10};
    double[] ANSWER2 = new double[] {2, 4, 6, 8, 10};
    double[] ANSWER3 = new double[] {4, 7, 10, 13, 16};
    String[] ANSWER4 = new String[] {"string2", "string2", "string2", "string2", "string2"};
    double[] ANSWER7 = new double[] {1, 4, 9, 16, 25};
    String[] ANSWER8 = new String[] {"string1", "string4", "string9", "string16", "string25"};
    double[] ANSWER9 = new double[] {2, 9, 28, 65, 126};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 5; i++) {
        statement.execute(SQL_FOR_SAMPLE_1[i]);
        statement.execute(SQL_FOR_SAMPLE_2[i]);
        statement.execute(SQL_FOR_SAMPLE_3[i]);
        statement.execute(SQL_FOR_SAMPLE_4[i]);
        statement.execute(SQL_FOR_SAMPLE_5[i]);
        statement.execute(SQL_FOR_SAMPLE_6[i]);
        statement.execute(SQL_FOR_SAMPLE_7[i]);
        statement.execute(SQL_FOR_SAMPLE_8[i]);
        statement.execute(SQL_FOR_SAMPLE_9[i]);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String functionName = "JEXL";
      String expr1 = "x -> {2 * x}";
      String expr2 = "x -> {x + x}";
      String expr3 = "x -> {x * 3 + 1}";
      String expr4 = "x -> {x + 2}";
      String expr5 = "x -> {x == true}";
      String expr6 = "x -> {x == x}";
      String expr7 = "(x, y) -> {x * y}";
      String expr8 = "(x, y, z) -> {x + y * z}";
      String expr9 = "(x, y, z, a) -> {x * y * z + (a ? 1 : -1)}";
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s(s1, 'expr'='%s'), "
                      + "%s(s2, 'expr'='%s'), "
                      + "%s(s3, 'expr'='%s'), "
                      + "%s(s4, 'expr'='%s'), "
                      + "%s(s5, 'expr'='%s'), "
                      + "%s(s6, 'expr'='%s'), "
                      + "%s(s7, s8, 'expr'='%s'), "
                      + "%s(s4, s7, s1, 'expr'='%s'), "
                      + "%s(s1, s7, s8, s5, 'expr'='%s') "
                      + "from root.sg.d7",
                  functionName,
                  expr1,
                  functionName,
                  expr2,
                  functionName,
                  expr3,
                  functionName,
                  expr4,
                  functionName,
                  expr5,
                  functionName,
                  expr6,
                  functionName,
                  expr7,
                  functionName,
                  expr8,
                  functionName,
                  expr9));
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 9, columnCount);
      for (int i = 0; i < 5; i++) {
        resultSet.next();
        assertEquals(ANSWER1[i], resultSet.getDouble(2), 0.01);
        assertEquals(ANSWER2[i], resultSet.getDouble(3), 0.01);
        assertEquals(ANSWER3[i], resultSet.getDouble(4), 0.01);
        assertEquals(ANSWER4[i], resultSet.getString(5));
        assertTrue(resultSet.getBoolean(6));
        assertTrue(resultSet.getBoolean(7));
        assertEquals(ANSWER7[i], resultSet.getDouble(8), 0.01);
        assertEquals(ANSWER8[i], resultSet.getString(9));
        assertEquals(ANSWER9[i], resultSet.getDouble(10), 0.01);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testStringFunctions() {
    String[] createSQLs =
        new String[] {
          "CREATE DATABASE root.testStringFunctions",
          "CREATE TIMESERIES root.testStringFunctions.d1.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
          "CREATE TIMESERIES root.testStringFunctions.d1.s2 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        };

    String[] insertSQLs =
        new String[] {
          "INSERT INTO root.testStringFunctions.d1(timestamp,s1,s2) values(1, \"1111test1111\", \"  1111test1111 \")",
          "INSERT INTO root.testStringFunctions.d1(timestamp,s1) values(2, \"2222test2222\")"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String createSQL : createSQLs) {
        statement.addBatch(createSQL);
      }

      for (String insertSQL : insertSQLs) {
        statement.addBatch(insertSQL);
      }
      statement.executeBatch();

      testStrLength(statement);
      testStrLocate(statement);
      testStartsWith(statement);
      testEndsWith(statement);
      testConcat(statement);
      testSubStr(statement);
      testUpper(statement);
      testLower(statement);
      testTrim(statement);
      testStrCmp(statement);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void testTrim(Statement statement) throws SQLException {
    String s2;
    ResultSet resultSet =
        statement.executeQuery("select s2, trim(s2) " + "from root.testStringFunctions.d1");
    while (resultSet.next()) {
      s2 = resultSet.getString(3);
      if (s2 == null) {
        continue;
      }
      assertEquals(resultSet.getString(2).trim(), resultSet.getString(3));
    }
  }

  private void testStrCmp(Statement statement) throws SQLException {

    ResultSet resultSet =
        statement.executeQuery(
            "select s1, s2, strcmp(s1, s2) " + "from root.testStringFunctions.d1");
    String s1, s2;
    while (resultSet.next()) {
      s1 = resultSet.getString(2);
      s2 = resultSet.getString(3);
      if (s1 == null || s2 == null) {
        continue;
      }
      assertEquals(s1.compareTo(s2), resultSet.getInt(4));
    }
  }

  private void testLower(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery("select s1, lower(s1) " + "from root.testStringFunctions.d1");
    while (resultSet.next()) {
      assertEquals(resultSet.getString(2).toLowerCase(), resultSet.getString(3));
    }
  }

  private void testUpper(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery("select s1, upper(s1) " + "from root.testStringFunctions.d1");
    while (resultSet.next()) {
      assertEquals(resultSet.getString(2).toUpperCase(), resultSet.getString(3));
    }
  }

  private void testSubStr(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery(
            "select s1, substr(s1, \"start\"=\"3\", \"end\"=\"7\") "
                + "from root.testStringFunctions.d1");
    while (resultSet.next()) {
      assertEquals(resultSet.getString(2).substring(3, 7), resultSet.getString(3));
    }
  }

  private void testConcat(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery(
            "select s1, s2, "
                + "concat(s1, s2, \"target1\"=\"IoT\", \"target2\"=\"DB\"), "
                + "concat(s1, s2, \"target1\"=\"IoT\", \"target2\"=\"DB\", \"series_behind\"=\"true\") "
                + "from root.testStringFunctions.d1");
    while (resultSet.next()) {
      assertEquals(
          (resultSet.getString(2) + resultSet.getString(3) + "IoTDB").replace("null", ""),
          resultSet.getString(4));
      assertEquals(
          ("IoTDB" + (resultSet.getString(2) + resultSet.getString(3))).replace("null", ""),
          resultSet.getString(5));
    }
  }

  private void testEndsWith(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery(
            "select s1, endsWith(s1, \"target\"=\"1111\") " + "from root.testStringFunctions.d1");
    while (resultSet.next()) {
      assertEquals(resultSet.getString(2).endsWith("1111"), resultSet.getBoolean(3));
    }
  }

  private void testStartsWith(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery(
            "select s1, startsWith(s1, \"target\"=\"1111\") " + "from root.testStringFunctions.d1");
    while (resultSet.next()) {
      assertEquals(resultSet.getString(2).startsWith("1111"), resultSet.getBoolean(3));
    }
  }

  private void testStrLocate(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery(
            "select s1, locate(s1, \"target\"=\"1111\"), locate(s1, \"target\"=\"1111\", \"reverse\"=\"true\") from root.testStringFunctions.d1");
    while (resultSet.next()) {
      assertEquals(resultSet.getString(2).indexOf("1111"), resultSet.getInt(3));
      assertEquals(resultSet.getString(2).lastIndexOf("1111"), resultSet.getInt(4));
    }
  }

  private void testStrLength(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery("select s1, length(s1) from root.testStringFunctions.d1");
    while (resultSet.next()) {
      assertEquals(resultSet.getString(2).length(), resultSet.getInt(3));
    }
  }

  @Test
  public void testMasterRepair() {
    // create time series with master data
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.testMasterRepair");
      statement.execute(
          "CREATE TIMESERIES root.testMasterRepair.d1.s1 with datatype=FLOAT,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.testMasterRepair.d1.s2 with datatype=FLOAT,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.testMasterRepair.d1.s3 with datatype=FLOAT,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.testMasterRepair.d1.m1 with datatype=FLOAT,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.testMasterRepair.d1.m2 with datatype=FLOAT,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.testMasterRepair.d1.m3 with datatype=FLOAT,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    String[] INSERT_SQL = {
      "insert into root.testMasterRepair.d1(time, s1, s2, s3, m1, m2, m3) values (1,1704,1154.55,0.195,1704,1154.55,0.195)",
      "insert into root.testMasterRepair.d1(time, s1, s2, s3, m1, m2, m3) values (2,1702,1152.30,0.193,1702,1152.30,0.193)",
      "insert into root.testMasterRepair.d1(time, s1, s2, s3, m1, m2, m3) values (3,1702,1148.65,0.192,1702,1148.65,0.192)",
      "insert into root.testMasterRepair.d1(time, s1, s2, s3, m1, m2, m3) values (4,1701,1145.20,0.194,1701,1145.20,0.194)",
      "insert into root.testMasterRepair.d1(time, s1, s2, s3, m1, m2, m3) values (7,1703,1150.55,0.195,1703,1150.55,0.195)",
      "insert into root.testMasterRepair.d1(time, s1, s2, s3, m1, m2, m3) values (8,1694,1151.55,0.193,1704,1151.55,0.193)",
      "insert into root.testMasterRepair.d1(time, s1, s2, s3, m1, m2, m3) values (9,1705,1153.55,0.194,1705,1153.55,0.194)",
      "insert into root.testMasterRepair.d1(time, s1, s2, s3, m1, m2, m3) values (10,1706,1152.30,0.190,1706,1152.30,0.190)",
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String dataGenerationSql : INSERT_SQL) {
        statement.execute(dataGenerationSql);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int[] timestamps = {1, 2, 3, 4, 7, 8, 9, 10};

      // test 1
      double[] r1 = {1704.0, 1702.0, 1702.0, 1701.0, 1703.0, 1702.0, 1705.0, 1706.0};
      try (ResultSet resultSet =
          statement.executeQuery(
              "select master_repair(s1,s2,s3,m1,m2,m3) from root.testMasterRepair.d1")) {
        int columnCount = resultSet.getMetaData().getColumnCount();
        assertEquals(1 + 1, columnCount);
        for (int i = 0; i < timestamps.length; i++) {
          resultSet.next();
          long expectedTimestamp = timestamps[i];
          long actualTimestamp = Long.parseLong(resultSet.getString(1));
          assertEquals(expectedTimestamp, actualTimestamp);
          double expectedResult = r1[i];
          double actualResult = resultSet.getDouble(2);
          double delta = 0.001;
          assertEquals(expectedResult, actualResult, delta);
        }
      }

      // test 2
      double[] r2 = {1154.55, 1152.30, 1148.65, 1145.20, 1150.55, 1152.30, 1153.55, 1152.30};

      try (ResultSet resultSet =
          statement.executeQuery(
              "select master_repair(s1,s2,s3,m1,m2,m3,'output_column'='2') from root.testMasterRepair.d1")) {
        int columnCount = resultSet.getMetaData().getColumnCount();
        assertEquals(1 + 1, columnCount);
        for (int i = 0; i < timestamps.length; i++) {
          resultSet.next();
          long expectedTimestamp = timestamps[i];
          long actualTimestamp = Long.parseLong(resultSet.getString(1));
          assertEquals(expectedTimestamp, actualTimestamp);

          double expectedResult = r2[i];
          double actualResult = resultSet.getDouble(2);
          double delta = 0.001;
          assertEquals(expectedResult, actualResult, delta);
        }
      }

      // test 3
      double[] r3 = {0.195, 0.193, 0.192, 0.194, 0.195, 0.193, 0.194, 0.190};
      try (ResultSet resultSet =
          statement.executeQuery(
              "select master_repair(s1,s2,s3,m1,m2,m3,'output_column'='3') from root.testMasterRepair.d1")) {
        int columnCount = resultSet.getMetaData().getColumnCount();
        assertEquals(1 + 1, columnCount);
        for (int i = 0; i < timestamps.length; i++) {
          resultSet.next();
          long expectedTimestamp = timestamps[i];
          long actualTimestamp = Long.parseLong(resultSet.getString(1));
          assertEquals(expectedTimestamp, actualTimestamp);

          double expectedResult = r3[i];
          double actualResult = resultSet.getDouble(2);
          double delta = 0.001;
          assertEquals(expectedResult, actualResult, delta);
        }
      }

      // test 4
      double[] r4 = {1704.0, 1702.0, 1702.0, 1701.0, 1703.0, 1704.0, 1705.0, 1706.0};
      try (ResultSet resultSet =
          statement.executeQuery(
              "select master_repair(s1,s2,s3,m1,m2,m3,'omega'='2','eta'='3.0','k'='5') from root.testMasterRepair.d1")) {
        int columnCount = resultSet.getMetaData().getColumnCount();
        assertEquals(1 + 1, columnCount);
        for (int i = 0; i < timestamps.length; i++) {
          resultSet.next();
          long expectedTimestamp = timestamps[i];
          long actualTimestamp = Long.parseLong(resultSet.getString(1));
          assertEquals(expectedTimestamp, actualTimestamp);

          double expectedResult = r4[i];
          double actualResult = resultSet.getDouble(2);
          double delta = 0.001;
          assertEquals(expectedResult, actualResult, delta);
        }
      }

      // test 5
      double[] r5 = {1154.55, 1152.30, 1148.65, 1145.20, 1150.55, 1151.55, 1153.55, 1152.30};
      try (ResultSet resultSet =
          statement.executeQuery(
              "select master_repair(s1,s2,s3,m1,m2,m3,'omega'='2','eta'='3.0','k'='5','output_column'='2') from root.testMasterRepair.d1")) {
        int columnCount = resultSet.getMetaData().getColumnCount();
        assertEquals(1 + 1, columnCount);
        for (int i = 0; i < timestamps.length; i++) {
          resultSet.next();
          long expectedTimestamp = timestamps[i];
          long actualTimestamp = Long.parseLong(resultSet.getString(1));
          assertEquals(expectedTimestamp, actualTimestamp);

          double expectedResult = r5[i];
          double actualResult = resultSet.getDouble(2);
          double delta = 0.001;
          assertEquals(expectedResult, actualResult, delta);
        }
      }

      // test 6
      double[] r6 = {0.195, 0.193, 0.192, 0.194, 0.195, 0.193, 0.194, 0.190};
      try (ResultSet resultSet =
          statement.executeQuery(
              "select master_repair(s1,s2,s3,m1,m2,m3,'omega'='2','eta'='3.0','k'='5','output_column'='3') from root.testMasterRepair.d1")) {
        int columnCount = resultSet.getMetaData().getColumnCount();
        assertEquals(1 + 1, columnCount);
        for (int i = 0; i < timestamps.length; i++) {
          resultSet.next();
          long expectedTimestamp = timestamps[i];
          long actualTimestamp = Long.parseLong(resultSet.getString(1));
          assertEquals(expectedTimestamp, actualTimestamp);

          double expectedResult = r6[i];
          double actualResult = resultSet.getDouble(2);
          double delta = 0.001;
          assertEquals(expectedResult, actualResult, delta);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testChangePoints() {
    String[] createSQLs =
        new String[] {
          "CREATE DATABASE root.testChangePoints",
          "CREATE TIMESERIES root.testChangePoints.d1.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
          "CREATE TIMESERIES root.testChangePoints.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
          "CREATE TIMESERIES root.testChangePoints.d1.s3 WITH DATATYPE=INT64, ENCODING=PLAIN",
          "CREATE TIMESERIES root.testChangePoints.d1.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
          "CREATE TIMESERIES root.testChangePoints.d1.s5 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
          "CREATE TIMESERIES root.testChangePoints.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        };

    String[] insertSQLs =
        new String[] {
          "INSERT INTO root.testChangePoints.d1(timestamp, s1, s2, s3, s4, s5, s6) values(1, true, 1, 1, 1.0, 1.0, \"1test1\")",
          "INSERT INTO root.testChangePoints.d1(timestamp, s1, s2, s3, s4, s5, s6) values(2, true, 2, 2, 2.0, 1.0, \"2test2\")",
          "INSERT INTO root.testChangePoints.d1(timestamp, s1, s2, s3, s4, s5, s6) values(3, false, 1, 2, 1.0, 1.0, \"2test2\")",
          "INSERT INTO root.testChangePoints.d1(timestamp, s1, s2, s3, s4, s5, s6) values(4, true, 1, 3, 1.0, 1.0, \"1test1\")",
          "INSERT INTO root.testChangePoints.d1(timestamp, s1, s2, s3, s4, s5, s6) values(5, true, 1, 3, 1.0, 1.0, \"1test1\")"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String createSQL : createSQLs) {
        statement.execute(createSQL);
      }

      for (String insertSQL : insertSQLs) {
        statement.execute(insertSQL);
      }

      String[] expectedHeader =
          new String[] {
            TIMESTAMP_STR,
            "change_points(root.testChangePoints.d1.s1)",
            "change_points(root.testChangePoints.d1.s2)",
            "change_points(root.testChangePoints.d1.s3)",
            "change_points(root.testChangePoints.d1.s4)",
            "change_points(root.testChangePoints.d1.s5)",
            "change_points(root.testChangePoints.d1.s6)"
          };

      String[] retArray =
          new String[] {
            "1,true,1,1,1.0,1.0,1test1,",
            "2,null,2,2,2.0,null,2test2,",
            "3,false,1,null,1.0,null,null,",
            "4,true,null,3,null,null,1test1,"
          };

      resultSetEqualTest(
          "select change_points(s1), change_points(s2), change_points(s3), change_points(s4), change_points(s5), change_points(s6) from root.testChangePoints.d1",
          expectedHeader,
          retArray);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
