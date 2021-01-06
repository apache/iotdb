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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBUDTFBuiltinFunctionIT {

  private static final double E = 0.0001;

  private final static String[] INSERTION_SQLS = {
      "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (0, 0, 0, 0, 0, true, '0')",
      "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (2, 1, 1, 1, 1, false, '1')",
      "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (4, 2, 2, 2, 2, false, '2')",
      "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (6, 3, 3, 3, 3, true, '3')",
      "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (8, 4, 4, 4, 4, true, '4')",
  };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.d1.s1"), TSDataType.INT32, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.d1.s2"), TSDataType.INT64, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.d1.s3"), TSDataType.FLOAT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.d1.s4"), TSDataType.DOUBLE, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.d1.s5"), TSDataType.BOOLEAN, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.d1.s6"), TSDataType.TEXT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
  }

  private static void generateData() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (String dataGenerationSql : INSERTION_SQLS) {
        statement.execute(dataGenerationSql);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testMathFunctions() {
    testMathFunction("sin", Math::sin);
    testMathFunction("cos", Math::cos);
    testMathFunction("tan", Math::tan);
    testMathFunction("asin", Math::asin);
    testMathFunction("acos", Math::acos);
    testMathFunction("atan", Math::atan);
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
    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(String
          .format("select %s(s1), %s(s2), %s(s3), %s(s4) from root.sg.d1", functionName,
              functionName, functionName, functionName));

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
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSelectorFunctions() {
    final String TOP_K = "TOP_K";
    final String BOTTOM_K = "BOTTOM_K";
    final String K = "'k'='2'";

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(String.format(
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
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(String.format(
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
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testStringProcessingFunctions() {
    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(
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
    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(String
          .format("select %s(s1), %s(s2), %s(s3), %s(s4) from root.sg.d1", functionName,
              functionName, functionName, functionName));

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length - 1; ++i) {
        resultSet.next();
        for (int j = 0; j < 4; ++j) {
          assertEquals(expected, Double.parseDouble(resultSet.getString(2 + j)), E);
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
