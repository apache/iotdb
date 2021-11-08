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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class IoTDBUDTFNonAlignQueryIT {

  protected static final int ITERATION_TIMES = Config.DEFAULT_FETCH_SIZE;

  protected static final int ADDEND = 500_000_000;

  protected static final int LIMIT = (int) (0.1 * ITERATION_TIMES);
  protected static final int OFFSET = (int) (0.1 * ITERATION_TIMES);

  protected static final int SLIMIT = 5;
  protected static final int SOFFSET = 2;

  @BeforeClass
  public static void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setUdfCollectorMemoryBudgetInMB(5);
    IoTDBDescriptor.getInstance().getConfig().setUdfTransformerMemoryBudgetInMB(5);
    IoTDBDescriptor.getInstance().getConfig().setUdfReaderMemoryBudgetInMB(5);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s2"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        statement.execute(
            (String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
      for (int i = 0; i < ITERATION_TIMES / 2; ++i) {
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setUdfCollectorMemoryBudgetInMB(100);
    IoTDBDescriptor.getInstance().getConfig().setUdfTransformerMemoryBudgetInMB(100);
    IoTDBDescriptor.getInstance().getConfig().setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void queryWithoutValueFilter1() {
    String sqlStr =
        "select udf(*, *, 'addend'='"
            + ADDEND
            + "'), *, udf(*, *) from root.vehicle.d1 disable align";

    Set<Integer> s1AndS2WithAddend = new HashSet<>(Arrays.asList(0, 1, 2, 3));
    Set<Integer> s1AndS2 = new HashSet<>(Arrays.asList(6, 7, 8, 9));
    Set<Integer> s1OrS2 = new HashSet<>(Arrays.asList(4, 5));

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(10 * 2, columnCount);
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          int originalIndex = (i - 1) / 2;
          if (i % 2 == 1) {
            assertEquals(count, (int) (Double.parseDouble(actualString)));
          } else {
            if (s1AndS2WithAddend.contains(originalIndex)) {
              assertEquals(count * 2 + ADDEND, (int) (Double.parseDouble(actualString)));
            } else if (s1AndS2.contains(originalIndex)) {
              assertEquals(count * 2, (int) (Double.parseDouble(actualString)));
            } else if (s1OrS2.contains(originalIndex)) {
              assertEquals(count, (int) (Double.parseDouble(actualString)));
            }
          }
        }
        ++count;
      }
      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithoutValueFilter2() {
    String sqlStr = "select udf(d1.s1, d1.s2), udf(d2.s1, d2.s2) from root.vehicle disable align";

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(2 * 2, columnCount);
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          int originalIndex = (i - 1) / 2;
          if (i % 2 == 1) {
            if (count < ITERATION_TIMES / 2 || originalIndex == 0) {
              assertEquals(count, (int) (Double.parseDouble(actualString)));
            } else {
              assertNull(actualString);
            }
          } else {
            if (count < ITERATION_TIMES / 2 || originalIndex == 0) {
              assertEquals(2 * count, (int) (Double.parseDouble(actualString)));
            } else {
              assertNull(actualString);
            }
          }
        }
        ++count;
      }
      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter1() {
    String sqlStr =
        "select udf(d2.s2, d2.s1), udf(d2.s1, d2.s2), d2.s1, d2.s2, udf(d2.s1, d2.s2), udf(d2.s2, d2.s1), d2.s1, d2.s2 from root.vehicle"
            + String.format(
                " where d2.s1 >= %d and d2.s2 < %d disable align",
                (int) (0.25 * ITERATION_TIMES), (int) (0.75 * ITERATION_TIMES));

    Set<Integer> s1s2 = new HashSet<>(Arrays.asList(0, 1, 4, 5));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(2, 6));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(3, 7));

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.25 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(8 * 2, columnCount);
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          int originalIndex = (i - 1) / 2;
          if (i % 2 == 1) {
            assertEquals(index, (int) (Double.parseDouble(actualString)));
          } else {
            if (s1s2.contains(originalIndex)) {
              assertEquals(index * 2, (int) (Double.parseDouble(actualString)));
            } else if (s1.contains(originalIndex)) {
              assertEquals(index, (int) (Double.parseDouble(actualString)));
            } else if (s2.contains(originalIndex)) {
              assertEquals(index, (int) (Double.parseDouble(actualString)));
            }
          }
        }
        ++index;
      }
      assertEquals((int) (0.25 * ITERATION_TIMES), index - (int) (0.25 * ITERATION_TIMES));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter2() {
    String sqlStr =
        "select udf(*, *) from root.vehicle.d1, root.vehicle.d1"
            + String.format(
                " where root.vehicle.d1.s1 >= %d and root.vehicle.d1.s2 < %d disable align",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.3 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(2 * 4 * 4, columnCount);
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (i % 2 == 1) {
            assertEquals(index, (int) (Double.parseDouble(actualString)));
          } else {
            assertEquals(2 * index, Double.parseDouble(actualString), 0);
          }
        }
        ++index;
      }
      assertEquals((int) (0.4 * ITERATION_TIMES), index - (int) (0.3 * ITERATION_TIMES));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter3() {
    String sqlStr =
        "select udf(d2.s2, d2.s1), udf(d2.s1, d2.s2), d2.s1, d2.s2, udf(d2.s1, d2.s2), udf(d2.s2, d2.s1), d2.s1, d2.s2 from root.vehicle"
            + String.format(
                " where d2.s1 >= %d and d2.s2 < %d slimit %d soffset %d disable align",
                (int) (0.25 * ITERATION_TIMES), (int) (0.75 * ITERATION_TIMES), SLIMIT, SOFFSET);

    Set<Integer> s1s2 = new HashSet<>(Arrays.asList(0, 1, 4, 5));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(2, 6));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(3, 7));

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.25 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(2 * SLIMIT, columnCount);
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          int originalIndex = (i - 1) / 2 + SOFFSET;
          if (i % 2 == 1) {
            assertEquals(index, (int) (Double.parseDouble(actualString)));
          } else {
            if (s1s2.contains(originalIndex)) {
              assertEquals(index * 2, (int) (Double.parseDouble(actualString)));
            } else if (s1.contains(originalIndex)) {
              assertEquals(index, (int) (Double.parseDouble(actualString)));
            } else if (s2.contains(originalIndex)) {
              assertEquals(index, (int) (Double.parseDouble(actualString)));
            }
          }
        }
        ++index;
      }
      assertEquals((int) (0.25 * ITERATION_TIMES), index - (int) (0.25 * ITERATION_TIMES));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter4() {
    String sqlStr =
        "select udf(*, *) from root.vehicle.d1, root.vehicle.d1"
            + String.format(
                " where root.vehicle.d1.s1 >= %d and root.vehicle.d1.s2 < %d limit %d offset %d disable align",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES), LIMIT, OFFSET);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.3 * ITERATION_TIMES) + OFFSET;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(2 * 4 * 4, columnCount);
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (i % 2 == 1) {
            assertEquals(index, (int) (Double.parseDouble(actualString)));
          } else {
            assertEquals(2 * index, Double.parseDouble(actualString), 0);
          }
        }
        ++index;
      }
      assertEquals(LIMIT, index - ((int) (0.3 * ITERATION_TIMES) + OFFSET));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
