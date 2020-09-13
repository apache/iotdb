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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBUDTFAlignByTimeQueryIT {

  protected final static int ITERATION_TIMES = 10_000;

  protected final static int ADDEND = 500_000;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup("root.vehicle");
    IoTDB.metaManager.createTimeseries("root.vehicle.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager.createTimeseries("root.vehicle.d1.s2", TSDataType.INT64, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager.createTimeseries("root.vehicle.d2.s1", TSDataType.FLOAT, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager.createTimeseries("root.vehicle.d2.s2", TSDataType.DOUBLE, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager.createTimeseries("root.vehicle.d3.s1", TSDataType.FLOAT, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager.createTimeseries("root.vehicle.d3.s2", TSDataType.DOUBLE, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
  }

  private static void generateData() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        statement.execute((i % 3 != 0
            ? String
            .format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)
            : i % 2 == 0
                ? String.format("insert into root.vehicle.d1(timestamp,s1) values(%d,%d)", i, i)
                : String.format("insert into root.vehicle.d1(timestamp,s2) values(%d,%d)", i, i)));
        statement.execute((String
            .format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
        statement.execute((String
            .format("insert into root.vehicle.d3(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void queryWithoutValueFilter1() {
    String sqlStr = "select udf(d1.s2, d1.s1), udf(d1.s1, d1.s2), d1.s1, d1.s2, udf(d1.s1, d1.s2), udf(d1.s2, d1.s1), d1.s1, d1.s2 from root.vehicle";

    Set<Integer> s1s2 = new HashSet<>(Arrays.asList(0, 1, 4, 5));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(2, 6));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(3, 7));

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 8, columnCount);

      StringBuilder expected, actual;
      while (resultSet.next()) {
        expected = new StringBuilder();
        actual = new StringBuilder();
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          actual.append(actualString == null
              ? "null" : Float.parseFloat(actualString)).append(", ");

          if (s1s2.contains(i - 2)) {
            expected.append(count % 3 != 0 ? (float) (count * 2) : "null").append(", ");
          } else if (s1.contains(i - 2)) {
            expected.append(count % 3 != 0 || count % 2 == 0 ? (float) count : "null").append(", ");
          } else if (s2.contains(i - 2)) {
            expected.append(count % 3 != 0 || count % 2 != 0 ? (float) count : "null").append(", ");
          }
        }

        assertEquals(expected.toString(), actual.toString());
        ++count;
      }
      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithoutValueFilter2() {
    String sqlStr = "select udf(*, *) from root.vehicle.d1";

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          assertTrue(actualString == null || (int) (Float.parseFloat(actualString)) == count * 2);
        }
        ++count;
      }
      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithoutValueFilter3() {
    String sqlStr = "select *, udf(*, *), *, udf(*, *), * from root.vehicle.d1";

    Set<Integer> s1AndS2 = new HashSet<>(Arrays.asList(2, 3, 4, 5, 8, 9, 10, 11));
    Set<Integer> s1OrS2 = new HashSet<>(Arrays.asList(0, 1, 6, 7, 12, 13));

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 14, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1AndS2.contains(i - 2)) {
            assertTrue(actualString == null || (int) (Float.parseFloat(actualString)) == count * 2);
          } else if (s1OrS2.contains(i - 2)) {
            assertTrue(actualString == null || (int) (Float.parseFloat(actualString)) == count);
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
  public void queryWithoutValueFilter4() {
    String sqlStr =
        "select udf(*, *, \"addend\"=\"" + ADDEND + "\"), *, udf(*, *) from root.vehicle.d1";

    Set<Integer> s1AndS2WithAddend = new HashSet<>(Arrays.asList(0, 1, 2, 3));
    Set<Integer> s1AndS2 = new HashSet<>(Arrays.asList(6, 7, 8, 9));
    Set<Integer> s1OrS2 = new HashSet<>(Arrays.asList(4, 5));

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 10, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1AndS2WithAddend.contains(i - 2)) {
            assertTrue(actualString == null
                || (int) (Float.parseFloat(actualString)) == count * 2 + ADDEND);
          } else if (s1AndS2.contains(i - 2)) {
            assertTrue(actualString == null || (int) (Float.parseFloat(actualString)) == count * 2);
          } else if (s1OrS2.contains(i - 2)) {
            assertTrue(actualString == null || (int) (Float.parseFloat(actualString)) == count);
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
            + String.format(" where d2.s1 >= %d and d2.s2 < %d", (int) (0.25 * ITERATION_TIMES),
            (int) (0.75 * ITERATION_TIMES));

    Set<Integer> s1s2 = new HashSet<>(Arrays.asList(0, 1, 4, 5));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(2, 6));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(3, 7));

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.25 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 8, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1s2.contains(i - 2)) {
            assertEquals(index * 2, (int) (Float.parseFloat(actualString)));
          } else if (s1.contains(i - 2)) {
            assertEquals(index, (int) (Float.parseFloat(actualString)));
          } else if (s2.contains(i - 2)) {
            assertEquals(index, (int) (Float.parseFloat(actualString)));
          }
        }
        ++index;
      }
      assertEquals((int) (0.5 * ITERATION_TIMES), index - (int) (0.25 * ITERATION_TIMES));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter2() {
    String sqlStr =
        "select udf(*, *, \"addend\"=\"" + ADDEND + "\"), *, udf(*, *) from root.vehicle.d2"
            + String.format(" where s1 >= %d and s2 < %d", (int) (0.25 * ITERATION_TIMES),
            (int) (0.75 * ITERATION_TIMES));

    Set<Integer> s1AndS2WithAddend = new HashSet<>(Arrays.asList(0, 1, 2, 3));
    Set<Integer> s1AndS2 = new HashSet<>(Arrays.asList(6, 7, 8, 9));
    Set<Integer> s1OrS2 = new HashSet<>(Arrays.asList(4, 5));

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.25 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 10, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1AndS2WithAddend.contains(i - 2)) {
            assertEquals(index * 2 + ADDEND, (int) (Float.parseFloat(actualString)));
          } else if (s1AndS2.contains(i - 2)) {
            assertEquals(index * 2, (int) (Float.parseFloat(actualString)));
          } else if (s1OrS2.contains(i - 2)) {
            assertEquals(index, (int) (Float.parseFloat(actualString)));
          }
        }
        ++index;
      }
      assertEquals((int) (0.5 * ITERATION_TIMES), index - (int) (0.25 * ITERATION_TIMES));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter3() {
    String sqlStr =
        "select udf(d1.s2, d1.s1), udf(d1.s1, d1.s2), d1.s1, d1.s2, udf(d1.s1, d1.s2), udf(d1.s2, d1.s1), d1.s1, d1.s2 from root.vehicle"
            + String.format(" where d3.s1 >= %d and d3.s2 < %d", (int) (0.3 * ITERATION_TIMES),
            (int) (0.7 * ITERATION_TIMES));

    Set<Integer> s1s2 = new HashSet<>(Arrays.asList(0, 1, 4, 5));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(2, 6));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(3, 7));

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.3 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 8, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1s2.contains(i - 2)) {
            if (index % 3 != 0) {
              assertEquals(index * 2, Float.parseFloat(actualString), 0);
            } else {
              assertNull(actualString);
            }
          } else if (s1.contains(i - 2)) {
            if (index % 3 != 0 || index % 2 == 0) {
              assertEquals(index, Float.parseFloat(actualString), 0);
            } else {
              assertNull(actualString);
            }
          } else if (s2.contains(i - 2)) {
            if (index % 3 != 0 || index % 2 != 0) {
              assertEquals(index, Float.parseFloat(actualString), 0);
            } else {
              assertNull(actualString);
            }
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
  public void queryWithValueFilter4() {
    String sqlStr =
        "select udf(s2, s1), udf(s1, s2), s1, s2, udf(s1, s2), udf(s2, s1), s1, s2 from root.vehicle.d2, root.vehicle.d3"
            + String.format(" where root.vehicle.d2.s1 >= %d and root.vehicle.d3.s2 < %d",
            (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    Set<Integer> s1s2 = new HashSet<>(
        Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 16, 17, 18, 19));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(8, 9, 20, 21));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(10, 11, 22, 23));

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.3 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4 * 2 + 4 + 4 * 2 + 4, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1s2.contains(i - 2)) {
            assertEquals(index * 2, Float.parseFloat(actualString), 0);
          } else if (s1.contains(i - 2) || s2.contains(i - 2)) {
            assertEquals(index, Float.parseFloat(actualString), 0);
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
  public void queryWithValueFilter5() {
    String sqlStr =
        "select *, udf(*, *), udf(*, *) from root.vehicle.d2, root.vehicle.d3, root.vehicle.d2"
            + String.format(" where root.vehicle.d2.s1 >= %d and root.vehicle.d3.s2 < %d",
            (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    try (Statement statement = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int index = (int) (0.3 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6 + 2 * 2 * 3 * 2 * 3, columnCount); // time + * + 2 * udf(*, *)
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          assertEquals(i - 2 < 6 ? index : 2 * index, Float.parseFloat(actualString), 0);
        }
        ++index;
      }
      assertEquals((int) (0.4 * ITERATION_TIMES), index - (int) (0.3 * ITERATION_TIMES));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
