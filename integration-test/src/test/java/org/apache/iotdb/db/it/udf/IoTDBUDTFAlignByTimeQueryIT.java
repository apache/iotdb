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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDTFAlignByTimeQueryIT {

  protected static final int ITERATION_TIMES = 100;

  protected static final int ADDEND = 500_000_000;

  protected static final int LIMIT = (int) (0.1 * ITERATION_TIMES);
  protected static final int OFFSET = (int) (0.1 * ITERATION_TIMES);

  protected static final int SLIMIT = 10;
  protected static final int SOFFSET = 2;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s1 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d2.s2 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d3.s1 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d3.s2 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d4.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d4.s2 with datatype=INT32,encoding=PLAIN");
      // create aligned timeseries
      statement.execute(("CREATE DATABASE root.sg1"));
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

  @Test
  public void queryWithUnquotedAttributes() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.executeQuery("select sum_sec(s1, 'interval'=3) from root.udf.d1;");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Attributes of functions should be quoted"));
      }
      try {
        statement.executeQuery(
            "select sum_sec(s1, 'max_interval'=1, 'standard'=udf) from root.udf.d1;");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Attributes of functions should be quoted"));
      }
    } catch (Exception throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithoutValueFilter1() {
    String sqlStr =
        "select udf(d1.s2, d1.s1), udf(d1.s1, d1.s2), d1.s1, d1.s2, udf(d1.s1, d1.s2), udf(d1.s2, d1.s1), d1.s1, d1.s2 from root.vehicle";

    Set<Integer> s1s2 = new HashSet<>(Arrays.asList(0, 1, 4, 5));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(2, 6));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(3, 7));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 8, columnCount);

      StringBuilder expected, actual;
      while (resultSet.next()) {
        expected = new StringBuilder();
        actual = new StringBuilder();
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          actual
              .append(actualString == null ? "null" : Double.parseDouble(actualString))
              .append(", ");

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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          assertTrue(actualString == null || (int) (Double.parseDouble(actualString)) == count * 2);
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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 14, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1AndS2.contains(i - 2)) {
            assertTrue(
                actualString == null || (int) (Double.parseDouble(actualString)) == count * 2);
          } else if (s1OrS2.contains(i - 2)) {
            assertTrue(actualString == null || (int) (Double.parseDouble(actualString)) == count);
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
        "select udf(*, *, 'addend'='" + ADDEND + "'), *, udf(*, *) from root.vehicle.d1";

    Set<Integer> s1AndS2WithAddend = new HashSet<>(Arrays.asList(0, 1, 2, 3));
    Set<Integer> s1AndS2 = new HashSet<>(Arrays.asList(6, 7, 8, 9));
    Set<Integer> s1OrS2 = new HashSet<>(Arrays.asList(4, 5));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int count = 0;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 10, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1AndS2WithAddend.contains(i - 2)) {
            assertTrue(
                actualString == null
                    || (int) (Double.parseDouble(actualString)) == count * 2 + ADDEND);
          } else if (s1AndS2.contains(i - 2)) {
            assertTrue(
                actualString == null || (int) (Double.parseDouble(actualString)) == count * 2);
          } else if (s1OrS2.contains(i - 2)) {
            assertTrue(actualString == null || (int) (Double.parseDouble(actualString)) == count);
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
  public void queryWithoutValueFilter5() {
    String sqlStr = "select multiplier(s2, 'a'='2', 'b'='5') from root.vehicle.d1";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());
      assertEquals("Time", resultSet.getMetaData().getColumnName(1));
      assertEquals(
          "multiplier(root.vehicle.d1.s2, \"a\"=\"2\", \"b\"=\"5\")",
          resultSet.getMetaData().getColumnName(2));

      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i % 3 != 0 || i % 2 != 0) {
          assertTrue(resultSet.next());
          assertEquals(i * 2 * 5, Integer.parseInt(resultSet.getString(2)));
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithoutValueFilter6() {
    String sqlStr = "select max(s1), max(s2) from root.vehicle.d4";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
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
      assertEquals(3 * (ITERATION_TIMES - 1), Integer.parseInt(resultSet.getString(columnS1)));
      assertNull(resultSet.getString(columnS2));
      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithoutValueFilter7() {
    String sqlStr = "select terminate(s1), terminate(s2) from root.vehicle.d4";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
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

      for (int i = 0; i < ITERATION_TIMES; ++i) {
        assertTrue(resultSet.next());
        assertEquals(1, Integer.parseInt(resultSet.getString(columnS1)));
        assertNull(resultSet.getString(columnS2));
      }

      assertTrue(resultSet.next());
      assertEquals(ITERATION_TIMES, Integer.parseInt(resultSet.getString(columnS1)));
      assertNull(resultSet.getString(columnS2));

      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithoutValueFilter8() {
    String sqlStr = "select validate(s1, s2, 'k'='') from root.vehicle.d3";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sqlStr);
      fail();
    } catch (SQLException throwable) {
      System.out.println(throwable.getMessage());
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64]. actual: FLOAT."));
    }
  }

  @Test
  public void queryWithoutValueFilter9() {
    String sqlStr = "select validate(s1, s2, s1, 'k'=''), * from root.vehicle.d1";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sqlStr);
      fail();
    } catch (SQLException throwable) {
      System.out.println(throwable.getMessage());
      assertTrue(
          throwable
              .getMessage()
              .contains("the number of the input series is not valid. expected: 2. actual: 3."));
    }
  }

  @Test
  public void queryWithoutValueFilter10() {
    String sqlStr = "select validate(s1, s2), * from root.vehicle.d1";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sqlStr);
      fail();
    } catch (SQLException throwable) {
      System.out.println(throwable.getMessage());
      assertTrue(
          throwable.getMessage().contains("attribute \"k\" is required but was not provided."));
    }
  }

  @Test
  public void queryWithValueFilter1() {
    String sqlStr =
        "select udf(d2.s2, d2.s1), udf(d2.s1, d2.s2), d2.s1, d2.s2, udf(d2.s1, d2.s2), udf(d2.s2, d2.s1), d2.s1, d2.s2 from root.vehicle"
            + String.format(
                " where d2.s1 >= %d and d2.s2 < %d",
                (int) (0.25 * ITERATION_TIMES), (int) (0.75 * ITERATION_TIMES));

    Set<Integer> s1s2 = new HashSet<>(Arrays.asList(0, 1, 4, 5));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(2, 6));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(3, 7));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int index = (int) (0.25 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 8, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1s2.contains(i - 2)) {
            assertEquals(index * 2, (int) (Double.parseDouble(actualString)));
          } else if (s1.contains(i - 2)) {
            assertEquals(index, (int) (Double.parseDouble(actualString)));
          } else if (s2.contains(i - 2)) {
            assertEquals(index, (int) (Double.parseDouble(actualString)));
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
        "select udf(*, *, 'addend'='"
            + ADDEND
            + "'), *, udf(*, *) from root.vehicle.d2"
            + String.format(
                " where s1 >= %d and s2 < %d",
                (int) (0.25 * ITERATION_TIMES), (int) (0.75 * ITERATION_TIMES));

    Set<Integer> s1AndS2WithAddend = new HashSet<>(Arrays.asList(0, 1, 2, 3));
    Set<Integer> s1AndS2 = new HashSet<>(Arrays.asList(6, 7, 8, 9));
    Set<Integer> s1OrS2 = new HashSet<>(Arrays.asList(4, 5));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int index = (int) (0.25 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 10, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1AndS2WithAddend.contains(i - 2)) {
            assertEquals(index * 2 + ADDEND, (int) (Double.parseDouble(actualString)));
          } else if (s1AndS2.contains(i - 2)) {
            assertEquals(index * 2, (int) (Double.parseDouble(actualString)));
          } else if (s1OrS2.contains(i - 2)) {
            assertEquals(index, (int) (Double.parseDouble(actualString)));
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
            + String.format(
                " where d3.s1 >= %d and d3.s2 < %d",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    Set<Integer> s1s2 = new HashSet<>(Arrays.asList(0, 1, 4, 5));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(2, 6));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(3, 7));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int index = (int) (0.3 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 8, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1s2.contains(i - 2)) {
            if (index % 3 != 0) {
              assertEquals(index * 2, Double.parseDouble(actualString), 0);
            } else {
              assertNull(actualString);
            }
          } else if (s1.contains(i - 2)) {
            if (index % 3 != 0 || index % 2 == 0) {
              assertEquals(index, Double.parseDouble(actualString), 0);
            } else {
              assertNull(actualString);
            }
          } else if (s2.contains(i - 2)) {
            if (index % 3 != 0 || index % 2 != 0) {
              assertEquals(index, Double.parseDouble(actualString), 0);
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
            + String.format(
                " where root.vehicle.d2.s1 >= %d and root.vehicle.d3.s2 < %d",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    Set<Integer> s1s2 =
        new HashSet<>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 16, 17, 18, 19));
    Set<Integer> s1 = new HashSet<>(Arrays.asList(8, 9, 20, 21));
    Set<Integer> s2 = new HashSet<>(Arrays.asList(10, 11, 22, 23));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int index = (int) (0.3 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4 * 2 + 4 + 4 * 2 + 4, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          if (s1s2.contains(i - 2)) {
            assertEquals(index * 2, Double.parseDouble(actualString), 0);
          } else if (s1.contains(i - 2) || s2.contains(i - 2)) {
            assertEquals(index, Double.parseDouble(actualString), 0);
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
            + String.format(
                " where root.vehicle.d2.s1 >= %d and root.vehicle.d3.s2 < %d",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int index = (int) (0.3 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6 + 2 * 2 * 3 * 2 * 3, columnCount); // time + * + 2 * udf(*, *)
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          assertEquals(i - 2 < 6 ? index : 2 * index, Double.parseDouble(actualString), 0);
        }
        ++index;
      }
      assertEquals((int) (0.4 * ITERATION_TIMES), index - (int) (0.3 * ITERATION_TIMES));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter6() {
    String sqlStr =
        "select *, udf(*, *), udf(*, *) from root.vehicle.d2, root.vehicle.d3, root.vehicle.d2"
            + String.format(
                " where root.vehicle.d2.s1 >= %d and root.vehicle.d3.s2 < %d ",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES))
            + String.format(" limit %d offset %d", LIMIT, OFFSET);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int index = (int) (0.3 * ITERATION_TIMES) + OFFSET;
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6 + 2 * 2 * 3 * 2 * 3, columnCount); // time + * + 2 * udf(*, *)
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          assertEquals(i - 2 < 6 ? index : 2 * index, Double.parseDouble(actualString), 0);
        }
        ++index;
      }
      assertEquals(LIMIT, index - ((int) (0.3 * ITERATION_TIMES) + OFFSET));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter7() {
    String sqlStr =
        "select *, udf(*, *), udf(*, *) from root.vehicle.d2, root.vehicle.d3, root.vehicle.d2"
            + String.format(
                " where root.vehicle.d2.s1 >= %d and root.vehicle.d3.s2 < %d ",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES))
            + String.format(" slimit %d soffset %d", SLIMIT, SOFFSET);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      int index = (int) (0.3 * ITERATION_TIMES);
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + SLIMIT, columnCount);
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);
          assertEquals(
              i - 2 + SOFFSET < 6 ? index : 2 * index, Double.parseDouble(actualString), 0);
        }
        ++index;
      }
      assertEquals((int) (0.4 * ITERATION_TIMES), index - (int) (0.3 * ITERATION_TIMES));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter8() {
    String sqlStr =
        "select max(s1), max(s2) from root.vehicle.d4"
            + String.format(
                " where root.vehicle.d4.s1 >= %d and root.vehicle.d4.s2 < %d ",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
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

      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithValueFilter9() {
    String sqlStr =
        "select max(s1), max(s2) from root.vehicle.d4"
            + String.format(
                " where root.vehicle.d4.s1 >= %d and root.vehicle.d4.s1 < %d ",
                (int) (0.3 * ITERATION_TIMES), (int) (0.7 * ITERATION_TIMES));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
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
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
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

  @Test
  public void queryNonexistentSeries() {
    String sqlStr =
        "select max(s100), udf(*, s100), udf(*, s100), udf(s100, s100) from root.vehicle.d4";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
      assertEquals(1, resultSet.getMetaData().getColumnCount());
      assertEquals("Time", resultSet.getMetaData().getColumnName(1));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRowByRowUDFWithConstants() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String query = "SELECT udf(s1, 1) FROM root.vehicle.d2";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 0; i < ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1.0D, rs.getLong(2), 0.001);
        }
        Assert.assertFalse(rs.next());
      }

      query = "SELECT udf(s1, (1 + 4) * 2 / 10) FROM root.vehicle.d2";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 0; i < ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1.0D, rs.getLong(2), 0.001);
        }
        Assert.assertFalse(rs.next());
      }

      query = "SELECT udf(s1 + 1, 0) FROM root.vehicle.d2";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 0; i < ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1.0D, rs.getLong(2), 0.001);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
