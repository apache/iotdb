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

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBUDTFFragmentIT {

  protected static final int ITERATION_TIMES = 15_000;
  protected static final double ERROR_DELTA = 0.001;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg.d1.s with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d2.s with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d3.s with datatype=FLOAT,encoding=PLAIN");

      statement.execute("CREATE TIMESERIES root.sg.d4.s with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d5.s with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d6.s with datatype=DOUBLE,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i % 2 == 0) {
          statement.execute(
              String.format("insert into root.sg.d1(timestamp,s) values(%d,%d)", i, i));
        } else {
          statement.execute(
              (String.format("insert into root.sg.d2(timestamp,s) values(%d,%d)", i, i)));
        }
        statement.execute(
            (String.format("insert into root.sg.d3(timestamp,s) values(%d,%d)", i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void queryWith4Fragments() {
    final String sql = "select sin(d1.s), sin(d2.s), sin(d3.s), sin(d4.s) from root.sg";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);

      int rowCount = 0;
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);

          if (i == 2) {
            if (rowCount % 2 == 0) {
              assertEquals(Math.sin(rowCount), Double.parseDouble(actualString), ERROR_DELTA);
            } else {
              assertNull(actualString);
            }
          }

          if (i == 2 + 1) {
            if (rowCount % 2 == 0) {
              assertNull(actualString);
            } else {
              assertEquals(Math.sin(rowCount), Double.parseDouble(actualString), ERROR_DELTA);
            }
          }

          if (i == 2 + 2) {
            assertEquals(Math.sin(rowCount), Double.parseDouble(actualString), ERROR_DELTA);
          }

          if (i == 2 + 3) {
            assertNull(actualString);
          }
        }
        ++rowCount;
      }
      assertEquals(ITERATION_TIMES, rowCount);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithEmptyColumns() {
    final String sql = "select d4.s, sin(d5.s), cos(d6.s), tan(d4.s) from root.sg";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertEquals(1 + 4, resultSet.getMetaData().getColumnCount());
      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithNullFields() {
    final String sql = "select d1.s, d3.s / d3.s, d2.s / (d3.s / d3.s), d4.s from root.sg";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);

      int rowCount = 0;
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);

          if (i == 2) {
            if (rowCount % 2 == 0) {
              assertEquals(rowCount, Double.parseDouble(actualString), ERROR_DELTA);
            } else {
              assertNull(actualString);
            }
          }

          if (i == 2 + 1) {
            assertEquals(
                rowCount == 0 ? Double.NaN : 1.0, Double.parseDouble(actualString), ERROR_DELTA);
          }

          if (i == 2 + 2) {
            if (rowCount % 2 == 0) {
              assertNull(actualString);
            } else {
              assertEquals(rowCount, Double.parseDouble(actualString), ERROR_DELTA);
            }
          }

          if (i == 2 + 3) {
            assertNull(actualString);
          }
        }
        ++rowCount;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void queryWithComplexExpression() {
    final String sql =
        "select d1.s * 2, d2.s, sin(d1.s), d4.s, d3.s / d3.s, d5.s, d6.s, d1.s, d5.s + d6.s, d5.s + d6.s from root.sg";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 10, columnCount);

      int rowCount = 0;
      while (resultSet.next()) {
        for (int i = 2; i <= columnCount; ++i) {
          String actualString = resultSet.getString(i);

          if (i == 2) {
            if (rowCount % 2 == 0) {
              assertEquals(2 * rowCount, Double.parseDouble(actualString), ERROR_DELTA);
            } else {
              assertNull(actualString);
            }
          }

          if (i == 2 + 1) {
            if (rowCount % 2 == 0) {
              assertNull(actualString);
            } else {
              assertEquals(rowCount, Double.parseDouble(actualString), ERROR_DELTA);
            }
          }

          if (i == 2 + 2) {
            if (rowCount % 2 == 0) {
              assertEquals(Math.sin(rowCount), Double.parseDouble(actualString), ERROR_DELTA);
            } else {
              assertNull(actualString);
            }
          }

          if (i == 2 + 3) {
            assertNull(actualString);
          }

          if (i == 2 + 4) {
            assertEquals(
                rowCount == 0 ? Double.NaN : 1.0, Double.parseDouble(actualString), ERROR_DELTA);
          }

          if (i == 2 + 5) {
            assertNull(actualString);
          }

          if (i == 2 + 6) {
            assertNull(actualString);
          }

          if (i == 2 + 7) {
            if (rowCount % 2 == 0) {
              assertEquals(rowCount, Double.parseDouble(actualString), ERROR_DELTA);
            } else {
              assertNull(actualString);
            }
          }

          if (i == 2 + 8) {
            assertNull(actualString);
          }

          if (i == 2 + 9) {
            assertNull(actualString);
          }

          if (i == 2 + 10) {
            assertNull(actualString);
          }
        }
        ++rowCount;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
