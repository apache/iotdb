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
package org.apache.iotdb.db.it.cq;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.runtime.ClusterTestResultSet;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCQExecIT {

  private static final long CQ_RESULT_POLL_INTERVAL_IN_MS = 100;
  private static final long CQ_RESULT_TIMEOUT_IN_SECONDS = 30;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // A 2s resampling CQ must write the expected maximum for every 1s window after its future
  // boundary.
  @Test
  public void testCQExecution1() {
    String insertTemplate =
        "INSERT INTO root.sg.d1(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d1.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d1.s1_max WITH DATATYPE=INT64");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d1(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq1\n"
              + "RESAMPLE EVERY 2s\n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "BEGIN \n"
              + "  SELECT max_value(s1) \n"
              + "  INTO root.sg.d1(s1_max)\n"
              + "  FROM root.sg.d1\n"
              + "  GROUP BY(1s) \n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq1");
        return;
      }

      long[] expectedTime = {
        startTime + 1_000, startTime + 2_000, startTime + 3_000, startTime + 4_000
      };
      long[] expectedValue = {4, 6, 8, 10};

      try {
        awaitCQResultCount(statement, "select s1_max from root.sg.d1", expectedTime.length);
        try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d1")) {
          int cnt = 0;
          while (resultSet.next()) {
            assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
            assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d1.s1_max"));
            cnt++;
          }
          assertEquals(expectedTime.length, cnt);
        }
      } finally {
        statement.execute("DROP CQ cq1");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // A CQ without EVERY and with a 4s range must produce the five expected 1s-window maxima.
  @Test
  public void testCQExecution2() {
    String insertTemplate =
        "INSERT INTO root.sg.d2(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d2.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d2.s1_max WITH DATATYPE=INT64");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d2(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq2\n"
              + "RESAMPLE \n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "RANGE 4s \n"
              + "BEGIN \n"
              + "  SELECT max_value(s1) \n"
              + "  INTO root.sg.d2(s1_max)\n"
              + "  FROM root.sg.d2\n"
              + "  GROUP BY(1s) \n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq2");
        return;
      }

      long[] expectedTime = {
        startTime, startTime + 1_000, startTime + 2_000, startTime + 3_000, startTime + 4_000
      };
      long[] expectedValue = {2, 4, 6, 8, 10};

      try {
        awaitCQResultCount(statement, "select s1_max from root.sg.d2", expectedTime.length);
        try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d2")) {
          int cnt = 0;
          while (resultSet.next()) {
            assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
            assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d2.s1_max"));
            cnt++;
          }
          assertEquals(expectedTime.length, cnt);
        }
      } finally {
        statement.execute("DROP CQ cq2");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // A 4s range with FILL must materialize the missing leading 1s window and retain later maxima.
  @Test
  public void testCQExecution3() {
    String insertTemplate =
        "INSERT INTO root.sg.d3(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d3.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d3.s1_max WITH DATATYPE=INT64");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d3(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq3\n"
              + "RESAMPLE EVERY 2s\n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "RANGE 4s\n"
              + "BEGIN \n"
              + "  SELECT max_value(s1) \n"
              + "  INTO root.sg.d3(s1_max)\n"
              + "  FROM root.sg.d3\n"
              + "  GROUP BY(1s) \n"
              + "  FILL(100)\n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq3");
        return;
      }

      long[] expectedTime = {
        startTime - 1_000,
        startTime,
        startTime + 1_000,
        startTime + 2_000,
        startTime + 3_000,
        startTime + 4_000
      };
      long[] expectedValue = {100, 2, 4, 6, 8, 10};

      String query =
          "select s1_max from root.sg.d3 where time between "
              + (startTime - 1_000)
              + " and "
              + (startTime + 4_000);
      try {
        awaitCQResultCount(statement, query, expectedTime.length);
        try (ResultSet resultSet = statement.executeQuery(query)) {
          int cnt = 0;
          while (resultSet.next()) {
            assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
            assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d3.s1_max"));
            cnt++;
          }
          assertEquals(expectedTime.length, cnt);
        }
      } finally {
        statement.execute("DROP CQ cq3");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // RANGE 2s, 1s must select two separated 1s windows as resampling advances.
  @Test
  public void testCQExecution4() {
    String insertTemplate =
        "INSERT INTO root.sg.d4(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d4.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d4.s1_max WITH DATATYPE=INT64");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d4(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq4\n"
              + "RESAMPLE EVERY 2s\n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "RANGE 2s, 1s\n"
              + "BEGIN \n"
              + "  SELECT max_value(s1) \n"
              + "  INTO root.sg.d4(s1_max)\n"
              + "  FROM root.sg.d4\n"
              + "  GROUP BY(1s) \n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq4");
        return;
      }

      long[] expectedTime = {startTime + 1_000, startTime + 3_000};
      long[] expectedValue = {4, 8};

      try {
        awaitCQResultCount(statement, "select s1_max from root.sg.d4", expectedTime.length);
        try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d4")) {
          int cnt = 0;
          while (resultSet.next()) {
            assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
            assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d4.s1_max"));
            cnt++;
          }
          assertEquals(expectedTime.length, cnt);
        }
      } finally {
        statement.execute("DROP CQ cq4");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // An aligned-by-device CQ must write the expression result for each of the ten input timestamps.
  @Test
  public void testCQExecution5() {
    String insertTemplate =
        "INSERT INTO root.sg.d5(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d5.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d5.precalculated_s1 WITH DATATYPE=DOUBLE");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d5(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq5\n"
              + "RESAMPLE EVERY 2s\n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "RANGE 4s\n"
              + "BEGIN \n"
              + "  SELECT s1 + 1 \n"
              + "  INTO root.sg.d5(precalculated_s1)\n"
              + "  FROM root.sg.d5\n"
              + "  align by device\n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq5");
        return;
      }

      long[] expectedTime = {
        startTime,
        startTime + 500,
        startTime + 1_000,
        startTime + 1_500,
        startTime + 2_000,
        startTime + 2_500,
        startTime + 3_000,
        startTime + 3_500,
        startTime + 4_000,
        startTime + 4_500
      };
      double[] expectedValue = {2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0};

      String query =
          "select precalculated_s1 from root.sg.d5 where time between "
              + startTime
              + " and "
              + (startTime + 4_500);
      try {
        awaitCQResultCount(statement, query, expectedTime.length);
        try (ResultSet resultSet = statement.executeQuery(query)) {
          int cnt = 0;
          while (resultSet.next()) {
            assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
            assertEquals(
                expectedValue[cnt], resultSet.getDouble("root.sg.d5.precalculated_s1"), 0.00001);
            cnt++;
          }
          assertEquals(expectedTime.length, cnt);
        }
      } finally {
        statement.execute("DROP CQ cq5");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void awaitCQResultCount(Statement statement, String query, int expectedRowCount)
      throws Exception {
    // CQ writes can fall between DataNode query snapshots. Count each snapshot independently
    // while waiting; the caller then compares all timestamps and values through the cluster
    // wrapper. Query and transport errors still propagate immediately.
    Awaitility.await("continuous query output row count")
        .atMost(CQ_RESULT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(CQ_RESULT_POLL_INTERVAL_IN_MS, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              try (ResultSet resultSet = statement.executeQuery(query)) {
                for (ResultSet nodeResultSet : ClusterTestResultSet.getNodeResultSets(resultSet)) {
                  int rowCount = 0;
                  while (nodeResultSet.next()) {
                    rowCount++;
                  }
                  if (rowCount < expectedRowCount) {
                    return false;
                  }
                }
                return true;
              }
            });
  }
}
