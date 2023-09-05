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

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCQExecution1() {
    String insertTemplate =
        "INSERT INTO root.sg.d1(time, s1) VALUES (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d1.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d1.s1_max WITH DATATYPE=INT64");

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

      long targetTime = firstExecutionTime + 5_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
      }

      long[] expectedTime = {
        startTime + 1_000, startTime + 2_000, startTime + 3_000, startTime + 4_000
      };
      long[] expectedValue = {4, 6, 8, 10};

      try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d1.s1_max"));
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq1");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCQExecution2() {
    String insertTemplate =
        "INSERT INTO root.sg.d2(time, s1) VALUES (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d2.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d2.s1_max WITH DATATYPE=INT64");

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

      long targetTime = firstExecutionTime + 5_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
      }

      long[] expectedTime = {
        startTime, startTime + 1_000, startTime + 2_000, startTime + 3_000, startTime + 4_000
      };
      long[] expectedValue = {2, 4, 6, 8, 10};

      try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d2")) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d2.s1_max"));
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq2");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCQExecution3() {
    String insertTemplate =
        "INSERT INTO root.sg.d3(time, s1) VALUES (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d3.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d3.s1_max WITH DATATYPE=INT64");

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

      long targetTime = firstExecutionTime + 5_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
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

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1_max from root.sg.d3 where time between "
                  + (startTime - 1_000)
                  + " and "
                  + (startTime + 4_000))) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d3.s1_max"));
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq3");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCQExecution4() {
    String insertTemplate =
        "INSERT INTO root.sg.d4(time, s1) VALUES (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d4.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d4.s1_max WITH DATATYPE=INT64");

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

      long targetTime = firstExecutionTime + 5_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
      }

      long[] expectedTime = {startTime + 1_000, startTime + 3_000};
      long[] expectedValue = {4, 8};

      try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d4")) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d4.s1_max"));
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq4");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCQExecution5() {
    String insertTemplate =
        "INSERT INTO root.sg.d5(time, s1) VALUES (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d) (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d5.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d5.precalculated_s1 WITH DATATYPE=DOUBLE");

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

      long targetTime = firstExecutionTime + 5_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
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

      try (ResultSet resultSet =
          statement.executeQuery(
              "select precalculated_s1 from root.sg.d5 where time between "
                  + startTime
                  + " and "
                  + (startTime + 4_500))) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(
              expectedValue[cnt], resultSet.getDouble("root.sg.d5.precalculated_s1"), 0.00001);
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq5");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
