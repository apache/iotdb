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

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.constant.TestConstant.minTime;
import static org.apache.iotdb.db.constant.TestConstant.minValue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBRecoverUnclosedIT {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final String TIMESTAMP_STR = "Time";
  private static final String TEMPERATURE_STR = "root.ln.wf01.wt01.temperature";
  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "SET STORAGE GROUP TO root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
      };
  private static String[] dataSet2 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(5, 5.5, false, 55)"
      };
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";
  private String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)" + " VALUES(%d,%d,%d,%f,%s,%s)";

  private WALMode prevWALMode;

  @Before
  public void setUp() throws Exception {
    prevWALMode = config.getWalMode();
    config.setWalMode(WALMode.SYNC);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setWalMode(prevWALMode);
  }

  @Test
  public void test() throws SQLException, IOException, StartupException {
    String[] retArray = new String[] {"0,2", "0,4", "0,3"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select count(temperature) from root.ln.wf01.wt01 where time > 3");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute("select min_time(temperature) from root.ln.wf01.wt01 where time > 3");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(minTime(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      hasResultSet =
          statement.execute(
              "select min_time(temperature) from root.ln.wf01.wt01 where temperature > 3");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(minTime(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }
    insertMoreData();
    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    // test count, max, min value
    retArray = new String[] {"0,8499,500.0", "0,2499,500.0"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select count(*) from root.vehicle.d0");
      Assert.assertTrue(hasResultSet);
      statement.getResultSet().next();
      Assert.assertEquals(7500, statement.getResultSet().getInt("count(" + d0s0 + ")"));

      hasResultSet =
          statement.execute(
              "select max_value(s0),min_value(s2) "
                  + "from root.vehicle.d0 where time >= 100 and time < 9000");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue(d0s0))
                  + ","
                  + resultSet.getString(minValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "select max_value(s0),min_value(s2) " + "from root.vehicle.d0 where time < 2500");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue(d0s0))
                  + ","
                  + resultSet.getString(minValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      for (String sql : dataSet2) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void insertMoreData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // prepare BufferWrite file
      for (int i = 5000; i < 7000; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      for (int i = 7500; i < 8500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      // prepare Unseq-File
      for (int i = 500; i < 1500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      for (int i = 3000; i < 6500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }

      // prepare BufferWrite cache
      for (int i = 9000; i < 10000; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      // prepare Overflow cache
      for (int i = 2000; i < 2500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
