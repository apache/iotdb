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

package org.apache.iotdb.db.integration.m4;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.apache.iotdb.db.query.udf.builtin.UDTFM4.DISPLAY_WINDOW_BEGIN_KEY;
import static org.apache.iotdb.db.query.udf.builtin.UDTFM4.DISPLAY_WINDOW_END_KEY;
import static org.apache.iotdb.db.query.udf.builtin.UDTFM4.SLIDING_STEP_KEY;
import static org.apache.iotdb.db.query.udf.builtin.UDTFM4.TIME_INTERVAL_KEY;
import static org.apache.iotdb.db.query.udf.builtin.UDTFM4.WINDOW_SIZE_KEY;
import static org.junit.Assert.fail;

public class MyTest5 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=DOUBLE,ENCODING=PLAIN",
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%d)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static boolean originalEnableCPV;
  private static CompactionStrategy originalCompactionStrategy;
  private static boolean originalUseChunkIndex;

  @Before
  public void setUp() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("PLAIN");
    originalCompactionStrategy = config.getCompactionStrategy();
    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    originalEnableCPV = config.isEnableCPV();
    //    config.setEnableCPV(false); // MOC
    config.setEnableCPV(true); // CPV

    originalUseChunkIndex = TSFileDescriptor.getInstance().getConfig().isUseTimeIndex();
    TSFileDescriptor.getInstance().getConfig().setUseTimeIndex(false);

    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    config.setTimestampPrecision("ms");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setCompactionStrategy(originalCompactionStrategy);
    config.setEnableCPV(originalEnableCPV);
    TSFileDescriptor.getInstance().getConfig().setUseTimeIndex(originalUseChunkIndex);
  }

  @Test
  public void testM4Function() {
    // create timeseries
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.m4");
      statement.execute("CREATE TIMESERIES root.m4.d1.s1 with datatype=double,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.m4.d1.s2 with datatype=INT64,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    // insert data
    String insertTemplate = "INSERT INTO root.m4.d1(timestamp,%s)" + " VALUES(%d,%d)";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // "root.m4.d1.s1" data illustration:
      // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 25, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 54, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 120, 8));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 20, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 40, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 54, 18));
      statement.execute("FLUSH");

      // "root.m4.d1.s2" data: constant value 1
      for (int i = 0; i < 100; i++) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s2", i, 1));
      }
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }

    // query tests
    test_M4_firstWindowEmpty();
    test_M4_slidingTimeWindow();
    test_M4_slidingSizeWindow();
    test_M4_constantTimeSeries();
  }

  private void test_M4_firstWindowEmpty() {
    String[] res = new String[] {"120,8.0"};

    String sql =
        String.format(
            "select M4_TW(s1, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.m4.d1",
            TIME_INTERVAL_KEY,
            25,
            SLIDING_STEP_KEY,
            25,
            DISPLAY_WINDOW_BEGIN_KEY,
            75,
            DISPLAY_WINDOW_END_KEY,
            150);

    try (Connection conn =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(res[count], str);
        count++;
      }
      Assert.assertEquals(res.length, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private void test_M4_slidingTimeWindow() {
    String[] res =
        new String[] {
          "1,5.0", "10,30.0", "20,20.0", "25,8.0", "30,40.0", "45,30.0", "52,8.0", "54,18.0",
          "120,8.0"
        };

    String sql =
        String.format(
            "select M4_TW(s1, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.m4.d1",
            TIME_INTERVAL_KEY,
            25,
            SLIDING_STEP_KEY,
            25,
            DISPLAY_WINDOW_BEGIN_KEY,
            0,
            DISPLAY_WINDOW_END_KEY,
            150);

    try (Connection conn =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(res[count], str);
        count++;
      }
      Assert.assertEquals(res.length, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private void test_M4_slidingSizeWindow() {
    String[] res = new String[] {"1,5.0", "30,40.0", "33,9.0", "35,10.0", "45,30.0", "120,8.0"};

    String sql =
        String.format(
            "select M4_TW(s1,'%s'='%s','%s'='%s') from root.m4.d1",
            WINDOW_SIZE_KEY, 10, SLIDING_STEP_KEY, 10);

    try (Connection conn =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(res[count], str);
        count++;
      }
      Assert.assertEquals(res.length, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private void test_M4_constantTimeSeries() {
    /* Result: 0,1 24,1 25,1 49,1 50,1 74,1 75,1 99,1 */
    String sql =
        String.format(
            "select M4_TW(s2, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.m4.d1",
            TIME_INTERVAL_KEY,
            25,
            SLIDING_STEP_KEY,
            25,
            DISPLAY_WINDOW_BEGIN_KEY,
            0,
            DISPLAY_WINDOW_END_KEY,
            100);

    try (Connection conn =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String expStr;
        if (count % 2 == 0) {
          expStr = 25 * (count / 2) + ",1";
        } else {
          expStr = 25 * (count / 2) + 24 + ",1";
        }
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(expStr, str);
        count++;
      }
      Assert.assertEquals(8, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
