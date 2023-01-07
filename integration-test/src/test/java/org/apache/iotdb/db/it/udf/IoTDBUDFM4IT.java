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
import java.util.Locale;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDFM4IT {

  public static final String WINDOW_SIZE_KEY = "windowSize";
  public static final String TIME_INTERVAL_KEY = "timeInterval";
  public static final String SLIDING_STEP_KEY = "slidingStep";
  public static final String DISPLAY_WINDOW_BEGIN_KEY = "displayWindowBegin";
  public static final String DISPLAY_WINDOW_END_KEY = "displayWindowEnd";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test_M4_firstWindowEmpty() {
    String[] res = new String[] {"120,8.0"};

    String sql =
        String.format(
            "select M4(s1, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.vehicle.d1",
            TIME_INTERVAL_KEY,
            25,
            SLIDING_STEP_KEY,
            25,
            DISPLAY_WINDOW_BEGIN_KEY,
            75,
            DISPLAY_WINDOW_END_KEY,
            150);

    try (Connection conn = EnvFactory.getEnv().getConnection();
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

  @Test
  public void test_M4_slidingTimeWindow() {
    String[] res =
        new String[] {
          "1,5.0", "10,30.0", "20,20.0", "25,8.0", "30,40.0", "45,30.0", "52,8.0", "54,18.0",
          "120,8.0"
        };

    String sql =
        String.format(
            "select M4(s1, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.vehicle.d1",
            TIME_INTERVAL_KEY,
            25,
            SLIDING_STEP_KEY,
            25,
            DISPLAY_WINDOW_BEGIN_KEY,
            0,
            DISPLAY_WINDOW_END_KEY,
            150);

    try (Connection conn = EnvFactory.getEnv().getConnection();
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

  @Test
  public void test_M4_slidingSizeWindow() {
    String[] res = new String[] {"1,5.0", "30,40.0", "33,9.0", "35,10.0", "45,30.0", "120,8.0"};

    String sql =
        String.format(
            "select M4(s1,'%s'='%s','%s'='%s') from root.vehicle.d1",
            WINDOW_SIZE_KEY, 10, SLIDING_STEP_KEY, 10);

    try (Connection conn = EnvFactory.getEnv().getConnection();
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

  @Test
  public void test_M4_constantTimeSeries() {
    /** Result: 0,1 24,1 25,1 49,1 50,1 74,1 75,1 99,1 */
    String sql =
        String.format(
            "select M4(s2, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.vehicle.d1",
            TIME_INTERVAL_KEY,
            25,
            SLIDING_STEP_KEY,
            25,
            DISPLAY_WINDOW_BEGIN_KEY,
            0,
            DISPLAY_WINDOW_END_KEY,
            100);

    try (Connection conn = EnvFactory.getEnv().getConnection();
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

  @Test
  public void test_EQUAL_SIZE_BUCKET_M4_SAMPLE() {
    String[] res =
        new String[] {
          "1,5.0", "8,8.0", "10,30.0", "27,20.0", "30,40.0", "45,30.0", "52,8.0", "120,8.0"
        };

    String sql = "select EQUAL_SIZE_BUCKET_M4_SAMPLE(s1,'proportion'='0.5') from root.vehicle.d1";
    // the window size is 4*(int)(1/proportion) = 8

    try (Connection conn = EnvFactory.getEnv().getConnection();
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

  @Test
  public void test_EQUAL_SIZE_BUCKET_M4_SAMPLE_constantTimeSeries() {
    String sql = "select EQUAL_SIZE_BUCKET_M4_SAMPLE(s2, 'proportion'='0.5') from root.vehicle.d1";
    // the window size is 4*(int)(1/proportion) = 8

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String expStr;
        if (count / 4 * 8 < 8 * 12) { // each 8-point window sample 4 different points
          if (count % 4 == 0) {
            expStr = 8 * (count / 4) + ",1";
          } else if (count % 4 == 1) {
            expStr = 8 * (count / 4) + 1 + ",1";
          } else if (count % 4 == 2) {
            expStr = 8 * (count / 4 + 1) - 2 + ",1";
          } else {
            expStr = 8 * (count / 4 + 1) - 1 + ",1";
          }
        } else { // the last 4 points
          expStr = count - 48 + 96 + ",1";
        }
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(expStr, str);
        count++;
      }
      Assert.assertEquals(52, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 with datatype=double,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d1(timestamp,%s)" + " VALUES(%d,%d)";

  private static void generateData() {
    // data:
    // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

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

      for (int i = 0; i < 100; i++) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s2", i, 1));
      }
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
