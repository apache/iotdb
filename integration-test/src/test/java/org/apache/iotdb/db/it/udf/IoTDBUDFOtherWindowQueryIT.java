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
import org.apache.iotdb.itbase.constant.UDFTestConstant;

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
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Test for SessionWindow, StateWindow and UserDefinedWindow. */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBUDFOtherWindowQueryIT {

  protected static final int ITERATION_TIMES = 1000;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setUdfMemoryBudgetInMB(5);
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
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s3 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s4 with datatype=INT32,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.vehicle.d1.s5 with datatype=BOOLEAN,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s6 with datatype=TEXT,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    // SessionWindow
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i == 5 || i == 6 || i == 7 || i == 51 || i == 52 || i == 53 || i == 54 || i == 996
            || i == 997 || i == 998) {
          continue;
        }
        statement.execute(
            (String.format("insert into root.vehicle.d1(timestamp,s3) values(%d,%d)", i, i)));
      }

      // StateWindow INT32
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i == 1 || i == 2 || i == 3 || i == 15 || i == 17 || i == 53 || i == 54 || i == 996
            || i == 997 || i == 998) {
          statement.execute(
              (String.format(
                  "insert into root.vehicle.d1(timestamp,s4) values(%d,%d)", i, i + 100)));
        } else if (i == 500 || i == 501) {
          statement.execute(
              (String.format("insert into root.vehicle.d1(timestamp,s4) values(%d,%d)", i, 12)));
        } else if (i % 2 == 0) {
          statement.execute(
              (String.format("insert into root.vehicle.d1(timestamp,s4) values(%d,%d)", i, 0)));
        } else {
          statement.execute(
              (String.format("insert into root.vehicle.d1(timestamp,s4) values(%d,%d)", i, 3)));
        }
      }

      // StateWindow BOOLEAN
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i == 1 || i == 2 || i == 3 || i == 15 || i == 17 || i == 53 || i == 54 || i == 996
            || i == 997 || i == 998) {
          statement.execute(
              (String.format("insert into root.vehicle.d1(timestamp,s5) values(%d, true)", i)));
        } else {
          statement.execute(
              (String.format("insert into root.vehicle.d1(timestamp,s5) values(%d, false)", i)));
        }
      }

      // StateWindow TEXT
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i < 500) {
          statement.execute(
              (String.format("insert into root.vehicle.d1(timestamp,s6) values(%d, '<500')", i)));
        } else if (i < 993) {
          statement.execute(
              (String.format("insert into root.vehicle.d1(timestamp,s6) values(%d, '<993')", i)));
        } else {
          statement.execute(
              (String.format("insert into root.vehicle.d1(timestamp,s6) values(%d, '>=994')", i)));
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function window_start_end as 'org.apache.iotdb.db.query.udf.example.WindowStartEnd'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private void testSessionTimeWindowSS(
      String sessionGap, long[] windowStart, long[] windowEnd, Long displayBegin, Long displayEnd) {
    String sql;
    if (displayBegin == null) {
      sql =
          String.format(
              "select window_start_end(s3, '%s'='%s', '%s'='%s') from root.vehicle.d1",
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_SESSION,
              UDFTestConstant.SESSION_GAP_KEY,
              sessionGap);
    } else {
      sql =
          String.format(
              "select window_start_end(s3, '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s') from root.vehicle.d1",
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_SESSION,
              UDFTestConstant.DISPLAY_WINDOW_BEGIN_KEY,
              displayBegin,
              UDFTestConstant.DISPLAY_WINDOW_END_KEY,
              displayEnd,
              UDFTestConstant.SESSION_GAP_KEY,
              sessionGap);
    }

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      int cnt = 0;
      while (resultSet.next()) {
        Assert.assertEquals(resultSet.getLong(1), windowStart[cnt]);
        Assert.assertEquals(resultSet.getLong(2), windowEnd[cnt]);
        cnt++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSessionTimeWindowSS1() {
    String sessionGap = "2";
    long[] windowStart = new long[] {0, 8, 55, 999};
    long[] windowEnd = new long[] {4, 50, 995, 999};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, null, null);
  }

  @Test
  public void testSessionTimeWindowSS2() {
    String sessionGap = "4";
    long[] windowStart = new long[] {0, 55};
    long[] windowEnd = new long[] {50, 999};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, null, null);
  }

  @Test
  public void testSessionTimeWindowSS3() {
    String sessionGap = "6";
    long[] windowStart = new long[] {0};
    long[] windowEnd = new long[] {999};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, null, null);
  }

  @Test
  public void testSessionTimeWindowSS4() {
    String sessionGap = "2";
    Long displayBegin = 1L;
    Long displayEnd = 993L;
    long[] windowStart = new long[] {1, 8, 55};
    long[] windowEnd = new long[] {4, 50, 992};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, displayBegin, displayEnd);
  }

  @Test
  public void testSessionTimeWindowSS5() {
    String sessionGap = "4";
    Long displayBegin = 43L;
    Long displayEnd = 100L;
    long[] windowStart = new long[] {43, 55};
    long[] windowEnd = new long[] {50, 99};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, displayBegin, displayEnd);
  }

  @Test
  public void testSessionTimeWindowSS6() {
    String sessionGap = "1";
    Long displayBegin = 2L;
    Long displayEnd = 20000L;
    long[] windowStart = new long[] {2, 8, 55, 999};
    long[] windowEnd = new long[] {4, 50, 995, 999};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, displayBegin, displayEnd);
  }

  private void testSessionTimeWindowSSOutOfRange(
      String sessionGap, Long displayBegin, Long displayEnd) {
    String sql;
    if (displayBegin == null) {
      sql =
          String.format(
              "select window_start_end(s3, '%s'='%s', '%s'='%s') from root.vehicle.d1",
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_SESSION,
              UDFTestConstant.SESSION_GAP_KEY,
              sessionGap);
    } else {
      sql =
          String.format(
              "select window_start_end(s3, '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s') from root.vehicle.d1",
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_SESSION,
              UDFTestConstant.DISPLAY_WINDOW_BEGIN_KEY,
              displayBegin,
              UDFTestConstant.DISPLAY_WINDOW_END_KEY,
              displayEnd,
              UDFTestConstant.SESSION_GAP_KEY,
              sessionGap);
    }

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(count, 0);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSessionTimeWindowSS7() {
    String sessionGap = "2";
    Long displayBegin = 1000000L;
    Long displayEnd = 2000000L;
    testSessionTimeWindowSSOutOfRange(sessionGap, displayBegin, displayEnd);
  }

  @Test
  public void testSessionTimeWindowSS8() {
    String sessionGap = "2";
    Long displayBegin = -2000000L;
    Long displayEnd = -100L;
    testSessionTimeWindowSSOutOfRange(sessionGap, displayBegin, displayEnd);
  }

  @Test
  public void testSessionTimeWindowSS9() {
    String sessionGap = "0";
    Long displayBegin = 2L;
    Long displayEnd = 20000L;
    ArrayList<Long> windowStart = new ArrayList<>();
    ArrayList<Long> windowEnd = new ArrayList<>();
    for (long i = displayBegin; i < ITERATION_TIMES; i++) {
      if (i == 5 || i == 6 || i == 7 || i == 51 || i == 52 || i == 53 || i == 54 || i == 996
          || i == 997 || i == 998) {
        continue;
      }
      windowStart.add(i);
      windowEnd.add(i);
    }
    testSessionTimeWindowSS(
        sessionGap,
        windowStart.stream().mapToLong(t -> t).toArray(),
        windowEnd.stream().mapToLong(t -> t).toArray(),
        displayBegin,
        displayEnd);
  }

  private void testStateWindowSS(
      String measurement,
      String delta,
      long[] windowStart,
      long[] windowEnd,
      Long displayBegin,
      Long displayEnd) {
    String sql;
    if (displayBegin == null) {
      if (delta == null) {
        sql =
            String.format(
                "select window_start_end(%s, '%s'='%s') from root.vehicle.d1",
                measurement,
                UDFTestConstant.ACCESS_STRATEGY_KEY,
                UDFTestConstant.ACCESS_STRATEGY_STATE);
      } else {
        sql =
            String.format(
                "select window_start_end(%s, '%s'='%s', '%s'='%s') from root.vehicle.d1",
                measurement,
                UDFTestConstant.ACCESS_STRATEGY_KEY,
                UDFTestConstant.ACCESS_STRATEGY_STATE,
                UDFTestConstant.STATE_DELTA_KEY,
                delta);
      }
    } else {
      sql =
          String.format(
              "select window_start_end(%s, '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s') from root.vehicle.d1",
              measurement,
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_STATE,
              UDFTestConstant.DISPLAY_WINDOW_BEGIN_KEY,
              displayBegin,
              UDFTestConstant.DISPLAY_WINDOW_END_KEY,
              displayEnd,
              UDFTestConstant.STATE_DELTA_KEY,
              delta);
    }

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      int cnt = 0;
      while (resultSet.next()) {
        Assert.assertEquals(resultSet.getLong(1), windowStart[cnt]);
        Assert.assertEquals(resultSet.getLong(2), windowEnd[cnt]);
        cnt++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testStateWindowSS1() {
    String delta = "10";
    Long displayBegin = 10L;
    Long displayEnd = 976L;
    long[] windowStart = new long[] {10, 15, 16, 17, 18, 53, 55};
    long[] windowEnd = new long[] {14, 15, 16, 17, 52, 54, 975};
    testStateWindowSS("s4", delta, windowStart, windowEnd, displayBegin, displayEnd);
  }

  @Test
  public void testStateWindowSS2() {
    String delta = "9";
    Long displayBegin = 0L;
    Long displayEnd = 100000L;
    long[] windowStart = new long[] {0, 1, 4, 15, 16, 17, 18, 53, 55, 996, 999};
    long[] windowEnd = new long[] {0, 3, 14, 15, 16, 17, 52, 54, 995, 998, 999};
    testStateWindowSS("s4", delta, windowStart, windowEnd, displayBegin, displayEnd);
  }

  @Test
  public void testStateWindowSS3() {
    String delta = "102";
    Long displayBegin = -100L;
    Long displayEnd = 100000L;
    long[] windowStart = new long[] {0, 3, 4, 15, 16, 17, 18, 53, 55, 996, 999};
    long[] windowEnd = new long[] {2, 3, 14, 15, 16, 17, 52, 54, 995, 998, 999};
    testStateWindowSS("s4", delta, windowStart, windowEnd, displayBegin, displayEnd);
  }

  @Test
  public void testStateWindowSS4() {
    String delta = "102";
    long[] windowStart = new long[] {0, 3, 4, 15, 16, 17, 18, 53, 55, 996, 999};
    long[] windowEnd = new long[] {2, 3, 14, 15, 16, 17, 52, 54, 995, 998, 999};
    testStateWindowSS("s4", delta, windowStart, windowEnd, null, null);
  }

  @Test
  public void testStateWindowSS5() {
    String delta = "2";
    Long displayBegin = -100L;
    Long displayEnd = 1L;
    long[] windowStart = new long[] {0};
    long[] windowEnd = new long[] {0};
    testStateWindowSS("s4", delta, windowStart, windowEnd, displayBegin, displayEnd);
  }

  private void testStateWindowSSOutOfRange(
      String measurement, String delta, Long displayBegin, Long displayEnd) {
    String sql;
    if (displayBegin == null) {
      if (delta == null) {
        sql =
            String.format(
                "select window_start_end(%s, '%s'='%s') from root.vehicle.d1",
                measurement,
                UDFTestConstant.ACCESS_STRATEGY_KEY,
                UDFTestConstant.ACCESS_STRATEGY_STATE);
      } else {
        sql =
            String.format(
                "select window_start_end(%s, '%s'='%s', '%s'='%s') from root.vehicle.d1",
                measurement,
                UDFTestConstant.ACCESS_STRATEGY_KEY,
                UDFTestConstant.ACCESS_STRATEGY_STATE,
                UDFTestConstant.STATE_DELTA_KEY,
                delta);
      }
    } else {
      sql =
          String.format(
              "select window_start_end(%s, '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s') from root.vehicle.d1",
              measurement,
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_STATE,
              UDFTestConstant.DISPLAY_WINDOW_BEGIN_KEY,
              displayBegin,
              UDFTestConstant.DISPLAY_WINDOW_END_KEY,
              displayEnd,
              UDFTestConstant.STATE_DELTA_KEY,
              delta);
    }

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(count, 0);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testStateWindowSS6() {
    String delta = "2";
    Long displayBegin = -100L;
    Long displayEnd = -1L;
    testStateWindowSSOutOfRange("s4", delta, displayBegin, displayEnd);
  }

  @Test
  public void testStateWindowSS7() {
    String delta = "2";
    Long displayBegin = 10000L;
    Long displayEnd = 100005L;
    testStateWindowSSOutOfRange("s4", delta, displayBegin, displayEnd);
  }

  @Test
  public void testStateWindowSS8() {
    long[] windowStart = new long[] {0, 1, 4, 15, 16, 17, 18, 53, 55, 996, 999};
    long[] windowEnd = new long[] {0, 3, 14, 15, 16, 17, 52, 54, 995, 998, 999};
    testStateWindowSS("s5", null, windowStart, windowEnd, null, null);
  }

  @Test
  public void testStateWindowSS9() {
    long[] windowStart = new long[] {0, 500, 993};
    long[] windowEnd = new long[] {499, 992, 999};
    testStateWindowSS("s6", null, windowStart, windowEnd, null, null);
  }

  @Test
  public void testStateWindowSS10() {
    String delta = "0";
    Long displayBegin = 2L;
    Long displayEnd = 20000L;
    ArrayList<Long> windowStart = new ArrayList<>();
    ArrayList<Long> windowEnd = new ArrayList<>();
    for (long i = displayBegin; i < ITERATION_TIMES; i++) {
      if (i == 500) {
        windowStart.add(i);
        windowEnd.add(i + 1);
        i++;
        continue;
      }
      windowStart.add(i);
      windowEnd.add(i);
    }
    testStateWindowSS(
        "s4",
        delta,
        windowStart.stream().mapToLong(t -> t).toArray(),
        windowEnd.stream().mapToLong(t -> t).toArray(),
        displayBegin,
        displayEnd);
  }
}
