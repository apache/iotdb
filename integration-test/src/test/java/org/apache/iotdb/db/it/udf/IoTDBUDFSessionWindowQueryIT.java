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

import org.apache.iotdb.it.env.ConfigFactory;
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

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBUDFSessionWindowQueryIT {

  protected static final int ITERATION_TIMES = 10000;

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  @BeforeClass
  public static void setUp() throws Exception {
    enableSeqSpaceCompaction = ConfigFactory.getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction = ConfigFactory.getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction = ConfigFactory.getConfig().isEnableCrossSpaceCompaction();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(false);
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(5)
        .setUdfTransformerMemoryBudgetInMB(5)
        .setUdfReaderMemoryBudgetInMB(5);
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(100)
        .setUdfTransformerMemoryBudgetInMB(100)
        .setUdfReaderMemoryBudgetInMB(100);
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s3 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i == 5 || i == 6 || i == 7 || i == 51 || i == 52 || i == 53 || i == 54 || i == 9996
            || i == 9997 || i == 9998) {
          continue;
        }
        statement.execute(
            (String.format("insert into root.vehicle.d1(timestamp,s3) values(%d,%d)", i, i)));
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
              displayBegin.longValue(),
              UDFTestConstant.DISPLAY_WINDOW_END_KEY,
              displayEnd.longValue(),
              UDFTestConstant.SESSION_GAP_KEY,
              sessionGap);
    }

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      for (int i = 0; i < windowStart.length; i++) {
        resultSet.next();
        Assert.assertEquals(resultSet.getLong(1), windowStart[i]);
        Assert.assertEquals(resultSet.getLong(2), windowEnd[i]);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSessionTimeWindowSS1() {
    String sessionGap = "2";
    long[] windowStart = new long[] {0, 8, 55, 9999};
    long[] windowEnd = new long[] {4, 50, 9995, 9999};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, null, null);
  }

  @Test
  public void testSessionTimeWindowSS2() {
    String sessionGap = "5";
    long[] windowStart = new long[] {0, 55};
    long[] windowEnd = new long[] {50, 9999};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, null, null);
  }

  @Test
  public void testSessionTimeWindowSS3() {
    String sessionGap = "6";
    long[] windowStart = new long[] {0};
    long[] windowEnd = new long[] {9999};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, null, null);
  }

  @Test
  public void testSessionTimeWindowSS4() {
    String sessionGap = "2";
    Long displayBegin = 1L;
    Long displayEnd = 9993L;
    long[] windowStart = new long[] {1, 8, 55};
    long[] windowEnd = new long[] {4, 50, 9992};
    testSessionTimeWindowSS(sessionGap, windowStart, windowEnd, displayBegin, displayEnd);
  }

  @Test
  public void testSessionTimeWindowSS5() {
    String sessionGap = "5";
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
    ArrayList<Long> windowStart = new ArrayList<>();
    ArrayList<Long> windowEnd = new ArrayList<>();
    for (long i = displayBegin; i <= 9999; i++) {
      if (i == 5 || i == 6 || i == 7 || i == 51 || i == 52 || i == 53 || i == 54 || i == 9996
          || i == 9997 || i == 9998) {
        continue;
      }
      windowStart.add(i);
      windowEnd.add(i);
    }
    testSessionTimeWindowSS(
        sessionGap,
        windowStart.stream().mapToLong(t -> t.longValue()).toArray(),
        windowEnd.stream().mapToLong(t -> t.longValue()).toArray(),
        displayBegin,
        displayEnd);
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
              displayBegin.longValue(),
              UDFTestConstant.DISPLAY_WINDOW_END_KEY,
              displayEnd.longValue(),
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
}
