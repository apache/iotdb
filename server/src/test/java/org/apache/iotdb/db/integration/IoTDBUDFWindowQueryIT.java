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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBUDFWindowQueryIT {

  public static final String ACCESS_STRATEGY_KEY = "access";
  public static final String ACCESS_STRATEGY_ROW_BY_ROW = "row-by-row";
  public static final String ACCESS_STRATEGY_SLIDING_SIZE = "size";
  public static final String ACCESS_STRATEGY_SLIDING_TIME = "time";

  public static final String WINDOW_SIZE_KEY = "windowSize";

  public static final String TIME_INTERVAL_KEY = "timeInterval";
  public static final String SLIDING_STEP_KEY = "slidingStep";
  public static final String DISPLAY_WINDOW_BEGIN_KEY = "displayWindowBegin";
  public static final String DISPLAY_WINDOW_END_KEY = "displayWindowEnd";

  protected static final int ITERATION_TIMES = 100_000;

  @BeforeClass
  public static void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setUdfCollectorMemoryBudgetInMB(5);
    IoTDBDescriptor.getInstance().getConfig().setUdfTransformerMemoryBudgetInMB(5);
    IoTDBDescriptor.getInstance().getConfig().setUdfReaderMemoryBudgetInMB(5);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        statement.execute(
            (String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function counter as 'org.apache.iotdb.db.query.udf.example.Counter'");
      statement.execute(
          "create function accumulator as 'org.apache.iotdb.db.query.udf.example.Accumulator'");
      statement.execute(
          "create function time_window_tester as 'org.apache.iotdb.db.query.udf.example.SlidingTimeWindowConstructionTester'");
      statement.execute(
          "create function size_window_0 as 'org.apache.iotdb.db.query.udf.example.SlidingSizeWindowConstructorTester0'");
      statement.execute(
          "create function size_window_1 as 'org.apache.iotdb.db.query.udf.example.SlidingSizeWindowConstructorTester1'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setUdfCollectorMemoryBudgetInMB(100);
    IoTDBDescriptor.getInstance().getConfig().setUdfTransformerMemoryBudgetInMB(100);
    IoTDBDescriptor.getInstance().getConfig().setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void testRowByRow() {
    String sql =
        String.format(
            "select counter(s1, '%s'='%s') from root.vehicle.d1",
            ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      while (resultSet.next()) {
        assertEquals(count++, (int) (Double.parseDouble(resultSet.getString(1))));
      }
      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSlidingSizeWindow1() {
    testSlidingSizeWindow((int) (0.1 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindow2() {
    testSlidingSizeWindow((int) (0.033 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindow3() {
    testSlidingSizeWindow((int) (0.333 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindow4() {
    testSlidingSizeWindow((int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindow5() {
    testSlidingSizeWindow(ITERATION_TIMES);
  }

  @Test
  public void testSlidingSizeWindow6() {
    testSlidingSizeWindow(3 * ITERATION_TIMES);
  }

  @Test
  public void testSlidingSizeWindow7() {
    testSlidingSizeWindow(0);
  }

  @Test
  public void testSlidingSizeWindow8() {
    testSlidingSizeWindow(-ITERATION_TIMES);
  }

  private void testSlidingSizeWindow(int windowSize) {
    String sql =
        String.format(
            "select accumulator(s1, '%s'='%s', '%s'='%s') from root.vehicle.d1",
            ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_SLIDING_SIZE, WINDOW_SIZE_KEY, windowSize);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      assertEquals(2, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        int expectedWindowSize =
            (count < ITERATION_TIMES / windowSize)
                ? windowSize
                : ITERATION_TIMES - (ITERATION_TIMES / windowSize) * windowSize;

        int expectedAccumulation = 0;
        for (int i = count * windowSize; i < count * windowSize + expectedWindowSize; ++i) {
          expectedAccumulation += i;
        }

        assertEquals(expectedAccumulation, (int) (Double.parseDouble(resultSet.getString(2))));
        ++count;
      }
    } catch (SQLException throwable) {
      if (0 < windowSize || !throwable.getMessage().contains(String.valueOf(windowSize))) {
        fail(throwable.getMessage());
      }
    }
  }

  @Test
  public void testSlidingTimeWindow1() {
    testSlidingTimeWindow(
        (int) (0.33 * ITERATION_TIMES),
        (int) (0.33 * ITERATION_TIMES),
        (int) (0.33 * ITERATION_TIMES),
        (int) (0.33 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingTimeWindow2() {
    testSlidingTimeWindow(
        (int) (0.033 * ITERATION_TIMES),
        (int) (2 * 0.033 * ITERATION_TIMES),
        ITERATION_TIMES / 2,
        ITERATION_TIMES);
  }

  @Test
  public void testSlidingTimeWindow3() {
    testSlidingTimeWindow(
        (int) (2 * 0.033 * ITERATION_TIMES),
        (int) (0.033 * ITERATION_TIMES),
        ITERATION_TIMES / 2,
        ITERATION_TIMES);
  }

  @Test
  public void testSlidingTimeWindow4() {
    testSlidingTimeWindow(
        (int) (0.033 * ITERATION_TIMES),
        (int) (0.033 * ITERATION_TIMES),
        ITERATION_TIMES / 2,
        ITERATION_TIMES);
  }

  @Test
  public void testSlidingTimeWindow5() {
    testSlidingTimeWindow(ITERATION_TIMES, ITERATION_TIMES, 0, ITERATION_TIMES);
  }

  @Test
  public void testSlidingTimeWindow6() {
    testSlidingTimeWindow(
        (int) (1.01 * ITERATION_TIMES), (int) (0.01 * ITERATION_TIMES), 0, ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingTimeWindow7() {
    testSlidingTimeWindow(
        (int) (0.01 * ITERATION_TIMES), (int) (1.01 * ITERATION_TIMES), 0, ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingTimeWindow8() {
    testSlidingTimeWindow(
        (int) (1.01 * ITERATION_TIMES), (int) (1.01 * ITERATION_TIMES), 0, ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingTimeWindow9() {
    testSlidingTimeWindow(
        (int) (0.01 * ITERATION_TIMES), (int) (0.05 * ITERATION_TIMES), ITERATION_TIMES / 2, 0);
  }

  @Test
  public void testSlidingTimeWindow10() {
    testSlidingTimeWindow(
        (int) (-0.01 * ITERATION_TIMES), (int) (0.05 * ITERATION_TIMES), 0, ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingTimeWindow11() {
    testSlidingTimeWindow(
        (int) (0.01 * ITERATION_TIMES), (int) (-0.05 * ITERATION_TIMES), 0, ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingTimeWindow12() {
    testSlidingTimeWindow((int) (0.01 * ITERATION_TIMES), 0, 0, ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingTimeWindow13() {
    testSlidingTimeWindow(0, (int) (0.05 * ITERATION_TIMES), 0, ITERATION_TIMES / 2);
  }

  private void testSlidingTimeWindow(
      int timeInterval, int slidingStep, int displayWindowBegin, int displayWindowEnd) {
    String sql =
        String.format(
            "select accumulator(s1, s1, s1, '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s') from root.vehicle.d1",
            ACCESS_STRATEGY_KEY,
            ACCESS_STRATEGY_SLIDING_TIME,
            TIME_INTERVAL_KEY,
            timeInterval,
            SLIDING_STEP_KEY,
            slidingStep,
            DISPLAY_WINDOW_BEGIN_KEY,
            displayWindowBegin,
            DISPLAY_WINDOW_END_KEY,
            displayWindowEnd);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      assertEquals(2, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        int begin = displayWindowBegin + count * slidingStep;
        int expectedWindowSize =
            begin + timeInterval < displayWindowEnd ? timeInterval : displayWindowEnd - begin;

        int expectedAccumulation = 0;
        for (int i = displayWindowBegin + count * slidingStep;
            i < displayWindowBegin + count * slidingStep + expectedWindowSize;
            ++i) {
          expectedAccumulation += i;
        }

        assertEquals(expectedAccumulation, (int) (Double.parseDouble(resultSet.getString(2))));
        ++count;
      }
    } catch (SQLException throwable) {
      if (slidingStep > 0 && timeInterval > 0 && displayWindowEnd >= displayWindowBegin) {
        fail(throwable.getMessage());
      }
    }
  }

  @Test
  public void testSlidingTimeWindowWithTimeIntervalOnly1() {
    testSlidingTimeWindowWithTimeIntervalOnly(1);
  }

  @Test
  public void testSlidingTimeWindowWithTimeIntervalOnly2() {
    testSlidingTimeWindowWithTimeIntervalOnly(ITERATION_TIMES / 10);
  }

  @Test
  public void testSlidingTimeWindowWithTimeIntervalOnly3() {
    testSlidingTimeWindowWithTimeIntervalOnly(ITERATION_TIMES / 33);
  }

  @Test
  public void testSlidingTimeWindowWithTimeIntervalOnly4() {
    testSlidingTimeWindowWithTimeIntervalOnly(ITERATION_TIMES);
  }

  @Test
  public void testSlidingTimeWindowWithTimeIntervalOnly5() {
    testSlidingTimeWindowWithTimeIntervalOnly(2 * ITERATION_TIMES);
  }

  @Test
  public void testSlidingTimeWindowWithTimeIntervalOnly6() {
    testSlidingTimeWindowWithTimeIntervalOnly(-ITERATION_TIMES);
  }

  public void testSlidingTimeWindowWithTimeIntervalOnly(int timeInterval) {
    String sql =
        String.format(
            "select time_window_tester(s1, '%s'='%s') from root.vehicle.d1",
            TIME_INTERVAL_KEY, timeInterval);

    int displayWindowBegin = 0;
    int displayWindowEnd = ITERATION_TIMES;
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      assertEquals(2, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        int begin = displayWindowBegin + count * timeInterval;
        int expectedWindowSize =
            begin + timeInterval < displayWindowEnd ? timeInterval : displayWindowEnd - begin;

        int expectedAccumulation = 0;
        for (int i = displayWindowBegin + count * timeInterval;
            i < displayWindowBegin + count * timeInterval + expectedWindowSize;
            ++i) {
          expectedAccumulation += i;
        }

        assertEquals(expectedAccumulation, (int) (Double.parseDouble(resultSet.getString(2))));
        ++count;
      }
    } catch (SQLException throwable) {
      if (timeInterval > 0) {
        fail(throwable.getMessage());
      }
    }
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep1() {
    testSlidingSizeWindowWithSlidingStep(1, 1, 0);
    testSlidingSizeWindowWithSlidingStep(1, 1, 1);
    testSlidingSizeWindowWithSlidingStep(1, 1, 10);
    testSlidingSizeWindowWithSlidingStep(1, 1, (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(1, 1, (int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep2() {
    testSlidingSizeWindowWithSlidingStep(100, 100, 0);
    testSlidingSizeWindowWithSlidingStep(100, 100, 100);
    testSlidingSizeWindowWithSlidingStep(100, 100, 10000);
    testSlidingSizeWindowWithSlidingStep(100, 100, (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(100, 100, (int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep3() {
    testSlidingSizeWindowWithSlidingStep(111, 123, 0);
    testSlidingSizeWindowWithSlidingStep(111, 123, (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(111, 123, (int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep4() {
    testSlidingSizeWindowWithSlidingStep(123, 111, 0);
    testSlidingSizeWindowWithSlidingStep(123, 111, (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(123, 111, (int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep5() {
    testSlidingSizeWindowWithSlidingStep(100, 10000, 0);
    testSlidingSizeWindowWithSlidingStep(100, 10000, 100);
    testSlidingSizeWindowWithSlidingStep(100, 10000, 10000);
    testSlidingSizeWindowWithSlidingStep(100, 10000, (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(100, 10000, (int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep6() {
    testSlidingSizeWindowWithSlidingStep(10000, 1000, 0);
    testSlidingSizeWindowWithSlidingStep(10000, 1000, 1000);
    testSlidingSizeWindowWithSlidingStep(10000, 1000, 10000);
    testSlidingSizeWindowWithSlidingStep(10000, 1000, (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(10000, 1000, (int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep7() {
    testSlidingSizeWindowWithSlidingStep((int) (1.5 * ITERATION_TIMES), 4333, 0);
    testSlidingSizeWindowWithSlidingStep(
        (int) (1.5 * ITERATION_TIMES), 4333, (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(
        (int) (1.5 * ITERATION_TIMES), 4333, (int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep8() {
    testSlidingSizeWindowWithSlidingStep(10000, (int) (1.5 * ITERATION_TIMES), 0);
    testSlidingSizeWindowWithSlidingStep(
        10000, (int) (1.5 * ITERATION_TIMES), (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(
        10000, (int) (1.5 * ITERATION_TIMES), (int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingSizeWindowWithSlidingStep9() {
    testSlidingSizeWindowWithSlidingStep(
        (int) (1.5 * ITERATION_TIMES), (int) (1.5 * ITERATION_TIMES), 0);
    testSlidingSizeWindowWithSlidingStep(
        (int) (1.5 * ITERATION_TIMES),
        (int) (1.5 * ITERATION_TIMES),
        (int) (0.434 * ITERATION_TIMES));
    testSlidingSizeWindowWithSlidingStep(
        (int) (1.5 * ITERATION_TIMES),
        (int) (1.5 * ITERATION_TIMES),
        (int) (1.5 * ITERATION_TIMES));
  }

  public void testSlidingSizeWindowWithSlidingStep(
      int windowSize, int slidingStep, int consumptionPoint) {
    String sql =
        String.format(
            "select size_window_0(s1, '%s'='%s', '%s'='%s'), size_window_1(s1, '%s'='%s') from root.vehicle.d1",
            "windowSize",
            windowSize,
            "slidingStep",
            slidingStep,
            "consumptionPoint",
            consumptionPoint);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      assertEquals(3, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        if (ITERATION_TIMES < windowSize) {
          String actual = resultSet.getString(2);
          if (actual != null) {
            assertEquals(ITERATION_TIMES - count * slidingStep, Integer.parseInt(actual));
            ++count;
          }
        } else if (count * slidingStep + windowSize < ITERATION_TIMES) {
          String actual = resultSet.getString(2);
          if (actual != null) {
            assertEquals(windowSize, Integer.parseInt(resultSet.getString(2)));
            ++count;
          }
        } else {
          String actual = resultSet.getString(2);
          if (actual != null) {
            assertEquals(
                ITERATION_TIMES - count * slidingStep, Integer.parseInt(resultSet.getString(2)));
            ++count;
          }
        }
      }
      assertEquals((int) Math.ceil(ITERATION_TIMES / (double) slidingStep), count);
    } catch (SQLException throwable) {
      if (windowSize > 0) {
        fail(throwable.getMessage());
      }
    }
  }
}
