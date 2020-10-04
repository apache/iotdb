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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBUDFWindowQueryIT {

  public static final String ACCESS_STRATEGY_KEY = "access";
  public static final String ACCESS_STRATEGY_ONE_BY_ONE = "one-by-one";
  public static final String ACCESS_STRATEGY_TUMBLING = "tumbling";
  public static final String ACCESS_STRATEGY_SLIDING = "sliding";

  public static final String WINDOW_SIZE_KEY = "windowSize";

  public static final String TIME_INTERVAL_KEY = "timeInterval";
  public static final String SLIDING_STEP_KEY = "slidingStep";
  public static final String DISPLAY_WINDOW_BEGIN_KEY = "displayWindowBegin";
  public static final String DISPLAY_WINDOW_END_KEY = "displayWindowEnd";

  protected final static int ITERATION_TIMES = 10_000;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup("root.vehicle");
    IoTDB.metaManager.createTimeseries("root.vehicle.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager.createTimeseries("root.vehicle.d1.s2", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
  }

  private static void generateData() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        statement.execute((String
            .format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement
          .execute("create function counter as \"org.apache.iotdb.db.query.udf.example.Counter\"");
      statement
          .execute(
              "create function accumulator as \"org.apache.iotdb.db.query.udf.example.Accumulator\"");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRowByRow() {
    String sql = String.format("select counter(s1, \"%s\"=\"%s\") from root.vehicle.d1",
        ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_ONE_BY_ONE);

    try (Statement statement = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root").createStatement()) {
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
  public void testTumblingWindow1() {
    testTumblingWindow((int) (0.1 * ITERATION_TIMES));
  }

  @Test
  public void testTumblingWindow2() {
    testTumblingWindow((int) (0.033 * ITERATION_TIMES));
  }

  @Test
  public void testTumblingWindow3() {
    testTumblingWindow((int) (0.333 * ITERATION_TIMES));
  }

  @Test
  public void testTumblingWindow4() {
    testTumblingWindow((int) (1.5 * ITERATION_TIMES));
  }

  @Test
  public void testTumblingWindow5() {
    testTumblingWindow(ITERATION_TIMES);
  }

  @Test
  public void testTumblingWindow6() {
    testTumblingWindow(3 * ITERATION_TIMES);
  }

  @Test
  public void testTumblingWindow7() {
    testTumblingWindow(0);
  }

  @Test
  public void testTumblingWindow8() {
    testTumblingWindow(-ITERATION_TIMES);
  }

  private void testTumblingWindow(int windowSize) {
    String sql = String
        .format("select accumulator(s1, \"%s\"=\"%s\", \"%s\"=\"%s\") from root.vehicle.d1",
            ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_TUMBLING, WINDOW_SIZE_KEY, windowSize);

    try (Statement statement = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      assertEquals(2, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        int expectedWindowSize = (count < ITERATION_TIMES / windowSize)
            ? windowSize : ITERATION_TIMES - (ITERATION_TIMES / windowSize) * windowSize;

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
  public void testSlidingWindow1() {
    testSlidingWindow((int) (0.33 * ITERATION_TIMES), (int) (0.33 * ITERATION_TIMES),
        (int) (0.33 * ITERATION_TIMES), (int) (0.33 * ITERATION_TIMES));
  }

  @Test
  public void testSlidingWindow2() {
    testSlidingWindow((int) (0.033 * ITERATION_TIMES), (int) (2 * 0.033 * ITERATION_TIMES),
        ITERATION_TIMES / 2, ITERATION_TIMES);
  }

  @Test
  public void testSlidingWindow3() {
    testSlidingWindow((int) (2 * 0.033 * ITERATION_TIMES), (int) (0.033 * ITERATION_TIMES),
        ITERATION_TIMES / 2, ITERATION_TIMES);
  }

  @Test
  public void testSlidingWindow4() {
    testSlidingWindow((int) (0.033 * ITERATION_TIMES), (int) (0.033 * ITERATION_TIMES),
        ITERATION_TIMES / 2, ITERATION_TIMES);
  }

  @Test
  public void testSlidingWindow5() {
    testSlidingWindow(ITERATION_TIMES, ITERATION_TIMES, 0, ITERATION_TIMES);
  }

  @Test
  public void testSlidingWindow6() {
    testSlidingWindow((int) (1.01 * ITERATION_TIMES), (int) (0.01 * ITERATION_TIMES), 0,
        ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingWindow7() {
    testSlidingWindow((int) (0.01 * ITERATION_TIMES), (int) (1.01 * ITERATION_TIMES), 0,
        ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingWindow8() {
    testSlidingWindow((int) (1.01 * ITERATION_TIMES), (int) (1.01 * ITERATION_TIMES), 0,
        ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingWindow9() {
    testSlidingWindow((int) (0.01 * ITERATION_TIMES), (int) (0.05 * ITERATION_TIMES),
        ITERATION_TIMES / 2, 0);
  }

  @Test
  public void testSlidingWindow10() {
    testSlidingWindow((int) (-0.01 * ITERATION_TIMES), (int) (0.05 * ITERATION_TIMES), 0,
        ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingWindow11() {
    testSlidingWindow((int) (0.01 * ITERATION_TIMES), (int) (-0.05 * ITERATION_TIMES), 0,
        ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingWindow12() {
    testSlidingWindow((int) (0.01 * ITERATION_TIMES), 0, 0, ITERATION_TIMES / 2);
  }

  @Test
  public void testSlidingWindow13() {
    testSlidingWindow(0, (int) (0.05 * ITERATION_TIMES), 0, ITERATION_TIMES / 2);
  }

  private void testSlidingWindow(int timeInterval, int slidingStep, int displayWindowBegin,
      int displayWindowEnd) {
    String sql = String.format(
        "select accumulator(s1, \"%s\"=\"%s\", \"%s\"=\"%s\", \"%s\"=\"%s\", \"%s\"=\"%s\", \"%s\"=\"%s\") from root.vehicle.d1",
        ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_SLIDING,
        TIME_INTERVAL_KEY, timeInterval,
        SLIDING_STEP_KEY, slidingStep,
        DISPLAY_WINDOW_BEGIN_KEY, displayWindowBegin,
        DISPLAY_WINDOW_END_KEY, displayWindowEnd
    );

    try (Statement statement = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      assertEquals(2, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        int begin = displayWindowBegin + count * slidingStep;
        int expectedWindowSize = begin + timeInterval < displayWindowEnd
            ? timeInterval : displayWindowEnd - begin;

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
}
