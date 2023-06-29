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

package org.apache.iotdb.db.it.udf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBUDTFTopKDTWIT {

  private static final long TIMESTAMP_STRIDE = 1000L;
  private static final long DATA_START_TIME = 954475200000L;

  private static final int PATTERN_COUNT = 10;
  private static final int PATTERN_LENGTH = 100;
  private static final double PATTERN_RANGE = 100.0;

  private static final int SERIES_LENGTH = 1000;
  private static final double SERIES_RANGE = 1000.0;

  private static long[] correctStartTimes;
  private static long[] correctEndTimes;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch("create database root.database");
      statement.addBatch(
          "create timeseries root.database.device.s with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.database.device.p with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Random random = new Random();

      double[] pattern = new double[PATTERN_LENGTH];
      for (int i = 0; i < PATTERN_LENGTH; i++) {
        // Construct pattern series with 100 points,
        // each point is a random double in [-100, 100]
        pattern[i] = PATTERN_RANGE * (2.0 * random.nextDouble() - 1.0);
        statement.addBatch(
            String.format(
                "insert into root.database.device(timestamp, p) values(%d, %f)",
                i * TIMESTAMP_STRIDE, pattern[i]));
      }

      long currentTime = DATA_START_TIME;
      correctStartTimes = new long[PATTERN_COUNT];
      correctEndTimes = new long[PATTERN_COUNT];
      for (int i = 0; i < PATTERN_COUNT; i++) {
        // Construct series with 1000 points,
        // each point is a random double in [-1000, 1000]
        for (int j = 0; j < SERIES_LENGTH; j++) {
          statement.addBatch(
              String.format(
                  "insert into root.database.device(timestamp, s) values(%d, %f)",
                  currentTime, SERIES_RANGE * (2.0 * random.nextDouble() - 1.0)));
          currentTime += TIMESTAMP_STRIDE;
        }

        correctStartTimes[i] = currentTime;
        for (int j = 0; j < PATTERN_LENGTH; j++) {
          // Construct a similar pattern
          int repeatTime = (int) Math.abs(random.nextGaussian()) + 1;
          if (j == 0 || j == PATTERN_LENGTH - 1) {
            // Make sure the first and last points are the same
            repeatTime = 1;
          }
          for (int k = 0; k < repeatTime; k++) {
            statement.addBatch(
                String.format(
                    "insert into root.database.device(timestamp, s) values(%d, %f)",
                    currentTime, pattern[j]));
            currentTime += TIMESTAMP_STRIDE;
          }
        }
        correctEndTimes[i] = currentTime - TIMESTAMP_STRIDE;
      }

      statement.executeBatch();

    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTopKDTWSlidingWindow() {
    String sqlStr =
        String.format(
            "select top_k_dtw_sliding_window(s, p, 'k'='%d') from root.database.device",
            PATTERN_COUNT);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery(sqlStr);
      checkResultSet(resultSet);

    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private void checkResultSet(ResultSet resultSet) throws SQLException {
    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();
    List<Double> distances = new ArrayList<>();
    while (resultSet.next()) {
      String[] result = resultSet.getString(2).split(",");
      startTimes.add(Long.parseLong(result[0].substring(result[0].indexOf('=') + 1)));
      endTimes.add(Long.parseLong(result[1].substring(result[1].indexOf('=') + 1)));
      distances.add(
          Double.parseDouble(
              result[2].substring(result[2].indexOf('=') + 1, result[2].length() - 1)));
    }
    Assert.assertEquals(PATTERN_COUNT, startTimes.size());

    for (int i = 0; i < PATTERN_COUNT; i++) {
      Assert.assertEquals(correctStartTimes[i], startTimes.get(i).longValue());
      Assert.assertEquals(correctEndTimes[i], endTimes.get(i).longValue());
      Assert.assertEquals(0.0, distances.get(i), 1e-6);
    }
  }
}
