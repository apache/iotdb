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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public class IoTDBContinuousQueryIT {

  private Statement statement;
  private Connection connection;
  private volatile Exception exception = null;

  private final Thread dataGenerator =
      new Thread() {
        @Override
        public void run() {
          try (Connection connection =
                  DriverManager.getConnection(
                      Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
              Statement statement = connection.createStatement()) {
            do {
              for (String timeSeries : timeSeriesArray) {
                statement.execute(
                    String.format(
                        "insert into %s(timestamp, temperature) values(now(), %.3f)",
                        timeSeries, 200 * Math.random()));
              }
            } while (!isInterrupted());
          } catch (Exception e) {
            exception = e;
          }
        }
      };

  private void startDataGenerator() {
    dataGenerator.start();
  }

  private void stopDataGenerator() throws InterruptedException {
    dataGenerator.interrupt();
    dataGenerator.join();
  }

  private final String[] timeSeriesArray = {
    "root.ln.wf01.wt01.ws01",
    "root.ln.wf01.wt01.ws02",
    "root.ln.wf01.wt02.ws01",
    "root.ln.wf01.wt02.ws02",
    "root.ln.wf02.wt01.ws01",
    "root.ln.wf02.wt01.ws02",
    "root.ln.wf02.wt02.ws01",
    "root.ln.wf02.wt02.ws02"
  };

  private void createTimeSeries() throws SQLException {
    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s.temperature with datatype=FLOAT,encoding=RLE", timeSeries));
    }
  }

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreateAndDropContinuousQuery() throws Exception {
    createTimeSeries();

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
            + "GROUP BY time(1s) END");
    statement.execute(
        "CREATE CONTINUOUS QUERY cq2 "
            + "BEGIN SELECT count(temperature) INTO temperature_cnt FROM root.ln.wf01.*.* "
            + " GROUP BY time(1s), level=3 END");
    statement.execute(
        "CREATE CONTINUOUS QUERY cq3 "
            + "RESAMPLE EVERY 2s FOR 2s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    statement.execute("DROP CONTINUOUS QUERY cq1");
    statement.execute("DROP CONTINUOUS QUERY cq2");

    checkShowContinuousQueriesResult(new String[] {"cq3"});

    statement.close();
    connection.close();
    EnvironmentUtils.shutdownDaemon();
    EnvironmentUtils.stopDaemon();
    setUp();

    checkShowContinuousQueriesResult(new String[] {"cq3"});

    try {
      statement.execute(
          "CREATE CONTINUOUS QUERY cq3 "
              + "RESAMPLE EVERY 2s FOR 2s "
              + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
              + "GROUP BY time(1s), level=2 END");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("already exists"));
    }

    try {
      statement.execute("DROP CONTINUOUS QUERY cq1");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("not exist"));
    }

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
            + "GROUP BY time(1s) END");
    statement.execute(
        "CREATE CONTINUOUS QUERY cq2 "
            + "BEGIN SELECT count(temperature) INTO temperature_cnt FROM root.ln.wf01.*.* "
            + " GROUP BY time(1s), level=3 END");

    checkShowContinuousQueriesResult(new String[] {"cq3", "cq1", "cq2"});

    statement.execute("DROP CONTINUOUS QUERY cq1");
    statement.execute("DROP CONTINUOUS QUERY cq2");
    statement.execute("DROP CONTINUOUS QUERY cq3");
  }

  @Test
  public void testContinuousQueryResultSeries() throws Exception {
    createTimeSeries();
    startDataGenerator();

    Thread.sleep(500);

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT count(temperature) INTO temperature_cnt FROM root.ln.*.*.* "
            + "GROUP BY time(1s), level=1,2 END");

    Thread.sleep(5500);

    checkShowTimeSeriesResult(
        new String[] {
          "root.ln.wf01.wt01.ws01.temperature",
          "root.ln.wf01.wt01.ws02.temperature",
          "root.ln.wf01.wt02.ws01.temperature",
          "root.ln.wf01.wt02.ws02.temperature",
          "root.ln.wf02.wt01.ws01.temperature",
          "root.ln.wf02.wt01.ws02.temperature",
          "root.ln.wf02.wt02.ws01.temperature",
          "root.ln.wf02.wt02.ws02.temperature",
          "root.ln.wf01.temperature_cnt",
          "root.ln.wf02.temperature_cnt"
        });

    statement.execute("DROP CONTINUOUS QUERY cq1");

    stopDataGenerator();
  }

  @Test
  public void testContinuousQueryResult1() throws Exception {
    createTimeSeries();
    startDataGenerator();

    Thread.sleep(500);

    statement.execute(
        "CREATE CQ cq1 "
            + "RESAMPLE EVERY 1s FOR 1s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=1,2 END");

    final long creationTime = System.currentTimeMillis();

    Thread.sleep(5500);

    Assert.assertTrue(statement.execute("select temperature_avg from root.ln.wf01"));
    checkCQExecutionResult(creationTime, 0, 5000, 1000, 1000, 1000, 2);

    statement.execute("DROP CQ cq1");
    stopDataGenerator();
  }

  @Test
  public void testContinuousQueryResult2() throws Exception {
    createTimeSeries();
    startDataGenerator();

    Thread.sleep(500);

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "RESAMPLE EVERY 2s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=1,2 END");

    final long creationTime = System.currentTimeMillis();

    Thread.sleep(5500);

    Assert.assertTrue(statement.execute("select temperature_avg from root.ln.wf01"));
    checkCQExecutionResult(creationTime, 0, 5000, 1000, 2000, 1000, 2);

    statement.execute("DROP CQ cq1");
    stopDataGenerator();
  }

  @Test
  public void testContinuousQueryResult3() throws Exception {
    createTimeSeries();
    startDataGenerator();

    Thread.sleep(500);

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=1,2 END");

    final long creationTime = System.currentTimeMillis();

    Thread.sleep(5500);

    Assert.assertTrue(statement.execute("select temperature_avg from root.ln.wf01"));
    checkCQExecutionResult(creationTime, 0, 5000, 1000, 1000, 1000, 2);

    statement.execute("DROP CQ cq1");
    stopDataGenerator();
  }

  @Test
  public void testContinuousQueryResult4() throws Exception {
    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=1,2 END");

    final long creationTime = System.currentTimeMillis();

    Thread.sleep(4500);

    createTimeSeries();
    startDataGenerator();

    Thread.sleep(6000);

    checkCQExecutionResult(creationTime, 5000, 5500, 1000, 1000, 1000, 2);

    statement.execute("DROP CQ cq1");
    stopDataGenerator();
  }

  private void checkCQExecutionResult(
      long creationTime,
      long delay,
      long duration,
      long forInterval,
      long everyInterval,
      long groupByInterval,
      int level)
      throws SQLException, InterruptedException {
    // IOTDB-1821
    // ignore the check when the background data generation thread's connection is broken
    if (exception != null) {
      return;
    }

    final long expectedSize = (duration / everyInterval + 1) * (forInterval / groupByInterval);
    long waitMillSeconds = 0;
    List<Pair<Long, String>> result;
    do {
      Thread.sleep(waitMillSeconds);
      waitMillSeconds += 100;

      statement.execute("select temperature_avg from root.ln.wf01");
      result = collectQueryResult();
    } while (result.size() < expectedSize);

    final long leftMost = result.get(0).left + forInterval;
    for (int i = 0; i < expectedSize; i++) {
      long left = result.get(i).left;

      if (i == 0) {
        assertTrue(Math.abs(creationTime + delay - forInterval - left) < 2 * forInterval);
      } else {
        long pointNumPerForInterval = forInterval / groupByInterval;
        Assert.assertEquals(
            leftMost
                + (i / pointNumPerForInterval) * everyInterval
                - (pointNumPerForInterval - i % pointNumPerForInterval) * groupByInterval,
            left);
      }

      statement.execute(
          String.format(
              "select avg(temperature) from root.ln.wf01.*.* GROUP BY ([%d, %d), %dms), level=%d",
              left, left + groupByInterval, groupByInterval, level));

      List<Pair<Long, String>> correctAnswer = collectQueryResult();
      Assert.assertEquals(1, correctAnswer.size());
      Assert.assertEquals(correctAnswer.get(0).right, result.get(i).right);
    }
  }

  private List<Pair<Long, String>> collectQueryResult() {
    List<Pair<Long, String>> result = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timestamp = resultSet.getString(1);
        String value = resultSet.getString(2);
        result.add(new Pair<>(Long.parseLong(timestamp), value));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
    return result;
  }

  private void checkShowContinuousQueriesResult(String[] continuousQueryArray) throws SQLException {
    Assert.assertTrue(statement.execute("show continuous queries"));

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        resultList.add(resultSet.getString("cq name"));
      }
    }
    Assert.assertEquals(continuousQueryArray.length, resultList.size());

    List<String> collect =
        resultList.stream()
            .sorted(Comparator.comparingInt(e -> e.split("\\.").length))
            .collect(Collectors.toList());

    for (String s : continuousQueryArray) {
      Assert.assertTrue(collect.contains(s));
    }
  }

  private void checkShowTimeSeriesResult(String[] timeSeriesArray) throws SQLException {
    Assert.assertTrue(statement.execute("show timeseries"));

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        resultList.add(resultSet.getString("timeseries"));
      }
    }
    Assert.assertEquals(timeSeriesArray.length, resultList.size());

    List<String> collect =
        resultList.stream()
            .sorted(Comparator.comparingInt(e -> e.split("\\.").length))
            .collect(Collectors.toList());

    for (String s : timeSeriesArray) {
      Assert.assertTrue(collect.contains(s));
    }
  }
}
