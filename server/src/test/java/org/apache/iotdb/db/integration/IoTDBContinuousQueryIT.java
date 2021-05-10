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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBContinuousQueryIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBContinuousQueryIT.class);

  private Statement statement;
  private Connection connection;
  private DataGenerator generator;

  String[] timeSeriesArray = {
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
    generator = new DataGenerator();
  }

  @After
  public void tearDown() throws Exception {
    generator.close();
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
            + "GROUP BY time(10s) END");

    statement.execute(
        "CREATE CONTINUOUS QUERY cq2 "
            + "BEGIN SELECT count(temperature) INTO temperature_cnt FROM root.ln.wf01.*.* "
            + " GROUP BY time(10s), level=3 END");

    statement.execute(
        "CREATE CONTINUOUS QUERY cq3 "
            + "RESAMPLE EVERY 20s FOR 20s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(10s), level=2 END");

    statement.execute("DROP CONTINUOUS QUERY cq1");
    statement.execute("DROP CONTINUOUS QUERY cq2");

    checkContinuousQueries(new String[] {"cq3"});

    EnvironmentUtils.stopDaemon();

    setUp();

    checkContinuousQueries(new String[] {"cq3"});

    try {

      statement.execute(
          "CREATE CONTINUOUS QUERY cq3 "
              + "RESAMPLE EVERY 20s FOR 20s "
              + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
              + "GROUP BY time(10s), level=2 END");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("already exists"));
    }
  }

  @Test
  public void testContinuousQueryResultSeries() throws Exception {
    createTimeSeries();
    generator.start();

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "BEGIN SELECT count(temperature) INTO temperature_cnt FROM root.ln.*.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(10000);

    checkTimeSeries(
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
  }

  @Test
  public void testContinuousQueryResult() throws Exception {
    createTimeSeries();

    generator.start();

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "RESAMPLE EVERY 1s FOR 1s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(10100);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    checkResultSize(10000 / 1000);
  }

  @Test
  public void testContinuousQueryResult2() throws Exception {
    createTimeSeries();

    generator.start();

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "RESAMPLE EVERY 20s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(40100);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    checkResultSize(40000 / 1000);
  }

  @Test
  public void testContinuousQueryResult3() throws Exception {
    createTimeSeries();

    generator.start();

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(10100);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    checkResultSize(10000 / 1000);
  }

  @Test
  public void testContinuousQueryResult4() throws Exception {

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(10000);

    createTimeSeries();

    generator.start();

    Thread.sleep(10000);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    checkResultSize(10000 / 1000);
  }

  private void checkResultSize(int expectedSize) throws SQLException {
    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timestamp = resultSet.getString("Time");
        resultList.add(timestamp);
      }
    }
    Assert.assertEquals(expectedSize, resultList.size());
  }

  private void checkContinuousQueries(String[] continuousQueryArray) throws SQLException {
    boolean hasResult = statement.execute("show continuous queries");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String cq = resultSet.getString("cq name");
        resultList.add(cq);
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

  private void checkTimeSeries(String[] timeSeriesArray) throws SQLException {
    boolean hasResult = statement.execute("show timeseries");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timeseries = resultSet.getString("timeseries");
        resultList.add(timeseries);
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

  class DataGenerator {

    private final Connection producerConnection;
    private final Statement producerStatement;
    private ScheduledExecutorService executor;

    public DataGenerator() throws SQLException {
      producerConnection =
          DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      producerStatement = producerConnection.createStatement();
    }

    public void start() {

      executor = Executors.newSingleThreadScheduledExecutor();
      executor.scheduleAtFixedRate(
          () -> {
            for (String timeSeries : timeSeriesArray) {
              try {
                producerStatement.execute(
                    String.format(
                        "insert into %s(timestamp, temperature) values(now(), %.3f)",
                        timeSeries, 200 * Math.random()));
              } catch (SQLException throwables) {
                LOGGER.error(throwables.getMessage());
              }
            }
          },
          0,
          100,
          TimeUnit.MILLISECONDS);
    }

    public void close() throws SQLException {
      if (executor != null) {
        executor.shutdownNow();
      }
      if (producerStatement != null) {
        producerStatement.close();
      }
      if (producerConnection != null) {
        producerConnection.close();
      }
    }
  }
}
