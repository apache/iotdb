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

import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBCreateContinuousQueryIT {
  private Statement statement;
  private Connection connection;
  private final ExecutorService pool = Executors.newCachedThreadPool();

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

  @Before
  public void setUp() throws Exception {

    EnvironmentUtils.envSetUp();

    Class.forName(Config.JDBC_DRIVER_NAME);
    connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    pool.shutdownNow();
    statement.close();
    connection.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreateContinuousQuery1() throws Exception {
    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s.temperature with datatype=FLOAT,encoding=RLE", timeSeries));
    }

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
            + "GROUP BY time(10s) END");

    statement.execute(
        "CREATE CONTINUOUS QUERY cq2 "
            + "BEGIN SELECT count(temperature) INTO temperature_cnt FROM root.ln.wf01.*.* "
            + "WHERE temperature > 80.0 GROUP BY time(10s), level=3 END");

    statement.execute(
        "CREATE CONTINUOUS QUERY cq3 "
            + "RESAMPLE EVERY 20s FOR 20s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(10s), level=2 END");

    statement.execute("DROP CONTINUOUS QUERY cq1");
    statement.execute("DROP CONTINUOUS QUERY cq2");

    createCreateContinuousQuery1Tool(new String[] {"cq3"});

    EnvironmentUtils.stopDaemon();
    setUp();

    createCreateContinuousQuery1Tool(new String[] {"cq3"});
  }

  private void createCreateContinuousQuery1Tool(String[] continuousQueryArray) throws SQLException {
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

  @Test
  public void testCreateContinuousQuery2() throws Exception {

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s.temperature with datatype=FLOAT,encoding=RLE", timeSeries));
    }

    Connection producerConnection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    pool.execute(new Producer(producerConnection));

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "RESAMPLE EVERY 1s FOR 1s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(10000);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timestamp = resultSet.getString("Time");
        System.out.println(timestamp);
        resultList.add(timestamp);
      }
    }
    Assert.assertEquals(10000 / 1000, resultList.size());
  }

  @Test
  public void testCreateContinuousQuery3() throws Exception {

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s.temperature with datatype=FLOAT,encoding=RLE", timeSeries));
    }

    Connection producerConnection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    pool.execute(new Producer(producerConnection));

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "RESAMPLE EVERY 20s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(40000);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timestamp = resultSet.getString("Time");
        System.out.println(timestamp);
        resultList.add(timestamp);
      }
    }
    Assert.assertEquals(40000 / 1000, resultList.size());
  }

  @Test
  public void testCreateContinuousQuery4() throws Exception {

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s.temperature with datatype=FLOAT,encoding=RLE", timeSeries));
    }

    Connection producerConnection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    pool.execute(new Producer(producerConnection));

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(10000);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timestamp = resultSet.getString("Time");
        System.out.println(timestamp);
        resultList.add(timestamp);
      }
    }
    Assert.assertEquals(10000 / 1000, resultList.size());
  }

  @Test
  public void testCreateContinuousQuery5() throws Exception {

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s.temperature with datatype=FLOAT,encoding=RLE", timeSeries));
    }

    Connection producerConnection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    pool.execute(new Producer(producerConnection));

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* WHERE temperature < 0 "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(10000);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timestamp = resultSet.getString("Time");
        System.out.println(timestamp);
        resultList.add(timestamp);
      }
    }
    Assert.assertEquals(0, resultList.size());
  }

  @Test
  public void testCreateContinuousQuery6() throws Exception {

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s.temperature with datatype=FLOAT,encoding=RLE", timeSeries));
    }

    Connection producerConnection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    pool.execute(new Producer(producerConnection));

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
  public void testCreateContinuousQuery7() throws Exception {

    statement.execute(
        "CREATE CONTINUOUS QUERY cq "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=2 END");

    Thread.sleep(10000);

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s.temperature with datatype=FLOAT,encoding=RLE", timeSeries));
    }

    Connection producerConnection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    pool.execute(new Producer(producerConnection));

    Thread.sleep(10000);

    boolean hasResult = statement.execute("select temperature_avg from root.ln.wf01");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timestamp = resultSet.getString("Time");
        System.out.println(timestamp);
        resultList.add(timestamp);
      }
    }
    Assert.assertEquals(10000 / 1000, resultList.size());
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

  class Producer implements Runnable {
    private final Statement producerStatement;
    private final Connection producerConnection;

    public Producer(Connection producerConnection) throws SQLException {
      this.producerConnection = producerConnection;
      this.producerStatement = producerConnection.createStatement();
    }

    protected void finalize() throws SQLException {
      producerStatement.close();
      producerConnection.close();
    }

    @Override
    public void run() {

      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(100);
          for (String timeSeries : timeSeriesArray) {
            this.producerStatement.execute(
                String.format(
                    "insert into %s(timestamp, temperature) values(now(), %.3f)",
                    timeSeries, 200 * Math.random()));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (SQLException throwables) {
          throwables.printStackTrace();
        }
      }
    }
  }
}
