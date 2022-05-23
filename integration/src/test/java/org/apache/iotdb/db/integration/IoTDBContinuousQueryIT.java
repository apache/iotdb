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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBContinuousQueryIT {

  private static final int EXPECTED_TEST_SIZE = 3;

  private Statement statement;
  private Connection connection;
  private volatile Exception exception = null;

  private PartialPath[] partialPathArray;

  private final Thread dataGenerator =
      new Thread() {
        @Override
        public void run() {
          try (Connection connection =
                  DriverManager.getConnection(
                      Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
              Statement statement = connection.createStatement()) {
            do {
              for (PartialPath partialPath : partialPathArray) {
                statement.execute(
                    String.format(
                        "insert into %s(timestamp, %s) values(now(), %.3f)",
                        partialPath.getDevicePath(),
                        partialPath.getMeasurement(),
                        200 * Math.random()));
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

  private void createTimeSeries(String[] timeSeriesArray) throws SQLException {
    initPartialPaths(timeSeriesArray);
    for (PartialPath partialPath : partialPathArray) {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=FLOAT,encoding=RLE", partialPath.getFullPath()));
    }
  }

  private void initPartialPaths(String[] timeSeriesArray) {
    partialPathArray = new PartialPath[timeSeriesArray.length];
    for (int i = 0; i < timeSeriesArray.length; ++i) {
      try {
        partialPathArray[i] = new PartialPath(timeSeriesArray[i]);
      } catch (IllegalPathException e) {
        fail(e.getMessage());
      }
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
    createTimeSeries(
        new String[] {
          "root.ln.wf01.wt01.ws01.temperature",
          "root.ln.wf01.wt01.ws02.temperature",
          "root.ln.wf01.wt02.ws01.temperature",
          "root.ln.wf01.wt02.ws02.temperature",
          "root.ln.wf02.wt01.ws01.temperature",
          "root.ln.wf02.wt01.ws02.temperature",
          "root.ln.wf02.wt02.ws01.temperature",
          "root.ln.wf02.wt02.ws02.temperature"
        });

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* "
            + "GROUP BY time(1s) END");
    statement.execute(
        "CREATE CONTINUOUS QUERY cq2 "
            + "BEGIN SELECT avg(temperature) INTO temperature_cnt FROM root.ln.wf01.*.* "
            + " GROUP BY time(1s), level=3 END");
    statement.execute(
        "CREATE CONTINUOUS QUERY cq3 "
            + "RESAMPLE EVERY 2s FOR 2s "
            + "BEGIN SELECT min_value(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
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
            + "BEGIN SELECT sum(temperature) INTO temperature_max FROM root.ln.*.*.* "
            + "GROUP BY time(1s) END");
    statement.execute(
        "CREATE CONTINUOUS QUERY cq2 "
            + "BEGIN SELECT avg(temperature) INTO temperature_cnt FROM root.ln.wf01.*.* "
            + " GROUP BY time(1s), level=3 END");

    checkShowContinuousQueriesResult(new String[] {"cq3", "cq1", "cq2"});

    statement.execute("DROP CONTINUOUS QUERY cq1");
    statement.execute("DROP CONTINUOUS QUERY cq2");
    statement.execute("DROP CONTINUOUS QUERY cq3");
  }

  @Test
  public void testContinuousQueryResultSeriesWithLevels() throws Exception {
    createTimeSeries(
        new String[] {
          "root.ln.wf01.wt01.ws01.temperature",
          "root.ln.wf01.wt01.ws02.temperature",
          "root.ln.wf01.wt02.ws01.temperature",
          "root.ln.wf01.wt02.ws02.temperature",
          "root.ln.wf02.wt01.ws01.temperature",
          "root.ln.wf02.wt01.ws02.temperature",
          "root.ln.wf02.wt02.ws01.temperature",
          "root.ln.wf02.wt02.ws02.temperature"
        });
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

  public void testContinuousQueryResultSeriesWithLevels1() throws Exception {
    createTimeSeries(
        new String[] {
          "root.ln.wf01.wt01.ws01.`(temperature)`",
          "root.ln.wf01.wt01.ws02.`(temperature)`",
          "root.ln.wf01.wt02.ws01.`(temperature)`",
          "root.ln.wf01.wt02.ws02.`(temperature)`",
          "root.ln.wf02.wt01.ws01.`(temperature)`",
          "root.ln.wf02.wt01.ws02.`(temperature)`",
          "root.ln.wf02.wt02.ws01.`(temperature)`",
          "root.ln.wf02.wt02.ws02.`(temperature)`"
        });
    startDataGenerator();

    Thread.sleep(500);

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT count(`(temperature)`) INTO temperature_cnt FROM root.ln.*.*.* "
            + "GROUP BY time(1s), level=1,2 END");

    Thread.sleep(5500);

    checkShowTimeSeriesResult(
        new String[] {
          "root.ln.wf01.wt01.ws01.`(temperature)`",
          "root.ln.wf01.wt01.ws02.`(temperature)`",
          "root.ln.wf01.wt02.ws01.`(temperature)`",
          "root.ln.wf01.wt02.ws02.`(temperature)`",
          "root.ln.wf02.wt01.ws01.`(temperature)`",
          "root.ln.wf02.wt01.ws02.`(temperature)`",
          "root.ln.wf02.wt02.ws01.`(temperature)`",
          "root.ln.wf02.wt02.ws02.`(temperature)`",
          "root.ln.wf01.temperature_cnt",
          "root.ln.wf02.temperature_cnt"
        });

    statement.execute("DROP CONTINUOUS QUERY cq1");

    stopDataGenerator();
  }

  @Test
  public void testContinuousQueryResultSeriesWithDuplicatedTargetPaths() throws Exception {
    createTimeSeries(
        new String[] {
          "root.ln.wf01.ws02.temperature",
          "root.ln.wf01.ws01.temperature",
          "root.ln.wf02.wt01.temperature",
          "root.ln.wf02.wt02.temperature",
        });
    startDataGenerator();

    Thread.sleep(500);

    try {
      statement.execute(
          "CREATE CONTINUOUS QUERY cq1 "
              + "BEGIN SELECT avg(temperature) INTO root.target.{2}.{3}.avg FROM root.ln.*.* "
              + "GROUP BY time(1s) END");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("duplicated"));
    } finally {
      stopDataGenerator();
    }
  }

  @Test
  public void testContinuousQueryResultSeriesWithoutLevels1() throws Exception {
    String[] timeSeriesArray = new String[30];
    int wsIndex = 1;
    for (int i = 1; i <= 30; ++i) {
      timeSeriesArray[i - 1] =
          "root.ln.wf0" + (i < 15 ? 1 : 2) + ".ws" + wsIndex++ + ".temperature";
    }
    createTimeSeries(timeSeriesArray);
    startDataGenerator();

    Thread.sleep(500);

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT avg(temperature) INTO root.target.${2}.${3}_avg FROM root.ln.*.* "
            + "GROUP BY time(1s) END");

    Thread.sleep(5500);

    checkShowTimeSeriesCount(2 * timeSeriesArray.length);

    statement.execute("DROP CONTINUOUS QUERY cq1");

    stopDataGenerator();
  }

  @Test
  public void testContinuousQueryResultSeriesWithoutLevels2() throws Exception {
    String[] timeSeriesArray = new String[30];
    int wsIndex = 1;
    for (int i = 1; i <= 30; ++i) {
      timeSeriesArray[i - 1] =
          "root.ln.wf0" + (i < 15 ? 1 : 2) + ".ws" + wsIndex++ + ".temperature";
    }
    createTimeSeries(timeSeriesArray);
    startDataGenerator();

    Thread.sleep(500);

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT avg(temperature) INTO root.target.${2}.${3}.avg FROM root.ln.*.* "
            + "GROUP BY time(1s) END");

    Thread.sleep(5500);

    checkShowTimeSeriesCount(2 * timeSeriesArray.length);

    statement.execute("DROP CONTINUOUS QUERY cq1");

    stopDataGenerator();
  }

  @Test
  public void testInterval1000() throws Exception {
    createTimeSeries(
        new String[] {
          "root.ln.wf01.wt01.ws01.temperature",
          "root.ln.wf01.wt01.ws02.temperature",
          "root.ln.wf01.wt02.ws01.temperature",
          "root.ln.wf01.wt02.ws02.temperature",
          "root.ln.wf02.wt01.ws01.temperature",
          "root.ln.wf02.wt01.ws02.temperature",
          "root.ln.wf02.wt02.ws01.temperature",
          "root.ln.wf02.wt02.ws02.temperature"
        });
    startDataGenerator();

    statement.execute(
        "CREATE CQ cq1 "
            + "RESAMPLE EVERY 1s FOR 1s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(1s), level=1,2 END");
    checkCQExecutionResult(1000);
    statement.execute("DROP CQ cq1");

    stopDataGenerator();
  }

  @Test
  public void testInterval2000() throws Exception {
    createTimeSeries(
        new String[] {
          "root.ln.wf01.wt01.ws01.temperature",
          "root.ln.wf01.wt01.ws02.temperature",
          "root.ln.wf01.wt02.ws01.temperature",
          "root.ln.wf01.wt02.ws02.temperature",
          "root.ln.wf02.wt01.ws01.temperature",
          "root.ln.wf02.wt01.ws02.temperature",
          "root.ln.wf02.wt02.ws01.temperature",
          "root.ln.wf02.wt02.ws02.temperature"
        });
    startDataGenerator();

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "RESAMPLE EVERY 2s "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(2s), level=1,2 END");
    checkCQExecutionResult(2000);
    statement.execute("DROP CQ cq1");

    stopDataGenerator();
  }

  @Test
  public void testInterval3000() throws Exception {
    createTimeSeries(
        new String[] {
          "root.ln.wf01.wt01.ws01.temperature",
          "root.ln.wf01.wt01.ws02.temperature",
          "root.ln.wf01.wt02.ws01.temperature",
          "root.ln.wf01.wt02.ws02.temperature",
          "root.ln.wf02.wt01.ws01.temperature",
          "root.ln.wf02.wt01.ws02.temperature",
          "root.ln.wf02.wt02.ws01.temperature",
          "root.ln.wf02.wt02.ws02.temperature"
        });
    startDataGenerator();

    statement.execute(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.wf01.*.* "
            + "GROUP BY time(3s), level=1,2 END");
    checkCQExecutionResult(3000);
    statement.execute("DROP CQ cq1");

    stopDataGenerator();
  }

  private void checkCQExecutionResult(long groupByInterval)
      throws SQLException, InterruptedException {
    // IOTDB-1821
    // ignore the check when the background data generation thread's connection is broken
    if (exception != null) {
      return;
    }

    long waitMillSeconds = 0;
    List<Pair<Long, Double>> actualResult;
    do {
      Thread.sleep(waitMillSeconds);
      waitMillSeconds += 100;

      statement.execute("select temperature_avg from root.ln.wf01");
      actualResult = collectQueryResult();
    } while (actualResult.size() < EXPECTED_TEST_SIZE);

    long actualWindowBegin = actualResult.get(0).left;
    long actualWindowEnd = actualResult.get(actualResult.size() - 1).left;
    statement.execute(
        String.format(
            "select avg(temperature) from root.ln.wf01.*.* GROUP BY ([%d, %d), %dms), level=1,2 without null all",
            actualWindowBegin, actualWindowEnd + groupByInterval, groupByInterval));
    List<Pair<Long, Double>> expectedResult = collectQueryResult();

    assertEquals(expectedResult.size(), actualResult.size());
    final int size = expectedResult.size();
    for (int i = 0; i < size; ++i) {
      Pair<Long, Double> expected = expectedResult.get(i);
      Pair<Long, Double> actual = actualResult.get(i);
      assertEquals(expected.left, actual.left);
      assertEquals(expected.right, actual.right, 10e-6);
    }
  }

  private List<Pair<Long, Double>> collectQueryResult() {
    List<Pair<Long, Double>> result = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timestamp = resultSet.getString(1);
        String value = resultSet.getString(2);
        result.add(new Pair<>(Long.parseLong(timestamp), Double.parseDouble(value)));
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

  private void checkShowTimeSeriesCount(int expected) throws SQLException {
    Assert.assertTrue(statement.execute("show timeseries"));
    int autual = 0;
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        ++autual;
      }
    }
    Assert.assertEquals(expected, autual);
  }
}
