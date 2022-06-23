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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.IoTDBTestRunner;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCreateTimeseriesIT {
  private static Statement statement;
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    connection = EnvFactory.getEnv().getConnection();
    statement = connection.createStatement();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvFactory.getEnv().cleanAfterClass();
  }

  /** Test if creating a time series will cause the storage group with same name to disappear */
  @Test
  public void testCreateTimeseries() throws Exception {
    String storageGroup = "root.sg1.a.b.c";

    statement.execute(String.format("SET storage group TO %s", storageGroup));
    try {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              storageGroup));
    } catch (Exception ignored) {
    }

    // ensure that current storage group in cache is right.
    createTimeSeriesTool(storageGroup);

    //    tearDown();
    //    setUp();
    //
    //    // ensure storage group in cache is right after recovering.
    //    createTimeSeries2Tool(storageGroup);
  }

  private void createTimeSeriesTool(String storageGroup) throws SQLException {
    Set<String> resultList = new HashSet<>();
    try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
      while (resultSet.next()) {
        String str = resultSet.getString("timeseries");
        resultList.add(str);
      }
    }
    Assert.assertFalse(resultList.contains(storageGroup));
    resultList.clear();
    try (ResultSet resultSet = statement.executeQuery("show storage group")) {
      while (resultSet.next()) {
        String res = resultSet.getString("storage group");
        resultList.add(res);
      }
    }
    Assert.assertTrue(resultList.contains(storageGroup));
  }

  @Test
  public void testCreateTimeseriesWithSpecialCharacter() throws Exception {
    try {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              "root.sg.d.a\".\"b"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              "root.sg.d.a“（Φ）”b"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              "root.sg.d.a>b"));
      fail();
    } catch (SQLException ignored) {
    }

    String[] timeSeriesArray = {
      "root.sg.d.`a.b`", "root.sg.d.`a“（Φ）”b`", "root.sg.d.`a>b`",
    };
    String[] timeSeriesResArray = {
      "root.sg.d.`a.b`", "root.sg.d.`a“（Φ）”b`", "root.sg.d.`a>b`",
    };

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              timeSeries));
    }

    // ensure that current timeseries in cache is right.
    createTimeSeriesWithSpecialCharacterTool(timeSeriesResArray);

    //    tearDown();
    //    setUp();
    //
    //    // ensure timeseries in cache is right after recovered.
    //    createTimeSeriesWithSpecialCharacterTool(timeSeriesResArray);
  }

  private void createTimeSeriesWithSpecialCharacterTool(String[] timeSeriesArray)
      throws SQLException {

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.executeQuery("show timeseries root.sg.**")) {
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

    for (String s : collect) {
      System.out.println(s);
    }
    for (String timeseries : timeSeriesArray) {
      if (!collect.contains(timeseries)) {
        System.out.println(timeseries);
      }
      Assert.assertTrue(collect.contains(timeseries));
    }
  }

  @Test
  public void testCreateTimeSeriesWithWrongAttribute() {
    try {
      statement.execute(
          String.format("create timeseries %s with datatype=INT64, datatype = test", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(String.format("create timeseries %s with encoding=plain", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(
          String.format(
              "create timeseries %s with encoding=plain, compressor=snappy", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(
          String.format("create timeseries %s with datatype=float, encoding=plan", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(
          String.format("create timeseries %s with datatype=INT64, encoding=test", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=test, compression=test",
              "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(
          String.format("create timeseries %s with datatype=INT64,compression=test", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=test",
              "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testQueryDataFromTimeSeriesWithoutData() {
    try {
      statement.execute("create timeseries root.sg2.d.s1 with datatype=INT64");
    } catch (SQLException ignored) {
      fail();
    }
    int cnt = 0;
    try (ResultSet resultSet = statement.executeQuery("select s1 from root.sg2.d")) {
      while (resultSet.next()) {
        cnt++;
      }
    } catch (SQLException e) {
      fail();
    }
    Assert.assertEquals(0, cnt);
  }
}
