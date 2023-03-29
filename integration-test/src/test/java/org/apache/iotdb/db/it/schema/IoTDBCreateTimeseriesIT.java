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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCreateTimeseriesIT extends AbstractSchemaIT {

  public IoTDBCreateTimeseriesIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  /** Test if creating a time series will cause the database with same name to disappear */
  @Test
  public void testCreateTimeseries() throws Exception {
    String storageGroup = "root.sg1.a.b.c";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("CREATE DATABASE %s", storageGroup));
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              storageGroup));

    } catch (Exception ignored) {
    }

    // ensure that current database in cache is right.
    createTimeSeriesTool(storageGroup);
  }

  private void createTimeSeriesTool(String storageGroup) throws SQLException {
    Set<String> resultList = new HashSet<>();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show timeseries")) {
      while (resultSet.next()) {
        String str = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
        resultList.add(str);
      }
    }
    Assert.assertFalse(resultList.contains(storageGroup));
    resultList.clear();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      while (resultSet.next()) {
        String res = resultSet.getString(ColumnHeaderConstant.DATABASE);
        resultList.add(res);
      }
    }
    Assert.assertTrue(resultList.contains(storageGroup));
  }

  @Test
  public void testCreateTimeseriesWithSpecialCharacter() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
                "root.sg.d.a\".\"b"));
        fail();
      } catch (SQLException ignored) {
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
                "root.sg.d.a“（Φ）”b"));
        fail();
      } catch (SQLException ignored) {
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
                "root.sg.d.a>b"));
        fail();
      } catch (SQLException ignored) {
      }
    }

    String[] timeSeriesArray = {
      "root.sg.d.`a.b`", "root.sg.d.`a“（Φ）”b`", "root.sg.d.`a>b`",
    };
    String[] timeSeriesResArray = {
      "root.sg.d.`a.b`", "root.sg.d.`a“（Φ）”b`", "root.sg.d.`a>b`",
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String timeSeries : timeSeriesArray) {
        statement.execute(
            String.format(
                "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
                timeSeries));
      }
    }

    // ensure that current timeseries in cache is right.
    createTimeSeriesWithSpecialCharacterTool(timeSeriesResArray);
  }

  private void createTimeSeriesWithSpecialCharacterTool(String[] timeSeriesArray)
      throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("count timeseries root.sg.**")) {
      while (resultSet.next()) {
        long count = resultSet.getLong(1);
        Assert.assertEquals(timeSeriesArray.length, count);
      }
    }
  }

  @Test
  public void testCreateTimeSeriesWithWrongAttribute() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("create timeseries %s with datatype=INT64, datatype = test", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("create timeseries %s with encoding=plain", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create timeseries %s with encoding=plain, compressor=snappy", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("create timeseries %s with datatype=float, encoding=plan", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("create timeseries %s with datatype=INT64, encoding=test", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=test, compression=test",
              "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("create timeseries %s with datatype=INT64,compression=test", "root.sg.a"));
      fail();
    } catch (SQLException ignored) {
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.sg2.d.s1 with datatype=INT64");
    } catch (SQLException ignored) {
      fail();
    }
    int cnt = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select s1 from root.sg2.d")) {
      while (resultSet.next()) {
        cnt++;
      }
    } catch (SQLException e) {
      fail();
    }
    Assert.assertEquals(0, cnt);
  }
}
