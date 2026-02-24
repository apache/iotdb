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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertThrows;
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

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  /** Test if creating a time series will cause the database with same name to disappear */
  @Test
  public void testCreateTimeseries() throws Exception {
    String database = "root.sg1.a.b.c";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("CREATE DATABASE %s", database));
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              database));

    } catch (Exception ignored) {
    }

    // ensure that current database in cache is right.
    createTimeSeriesTool(database);
  }

  private void createTimeSeriesTool(String database) throws SQLException {
    Set<String> resultList = new HashSet<>();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show timeseries")) {
      while (resultSet.next()) {
        String str = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
        resultList.add(str);
      }
    }
    Assert.assertFalse(resultList.contains(database));
    resultList.clear();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      while (resultSet.next()) {
        String res = resultSet.getString(ColumnHeaderConstant.DATABASE);
        resultList.add(res);
      }
    }
    Assert.assertTrue(resultList.contains(database));
  }

  @Test
  public void testCreateTimeseriesWithSpecialCharacter() throws Exception {
    // Currently this test may fail in PBTree
    // Will solve this in the future
    if (schemaTestMode == SchemaTestMode.PBTree) {
      return;
    }
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
      "root.sg.d.`a.b`", "root.sg.d.`a“（Φ）”b`", "root.sg.d.`a>b`", "root.sg.d.`0e38`"
    };
    String[] timeSeriesResArray = {
      "root.sg.d.`a.b`", "root.sg.d.`a“（Φ）”b`", "root.sg.d.`a>b`", "root.sg.d.`0e38`",
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

  @Test
  public void testIllegalInput() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.sg2.d.s1 with datatype=INT64");
      assertThrows(
          "Unsupported datatype: UNKNOWN",
          SQLException.class,
          () -> statement.execute("create timeseries root.sg2.d.s1 with datatype=UNKNOWN"));
      assertThrows(
          "Unsupported datatype: VECTOR",
          SQLException.class,
          () -> statement.execute("create timeseries root.sg2.d.s1 with datatype=VECTOR"));
      assertThrows(
          "Unsupported datatype: YES",
          SQLException.class,
          () -> statement.execute("create timeseries root.sg2.d.s1 with datatype=YES"));
      assertThrows(
          "Unsupported datatype: UNKNOWN",
          SQLException.class,
          () -> statement.execute("create device template t1 (s1 UNKNOWN, s2 boolean)"));
      assertThrows(
          "Unsupported datatype: VECTOR",
          SQLException.class,
          () -> statement.execute("create device template t1 (s1 VECTOR, s2 boolean)"));
    } catch (SQLException ignored) {
      fail();
    }
  }

  @Test
  public void testDifferentDeviceAlignment() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.sg2.d.s1 with datatype=INT64");
      // Should ignore the alignment difference
      statement.execute("create aligned timeseries root.sg2.d (s2 int64, s3 int64)");
      // Should use the existing alignment
      statement.execute("insert into root.sg2.d (time, s4) aligned values (-1, 1)");
      TestUtils.assertResultSetEqual(
          statement.executeQuery("select * from root.sg2.d"),
          "Time,root.sg2.d.s3,root.sg2.d.s4,root.sg2.d.s1,root.sg2.d.s2,",
          Collections.singleton("-1,null,1.0,null,null,"));
    } catch (SQLException ignored) {
      fail();
    }
  }
}
