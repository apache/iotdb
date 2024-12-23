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
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.apache.iotdb.itbase.constant.TestConstant.count;
import static org.junit.Assert.fail;

@Category({ClusterIT.class})
public class IoTDBDeleteAlignedTimeseriesIT extends AbstractSchemaIT {

  public IoTDBDeleteAlignedTimeseriesIT(SchemaTestMode schemaTestMode) {
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

  @Test
  public void deleteTimeseriesAndCreateDifferentTypeTest() throws Exception {
    String[] retArray = new String[] {"1,1,", "2,1.1,"};
    int cnt = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create aligned timeseries root.turbine1.d1(s1 INT64 encoding=PLAIN compression=SNAPPY, "
              + "s2 INT64 encoding=PLAIN compression=SNAPPY)");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) ALIGNED VALUES(1,1,2)");
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }

      statement.execute("DELETE timeseries root.turbine1.d1.s1");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) ALIGNED VALUES(2,1.1)");
      statement.execute("FLUSH");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
    }

    // todo
    //    EnvironmentUtils.restartDaemon();
    //
    //    hasResult = statement.execute("SELECT * FROM root.**");
    //    Assert.assertTrue(hasResult);
  }

  @Test
  public void deleteTimeseriesAndCreateSameTypeTest() throws Exception {
    String[] retArray = new String[] {"1,1.0,", "2,5.0,"};
    int cnt = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create aligned timeseries root.turbine1.d1(s1 FLOAT encoding=PLAIN compression=SNAPPY, "
              + "s2 INT64 encoding=PLAIN compression=SNAPPY)");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) ALIGNED VALUES(1,1,2)");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
      statement.execute("DELETE timeseries root.turbine1.d1.s1");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) ALIGNED VALUES(2,5)");
      statement.execute("FLUSH");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
    }

    // todo
    //    EnvironmentUtils.restartDaemon();
    //
    //    hasResult = statement.execute("SELECT * FROM root.**");
    //    Assert.assertTrue(hasResult);
  }

  @Test
  public void deleteTimeseriesAndChangeDeviceAlignmentTest() throws Exception {
    String[] retArray = new String[] {"1,1.0,2.0,"};
    int cnt = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO root.sg3.d1(timestamp,s1,s2) ALIGNED VALUES(1,1,2)");
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES")) {
        while (resultSet.next()) {
          Assert.assertEquals("true", resultSet.getString(ColumnHeaderConstant.IS_ALIGNED));
        }
      }
      cnt = 0;
      statement.execute("DELETE timeseries root.sg3.d1.s1");
      statement.execute("DELETE timeseries root.sg3.d1.s2");
      statement.execute("INSERT INTO root.sg3.d1(timestamp,s1,s2) VALUES(1,1,2)");
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES")) {
        while (resultSet.next()) {
          Assert.assertEquals("false", resultSet.getString(ColumnHeaderConstant.IS_ALIGNED));
        }
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg3.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
      cnt = 0;
      statement.execute("DELETE timeseries root.sg3.d1.s1");
      statement.execute("DELETE timeseries root.sg3.d1.s2");
      statement.execute("INSERT INTO root.sg3.d1(timestamp,s1,s2) ALIGNED VALUES(1,1,2)");
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES")) {
        while (resultSet.next()) {
          Assert.assertEquals("true", resultSet.getString(ColumnHeaderConstant.IS_ALIGNED));
        }
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg3.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
    }
  }

  @Test
  public void deleteTimeSeriesMultiIntervalTest() {
    String[] retArray1 = new String[] {"0"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String insertSql = "insert into root.sg.d1(time, s1) aligned values(%d, %d)";
      for (int i = 1; i <= 4; i++) {
        statement.execute(String.format(insertSql, i, i));
      }
      statement.execute("flush");

      statement.execute("delete from root.sg.d1.s1 where time >= 1 and time <= 2");
      statement.execute("delete from root.sg.d1.s1 where time >= 3 and time <= 4");

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1) from root.sg.d1 where time >= 3 and time <= 4")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(count("root.sg.d1.s1"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
