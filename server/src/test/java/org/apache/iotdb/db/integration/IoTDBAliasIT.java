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

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBAliasIT {

  private static String[] sqls = new String[]{
      "SET STORAGE GROUP TO root.sg",
      "CREATE TIMESERIES root.sg.d1.s1(speed) WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.sg.d1.s2(temperature) WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.sg.d2.s1(speed) WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.sg.d2.s2(temperature) WITH DATATYPE=FLOAT, ENCODING=RLE",

      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 10.1, 20.7)",
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(200, 15.2, 22.9)",
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(300, 30.3, 25.1)",
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(400, 50.4, 28.3)",

      "INSERT INTO root.sg.d2(timestamp,speed,temperature) values(100, 11.1, 20.2)",
      "INSERT INTO root.sg.d2(timestamp,speed,temperature) values(200, 20.2, 21.8)",
      "INSERT INTO root.sg.d2(timestamp,speed,temperature) values(300, 45.3, 23.4)",
      "INSERT INTO root.sg.d2(timestamp,speed,temperature) values(400, 73.4, 26.3)"
  };

  private static final String TIMESTAMP_STR = "Time";
  private static final String TIMESEIRES_STR = "timeseries";
  private static final String VALUE_STR = "value";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

    insertData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void selectWithAliasTest() throws ClassNotFoundException {
    String[] retArray = new String[]{
        "100,10.1,20.7,",
        "200,15.2,22.9,",
        "300,30.3,25.1,",
        "400,50.4,28.3,"
    };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select speed, temperature from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        Assert.assertEquals("Time,root.sg.d1.speed,root.sg.d1.temperature,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastSelectWithAliasTest() throws ClassNotFoundException {
    String[] retArray = new String[]{
        "400,root.sg.d1.speed,50.4",
        "400,root.sg.d1.temperature,28.3"
    };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select last speed, temperature from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + ","
              + resultSet.getString(TIMESEIRES_STR) + ","
              + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectDuplicatedPathsWithAliasTest() throws ClassNotFoundException {
    String[] retArray = new String[]{
        "100,10.1,10.1,20.7,",
        "200,15.2,15.2,22.9,",
        "300,30.3,30.3,25.1,",
        "400,50.4,50.4,28.3,"
    };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select speed, speed, s2 from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        Assert.assertEquals("Time,root.sg.d1.speed,root.sg.d1.speed,root.sg.d1.s2,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        Assert.assertEquals(4, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastSelectDuplicatedPathsWithAliasTest() throws ClassNotFoundException {
    String[] retArray = new String[]{
        "400,root.sg.d1.speed,50.4",
        "400,root.sg.d1.speed,50.4",
        "400,root.sg.d1.s2,28.3"
    };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select last speed, speed, s2 from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + ","
              + resultSet.getString(TIMESEIRES_STR) + ","
              + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }


}
