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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBAliasIT {

  private static String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
        "CREATE TIMESERIES root.sg.d1.s1(speed) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d1.s2(temperature) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s1(speed) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s2(temperature) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s3(power) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 10.1, 20.7)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(200, 15.2, 22.9)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(300, 30.3, 25.1)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(400, 50.4, 28.3)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(100, 11.1, 20.2, 80.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(200, 20.2, 21.8, 81.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(300, 45.3, 23.4, 82.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(400, 73.4, 26.3, 83.0)"
      };

  private static final String TIMESTAMP_STR = "Time";
  private static final String TIMESEIRES_STR = "timeseries";
  private static final String VALUE_STR = "value";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
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
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select speed, temperature from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,root.sg.d1.speed,root.sg.d1.temperature,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastSelectWithAliasTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {"400,root.sg.d1.speed,50.4", "400,root.sg.d1.temperature,28.3"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select last speed, temperature from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR);
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectDuplicatedPathsWithAliasTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "100,10.1,10.1,20.7,", "200,15.2,15.2,22.9,", "300,30.3,30.3,25.1,", "400,50.4,50.4,28.3,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select speed, speed, s2 from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,root.sg.d1.speed,root.sg.d1.speed,root.sg.d1.s2,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(4, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastSelectDuplicatedPathsWithAliasTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "400,root.sg.d1.speed,50.4", "400,root.sg.d1.s1,50.4", "400,root.sg.d1.s2,28.3"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select last speed, s1, speed, s2 from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR);
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAggregationWithAliasTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"4,4,28.3,26.3,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select count(speed), max_value(temperature) from root.sg.*");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals(
            "count(root.sg.d1.speed),count(root.sg.d2.speed),"
                + "max_value(root.sg.d1.temperature),max_value(root.sg.d2.temperature),",
            header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void AlterAliasTest() throws ClassNotFoundException {
    String ret = "root.sg.d2.s3,powerNew,root.sg,FLOAT,RLE,SNAPPY";

    String[] retArray = {"100,80.0,", "200,81.0,", "300,82.0,", "400,83.0,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("ALTER timeseries root.sg.d2.s3 UPSERT ALIAS=powerNew");
      boolean hasResult = statement.execute("show timeseries root.sg.d2.s3");
      assertTrue(hasResult);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression");
          assertEquals(ret, ans);
        }
      }

      hasResult = statement.execute("select powerNew from root.sg.d2");
      assertTrue(hasResult);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,root.sg.d2.powerNew,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      fail(e.getMessage());
      e.printStackTrace();
    }
  }
}
