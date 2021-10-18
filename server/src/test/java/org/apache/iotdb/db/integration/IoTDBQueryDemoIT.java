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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class IoTDBQueryDemoIT {

  private static String[] sqls =
      new String[] {
        "set storage group to root.ln",
        "create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465660000,true)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465720000,false)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465780000,false)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465840000,false)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465900000,false)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465960000,false)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509466020000,false)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509466080000,false)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509466140000,false)",
        "create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,25.957603)",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465660000,24.359503)",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465720000,20.092794)",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465780000,20.182663)",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465840000,21.125198)",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465900000,22.720892)",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465960000,20.71);",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509466020000,21.451046);",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509466080000,22.57987);",
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509466140000,20.98177);",
        "create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465600000,\"v2\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465660000,\"v2\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465720000,\"v1\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465780000,\"v1\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465840000,\"v1\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465900000,\"v1\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465960000,\"v1\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509466020000,\"v1\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509466080000,\"v1\")",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509466140000,\"v1\")",
        "create timeseries root.ln.wf02.wt02.status with datatype=BOOLEAN,encoding=PLAIN",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509465600000,true)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509465660000,true)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509465720000,false)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509465780000,false)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509465840000,false)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509465900000,false)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509465960000,false)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509466020000,false)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509466080000,false)",
        "insert into root.ln.wf02.wt02(timestamp,status) values(1509466140000,false)",
        "set storage group to root.sgcc",
        "create timeseries root.sgcc.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509465600000,true)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509465660000,true)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509465720000,false)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509465780000,false)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509465840000,false)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509465900000,false)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509465960000,false)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509466020000,false)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509466080000,false)",
        "insert into root.sgcc.wf03.wt01(timestamp,status) values(1509466140000,false)",
        "create timeseries root.sgcc.wf03.wt01.temperature with datatype=FLOAT,encoding=RLE",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509465600000,25.957603)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509465660000,24.359503)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509465720000,20.092794)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509465780000,20.182663)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509465840000,21.125198)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509465900000,22.720892)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509465960000,20.71)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509466020000,21.451046)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509466080000,22.57987)",
        "insert into root.sgcc.wf03.wt01(timestamp,temperature) values(1509466140000,20.98177)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

    importData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void importData() throws ClassNotFoundException {
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
      fail(e.getMessage());
    }
  }

  @Test
  public void selectTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1509465600000,true,25.96,v2,true,true,25.96,",
          "1509465660000,true,24.36,v2,true,true,24.36,",
          "1509465720000,false,20.09,v1,false,false,20.09,",
          "1509465780000,false,20.18,v1,false,false,20.18,",
          "1509465840000,false,21.13,v1,false,false,21.13,",
          "1509465900000,false,22.72,v1,false,false,22.72,",
          "1509465960000,false,20.71,v1,false,false,20.71,",
          "1509466020000,false,21.45,v1,false,false,21.45,",
          "1509466080000,false,22.58,v1,false,false,22.58,",
          "1509466140000,false,20.98,v1,false,false,20.98,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select * from root where time>10");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.ln.wf01.wt01.status,root.ln.wf01.wt01.temperature,"
                    + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,root.sgcc.wf03.wt01.status,"
                    + "root.sgcc.wf03.wt01.temperature,",
                new int[] {
                  Types.TIMESTAMP,
                  Types.BOOLEAN,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                  Types.BOOLEAN,
                  Types.FLOAT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(10, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void LimitTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1509465780000,false,20.18,v1,false,false,20.18,",
          "1509465840000,false,21.13,v1,false,false,21.13,",
          "1509465900000,false,22.72,v1,false,false,22.72,",
          "1509465960000,false,20.71,v1,false,false,20.71,",
          "1509466020000,false,21.45,v1,false,false,21.45,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // test 1: fetchSize < limitNumber
      statement.setFetchSize(4);
      Assert.assertEquals(4, statement.getFetchSize());
      boolean hasResultSet = statement.execute("select * from root where time>10 limit 5 offset 3");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.ln.wf01.wt01.status,root.ln.wf01.wt01.temperature,"
                    + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,root.sgcc.wf03.wt01.status,"
                    + "root.sgcc.wf03.wt01.temperature,",
                new int[] {
                  Types.TIMESTAMP,
                  Types.BOOLEAN,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                  Types.BOOLEAN,
                  Types.FLOAT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(5, cnt);
      }

      // test 1: fetchSize > limitNumber
      statement.setFetchSize(10000);
      Assert.assertEquals(10000, statement.getFetchSize());
      hasResultSet = statement.execute("select * from root where time>10 limit 5 offset 3");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.ln.wf01.wt01.status,root.ln.wf01.wt01.temperature,"
                    + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,root.sgcc.wf03.wt01.status,"
                    + "root.sgcc.wf03.wt01.temperature,",
                new int[] {
                  Types.TIMESTAMP,
                  Types.BOOLEAN,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                  Types.BOOLEAN,
                  Types.FLOAT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(5, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void nowTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean execute = statement.execute("show now()");
      Assert.assertTrue(execute);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        Assert.assertEquals(5, metaData.getColumnCount());
        Assert.assertEquals("IpAddress", metaData.getColumnName(1));
        Assert.assertEquals("SystemTime", metaData.getColumnName(2));
        Assert.assertEquals("CpuLoad", metaData.getColumnName(3));
        Assert.assertEquals("TotalMemorySize", metaData.getColumnName(4));
        Assert.assertEquals("FreeMemorySize", metaData.getColumnName(5));

      } catch (Exception e) {
        e.printStackTrace();
      }

    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }

  @Test
  public void InTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1509465780000,false,20.18,v1,false,false,20.18,",
          "1509465840000,false,21.13,v1,false,false,21.13,",
          "1509465900000,false,22.72,v1,false,false,22.72,",
          "1509465960000,false,20.71,v1,false,false,20.71,",
          "1509466020000,false,21.45,v1,false,false,21.45,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // test 1: fetchSize < limitNumber
      statement.setFetchSize(4);
      Assert.assertEquals(4, statement.getFetchSize());
      boolean hasResultSet =
          statement.execute(
              "select * from root where time in (1509465780000, 1509465840000, 1509465900000, 1509465960000, 1509466020000)");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.ln.wf01.wt01.status,root.ln.wf01.wt01.temperature,"
                    + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,root.sgcc.wf03.wt01.status,"
                    + "root.sgcc.wf03.wt01.temperature,",
                new int[] {
                  Types.TIMESTAMP,
                  Types.BOOLEAN,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                  Types.BOOLEAN,
                  Types.FLOAT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(5, cnt);
      }

      retArray =
          new String[] {
            "1509465600000,true,25.96,v2,true,true,25.96,",
            "1509465660000,true,24.36,v2,true,true,24.36,",
            "1509465720000,false,20.09,v1,false,false,20.09,",
            "1509466080000,false,22.58,v1,false,false,22.58,",
            "1509466140000,false,20.98,v1,false,false,20.98,",
          };
      hasResultSet =
          statement.execute(
              "select * from root where time not in (1509465780000, 1509465840000, 1509465900000, 1509465960000, 1509466020000)");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.ln.wf01.wt01.status,root.ln.wf01.wt01.temperature,"
                    + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,root.sgcc.wf03.wt01.status,"
                    + "root.sgcc.wf03.wt01.temperature,",
                new int[] {
                  Types.TIMESTAMP,
                  Types.BOOLEAN,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                  Types.BOOLEAN,
                  Types.FLOAT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(5, cnt);
      }

      retArray =
          new String[] {
            "1509465780000,false,20.18,v1,false,false,20.18,",
            "1509465960000,false,20.71,v1,false,false,20.71,",
            "1509466080000,false,22.58,v1,false,false,22.58,",
          };
      hasResultSet =
          statement.execute(
              "select * from root where ln.wf01.wt01.temperature in (20.18, 20.71, 22.58)");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.ln.wf01.wt01.status,root.ln.wf01.wt01.temperature,"
                    + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,root.sgcc.wf03.wt01.status,"
                    + "root.sgcc.wf03.wt01.temperature,",
                new int[] {
                  Types.TIMESTAMP,
                  Types.BOOLEAN,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                  Types.BOOLEAN,
                  Types.FLOAT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      Assert.assertNotNull(typeIndex);
      Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
      actualIndexToExpectedIndexList.add(typeIndex);
    }
    return actualIndexToExpectedIndexList;
  }

  @Test
  public void testWrongTextQuery() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select * from root.ln.wf02.wt02 where hardware > 'v1'");
    } catch (Exception e) {
      Assert.assertEquals(
          e.getMessage(),
          "411: Error occurred in query process: For Basic operator,TEXT type only support EQUAL or NOTEQUAL operator");
    }
  }

  @Test
  public void testRightTextQuery() throws ClassNotFoundException {
    // Text type uses the equal operator to query the correct result
    String[] retArray =
        new String[] {
          "1509465600000,v2,", "1509465660000,v2,",
        };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select hardware from root.ln.wf02.wt02 where hardware = 'v2'");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.ln.wf02.wt02.hardware,",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

    } catch (Exception e) {
      Assert.assertNull(e.getMessage());
    }
  }

  @Test
  public void LikeTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1509465600000,v2,true,", "1509465660000,v2,true,", "1509465720000,v1,false,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.setFetchSize(4);
      Assert.assertEquals(4, statement.getFetchSize());
      // Matches a string consisting of one lowercase letter and one digit. such as: "v1","v2"
      boolean hasResultSet =
          statement.execute(
              "select hardware,status from root.ln.wf02.wt02 where hardware like '^[a-z][0-9]$' and time < 1509465780000");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time," + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

      retArray =
          new String[] {
            "1509465600000,v2,true,",
            "1509465660000,v2,true,",
            "1509465720000,v1,false,",
            "1509465780000,v1,false,",
            "1509465840000,v1,false,",
            "1509465900000,v1,false,",
            "1509465960000,v1,false,",
            "1509466020000,v1,false,",
            "1509466080000,v1,false,",
            "1509466140000,v1,false,",
          };
      hasResultSet =
          statement.execute(
              "select hardware,status from root.ln.wf02.wt02 where hardware like 'v*' ");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time," + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(10, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void LikeNonExistTest() throws ClassNotFoundException {

    // Match nonexistent string.'s.' is indicates that the starting with s and the last is any
    // single character
    String[] retArray =
        new String[] {
          "1509465600000,v2,true,",
          "1509465660000,v2,true,",
          "1509465720000,v1,false,",
          "1509465780000,v1,false,",
          "1509465840000,v1,false,",
          "1509465900000,v1,false,",
          "1509465960000,v1,false,",
          "1509466020000,v1,false,",
          "1509466080000,v1,false,",
          "1509466140000,v1,false,",
        };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select hardware,status from root.ln.wf02.wt02 where hardware like 's.' ");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time," + "root.ln.wf02.wt02.hardware,root.ln.wf02.wt02.status,",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
