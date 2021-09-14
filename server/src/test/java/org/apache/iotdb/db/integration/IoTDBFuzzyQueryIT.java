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
import java.util.Objects;

import static org.junit.Assert.fail;

public class IoTDBFuzzyQueryIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    initCreateSQLStatement();
    EnvironmentUtils.envSetUp();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvironmentUtils.cleanEnv();
  }

  private static void close() {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void initCreateSQLStatement() {
    sqls.add("SET STORAGE GROUP TO root.t1");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.status WITH DATATYPE=TEXT, ENCODING=PLAIN");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt02.status WITH DATATYPE=TEXT, ENCODING=PLAIN");

    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465600000,'1',12.1)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465660000,'14',13.1)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465720000,'616',5.5)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465780000,'626',8.1)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465840000,'6116',4.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465900000,'6%16',10.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465960000,'8[sS]*',11.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509466020000,'%123',18.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509466080000,'123%',18.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509466090000,'\\',10.3)");
    sqls.add("insert into root.t1.wf01.wt02 (time,status) values (1509465600000,'14')");
  }

  private static void insertData() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }
    statement.close();
  }

  @Test
  public void testLike() throws SQLException {
    Statement st0 = connection.createStatement();
    boolean hasResultSet =
        st0.execute("select status from root.t1.wf01.wt01 where status like '1'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("1", outputResultStr(st0.getResultSet()));
    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals(
        "1,14,616,626,6116,6%16,8[sS]*,%123,123%,\\", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '1%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("1,14,123%", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '%1%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("1,14,616,6116,6%16,%123,123%", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '6%6'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("616,626,6116,6%16", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '1_'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("14", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '6_1%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("6116,6%16", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '6\\%%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("6%16", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '\\%%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("%123", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '%\\%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("123%", outputResultStr(st0.getResultSet()));

    hasResultSet =
        st0.execute("select status from root.t1.wf01.wt01 where status like '%\\\\\\\\%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("\\", outputResultStr(st0.getResultSet()));
  }

  @Test(expected = Exception.class)
  public void testLikeNonTextCloumn() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("select * from root.t1.wf01.wt01 where temperature like '1'");
  }

  private String outputResultStr(ResultSet resultSet) throws SQLException {
    StringBuilder resultBuilder = new StringBuilder();
    while (resultSet.next()) {
      resultBuilder.append(resultSet.getString(2)).append(",");
    }
    String result = resultBuilder.toString();
    return result.substring(0, result.length() - 1);
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
  public void selectLikeAlignByDevice() throws ClassNotFoundException {
    String[] retArray =
        new String[] {"1509465660000,root.t1.wf01.wt01,14,", "1509465600000,root.t1.wf01.wt02,14"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select status from root.t1.wf01.wt0* where status like '14%' align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,status,",
                new int[] {Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR});

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
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectRegexpAlignByDevice() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1509465600000,root.t1.wf01.wt01,1,",
          "1509465660000,root.t1.wf01.wt01,14,",
          "1509466080000,root.t1.wf01.wt01,123%,",
          "1509465600000,root.t1.wf01.wt02,14,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select status from root.t1.wf01.wt0* where status regexp '^1.*' align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,status,",
                new int[] {Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR});

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
        Assert.assertEquals(4, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
