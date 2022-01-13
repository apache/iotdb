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

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBInIT {

  private static String[] sqls =
      new String[] {
        "set storage group to root.ln",
        "set storage group to root.sg",
        "create timeseries root.sg.d1.s1.qrcode with datatype=TEXT,encoding=PLAIN",
        "insert into root.sg.d1.s1(timestamp,qrcode) values(1509465600000,'qrcode001')",
        "insert into root.sg.d1.s1(timestamp,qrcode) values(1509465660000,'qrcode002')",
        "insert into root.sg.d1.s1(timestamp,qrcode) values(1509465720000,'qrcode003')",
        "insert into root.sg.d1.s1(timestamp,qrcode) values(1509465780000,'qrcode004')",
        "create timeseries root.sg.d1.s2.qrcode with datatype=TEXT,encoding=PLAIN",
        "insert into root.sg.d1.s2(timestamp,qrcode) values(1509465720000,'qrcode002')",
        "insert into root.sg.d1.s2(timestamp,qrcode) values(1509465780000,'qrcode003')",
        "insert into root.sg.d1.s2(timestamp,qrcode) values(1509465840000,'qrcode004')",
        "insert into root.sg.d1.s2(timestamp,qrcode) values(1509465900000,'qrcode005')",
        "create timeseries root.sg.d2.s1.qrcode with datatype=TEXT,encoding=PLAIN",
        "insert into root.sg.d2.s1(timestamp,qrcode) values(1509465780000,'qrcode002')",
        "insert into root.sg.d2.s1(timestamp,qrcode) values(1509465840000,'qrcode003')",
        "insert into root.sg.d2.s1(timestamp,qrcode) values(1509465900000,'qrcode004')",
        "insert into root.sg.d2.s1(timestamp,qrcode) values(1509465960000,'qrcode005')"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();

    importData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  private static void importData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test for IOTDB-1540 */
  @Test
  public void selectWithStarTest1() {
    String[] retArray = new String[] {"1509465720000,qrcode003,qrcode002,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select qrcode from root.sg.d1.* where qrcode in ('qrcode002', 'qrcode003')");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.sg.d1.s1.qrcode,root.sg.d1.s2.qrcode,",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR,
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
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test for IOTDB-1540 */
  @Test
  public void selectWithStarTest2() {
    String[] retArray = new String[] {"1509465780000,qrcode004,qrcode003,qrcode002,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select qrcode from root.sg.*.* where qrcode in ('qrcode002', 'qrcode003', 'qrcode004')");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.sg.d1.s1.qrcode,root.sg.d1.s2.qrcode,root.sg.d2.s1.qrcode,",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
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
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test for IOTDB-1540 */
  @Test
  public void selectWithAlignByDeviceTest() {
    Set<String> retSet = new HashSet();
    retSet.add("1509465660000,root.sg.d1.s1,qrcode002,");
    retSet.add("1509465780000,root.sg.d1.s1,qrcode004,");
    retSet.add("1509465720000,root.sg.d1.s2,qrcode002,");
    retSet.add("1509465840000,root.sg.d1.s2,qrcode004,");
    retSet.add("1509465780000,root.sg.d2.s1,qrcode002,");
    retSet.add("1509465900000,root.sg.d2.s1,qrcode004,");

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select qrcode from root.sg.*.* where qrcode in ('qrcode002', 'qrcode004') align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,qrcode,",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR,
                });

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
          }
          assertTrue(retSet.contains(actualBuilder.toString()));
          cnt++;
        }
        Assert.assertEquals(6, cnt);
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
}
