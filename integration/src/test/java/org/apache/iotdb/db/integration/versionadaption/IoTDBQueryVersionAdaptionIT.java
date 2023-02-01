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
package org.apache.iotdb.db.integration.versionadaption;

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;
import org.apache.iotdb.jdbc.Constant;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.constant.TestConstant.DATA_TYPE_STR;
import static org.apache.iotdb.db.constant.TestConstant.TIMESEIRES_STR;
import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.VALUE_STR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
@Ignore // No longer forward compatible since v0.14
public class IoTDBQueryVersionAdaptionIT {

  private static String[] sqls =
      new String[] {
        "CREATE DATABASE root.ln",
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
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465600000,'v2')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465660000,'v2')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465720000,'v1')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465780000,'v1')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465840000,'v1')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465900000,'v1')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509465960000,'v1')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509466020000,'v1')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509466080000,'v1')",
        "insert into root.ln.wf02.wt02(timestamp,hardware) values(1509466140000,'v1')",
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
        "CREATE DATABASE root.sgcc",
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
    EnvFactory.getEnv().initBeforeClass();

    importData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  private static void importData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
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
  public void selectTest() {
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

    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
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
  public void selectLastTest() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "1509466140000,root.sgcc.wf03.wt01.temperature,20.98177,FLOAT",
                "1509466140000,root.sgcc.wf03.wt01.status,false,BOOLEAN",
                "1509466140000,root.ln.wf02.wt02.hardware,v1,TEXT",
                "1509466140000,root.ln.wf02.wt02.status,false,BOOLEAN",
                "1509466140000,root.ln.wf01.wt01.temperature,20.98177,FLOAT",
                "1509466140000,root.ln.wf01.wt01.status,false,BOOLEAN"));

    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select last * from root order by time desc");
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        Assert.assertTrue(retSet.contains(ans));
        cnt++;
      }
      Assert.assertEquals(retSet.size(), cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          "1509465600000,root.ln.wf01.wt01,25.96,true,null,",
          "1509465600000,root.sgcc.wf03.wt01,25.96,true,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select * from root.* where temperature >= 25.957603 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,temperature,status,hardware",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.FLOAT, Types.BOOLEAN, Types.VARCHAR
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
        Assert.assertEquals(retArray.length, cnt);
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
