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

package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBInTableIT {
  private static final String DATABASE_NAME = "test";
  private static String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE sg(device1 STRING ID, device2 STRING ID, qrcode TEXT MEASUREMENT)",
        "insert into sg(time,device1,device2,qrcode) values(1509465600000,'d1','s1','qrcode001')",
        "insert into sg(time,device1,device2,qrcode) values(1509465660000,'d1','s1','qrcode002')",
        "insert into sg(time,device1,device2,qrcode) values(1509465720000,'d1','s1','qrcode003')",
        "insert into sg(time,device1,device2,qrcode) values(1509465780000,'d1','s1','qrcode004')",
        "insert into sg(time,device1,device2,qrcode) values(1509465720000,'d1','s2','qrcode002')",
        "insert into sg(time,device1,device2,qrcode) values(1509465780000,'d1','s2','qrcode003')",
        "insert into sg(time,device1,device2,qrcode) values(1509465840000,'d1','s2','qrcode004')",
        "insert into sg(time,device1,device2,qrcode) values(1509465900000,'d1','s2','qrcode005')",
        "insert into sg(time,device1,device2,qrcode) values(1509465780000,'d2','s1','qrcode002')",
        "insert into sg(time,device1,device2,qrcode) values(1509465840000,'d2','s1','qrcode003')",
        "insert into sg(time,device1,device2,qrcode) values(1509465900000,'d2','s1','qrcode004')",
        "insert into sg(time,device1,device2,qrcode) values(1509465960000,'d2','s1','qrcode005')",
        "CREATE TABLE table1(device STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 BOOLEAN MEASUREMENT)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();

    prepareTableData(sqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCastException() {
    tableAssertTestFail(
        "select * from table1 where s1 in ('test')",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": IN value and list items must be the same type or coercible to a common type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "select * from table1 where s2 in ('test')",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": IN value and list items must be the same type or coercible to a common type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "select * from table1 where s3 in ('test')",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": IN value and list items must be the same type or coercible to a common type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "select * from table1 where s4 in ('test')",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": IN value and list items must be the same type or coercible to a common type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "select * from table1 where s5 in ('test')",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": IN value and list items must be the same type or coercible to a common type.",
        DATABASE_NAME);
  }

  @Test
  public void selectWithAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          defaultFormatDataTime(1509465660000L) + ",d1,s1,qrcode002,",
          defaultFormatDataTime(1509465780000L) + ",d1,s1,qrcode004,",
          defaultFormatDataTime(1509465720000L) + ",d1,s2,qrcode002,",
          defaultFormatDataTime(1509465840000L) + ",d1,s2,qrcode004,",
          defaultFormatDataTime(1509465780000L) + ",d2,s1,qrcode002,",
          defaultFormatDataTime(1509465900000L) + ",d2,s1,qrcode004,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("USE " + DATABASE_NAME);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from sg where qrcode in ('d1','s1','qrcode002','qrcode004') order by device1,device2")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "time,device1,device2,qrcode,",
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
