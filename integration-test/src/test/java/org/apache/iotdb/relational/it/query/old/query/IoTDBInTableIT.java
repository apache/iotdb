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
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBInTableIT {
  private static final String DATABASE_NAME = "test";
  private static String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE sg(device1 STRING ID, device2 STRING ID, qrcode TEXT MEASUREMENT, date_v DATE MEASUREMENT, blob_v BLOB MEASUREMENT, timestamp_v TIMESTAMP MEASUREMENT)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465600000,'d1','s1','qrcode001', '2024-08-01', X'abc0',1509465600000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465660000,'d1','s1','qrcode002', '2024-08-02', X'abc1',1509465660000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465720000,'d1','s1','qrcode003', '2024-08-03', X'abc2',1509465720000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465780000,'d1','s1','qrcode004', '2024-08-04', X'abc3',1509465780000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465720000,'d1','s2','qrcode002', '2024-08-05', X'abc4',1509465720000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465780000,'d1','s2','qrcode003', '2024-08-06', X'abc5',1509465780000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465840000,'d1','s2','qrcode004', '2024-08-07', X'abc6',1509465840000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465900000,'d1','s2','qrcode005', '2024-08-08', X'abc7',1509465900000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465780000,'d2','s1','qrcode002', '2024-08-09', X'abc8',1509465780000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465840000,'d2','s1','qrcode003', '2024-08-10', X'abc9',1509465840000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465900000,'d2','s1','qrcode004', '2024-08-11', X'abca',1509465900000)",
        "insert into sg(time,device1,device2,qrcode,date_v,blob_v,timestamp_v) values(1509465960000,'d2','s1','qrcode005', '2024-08-12', X'abcb',1509465960000)",
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
              "select time,device1,device2,qrcode from sg where qrcode in ('d1','s1','qrcode002','qrcode004') order by device1,device2")) {
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

      // DATE IN
      try (ResultSet resultSet =
          statement.executeQuery(
              "select date_v from sg where date_v in (CAST('2024-08-05' AS DATE), CAST('2024-08-12' AS DATE), CAST('2024-08-22' AS DATE)) order by date_v")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "date_v,",
            new int[] {
              Types.DATE,
            });

        int cnt = 0;
        String expectedString = "2024-08-05,2024-08-12,";
        StringBuilder actualBuilder = new StringBuilder();
        while (resultSet.next()) {
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
          }
          cnt++;
        }
        Assert.assertEquals(expectedString, actualBuilder.toString());
        Assert.assertEquals(2, cnt);
      }

      // BLOB IN
      try (ResultSet resultSet =
          statement.executeQuery(
              "select blob_v from sg where blob_v in (X'abc3', X'abca', X'bbbb') order by blob_v")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "blob_v,",
            new int[] {
              Types.BLOB,
            });

        String expectedString = "0xabc3,0xabca,";
        StringBuilder actualBuilder = new StringBuilder();
        int cnt = 0;
        while (resultSet.next()) {
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
          }
          cnt++;
        }
        Assert.assertEquals(expectedString, actualBuilder.toString());
        Assert.assertEquals(2, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testTimestampIn() {
    // select time,device1,device2,timestamp_v from sg where timestamp_v in (1509465600000)
    // 1509465600000,'d1','s1',
    String[] expectedHeader = new String[] {"time", "device1", "device2", "timestamp_v"};
    String[] retArray =
        new String[] {
          "2017-10-31T16:00:00.000Z,d1,s1,2017-10-31T16:00:00.000Z,",
        };
    tableResultSetEqualTest(
        "select time,device1,device2,timestamp_v from sg where timestamp_v in (1509465600000)",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select time,device1,device2,timestamp_v from sg where timestamp_v=1509465600000",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select time,device1,device2,timestamp_v from sg where timestamp_v=2017-11-01T00:00:00.000+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select time,device1,device2,timestamp_v from sg where timestamp_v=CAST('2017-11-01T00:00:00.000+08:00' AS TIMESTAMP)",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select time,device1,device2,timestamp_v from sg where timestamp_v in (1509465600000.0)",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select time,device1,device2,timestamp_v from sg where timestamp_v=1509465600000.0",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "2017-10-31T16:03:00.000Z,d1,s1,2017-10-31T16:03:00.000Z,",
          "2017-10-31T16:03:00.000Z,d1,s2,2017-10-31T16:03:00.000Z,",
          "2017-10-31T16:04:00.000Z,d1,s2,2017-10-31T16:04:00.000Z,",
          "2017-10-31T16:05:00.000Z,d1,s2,2017-10-31T16:05:00.000Z,",
          "2017-10-31T16:03:00.000Z,d2,s1,2017-10-31T16:03:00.000Z,",
          "2017-10-31T16:04:00.000Z,d2,s1,2017-10-31T16:04:00.000Z,",
          "2017-10-31T16:05:00.000Z,d2,s1,2017-10-31T16:05:00.000Z,",
          "2017-10-31T16:06:00.000Z,d2,s1,2017-10-31T16:06:00.000Z,",
        };
    tableResultSetEqualTest(
        "select time,device1,device2,timestamp_v from sg where timestamp_v not in (2017-11-01T00:00:00.000+08:00,1509465660000.0,CAST('2017-11-01T00:02:00.000+08:00' AS TIMESTAMP)) order by device1,device2,time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
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
