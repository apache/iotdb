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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetFuzzyTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBNullIdQueryIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE testNullId(id1 STRING ID, id2 STRING ID, s1 INT32 MEASUREMENT, s2 BOOLEAN MEASUREMENT, s3 DOUBLE MEASUREMENT)",
        "INSERT INTO testNullId(time,id1,id2,s1,s2,s3) " + "values(1, null, null, 0, false, 11.1)",
        "CREATE TABLE table1(device_id STRING ID, color STRING ATTRIBUTE, s1 INT32 MEASUREMENT, s2 BOOLEAN MEASUREMENT, s3 INT64 MEASUREMENT)",
        // in seq disk
        "INSERT INTO table1(time,device_id,color,s1,s2,s3) "
            + "values(1, 'd1', 'green', 1, false, 11)",
        "INSERT INTO table1(time,device_id,s1) " + "values(5, 'd1', 5)",
        "FLUSH",
        // in uneq disk
        "INSERT INTO table1(time,device_id,s1,s2,s3) " + "values(4, 'd1', 4, true, 44)",
        "INSERT INTO table1(time,device_id,color,s1) " + "values(3, 'd1', 'green', 3)",
        "FLUSH",
        // in seq memtable
        "INSERT INTO table1(time,device_id,s1,s2,s3) " + "values(7, 'd1', 7, false, 77)",
        "INSERT INTO table1(time,device_id,s1) " + "values(6, 'd1', 6)",
        // in unseq memtable
        "INSERT INTO table1(time,device_id,color,s1) " + "values(2, 'd1', 'green', 2)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void nullFilterTest() throws Exception {
    String result = defaultFormatDataTime(1) + ",0,false,11.1";
    try (final Connection connectionIsNull =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connectionIsNull.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      ResultSet resultSet = statement.executeQuery("select * from testNullId where id1 is null");
      assertTrue(resultSet.next());
      String ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select * from testNullId where id1 is not null");
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select * from testNullId where id1 like '%'");
      assertFalse(resultSet.next());

      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null and id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // The second time we read from cache
      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null and id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test deduplication
      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null or id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test constant select item
      resultSet = statement.executeQuery("select *, 1 from testNullId");
      result = defaultFormatDataTime(1) + ",null,null,0,false,11.1,1";
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("id1")
              + ","
              + resultSet.getString("id2")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3")
              + ","
              + resultSet.getString("_col6");

      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test boolean between
      resultSet =
          statement.executeQuery("select * from testNullId where s2 between false and true");
      result = defaultFormatDataTime(1) + ",null,null,0,false,11.1";
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("id1")
              + ","
              + resultSet.getString("id2")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");

      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test boolean not between
      resultSet =
          statement.executeQuery("select * from testNullId where s2 not between false and true");
      assertFalse(resultSet.next());

      // Test same column name
      resultSet = statement.executeQuery("select time, s1 as a, s2 as a from testNullId");
      result = defaultFormatDataTime(1) + ",0,false";
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      assertEquals(3, resultSetMetaData.getColumnCount());
      assertEquals("time", resultSetMetaData.getColumnName(1));
      assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(1));
      assertEquals("a", resultSetMetaData.getColumnName(2));
      assertEquals(Types.INTEGER, resultSetMetaData.getColumnType(2));
      assertEquals("a", resultSetMetaData.getColumnName(3));
      assertEquals(Types.BOOLEAN, resultSetMetaData.getColumnType(3));

      assertTrue(resultSet.next());
      ans = resultSet.getString(1) + "," + resultSet.getString(2) + "," + resultSet.getString(3);

      assertEquals(result, ans);
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void nullSelectTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,false,11,",
          "1970-01-01T00:00:00.002Z,d1,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,",
          "1970-01-01T00:00:00.007Z,d1,false,77,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,",
          "1970-01-01T00:00:00.007Z,d1,false,77,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where time > 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,false,11,",
          "1970-01-01T00:00:00.002Z,d1,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where time < 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where time > 1 and time < 7 and s1 >= 3 and s1 <= 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where s2 is NULL",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,false,11,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.007Z,d1,false,77,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where s2 IS NOT NULL OR s3 IS NOT NULL",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void noMeasurementColumnsSelectTest() {
    String[] expectedHeader = new String[] {"time"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,",
          "1970-01-01T00:00:00.002Z,",
          "1970-01-01T00:00:00.003Z,",
          "1970-01-01T00:00:00.004Z,",
          "1970-01-01T00:00:00.005Z,",
          "1970-01-01T00:00:00.006Z,",
          "1970-01-01T00:00:00.007Z,"
        };
    tableResultSetEqualTest("select time from table1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"device_id"};
    retArray = new String[] {"d1,", "d1,", "d1,", "d1,", "d1,", "d1,"};
    tableResultSetEqualTest(
        "select device_id from table1 where time > 1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"color"};
    retArray = new String[] {"green,", "green,", "green,", "green,", "green,", "green,"};
    tableResultSetEqualTest(
        "select color from table1 where time > 1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,",
          "1970-01-01T00:00:00.002Z,d1,",
          "1970-01-01T00:00:00.003Z,d1,",
          "1970-01-01T00:00:00.004Z,d1,",
          "1970-01-01T00:00:00.005Z,d1,",
          "1970-01-01T00:00:00.006Z,d1,",
        };
    tableResultSetEqualTest(
        "select time, device_id from table1 where time < 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "color"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,green,",
          "1970-01-01T00:00:00.002Z,d1,green,",
          "1970-01-01T00:00:00.003Z,d1,green,",
          "1970-01-01T00:00:00.004Z,d1,green,",
          "1970-01-01T00:00:00.005Z,d1,green,",
          "1970-01-01T00:00:00.006Z,d1,green,",
        };
    tableResultSetEqualTest(
        "select time, device_id, color from table1 where time < 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "color"};
    retArray =
        new String[] {
          "d1,green,",
        };
    tableResultSetEqualTest(
        "select device_id, color from table1 where device_id='d1' limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"color"};
    retArray =
        new String[] {
          "green,",
        };
    tableResultSetEqualTest(
        "select color from table1 where device_id='d1' limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,", "1970-01-01T00:00:00.004Z,", "1970-01-01T00:00:00.005Z,",
        };
    tableResultSetEqualTest(
        "select time from table1 where time >= 3 and time <= 5 and device_id='d1'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,", "1970-01-01T00:00:00.004Z,", "1970-01-01T00:00:00.005Z,",
        };
    tableResultSetEqualTest(
        "select time from table1 where time >= 3 and time < 6 and color='green'",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  public void limitOffsetTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 OFFSET 2 limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 limit 1 OFFSET 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,",
          "1970-01-01T00:00:00.007Z,d1,false,77,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 OFFSET 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,false,11,", "1970-01-01T00:00:00.002Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 limit 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void showStatementTest() {
    String[] expectedHeader = new String[] {"CurrentSqlDialect"};
    String[] retArray =
        new String[] {
          "TABLE,",
        };
    tableResultSetEqualTest("show current_sql_dialect", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"CurrentUser"};
    retArray =
        new String[] {
          "root,",
        };
    tableResultSetEqualTest("show current_user", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"CurrentDatabase"};
    retArray =
        new String[] {
          DATABASE_NAME + ",",
        };
    tableResultSetEqualTest("show current_database", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"Version", "BuildInfo"};
    tableResultSetFuzzyTest("show version", expectedHeader, 1, DATABASE_NAME);

    expectedHeader = new String[] {"Variable", "Value"};
    tableResultSetFuzzyTest("show variables", expectedHeader, 15, DATABASE_NAME);

    expectedHeader = new String[] {"ClusterId"};
    tableResultSetFuzzyTest("show clusterid", expectedHeader, 1, DATABASE_NAME);

    expectedHeader = new String[] {"ClusterId"};
    tableResultSetFuzzyTest("show cluster_id", expectedHeader, 1, DATABASE_NAME);

    expectedHeader = new String[] {"CurrentTimestamp"};
    tableResultSetFuzzyTest("show current_timestamp", expectedHeader, 1, DATABASE_NAME);
  }
}
