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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBWindowFunctionIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqlsWithoutNulls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table demo (device string tag, value double field)",
        "insert into demo values (2021-01-01T09:05:00, 'd1', 3)",
        "insert into demo values (2021-01-01T09:07:00, 'd1', 5)",
        "insert into demo values (2021-01-01T09:09:00, 'd1', 3)",
        "insert into demo values (2021-01-01T09:10:00, 'd1', 1)",
        "insert into demo values (2021-01-01T09:08:00, 'd2', 2)",
        "insert into demo values (2021-01-01T09:15:00, 'd2', 4)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };
  private static final String[] sqlsWithNulls =
      new String[] {
        "create table demo2 (device string tag, value double field)",
        "insert into demo2 values (2021-01-01T09:04:00, 'd1', null)",
        "insert into demo2 values (2021-01-01T09:05:00, 'd1', 3)",
        "insert into demo2 values (2021-01-01T09:07:00, 'd1', 5)",
        "insert into demo2 values (2021-01-01T09:09:00, 'd1', 3)",
        "insert into demo2 values (2021-01-01T09:10:00, 'd1', 1)",
        "insert into demo2 values (2021-01-01T09:06:00, 'd2', null)",
        "insert into demo2 values (2021-01-01T09:08:00, 'd2', 2)",
        "insert into demo2 values (2021-01-01T09:15:00, 'd2', 4)",
        "insert into demo2 values (2021-01-01T09:20:00, null, null)",
        "insert into demo2 values (2021-01-01T09:21:00, null, 1)",
        "insert into demo2 values (2021-01-01T09:22:00, null, 2)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };
  private static final String[] normalSqls =
      new String[] {
        "create table demo3 (Device string tag, t2 string tag, t3 string tag, a1 string attribute, Flow int64 field, f2 int32 field, f3 double field,f4 float field, date date field, timestamp timestamp field, string string field)",
        "insert into demo3 values (1970-01-01T08:00:00.000+08:00, 'd0', 'd0', 'd0', 'd0', 3, 3, 3.0, 3.0, '1970-01-01', 3, '3')",
        "insert into demo3 values (1970-01-01T08:00:00.001+08:00, 'd0', 'd0', 'd0', 'd0', 5, 5, 5.0, 5.0, '1970-01-01', 5, '5')",
        "insert into demo3 values (1970-01-01T08:00:00.002+08:00, 'd0', 'd0', 'd0', 'd0', 3, 3, 3.0, 3.0, '1970-01-01', 3, '3')",
        "insert into demo3 values (1970-01-01T08:00:00.003+08:00, 'd0', 'd0', 'd0', 'd0', 1, 1, 1.0, 1.0, '1970-01-01', 1, '1')",
        "insert into demo3 values (1970-01-01T08:00:00.004+08:00, 'd0', 'd0', 'd0', 'd0', null, null, null, null, null, null, null)",
        "FLUSH",
        "insert into demo3 values (1970-01-01T08:00:00.005+08:00, 'd1', 'd1', 'd1', 'd1', 2, 2, 2.0, 2.0, '1970-01-01', 2, '2')",
        "insert into demo3 values (1970-01-01T08:00:00.006+08:00, 'd1', 'd1', 'd1', 'd1', null, null, null, null, null, null, null)",
        "insert into demo3 values (1970-01-01T08:00:00.007+08:00, 'd1', 'd1', 'd1', 'd1', 4, 4, 4.0, 4.0, '1970-01-01', 4, '4')",
        "insert into demo3 values (1970-01-01T08:00:00.008+08:00,  null, null, null, null, null, null, null, null, null, null, null)",
        "CLEAR ATTRIBUTE CACHE",
      };

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqlsWithoutNulls) {
        statement.execute(sql);
      }
      for (String sql : sqlsWithNulls) {
        statement.execute(sql);
      }
      for (String sql : normalSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testEmptyOver() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,6,",
          "2021-01-01T09:07:00.000Z,d1,5.0,6,",
          "2021-01-01T09:09:00.000Z,d1,3.0,6,",
          "2021-01-01T09:10:00.000Z,d1,1.0,6,",
          "2021-01-01T09:08:00.000Z,d2,2.0,6,",
          "2021-01-01T09:15:00.000Z,d2,4.0,6,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER () AS cnt FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPartitionBy() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,4,",
          "2021-01-01T09:07:00.000Z,d1,5.0,4,",
          "2021-01-01T09:09:00.000Z,d1,3.0,4,",
          "2021-01-01T09:10:00.000Z,d1,1.0,4,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER (PARTITION BY device) AS cnt FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPartitionByWithNulls() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:04:00.000Z,d1,null,4,",
          "2021-01-01T09:05:00.000Z,d1,3.0,4,",
          "2021-01-01T09:07:00.000Z,d1,5.0,4,",
          "2021-01-01T09:09:00.000Z,d1,3.0,4,",
          "2021-01-01T09:10:00.000Z,d1,1.0,4,",
          "2021-01-01T09:06:00.000Z,d2,null,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
          "2021-01-01T09:20:00.000Z,null,null,2,",
          "2021-01-01T09:21:00.000Z,null,1.0,2,",
          "2021-01-01T09:22:00.000Z,null,2.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER (PARTITION BY device) AS cnt FROM demo2 ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testOrderBy() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2,",
          "2021-01-01T09:05:00.000Z,d1,3.0,4,",
          "2021-01-01T09:09:00.000Z,d1,3.0,4,",
          "2021-01-01T09:15:00.000Z,d2,4.0,5,",
          "2021-01-01T09:07:00.000Z,d1,5.0,6,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER (ORDER BY value) AS cnt FROM demo",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testOrderByWithNulls() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,2,",
          "2021-01-01T09:21:00.000Z,null,1.0,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,4,",
          "2021-01-01T09:22:00.000Z,null,2.0,4,",
          "2021-01-01T09:05:00.000Z,d1,3.0,6,",
          "2021-01-01T09:09:00.000Z,d1,3.0,6,",
          "2021-01-01T09:15:00.000Z,d2,4.0,7,",
          "2021-01-01T09:07:00.000Z,d1,5.0,8,",
          "2021-01-01T09:04:00.000Z,d1,null,8,",
          "2021-01-01T09:06:00.000Z,d2,null,8,",
          "2021-01-01T09:20:00.000Z,null,null,8,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER (ORDER BY value) AS cnt FROM demo2 ORDER BY value, device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPartitionByAndOrderBy() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rnk"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,2,",
          "2021-01-01T09:07:00.000Z,d1,5.0,4,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, rank() OVER (PARTITION BY device ORDER BY value) AS rnk FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPartitionByAndOrderByWithNulls() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,3,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,",
          "2021-01-01T09:07:00.000Z,d1,5.0,4,",
          "2021-01-01T09:04:00.000Z,d1,null,4,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
          "2021-01-01T09:06:00.000Z,d2,null,2,",
          "2021-01-01T09:21:00.000Z,null,1.0,1,",
          "2021-01-01T09:22:00.000Z,null,2.0,2,",
          "2021-01-01T09:20:00.000Z,null,null,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY value) AS cnt FROM demo2 ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testRowsFraming() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,2,",
          "2021-01-01T09:10:00.000Z,d1,1.0,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER (PARTITION BY device ROWS 1 PRECEDING) AS cnt FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testGroupsFraming() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,3,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,",
          "2021-01-01T09:07:00.000Z,d1,5.0,3,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY value GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cnt FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testRangeFraming() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cnt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,3,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,",
          "2021-01-01T09:07:00.000Z,d1,5.0,3,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY value RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testAggregation() {
    String[] expectedHeader = new String[] {"time", "device", "value", "sum"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1.0,",
          "2021-01-01T09:05:00.000Z,d1,3.0,7.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,7.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,12.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,6.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, sum(value) OVER (PARTITION BY device ORDER BY value) AS sum FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFirstValue() {
    String[] expectedHeader = new String[] {"time", "device", "value", "fv"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1.0,",
          "2021-01-01T09:05:00.000Z,d1,3.0,1.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,3.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, first_value(value) OVER (PARTITION BY device ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS fv FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testLastValue() {
    String[] expectedHeader = new String[] {"time", "device", "value", "lv"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,3.0,",
          "2021-01-01T09:05:00.000Z,d1,3.0,3.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,5.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,5.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,4.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,4.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, last_value(value) OVER (PARTITION BY device ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS lv FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testNthValue() {
    String[] expectedHeader = new String[] {"time", "device", "value", "nv"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,3.0,",
          "2021-01-01T09:05:00.000Z,d1,3.0,3.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,5.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,4.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,4.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, nth_value(value, 2) OVER (PARTITION BY device ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS nv FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testLead() {
    String[] expectedHeader = new String[] {"time", "device", "value", "ld"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,5.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,3.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,1.0,",
          "2021-01-01T09:10:00.000Z,d1,1.0,null,",
          "2021-01-01T09:08:00.000Z,d2,2.0,4.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,null,",
        };
    tableResultSetEqualTest(
        "SELECT *, lead(value) OVER (PARTITION BY device ORDER BY time) AS ld FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT *, lead(value) OVER (PARTITION BY device ORDER BY time ROWS 1 PRECEDING) AS ld FROM demo",
        "Cannot specify window frame for lead function",
        DATABASE_NAME);
  }

  @Test
  public void testLag() {
    String[] expectedHeader = new String[] {"time", "device", "value", "lg"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,null,",
          "2021-01-01T09:07:00.000Z,d1,5.0,3.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,5.0,",
          "2021-01-01T09:10:00.000Z,d1,1.0,3.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,null,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, lag(value) OVER (PARTITION BY device ORDER BY time) AS lg FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT *, lag(value) OVER (PARTITION BY device ORDER BY time ROWS 1 PRECEDING) AS lg FROM demo",
        "Cannot specify window frame for lag function",
        DATABASE_NAME);
  }

  @Test
  public void testRank() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rk"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,2,",
          "2021-01-01T09:07:00.000Z,d1,5.0,4,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, rank() OVER (PARTITION BY device ORDER BY value) AS rk FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDenseRank() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rk"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,2,",
          "2021-01-01T09:07:00.000Z,d1,5.0,3,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, dense_rank() OVER (PARTITION BY device ORDER BY value) AS rk FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testRowNumber() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,",
          "2021-01-01T09:07:00.000Z,d1,5.0,4,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, row_number() OVER (PARTITION BY device ORDER BY value) AS rn FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPercentRank() {
    String[] expectedHeader = new String[] {"time", "device", "value", "pr"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,0.0,",
          "2021-01-01T09:05:00.000Z,d1,3.0,0.3333333333333333,",
          "2021-01-01T09:09:00.000Z,d1,3.0,0.3333333333333333,",
          "2021-01-01T09:07:00.000Z,d1,5.0,1.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,0.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,1.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, percent_rank() OVER (PARTITION BY device ORDER BY value) AS pr FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testCumeDist() {
    String[] expectedHeader = new String[] {"time", "device", "value", "cd"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,0.25,",
          "2021-01-01T09:05:00.000Z,d1,3.0,0.75,",
          "2021-01-01T09:09:00.000Z,d1,3.0,0.75,",
          "2021-01-01T09:07:00.000Z,d1,5.0,1.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,0.5,",
          "2021-01-01T09:15:00.000Z,d2,4.0,1.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, cume_dist() OVER (PARTITION BY device ORDER BY value) AS cd FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testNTile() {
    String[] expectedHeader = new String[] {"time", "device", "value", "nt"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:09:00.000Z,d1,3.0,2,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, ntile(2) OVER (PARTITION BY device ORDER BY value) AS nt FROM demo ORDER BY device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testNegativeRowsFrameOffset() {
    tableAssertTestFail(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY time ROWS -1 PRECEDING) AS cnt FROM demo",
        "Window frame offset value must not be negative or null",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY time ROWS BETWEEN -2 PRECEDING AND -1 FOLLOWING) AS cnt FROM demo",
        "Window frame offset value must not be negative or null",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY time ROWS BETWEEN 1 PRECEDING AND -1 FOLLOWING) AS cnt FROM demo",
        "Window frame offset value must not be negative or null",
        DATABASE_NAME);
  }

  @Test
  public void testNegativeGroupsFrameOffset() {
    tableAssertTestFail(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY time GROUPS -1 PRECEDING) AS cnt FROM demo",
        "Window frame offset value must not be negative or null",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY time GROUPS BETWEEN -2 PRECEDING AND -1 FOLLOWING) AS cnt FROM demo",
        "Window frame offset value must not be negative or null",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT *, count(value) OVER (PARTITION BY device ORDER BY time GROUPS BETWEEN 1 PRECEDING AND -1 FOLLOWING) AS cnt FROM demo",
        "Window frame offset value must not be negative or null",
        DATABASE_NAME);
  }

  @Test
  public void testMultiPartitions() {
    String[] expectedHeader = new String[] {"time", "device", "flow", "cnt"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,d0,3,4,",
          "1970-01-01T00:00:00.001Z,d0,5,4,",
          "1970-01-01T00:00:00.002Z,d0,3,4,",
          "1970-01-01T00:00:00.003Z,d0,1,4,",
          "1970-01-01T00:00:00.004Z,d0,null,4,",
          "1970-01-01T00:00:00.005Z,d1,2,2,",
          "1970-01-01T00:00:00.006Z,d1,null,2,",
          "1970-01-01T00:00:00.007Z,d1,4,2,",
          "1970-01-01T00:00:00.008Z,null,null,0,",
        };
    tableResultSetEqualTest(
        "SELECT time, device, flow, count(flow) OVER(PARTITION BY device ORDER BY flow ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS cnt FROM demo3 ORDER BY time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testComplexQuery() {
    String[] expectedHeader = new String[] {"flow", "_col1"};
    String[] retArray =
        new String[] {
          "1,null,",
          "3,1970-01-01T00:00:00.003Z,",
          "5,1970-01-01T00:00:00.002Z,",
          "null,1970-01-01T00:00:00.001Z,",
        };
    tableResultSetEqualTest(
        " SELECT flow, \n"
            + "           lag(last(time)) OVER (order by flow)\n"
            + "    FROM VARIATION(\n"
            + "        DATA => (SELECT time, flow FROM demo3 WHERE device = 'd0'),\n"
            + "        COL => 'flow',\n"
            + "        DELTA => 0.0)\n"
            + "    GROUP BY flow",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
