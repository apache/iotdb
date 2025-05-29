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

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBWindowFunctionIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
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

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
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
  public void testOrderBy() {
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
}
