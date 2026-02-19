/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Integration tests for the LimitKRanking optimization. When using {@code ROW_NUMBER() OVER
 * (PARTITION BY device ORDER BY time)}, the planner should produce a streaming LimitKRankingNode
 * instead of the buffered TopKRankingNode, since data is already sorted by (device, time).
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLimitKRankingIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table demo (device string tag, value double field)",
        // d1: 4 rows
        "insert into demo values (2021-01-01T09:05:00, 'd1', 3)",
        "insert into demo values (2021-01-01T09:07:00, 'd1', 5)",
        "insert into demo values (2021-01-01T09:09:00, 'd1', 3)",
        "insert into demo values (2021-01-01T09:10:00, 'd1', 1)",
        // d2: 2 rows
        "insert into demo values (2021-01-01T09:08:00, 'd2', 2)",
        "insert into demo values (2021-01-01T09:15:00, 'd2', 4)",
        // d3: 5 rows
        "insert into demo values (2021-01-01T09:01:00, 'd3', 10)",
        "insert into demo values (2021-01-01T09:02:00, 'd3', 20)",
        "insert into demo values (2021-01-01T09:03:00, 'd3', 30)",
        "insert into demo values (2021-01-01T09:04:00, 'd3', 40)",
        "insert into demo values (2021-01-01T09:06:00, 'd3', 50)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  protected static void insertData() {
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
  public static void setUp() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(1024 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testFilterPushDownOrderByTime() {
    // ROW_NUMBER() OVER (PARTITION BY device ORDER BY time) WHERE rn <= 2
    // Should use LimitKRankingOperator: take first 2 time-ordered rows per device
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
          "2021-01-01T09:01:00.000Z,d3,10.0,1,",
          "2021-01-01T09:02:00.000Z,d3,20.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM ("
            + "SELECT *, row_number() OVER (PARTITION BY device ORDER BY time) as rn FROM demo"
            + ") WHERE rn <= 2 ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFilterPushDownOrderByTimeK1() {
    // K=1: only the earliest row per device
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:01:00.000Z,d3,10.0,1,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM ("
            + "SELECT *, row_number() OVER (PARTITION BY device ORDER BY time) as rn FROM demo"
            + ") WHERE rn <= 1 ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFilterPushDownOrderByTimeKLargerThanData() {
    // K=100: larger than any partition, so all rows are returned
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,",
          "2021-01-01T09:10:00.000Z,d1,1.0,4,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
          "2021-01-01T09:01:00.000Z,d3,10.0,1,",
          "2021-01-01T09:02:00.000Z,d3,20.0,2,",
          "2021-01-01T09:03:00.000Z,d3,30.0,3,",
          "2021-01-01T09:04:00.000Z,d3,40.0,4,",
          "2021-01-01T09:06:00.000Z,d3,50.0,5,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM ("
            + "SELECT *, row_number() OVER (PARTITION BY device ORDER BY time) as rn FROM demo"
            + ") WHERE rn <= 100 ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testLimitPushDownOrderByTime() {
    // LIMIT pushdown: row_number() OVER (PARTITION BY device ORDER BY time) LIMIT 4
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,",
          "2021-01-01T09:10:00.000Z,d1,1.0,4,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM ("
            + "SELECT *, row_number() OVER (PARTITION BY device ORDER BY time) as rn FROM demo"
            + ") ORDER BY device, time LIMIT 4",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFilterPushDownOrderByTimeWithLessThan() {
    // Use rn < 3 instead of rn <= 2 (should give same result as rn <= 2)
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
          "2021-01-01T09:01:00.000Z,d3,10.0,1,",
          "2021-01-01T09:02:00.000Z,d3,20.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM ("
            + "SELECT *, row_number() OVER (PARTITION BY device ORDER BY time) as rn FROM demo"
            + ") WHERE rn < 3 ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFilterPushDownOrderByTimeSelectSubsetColumns() {
    // Only select time and device (not the ranking column)
    String[] expectedHeader = new String[] {"time", "device", "value"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,",
          "2021-01-01T09:01:00.000Z,d3,10.0,",
          "2021-01-01T09:02:00.000Z,d3,20.0,",
          "2021-01-01T09:03:00.000Z,d3,30.0,",
        };
    tableResultSetEqualTest(
        "SELECT time, device, value FROM ("
            + "SELECT *, row_number() OVER (PARTITION BY device ORDER BY time) as rn FROM demo"
            + ") WHERE rn <= 3 ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFilterPushDownOrderByTimeEqual() {
    // rn = 2: only the second row per device
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
          "2021-01-01T09:02:00.000Z,d3,20.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM ("
            + "SELECT *, row_number() OVER (PARTITION BY device ORDER BY time) as rn FROM demo"
            + ") WHERE rn = 2 ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
