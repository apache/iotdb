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
public class IoTDBPatternAggregationIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        // TABLE: beidou
        "CREATE TABLE beidou(device_id STRING TAG, department STRING FIELD, altitude DOUBLE FIELD)",
        // d1 and DEP1s
        "INSERT INTO beidou VALUES (2025-01-01T00:00:00, 'd1', 'DEP1', 480.5)",
        "INSERT INTO beidou VALUES (2025-01-01T00:01:00, 'd1', 'DEP1', 510.2)",
        "INSERT INTO beidou VALUES (2025-01-01T00:02:00, 'd1', 'DEP1', 508.7)",
        "INSERT INTO beidou VALUES (2025-01-01T00:04:00, 'd1', 'DEP1', 495.0)",
        "INSERT INTO beidou VALUES (2025-01-01T00:05:00, 'd1', 'DEP1', 523.0)",
        "INSERT INTO beidou VALUES (2025-01-01T00:06:00, 'd1', 'DEP1', 517.4)",
        // d2 and DEP1
        "INSERT INTO beidou VALUES (2025-01-01T00:07:00, 'd2', 'DEP1', 530.1)",
        "INSERT INTO beidou VALUES (2025-01-01T00:08:00, 'd2', 'DEP1', 540.4)",
        "INSERT INTO beidou VALUES (2025-01-01T00:09:00, 'd2', 'DEP1', 498.2)",
        // DEP2
        "INSERT INTO beidou VALUES (2025-01-01T00:10:00, 'd3', 'DEP2', 470.0)",
        "INSERT INTO beidou VALUES (2025-01-01T00:11:00, 'd3', 'DEP2', 505.0)",
        "INSERT INTO beidou VALUES (2025-01-01T00:12:00, 'd3', 'DEP2', 480.0)",
        // altitude lower than 500
        "INSERT INTO beidou VALUES (2025-01-01T00:13:00, 'd4', 'DEP_1', 450)",
        "INSERT INTO beidou VALUES (2025-01-01T00:14:00, 'd4', 'DEP_1', 470)",
        "INSERT INTO beidou VALUES (2025-01-01T00:15:00, 'd4', 'DEP_1', 490)",
        // outside the time range
        "INSERT INTO beidou VALUES (2024-01-01T00:30:00, 'd1', 'DEP_1', 600)",
        "INSERT INTO beidou VALUES (2025-01-01T02:00:00, 'd1', 'DEP_1', 570)",

        // TABLE: t1
        "CREATE TABLE t1(totalprice DOUBLE FIELD)",
        "INSERT INTO t1 VALUES (2025-01-01T00:01:00, 10)",
        "INSERT INTO t1 VALUES (2025-01-01T00:02:00, 20)",
        "INSERT INTO t1 VALUES (2025-01-01T00:03:00, 30)",
        "INSERT INTO t1 VALUES (2025-01-01T00:04:00, 40)",
        "INSERT INTO t1 VALUES (2025-01-01T00:05:00, 10)",
        "INSERT INTO t1 VALUES (2025-01-01T00:06:00, 20)",
        "INSERT INTO t1 VALUES (2025-01-01T00:07:00, 30)",

        // TABLE: t2
        "CREATE TABLE t2(totalprice DOUBLE FIELD)",
        "INSERT INTO t2 VALUES (2025-01-01T00:01:00, 4)",
        "INSERT INTO t2 VALUES (2025-01-01T00:02:00, 6)",
        "INSERT INTO t2 VALUES (2025-01-01T00:03:00, 5)",
        "INSERT INTO t2 VALUES (2025-01-01T00:04:00, 13)",

        // TABLE: t3
        "CREATE TABLE t3(totalprice DOUBLE FIELD)",
        "INSERT INTO t3 VALUES (2025-01-01T00:01:00, 4)",
        "INSERT INTO t3 VALUES (2025-01-01T00:02:00, 6)",
        "INSERT INTO t3 VALUES (2025-01-01T00:03:00, 7)",
        "INSERT INTO t3 VALUES (2025-01-01T00:04:00, 7)",
        "INSERT INTO t3 VALUES (2025-01-01T00:05:00, -8)",

        // TABLE: orders
        "create table orders (customer_id TAG, region ATTRIBUTE, order_date DATE, product TEXT, status BOOLEAN, number INT64, totalprice DOUBLE, quantity INT32, discount FLOAT, receipt BLOB)",
        "insert into orders values(1748736000000, '100', 'beijing', '2025-06-01', 'table', true, 100, 55000.5, 5, 0.95, X'526563656970743130305F3230323530363031')",
        "insert into orders values(1748736600000, '100', 'beijing', '2025-06-01', 'table', true, 255, 13200.3, 10, 0.90, X'526563656970743130305F3230323530363031')",
        "insert into orders values(1748737200000, '100', 'beijing', '2025-06-01', 'table', true, 888, 12400, 20, 0.85, X'526563656970743130305F3230323530363031')",
        "insert into orders values(1748737800000, '100', 'beijing', '2025-06-01', 'table', true, 55, 9998.3, 2, 1.00, X'526563656970743130305F3230323530363031')",
        "insert into orders values(1748739600000, '100', 'beijing', '2025-06-01', 'table', true, 666, 9998.3, 15, 0.92, X'526563656970743130305F3230323530363031')",
        "insert into orders values(1748822400000, '101', 'shanghai', '2025-06-02', 'door', false, 608, 12350.5, 8, 0.88, X'526563656970743130315F3230323530363032')",
        "insert into orders values(1748826000000, '101', 'shanghai', '2025-06-02', 'door', true, 1000, 667849.9, 12, 0.80, X'526563656970743130315F3230323530363032')",
        "insert into orders values(1748831400000, '101', 'shanghai', '2025-06-02', 'door', true, 360, 33920.5, 6, 0.85, X'526563656970743130315F3230323530363032')",
        "insert into orders values(1748835000000, '101', 'shanghai', '2025-06-02', 'door', true, 150, 33920.5, 3, 1.00, X'526563656970743130315F3230323530363032')",
        "insert into orders values(1748923200000, '100', 'beijing', '2025-06-03', 'table', true, 150, 11230.4, 4, 0.97, X'526563656970743130305F3230323530363033')",
        "insert into orders values(1748923300000, '101', 'beijing', '2025-06-04', 'table', true, 50, 55000.00, 2, 0.90, X'526563656970743130315F3230323530363034')",
        "insert into orders values(1748924300000, '102', 'beijing', '2025-06-05', 'table', true, 50, 65000.00, 1, 0.85, X'526563656970743130325F3230323530363035')",
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
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Search range: all devices whose department is 'DEP_1', each device's data is grouped
   * separately, and the time range is between 2025-01-01T00:00:00 and 2025-01-01T01:00:00.
   *
   * <p>Event analysis: Whenever the altitude exceeds 500 and then drops below 500, it is marked as
   * an event.
   */
  @Test
  public void testEventRecognition() {
    String[] expectedHeader =
        new String[] {
          "device_id", "match", "event_start", "event_end", "max_altitude", "sum_altitude", "count"
        };
    String[] retArray =
        new String[] {
          "d1,1,2025-01-01T00:01:00.000Z,2025-01-01T00:02:00.000Z,510.2,1018.9,2,",
          "d1,2,2025-01-01T00:05:00.000Z,2025-01-01T00:06:00.000Z,523.0,1040.4,2,",
          "d2,1,2025-01-01T00:07:00.000Z,2025-01-01T00:08:00.000Z,540.4,1070.5,2,",
        };
    tableResultSetEqualTest(
        "SELECT * "
            + "FROM ( "
            + "    SELECT time, device_id, altitude "
            + "    FROM beidou "
            + "    WHERE department = 'DEP1' AND time >= 2025-01-01T00:00:00 AND time < 2025-01-01T01:00:00 "
            + ")"
            + "MATCH_RECOGNIZE ( "
            + "    PARTITION BY device_id "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        RPR_FIRST(A.time) AS event_start, "
            + "        RPR_LAST(A.time) AS event_end, "
            + "        MAX(A.altitude) AS max_altitude, "
            + "        SUM(A.altitude) AS sum_altitude, "
            + "        COUNT(A.altitude) AS count "
            + "    ONE ROW PER MATCH "
            + "    PATTERN (A+) "
            + "    DEFINE "
            + "        A AS A.altitude > 500 "
            + ") AS m "
            + "ORDER BY device_id, match ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void test1() {
    String[] expectedHeader =
        new String[] {
          "time",
          "match",
          "count1",
          "count2",
          "max",
          "min",
          "sum1",
          "sum2",
          "avg1",
          "avg2",
          "totalprice"
        };
    String[] retArray =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,1,4,10.0,10.0,10.0,100.0,10.0,25.0,10.0,",
          "2025-01-01T00:02:00.000Z,1,2,4,20.0,10.0,30.0,100.0,15.0,25.0,20.0,",
          "2025-01-01T00:03:00.000Z,1,3,4,30.0,10.0,60.0,100.0,20.0,25.0,30.0,",
          "2025-01-01T00:04:00.000Z,1,4,4,40.0,10.0,100.0,100.0,25.0,25.0,40.0,",
          "2025-01-01T00:05:00.000Z,2,1,3,10.0,10.0,10.0,60.0,10.0,20.0,10.0,",
          "2025-01-01T00:06:00.000Z,2,2,3,20.0,10.0,30.0,60.0,15.0,20.0,20.0,",
          "2025-01-01T00:07:00.000Z,2,3,3,30.0,10.0,60.0,60.0,20.0,20.0,30.0,",
        };
    tableResultSetEqualTest(
        "SELECT m.time, m.match, m.count1, m.count2, m.max, m.min, m.sum1, m.sum2, m.avg1, m.avg2, m.totalprice "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        COUNT(totalprice) AS count1, "
            + "        FINAL COUNT(totalprice) AS count2, "
            + "        MAX(totalprice) AS max, "
            + "        MIN(totalprice) AS min, "
            + "        SUM(totalprice) AS sum1, "
            + "        FINAL SUM(totalprice) AS sum2, "
            + "        AVG(totalprice) AS avg1, "
            + "        FINAL AVG(totalprice) AS avg2 "
            + "    ALL ROWS PER MATCH "
            + "    PATTERN (A B C D?) "
            + "    DEFINE "
            + "        A AS A.totalprice = 10, "
            + "        B AS B.totalprice = 20, "
            + "        C AS C.totalprice = 30, "
            + "        D AS D.totalprice = 40 "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void test2() {
    String[] expectedHeader =
        new String[] {
          "time",
          "match",
          "label",
          "count_0",
          "count_1",
          "count_2",
          "count_c",
          "final_count_c",
          "count_u",
          "final_count_u",
          "totalprice"
        };
    String[] retArray =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,A,1,1,1,0,1,0,2,10.0,",
          "2025-01-01T00:02:00.000Z,1,B,2,2,2,0,1,1,2,20.0,",
          "2025-01-01T00:03:00.000Z,1,C,3,3,3,1,1,1,2,30.0,",
          "2025-01-01T00:04:00.000Z,1,D,4,4,4,1,1,2,2,40.0,",
          "2025-01-01T00:05:00.000Z,2,A,1,1,1,0,1,0,1,10.0,",
          "2025-01-01T00:06:00.000Z,2,B,2,2,2,0,1,1,1,20.0,",
          "2025-01-01T00:07:00.000Z,2,C,3,3,3,1,1,1,1,30.0,",
        };
    tableResultSetEqualTest(
        "SELECT m.time, m.match, m.label, m.count_0, m.count_1, m.count_2, m.count_c, m.final_count_c, m.count_u, m.final_count_u, m.totalprice "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        CLASSIFIER() AS label, "
            + "        COUNT() AS count_0, "
            + "        COUNT(*) AS count_1, "
            + "        COUNT(totalprice) AS count_2, "
            + "        COUNT(C.totalprice) AS count_c, "
            + "        FINAL COUNT(C.totalprice) AS final_count_c, "
            + "        COUNT(U.totalprice) AS count_u, "
            + "        FINAL COUNT(U.totalprice) AS final_count_u "
            + "    ALL ROWS PER MATCH "
            + "    PATTERN (A B C D?) "
            + "    SUBSET U = (B, D) "
            + "    DEFINE "
            + "        A AS A.totalprice = 10, "
            + "        B AS B.totalprice = 20, "
            + "        C AS C.totalprice = 30, "
            + "        D AS D.totalprice = 40 "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void test3() {
    String[] expectedHeader =
        new String[] {
          "time",
          "match",
          "label",
          "sum",
          "sum_c",
          "final_sum_c",
          "sum_u",
          "final_sum_u",
          "totalprice"
        };
    String[] retArray =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,A,10.0,0.0,30.0,0.0,60.0,10.0,",
          "2025-01-01T00:02:00.000Z,1,B,30.0,0.0,30.0,20.0,60.0,20.0,",
          "2025-01-01T00:03:00.000Z,1,C,60.0,30.0,30.0,20.0,60.0,30.0,",
          "2025-01-01T00:04:00.000Z,1,D,100.0,30.0,30.0,60.0,60.0,40.0,",
          "2025-01-01T00:05:00.000Z,2,A,10.0,0.0,30.0,0.0,20.0,10.0,",
          "2025-01-01T00:06:00.000Z,2,B,30.0,0.0,30.0,20.0,20.0,20.0,",
          "2025-01-01T00:07:00.000Z,2,C,60.0,30.0,30.0,20.0,20.0,30.0,",
        };
    tableResultSetEqualTest(
        "SELECT m.time, m.match, m.label, m.sum, m.sum_c, m.final_sum_c, m.sum_u, m.final_sum_u, m.totalprice "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        CLASSIFIER() AS label, "
            + "        SUM(totalprice) AS sum, "
            + "        SUM(C.totalprice) AS sum_c, "
            + "        FINAL SUM(C.totalprice) AS final_sum_c, "
            + "        SUM(U.totalprice) AS sum_u, "
            + "        FINAL SUM(U.totalprice) AS final_sum_u "
            + "    ALL ROWS PER MATCH "
            + "    PATTERN (A B C D?) "
            + "    SUBSET U = (B, D) "
            + "    DEFINE "
            + "        A AS A.totalprice = 10, "
            + "        B AS B.totalprice = 20, "
            + "        C AS C.totalprice = 30, "
            + "        D AS D.totalprice = 40 "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void test4() {
    String[] expectedHeader =
        new String[] {
          "time",
          "match",
          "label",
          "avg",
          "avg_c",
          "final_avg_c",
          "avg_u",
          "final_avg_u",
          "totalprice"
        };
    String[] retArray =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,A,10.0,0.0,30.0,0.0,30.0,10.0,",
          "2025-01-01T00:02:00.000Z,1,B,15.0,0.0,30.0,20.0,30.0,20.0,",
          "2025-01-01T00:03:00.000Z,1,C,20.0,30.0,30.0,20.0,30.0,30.0,",
          "2025-01-01T00:04:00.000Z,1,D,25.0,30.0,30.0,30.0,30.0,40.0,",
          "2025-01-01T00:05:00.000Z,2,A,10.0,0.0,30.0,0.0,20.0,10.0,",
          "2025-01-01T00:06:00.000Z,2,B,15.0,0.0,30.0,20.0,20.0,20.0,",
          "2025-01-01T00:07:00.000Z,2,C,20.0,30.0,30.0,20.0,20.0,30.0,",
        };
    tableResultSetEqualTest(
        "SELECT m.time, m.match, m.label, m.avg, m.avg_c, m.final_avg_c, m.avg_u, m.final_avg_u, m.totalprice "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        CLASSIFIER() AS label, "
            + "        AVG(totalprice) AS avg, "
            + "        AVG(C.totalprice) AS avg_c, "
            + "        FINAL AVG(C.totalprice) AS final_avg_c, "
            + "        AVG(U.totalprice) AS avg_u, "
            + "        FINAL AVG(U.totalprice) AS final_avg_u "
            + "    ALL ROWS PER MATCH "
            + "    PATTERN (A B C D?) "
            + "    SUBSET U = (B, D) "
            + "    DEFINE "
            + "        A AS A.totalprice = 10, "
            + "        B AS B.totalprice = 20, "
            + "        C AS C.totalprice = 30, "
            + "        D AS D.totalprice = 40 "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void test5() {
    String[] expectedHeader =
        new String[] {
          "time", "match", "firstTime", "lastTime", "maxTime", "minTime", "firstVal", "lastVal"
        };
    String[] retArray =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,2025-01-01T00:01:00.000Z,2025-01-01T00:01:00.000Z,2025-01-01T00:01:00.000Z,2025-01-01T00:01:00.000Z,10.0,10.0,",
          "2025-01-01T00:02:00.000Z,1,2025-01-01T00:01:00.000Z,2025-01-01T00:02:00.000Z,2025-01-01T00:02:00.000Z,2025-01-01T00:01:00.000Z,10.0,20.0,",
          "2025-01-01T00:03:00.000Z,1,2025-01-01T00:01:00.000Z,2025-01-01T00:03:00.000Z,2025-01-01T00:03:00.000Z,2025-01-01T00:01:00.000Z,10.0,30.0,",
          "2025-01-01T00:04:00.000Z,1,2025-01-01T00:01:00.000Z,2025-01-01T00:04:00.000Z,2025-01-01T00:04:00.000Z,2025-01-01T00:01:00.000Z,10.0,40.0,",
          "2025-01-01T00:05:00.000Z,2,2025-01-01T00:05:00.000Z,2025-01-01T00:05:00.000Z,2025-01-01T00:05:00.000Z,2025-01-01T00:05:00.000Z,10.0,10.0,",
          "2025-01-01T00:06:00.000Z,2,2025-01-01T00:05:00.000Z,2025-01-01T00:06:00.000Z,2025-01-01T00:06:00.000Z,2025-01-01T00:05:00.000Z,10.0,20.0,",
          "2025-01-01T00:07:00.000Z,2,2025-01-01T00:05:00.000Z,2025-01-01T00:07:00.000Z,2025-01-01T00:07:00.000Z,2025-01-01T00:05:00.000Z,10.0,30.0,"
        };
    tableResultSetEqualTest(
        "SELECT m.time, m.match, m.firstTime, m.lastTime, m.maxTime, m.minTime, m.firstVal, m.lastVal "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "      MATCH_NUMBER() AS match, "
            + "      FIRST_BY(time, totalprice) AS firstTime, "
            + "      LAST_BY(time, totalprice) AS lastTime, "
            + "      MAX_BY(time, totalprice) AS maxTime, "
            + "      MIN_BY(time, totalprice) AS minTime, "
            + "      FIRST(totalprice) AS firstVal, "
            + "      LAST(totalprice) AS lastVal "
            + "    ALL ROWS PER MATCH "
            + "    PATTERN (A B C D?) "
            + "    DEFINE "
            + "      A AS totalprice = 10, "
            + "      B AS totalprice = 20, "
            + "      C AS totalprice = 30, "
            + "      D AS totalprice = 40 "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void test6() {
    String[] expectedHeader =
        new String[] {
          "time", "match", "mode", "extreme", "avg", "var_0", "var_1", "var_2", "std_0", "std_1",
          "std_2"
        };
    String[] retArray =
        new String[] {
          // [4]
          "2025-01-01T00:01:00.000Z,1,4.0,4.0,4.0,0.0,0.0,0.0,0.0,0.0,0.0,",
          // [4, 6]
          "2025-01-01T00:02:00.000Z,1,4.0,6.0,5.0,2.0,2.0,1.0,1.414214,1.414214,1.0,",
          // [4, 6, 7]
          "2025-01-01T00:03:00.000Z,1,4.0,7.0,5.666667,2.333333,2.333333,1.555556,1.527525,1.527525,1.247219,",
          // [4, 6, 7, 7]
          "2025-01-01T00:04:00.000Z,1,7.0,7.0,6.0,2.0,2.0,1.5,1.414214,1.414214,1.224745,",
          // [4, 6, 7, 7, -8]
          "2025-01-01T00:05:00.000Z,1,7.0,-8.0,3.2,40.7,40.7,32.56,6.379655,6.379655,5.706137,"
        };

    tableResultSetEqualTest(
        "SELECT m.time, m.match, m.mode, m.extreme, "
            + "ROUND(m.avg, 6) AS avg, "
            + "ROUND(m.var_0, 6) AS var_0, ROUND(m.var_1, 6) AS var_1, ROUND(m.var_2, 6) AS var_2, "
            + "ROUND(m.std_0, 6) AS std_0, ROUND(m.std_1, 6) AS std_1, ROUND(m.std_2, 6) AS std_2 "
            + "FROM t3 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "      MATCH_NUMBER() AS match, "
            + "      MODE(totalprice) AS mode, "
            + "      EXTREME(totalprice) AS extreme, "
            + "      AVG(totalprice) AS avg, "
            + "      VARIANCE(totalprice) AS var_0, "
            + "      VAR_SAMP(totalprice) AS var_1, "
            + "      VAR_POP(totalprice) AS var_2, "
            + "      STDDEV(totalprice) AS std_0, "
            + "      STDDEV_SAMP(totalprice) AS std_1, "
            + "      STDDEV_POP(totalprice) AS std_2 "
            + "    ALL ROWS PER MATCH "
            + "    PATTERN (A+) "
            + "    DEFINE "
            + "      A AS true "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testAggregationsInDefineClause() {
    String[] expectedHeader =
        new String[] {"time", "match", "label", "avg", "running_avg_b", "totalprice"};
    String[] retArray =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,B,4.0,4.0,4.0,",
          "2025-01-01T00:02:00.000Z,1,A,5.0,4.0,6.0,",
          "2025-01-01T00:03:00.000Z,1,A,5.0,4.0,5.0,",
          "2025-01-01T00:04:00.000Z,1,B,7.0,8.5,13.0,",
        };
    tableResultSetEqualTest(
        "SELECT m.time, m.match, m.label, m.avg, m.running_avg_b, m.totalprice "
            + "FROM t2 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        CLASSIFIER() AS label, "
            + "        RUNNING AVG(totalprice) AS avg, "
            + "        RUNNING AVG(B.totalprice) AS running_avg_b "
            + "    ALL ROWS PER MATCH "
            + "    PATTERN ((A | B)*) "
            + "    DEFINE "
            + "        A AS AVG(totalprice) = 5 "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDifferentTypes() {
    String[] expectedHeader = new String[] {"match", "count_total"};
    String[] retArray = new String[] {"1,12,"};
    tableResultSetEqualTest(
        "SELECT m.match, m.count_total FROM orders "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        COUNT(totalprice) AS count_total, "
            + "        L.time AS time "
            + "    ONE ROW PER MATCH "
            + "    PATTERN (A B C D E F G H I J K L) "
            + "    DEFINE "
            + "        A AS A.customer_id = '100', "
            + "        B AS B.region = 'beijing', "
            + "        C AS C.order_date = CAST('2025-06-01' AS DATE), "
            + "        D AS D.product = 'table', "
            + "        E AS E.status = true, "
            + "        F AS F.number = 608, "
            + "        G AS G.totalprice = 667849.9, "
            + "        H AS H.quantity = 6, "
            + "        I AS I.discount = 1.00, "
            + "        J AS J.receipt = X'526563656970743130305F3230323530363033', "
            + "        K AS K.time = 1748923300000, "
            + "        L AS L.time = CAST('2025-06-03 04:18:20' AS TIMESTAMP) "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDifferentCast() {
    String[] expectedHeader = new String[] {"match", "cnt"};
    String[] retArray = new String[] {"1,1,"};
    tableResultSetEqualTest(
        "SELECT m.match, m.cnt FROM orders "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        COUNT(*) AS cnt "
            + "    ONE ROW PER MATCH "
            + "    PATTERN (A) "
            + "    DEFINE "
            + "        A AS "
            + "            A.order_date = CAST('2025-06-01' AS DATE) "
            + "        AND A.time = CAST('2025-06-01 00:00:00' AS TIMESTAMP) "
            + "        AND A.number = CAST('100' AS INT64) "
            + "        AND A.quantity = CAST('5' AS INT32) "
            + "        AND A.quantity = CAST('5' AS INT64) "
            + "        AND A.discount = 0.95 "
            + "        AND A.discount = CAST('0.95' AS FLOAT) "
            + "        AND A.discount = CAST('0.95' AS DOUBLE) "
            + "        AND A.totalprice = CAST('55000.5' AS DOUBLE) "
            + "        AND A.status = CAST('true' AS BOOLEAN) "
            + "        AND A.receipt = CAST(X'526563656970743130305F3230323530363031' AS BLOB) "
            + "        AND CAST(A.quantity AS INT64) = CAST('5' AS INT64) "
            + "        AND CAST(A.quantity AS INT64) = CAST(5 AS INT64) "
            + ") AS m ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
