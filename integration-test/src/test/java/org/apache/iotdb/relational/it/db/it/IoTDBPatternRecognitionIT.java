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

import static java.lang.String.format;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBPatternRecognitionIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        // TABLE: beidou
        "CREATE TABLE beidou(device_id STRING TAG, department STRING FIELD, altitude DOUBLE FIELD)",
        // d1 and DEP1
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
        "INSERT INTO t1 VALUES (2025-01-01T00:01:00, 90)",
        "INSERT INTO t1 VALUES (2025-01-01T00:02:00, 80)",
        "INSERT INTO t1 VALUES (2025-01-01T00:03:00, 70)",
        "INSERT INTO t1 VALUES (2025-01-01T00:04:00, 70)",

        // TABLE: t2
        "CREATE TABLE t2(totalprice DOUBLE FIELD)",
        "INSERT INTO t2 VALUES (2025-01-01T00:01:00, 10)",
        "INSERT INTO t2 VALUES (2025-01-01T00:02:00, 20)",
        "INSERT INTO t2 VALUES (2025-01-01T00:03:00, 30)",

        // TABLE: t3
        "CREATE TABLE t3(totalprice DOUBLE FIELD)",
        "INSERT INTO t3 VALUES (2025-01-01T00:01:00, 10)",
        "INSERT INTO t3 VALUES (2025-01-01T00:02:00, 20)",
        "INSERT INTO t3 VALUES (2025-01-01T00:03:00, 30)",
        "INSERT INTO t3 VALUES (2025-01-01T00:04:00, 30)",
        "INSERT INTO t3 VALUES (2025-01-01T00:05:00, 40)",

        // TABLE: t4
        "CREATE TABLE t4(totalprice DOUBLE FIELD)",
        "INSERT INTO t4 VALUES (2025-01-01T00:01:00, 90)",
        "INSERT INTO t4 VALUES (2025-01-01T00:02:00, 80)",
        "INSERT INTO t4 VALUES (2025-01-01T00:03:00, 70)",
        "INSERT INTO t4 VALUES (2025-01-01T00:04:00, 80)",

        // TABLE: t5
        "CREATE TABLE t5(part STRING TAG, num INT32 FIELD, totalprice DOUBLE FIELD)",
        "INSERT INTO t5 VALUES (2025-01-01T00:01:00, 'p1', 1, 10.0)",
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

  @Test
  public void testEventRecognition() {
    String[] expectedHeader =
        new String[] {"device_id", "match", "event_start", "event_end", "last_altitude"};
    String[] retArray =
        new String[] {
          "d1,1,2024-01-01T00:30:00.000Z,2024-01-01T00:30:00.000Z,600.0,",
          "d1,2,2025-01-01T00:01:00.000Z,2025-01-01T00:02:00.000Z,508.7,",
          "d1,3,2025-01-01T00:05:00.000Z,2025-01-01T02:00:00.000Z,570.0,",
          "d2,1,2025-01-01T00:07:00.000Z,2025-01-01T00:08:00.000Z,540.4,",
          "d3,1,2025-01-01T00:11:00.000Z,2025-01-01T00:11:00.000Z,505.0,",
        };
    tableResultSetEqualTest(
        "SELECT * "
            + "FROM beidou "
            + "MATCH_RECOGNIZE ( "
            + "    PARTITION BY device_id "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        RPR_FIRST(A.time) AS event_start, "
            + "        RPR_LAST(A.time) AS event_end, "
            + "        RPR_LAST(A.altitude) AS last_altitude "
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

  /**
   * Search range: all devices whose department is 'DEP_1', each device's data is grouped
   * separately, and the time range is between 2025-01-01T00:00:00 and 2025-01-01T01:00:00.
   *
   * <p>Event analysis: Whenever the altitude exceeds 500 and then drops below 500, it is marked as
   * an event.
   */
  @Test
  public void testEventRecognitionWithSubquery() {
    String[] expectedHeader =
        new String[] {"device_id", "match", "event_start", "event_end", "last_altitude"};
    String[] retArray =
        new String[] {
          "d1,1,2025-01-01T00:01:00.000Z,2025-01-01T00:02:00.000Z,508.7,",
          "d1,2,2025-01-01T00:05:00.000Z,2025-01-01T00:06:00.000Z,517.4,",
          "d2,1,2025-01-01T00:07:00.000Z,2025-01-01T00:08:00.000Z,540.4,",
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
            + "        RPR_LAST(A.altitude) AS last_altitude "
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
  public void testOutputLayout() {
    // ONE ROW PER MATCH: PK, Measures
    String[] expectedHeader1 = new String[] {"part", "match", "label"};
    String[] retArray1 =
        new String[] {
          "p1,1,A,",
        };

    // ALL ROWS PER MATCH: PK, OK, Measures, Others
    String[] expectedHeader2 = new String[] {"part", "num", "match", "label", "time", "totalprice"};
    String[] retArray2 =
        new String[] {
          "p1,1,1,A,2025-01-01T00:01:00.000Z,10.0,",
        };

    String sql =
        "SELECT * "
            + "FROM t5 "
            + "MATCH_RECOGNIZE ( "
            + "    PARTITION BY part "
            + "    ORDER BY num "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        CLASSIFIER() AS label "
            + "    %s "
            + "    PATTERN (A+) "
            + "    DEFINE "
            + "        A AS true "
            + ") AS m ";

    tableResultSetEqualTest(
        format(sql, "ONE ROW PER MATCH"), expectedHeader1, retArray1, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql, "ALL ROWS PER MATCH"), expectedHeader2, retArray2, DATABASE_NAME);
  }

  @Test
  public void testOutputMode() {
    String[] expectedHeader = new String[] {"match", "price", "label"};
    String[] retArray1 =
        new String[] {
          "1,null,null,", "2,70.0,B,", "3,null,null,",
        };
    String[] retArray2 =
        new String[] {
          "1,null,null,", "2,80.0,B,", "2,70.0,B,", "3,null,null,",
        };
    String[] retArray3 =
        new String[] {
          "1,80.0,B,", "1,70.0,B,",
        };
    String[] retArray4 =
        new String[] {
          "2,80.0,B,", "2,70.0,B,",
        };
    String[] retArray5 =
        new String[] {
          "null,null,null,", "1,80.0,B,", "1,70.0,B,", "null,null,null,",
        };

    String sql =
        "SELECT m.match, m.price, m.label "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        RUNNING RPR_LAST(totalprice) AS price,  "
            + "        CLASSIFIER() AS label "
            + "    %s "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    %s "
            + "    DEFINE "
            + "        B AS B.totalprice < PREV(B.totalprice) "
            + ") AS m";

    tableResultSetEqualTest(
        format(sql, "ONE ROW PER MATCH", "PATTERN (B*)"), expectedHeader, retArray1, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "ALL ROWS PER MATCH", "PATTERN (B*)"),
        expectedHeader,
        retArray2,
        DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "ALL ROWS PER MATCH", "PATTERN (B+)"),
        expectedHeader,
        retArray3,
        DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "ALL ROWS PER MATCH OMIT EMPTY MATCHES", "PATTERN (B*)"),
        expectedHeader,
        retArray4,
        DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "ALL ROWS PER MATCH WITH UNMATCHED ROWS", "PATTERN (B+)"),
        expectedHeader,
        retArray5,
        DATABASE_NAME);
  }

  @Test
  public void testLogicalNavigationFunction() {
    String[] expectedHeader = new String[] {"time", "price"};
    String[] retArray1 =
        new String[] {
          "2025-01-01T00:01:00.000Z,10.0,",
          "2025-01-01T00:02:00.000Z,20.0,",
          "2025-01-01T00:03:00.000Z,30.0,",
        };
    String[] retArray2 =
        new String[] {
          "2025-01-01T00:01:00.000Z,30.0,",
          "2025-01-01T00:02:00.000Z,30.0,",
          "2025-01-01T00:03:00.000Z,30.0,",
        };
    String[] retArray3 =
        new String[] {
          "2025-01-01T00:01:00.000Z,10.0,",
          "2025-01-01T00:02:00.000Z,10.0,",
          "2025-01-01T00:03:00.000Z,10.0,",
        };
    String[] retArray4 =
        new String[] {
          "2025-01-01T00:01:00.000Z,null,",
          "2025-01-01T00:02:00.000Z,null,",
          "2025-01-01T00:03:00.000Z,10.0,",
        };
    String[] retArray5 =
        new String[] {
          "2025-01-01T00:01:00.000Z,10.0,",
          "2025-01-01T00:02:00.000Z,10.0,",
          "2025-01-01T00:03:00.000Z,10.0,",
        };
    String[] retArray6 =
        new String[] {
          "2025-01-01T00:01:00.000Z,30.0,",
          "2025-01-01T00:02:00.000Z,30.0,",
          "2025-01-01T00:03:00.000Z,30.0,",
        };

    String sql =
        "SELECT m.time, m.price "
            + "FROM t2 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        %s AS price "
            + "    ALL ROWS PER MATCH "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    PATTERN (A+) "
            + "    DEFINE "
            + "        A AS true "
            + ") AS m";
    // LAST(totalprice)
    tableResultSetEqualTest(format(sql, "totalprice"), expectedHeader, retArray1, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql, "RPR_LAST(totalprice)"), expectedHeader, retArray1, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql, "RPR_LAST(totalprice, 0)"), expectedHeader, retArray1, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql, "RUNNING RPR_LAST(totalprice)"), expectedHeader, retArray1, DATABASE_NAME);

    // FINAL LAST(totalprice)
    tableResultSetEqualTest(
        format(sql, "FINAL RPR_LAST(totalprice)"), expectedHeader, retArray2, DATABASE_NAME);

    // FIRST(totalprice)
    tableResultSetEqualTest(
        format(sql, "RPR_FIRST(totalprice)"), expectedHeader, retArray3, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql, "RPR_FIRST(totalprice, 0)"), expectedHeader, retArray3, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql, "RUNNING RPR_FIRST(totalprice)"), expectedHeader, retArray3, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql, "FINAL RPR_FIRST(totalprice)"), expectedHeader, retArray3, DATABASE_NAME);

    // LAST(totalprice, 2)
    tableResultSetEqualTest(
        format(sql, "RPR_LAST(totalprice, 2)"), expectedHeader, retArray4, DATABASE_NAME);

    // FINAL LAST(totalprice, 2)
    tableResultSetEqualTest(
        format(sql, "FINAL RPR_LAST(totalprice, 2)"), expectedHeader, retArray5, DATABASE_NAME);

    // FIRST(totalprice, 2)
    tableResultSetEqualTest(
        format(sql, "RPR_FIRST(totalprice, 2)"), expectedHeader, retArray6, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql, "FINAL RPR_FIRST(totalprice, 2)"), expectedHeader, retArray6, DATABASE_NAME);
  }

  @Test
  public void testPhysicalNavigationFunction() {
    String[] expectedHeader1 = new String[] {"time", "price"};
    String[] retArray1 =
        new String[] {
          "2025-01-01T00:01:00.000Z,null,",
          "2025-01-01T00:02:00.000Z,10.0,",
          "2025-01-01T00:03:00.000Z,20.0,",
        };
    String[] retArray2 =
        new String[] {
          "2025-01-01T00:01:00.000Z,null,",
          "2025-01-01T00:02:00.000Z,null,",
          "2025-01-01T00:03:00.000Z,10.0,",
        };
    String[] retArray3 =
        new String[] {
          "2025-01-01T00:01:00.000Z,20.0,",
          "2025-01-01T00:02:00.000Z,30.0,",
          "2025-01-01T00:03:00.000Z,null,",
        };
    String[] retArray4 =
        new String[] {
          "2025-01-01T00:01:00.000Z,30.0,",
          "2025-01-01T00:02:00.000Z,null,",
          "2025-01-01T00:03:00.000Z,null,",
        };
    String[] retArray5 =
        new String[] {
          "30.0,",
        };
    String[] retArray6 =
        new String[] {
          "20.0,",
        };
    String[] retArray7 =
        new String[] {
          "40.0,",
        };
    String[] retArray8 =
        new String[] {
          "null,",
        };

    String sql1 =
        "SELECT m.time, m.price "
            + "FROM t2 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        %s AS price "
            + "    ALL ROWS PER MATCH "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    PATTERN (A+) "
            + "    DEFINE "
            + "        A AS true "
            + ") AS m";
    // PREV(totalprice)
    tableResultSetEqualTest(
        format(sql1, "PREV(totalprice)"), expectedHeader1, retArray1, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql1, "PREV(totalprice, 1)"), expectedHeader1, retArray1, DATABASE_NAME);

    // PREV(totalprice, 2)
    tableResultSetEqualTest(
        format(sql1, "PREV(totalprice, 2)"), expectedHeader1, retArray2, DATABASE_NAME);

    // NEXT(totalprice)
    tableResultSetEqualTest(
        format(sql1, "NEXT(totalprice)"), expectedHeader1, retArray3, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql1, "NEXT(totalprice, 1)"), expectedHeader1, retArray3, DATABASE_NAME);

    // NEXT(totalprice, 2)
    tableResultSetEqualTest(
        format(sql1, "NEXT(totalprice, 2)"), expectedHeader1, retArray4, DATABASE_NAME);

    String[] expectedHeader2 = new String[] {"price"};
    String sql2 =
        "SELECT m.price "
            + "FROM t3 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        %s AS price "
            + "    ONE ROW PER MATCH "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    PATTERN (A) "
            + "    DEFINE "
            + "        A AS A.totalprice = PREV(A.totalprice) "
            + ") AS m";
    // PREV(A.totalprice)
    tableResultSetEqualTest(
        format(sql2, "PREV(A.totalprice)"), expectedHeader2, retArray5, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql2, "PREV(A.totalprice, 1)"), expectedHeader2, retArray5, DATABASE_NAME);

    // PREV(A.totalprice, 2)
    tableResultSetEqualTest(
        format(sql2, "PREV(A.totalprice, 2)"), expectedHeader2, retArray6, DATABASE_NAME);

    // NEXT(A.totalprice)
    tableResultSetEqualTest(
        format(sql2, "NEXT(A.totalprice)"), expectedHeader2, retArray7, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql2, "NEXT(A.totalprice, 1)"), expectedHeader2, retArray7, DATABASE_NAME);

    // out of partition
    tableResultSetEqualTest(
        format(sql2, "PREV(A.totalprice, 4)"), expectedHeader2, retArray8, DATABASE_NAME);
    tableResultSetEqualTest(
        format(sql2, "NEXT(A.totalprice, 2)"), expectedHeader2, retArray8, DATABASE_NAME);
  }

  @Test
  public void testNestedNavigation() {
    String[] expectedHeader = new String[] {"time", "price"};
    String[] retArray1 =
        new String[] {
          "2025-01-01T00:01:00.000Z,null,",
          "2025-01-01T00:02:00.000Z,null,",
          "2025-01-01T00:03:00.000Z,null,",
        };
    String[] retArray2 =
        new String[] {
          "2025-01-01T00:01:00.000Z,20.0,",
          "2025-01-01T00:02:00.000Z,20.0,",
          "2025-01-01T00:03:00.000Z,20.0,",
        };
    String[] retArray3 =
        new String[] {
          "2025-01-01T00:01:00.000Z,10.0,",
          "2025-01-01T00:02:00.000Z,10.0,",
          "2025-01-01T00:03:00.000Z,10.0,",
        };
    String[] retArray4 =
        new String[] {
          "2025-01-01T00:01:00.000Z,null,",
          "2025-01-01T00:02:00.000Z,10.0,",
          "2025-01-01T00:03:00.000Z,20.0,",
        };
    String[] retArray5 =
        new String[] {
          "2025-01-01T00:01:00.000Z,null,",
          "2025-01-01T00:02:00.000Z,null,",
          "2025-01-01T00:03:00.000Z,10.0,",
        };
    String[] retArray6 =
        new String[] {
          "2025-01-01T00:01:00.000Z,30.0,",
          "2025-01-01T00:02:00.000Z,30.0,",
          "2025-01-01T00:03:00.000Z,30.0,",
        };
    String[] retArray7 =
        new String[] {
          "2025-01-01T00:01:00.000Z,20.0,",
          "2025-01-01T00:02:00.000Z,30.0,",
          "2025-01-01T00:03:00.000Z,null,",
        };
    String[] retArray8 =
        new String[] {
          "2025-01-01T00:01:00.000Z,30.0,",
          "2025-01-01T00:02:00.000Z,null,",
          "2025-01-01T00:03:00.000Z,null,",
        };
    String[] retArray9 =
        new String[] {
          "2025-01-01T00:01:00.000Z,null,",
          "2025-01-01T00:02:00.000Z,null,",
          "2025-01-01T00:03:00.000Z,20.0,",
        };
    String[] retArray10 =
        new String[] {
          "2025-01-01T00:01:00.000Z,null,",
          "2025-01-01T00:02:00.000Z,null,",
          "2025-01-01T00:03:00.000Z,30.0,",
        };

    String sql =
        "SELECT m.time, m.price "
            + "FROM t2 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        %s AS price "
            + "    ALL ROWS PER MATCH "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    PATTERN (A+) "
            + "    DEFINE "
            + "        A AS true "
            + ") AS m";
    // PREV(FIRST(totalprice))
    tableResultSetEqualTest(
        format(sql, "PREV(RPR_FIRST(totalprice))"), expectedHeader, retArray1, DATABASE_NAME);
    // PREV(FIRST(totalprice), 2)
    tableResultSetEqualTest(
        format(sql, "PREV(RPR_FIRST(totalprice), 2)"), expectedHeader, retArray1, DATABASE_NAME);

    // PREV(FIRST(totalprice, 2))
    tableResultSetEqualTest(
        format(sql, "PREV(RPR_FIRST(totalprice, 2))"), expectedHeader, retArray2, DATABASE_NAME);
    // PREV(FIRST(totalprice, 2), 2)
    tableResultSetEqualTest(
        format(sql, "PREV(RPR_FIRST(totalprice, 2), 2)"), expectedHeader, retArray3, DATABASE_NAME);

    // PREV(LAST(totalprice))
    tableResultSetEqualTest(
        format(sql, "PREV(RPR_LAST(totalprice))"), expectedHeader, retArray4, DATABASE_NAME);
    // PREV(LAST(totalprice), 2)
    tableResultSetEqualTest(
        format(sql, "PREV(RPR_LAST(totalprice), 2)"), expectedHeader, retArray5, DATABASE_NAME);

    // PREV(LAST(totalprice, 1))
    tableResultSetEqualTest(
        format(sql, "PREV(RPR_LAST(totalprice, 1))"), expectedHeader, retArray5, DATABASE_NAME);
    // PREV(LAST(totalprice, 1), 2)
    tableResultSetEqualTest(
        format(sql, "PREV(RPR_LAST(totalprice, 1), 2)"), expectedHeader, retArray1, DATABASE_NAME);

    // NEXT(FIRST(totalprice))
    tableResultSetEqualTest(
        format(sql, "NEXT(RPR_FIRST(totalprice))"), expectedHeader, retArray2, DATABASE_NAME);
    // NEXT(FIRST(totalprice), 2)
    tableResultSetEqualTest(
        format(sql, "NEXT(RPR_FIRST(totalprice), 2)"), expectedHeader, retArray6, DATABASE_NAME);

    // NEXT(FIRST(totalprice, 1))
    tableResultSetEqualTest(
        format(sql, "NEXT(RPR_FIRST(totalprice, 1))"), expectedHeader, retArray6, DATABASE_NAME);
    // NEXT(FIRST(totalprice, 1), 2)
    tableResultSetEqualTest(
        format(sql, "NEXT(RPR_FIRST(totalprice, 1), 2)"), expectedHeader, retArray1, DATABASE_NAME);

    // NEXT(LAST(totalprice))
    tableResultSetEqualTest(
        format(sql, "NEXT(RPR_LAST(totalprice))"), expectedHeader, retArray7, DATABASE_NAME);
    // NEXT(LAST(totalprice), 2)
    tableResultSetEqualTest(
        format(sql, "NEXT(RPR_LAST(totalprice), 2)"), expectedHeader, retArray8, DATABASE_NAME);

    // NEXT(LAST(totalprice, 2))
    tableResultSetEqualTest(
        format(sql, "NEXT(RPR_LAST(totalprice, 2))"), expectedHeader, retArray9, DATABASE_NAME);
    // NEXT(LAST(totalprice, 2), 2)
    tableResultSetEqualTest(
        format(sql, "NEXT(RPR_LAST(totalprice, 2), 2)"), expectedHeader, retArray10, DATABASE_NAME);
  }

  @Test
  public void testUnionVariable() {
    String[] expectedHeader = new String[] {"time", "match", "price", "lower_or_higher", "label"};
    String[] retArray1 =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,90.0,H,H,",
          "2025-01-01T00:02:00.000Z,1,80.0,H,A,",
          "2025-01-01T00:03:00.000Z,2,70.0,L,L,",
          "2025-01-01T00:04:00.000Z,2,80.0,L,A,",
        };

    String sql =
        "SELECT m.time, m.match, m.price, m.lower_or_higher, m.label "
            + "FROM t4 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        RUNNING RPR_LAST(totalprice) AS price, "
            + "        CLASSIFIER(U) AS lower_or_higher, "
            + "        CLASSIFIER(W) AS label "
            + "    ALL ROWS PER MATCH "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    PATTERN ((L | H) A) "
            + "    SUBSET "
            + "        U = (L, H), "
            + "        W = (A, L, H) "
            + "    DEFINE "
            + "        A AS A.totalprice = 80, "
            + "        L AS L.totalprice < 80, "
            + "        H AS H.totalprice > 80 "
            + ") AS m";

    tableResultSetEqualTest(sql, expectedHeader, retArray1, DATABASE_NAME);
  }

  @Test
  public void testClassifierFunction() {
    String[] expectedHeader =
        new String[] {"time", "match", "price", "label", "prev_label", "next_label"};
    // The scope of the CLASSIFIER() is within match
    String[] retArray1 =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,90.0,H,null,A,",
          "2025-01-01T00:02:00.000Z,1,80.0,A,H,null,",
          "2025-01-01T00:03:00.000Z,2,70.0,L,null,A,",
          "2025-01-01T00:04:00.000Z,2,80.0,A,L,null,",
        };

    String sql =
        "SELECT m.time, m.match, m.price, m.label, m.prev_label, m.next_label "
            + "FROM t4 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        RUNNING RPR_LAST(totalprice) AS price, "
            + "        CLASSIFIER() AS label, "
            + "        PREV(CLASSIFIER()) AS prev_label, "
            + "        NEXT(CLASSIFIER()) AS next_label "
            + "    ALL ROWS PER MATCH "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    PATTERN ((L | H) A) "
            + "    DEFINE "
            + "        A AS A.totalprice = 80, "
            + "        L AS L.totalprice < 80, "
            + "        H AS H.totalprice > 80 "
            + ") AS m";

    tableResultSetEqualTest(sql, expectedHeader, retArray1, DATABASE_NAME);
  }

  @Test
  public void testRowPattern() {
    String[] expectedHeader = new String[] {"time", "match", "price", "label"};
    String[] retArray1 =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,90.0,A,",
        };
    String[] retArray2 =
        new String[] {
          "2025-01-01T00:04:00.000Z,1,70.0,A,",
        };
    String[] retArray3 =
        new String[] {
          "2025-01-01T00:02:00.000Z,1,80.0,A,",
          "2025-01-01T00:03:00.000Z,1,70.0,B,",
          "2025-01-01T00:04:00.000Z,1,70.0,C,",
        };
    String[] retArray4 =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,90.0,A,",
          "2025-01-01T00:02:00.000Z,2,80.0,B,",
          "2025-01-01T00:03:00.000Z,3,70.0,B,",
          "2025-01-01T00:04:00.000Z,4,70.0,C,",
        };
    String[] retArray5 =
        new String[] {
          "2025-01-01T00:02:00.000Z,1,80.0,B,", "2025-01-01T00:03:00.000Z,1,70.0,C,",
        };

    String sql =
        "SELECT m.time, m.match, m.price, m.label "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        RUNNING RPR_LAST(totalprice) AS price,  "
            + "        CLASSIFIER() AS label "
            + "    ALL ROWS PER MATCH "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    %s " // PATTERN and DEFINE
            + ") AS m";

    // anchor pattern: partition start
    tableResultSetEqualTest(
        format(sql, "PATTERN (^A) " + "DEFINE A AS true "),
        expectedHeader,
        retArray1,
        DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (A^) " + "DEFINE A AS true "),
        expectedHeader,
        new String[] {},
        DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (^A^) " + "DEFINE A AS true "),
        expectedHeader,
        new String[] {},
        DATABASE_NAME);

    // anchor pattern: partition end
    tableResultSetEqualTest(
        format(sql, "PATTERN (A$) " + "DEFINE A AS true "),
        expectedHeader,
        retArray2,
        DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN ($A) " + "DEFINE A AS true "),
        expectedHeader,
        new String[] {},
        DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN ($A$) " + "DEFINE A AS true "),
        expectedHeader,
        new String[] {},
        DATABASE_NAME);

    // pattern concatenation
    tableResultSetEqualTest(
        format(
            sql,
            "PATTERN (A B C) "
                + "DEFINE "
                + "    B AS B.totalprice < PREV (B.totalprice), "
                + "    C AS C.totalprice = PREV (C.totalprice)"),
        expectedHeader,
        retArray3,
        DATABASE_NAME);

    // pattern alternation
    tableResultSetEqualTest(
        format(
            sql,
            "PATTERN (B | C | A) "
                + "DEFINE "
                + "    B AS B.totalprice < PREV (B.totalprice), "
                + "    C AS C.totalprice <= PREV (C.totalprice)"),
        expectedHeader,
        retArray4,
        DATABASE_NAME);

    // pattern permutation
    tableResultSetEqualTest(
        format(
            sql,
            "PATTERN (PERMUTE(B, C)) "
                + "DEFINE "
                + "    B AS B.totalprice < PREV (B.totalprice), "
                + "    C AS C.totalprice < PREV (C.totalprice)"),
        expectedHeader,
        retArray5,
        DATABASE_NAME);

    // grouped pattern
    tableResultSetEqualTest(
        format(
            sql,
            "PATTERN (((A) (B (C)))) "
                + "DEFINE "
                + "    B AS B.totalprice < PREV (B.totalprice), "
                + "    C AS C.totalprice = PREV (C.totalprice)"),
        expectedHeader,
        retArray3,
        DATABASE_NAME);
  }

  @Test
  public void testPatternQuantifier() {
    String[] expectedHeader = new String[] {"time", "match", "price", "label"};
    String[] retArray1 =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,null,null,",
          "2025-01-01T00:02:00.000Z,2,80.0,B,",
          "2025-01-01T00:03:00.000Z,2,70.0,B,",
          "2025-01-01T00:04:00.000Z,2,70.0,B,",
        };
    String[] retArray2 =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,null,null,",
          "2025-01-01T00:02:00.000Z,2,null,null,",
          "2025-01-01T00:03:00.000Z,3,null,null,",
          "2025-01-01T00:04:00.000Z,4,null,null,",
        };
    String[] retArray3 =
        new String[] {
          "2025-01-01T00:02:00.000Z,1,80.0,B,",
          "2025-01-01T00:03:00.000Z,1,70.0,B,",
          "2025-01-01T00:04:00.000Z,1,70.0,B,",
        };
    String[] retArray4 =
        new String[] {
          "2025-01-01T00:02:00.000Z,1,80.0,B,",
          "2025-01-01T00:03:00.000Z,2,70.0,B,",
          "2025-01-01T00:04:00.000Z,3,70.0,B,",
        };
    String[] retArray5 =
        new String[] {
          "2025-01-01T00:01:00.000Z,1,null,null,",
          "2025-01-01T00:02:00.000Z,2,80.0,B,",
          "2025-01-01T00:03:00.000Z,3,70.0,B,",
          "2025-01-01T00:04:00.000Z,4,70.0,B,",
        };
    String[] retArray6 =
        new String[] {
          "2025-01-01T00:02:00.000Z,1,80.0,B,", "2025-01-01T00:03:00.000Z,1,70.0,B,",
        };

    String sql =
        "SELECT m.time, m.match, m.price, m.label "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
            + "    ORDER BY time "
            + "    MEASURES "
            + "        MATCH_NUMBER() AS match, "
            + "        RUNNING RPR_LAST(totalprice) AS price,  "
            + "        CLASSIFIER() AS label "
            + "    ALL ROWS PER MATCH "
            + "    AFTER MATCH SKIP PAST LAST ROW "
            + "    %s " // PATTERN
            + "    DEFINE "
            + "        B AS B.totalprice <= PREV(B.totalprice) "
            + ") AS m";

    tableResultSetEqualTest(format(sql, "PATTERN (B*)"), expectedHeader, retArray1, DATABASE_NAME);

    tableResultSetEqualTest(format(sql, "PATTERN (B*?)"), expectedHeader, retArray2, DATABASE_NAME);

    tableResultSetEqualTest(format(sql, "PATTERN (B+)"), expectedHeader, retArray3, DATABASE_NAME);

    tableResultSetEqualTest(format(sql, "PATTERN (B+?)"), expectedHeader, retArray4, DATABASE_NAME);

    tableResultSetEqualTest(format(sql, "PATTERN (B?)"), expectedHeader, retArray5, DATABASE_NAME);

    tableResultSetEqualTest(format(sql, "PATTERN (B??)"), expectedHeader, retArray2, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{,})"), expectedHeader, retArray1, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{,}?)"), expectedHeader, retArray2, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{1,})"), expectedHeader, retArray3, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{1,}?)"), expectedHeader, retArray4, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{2,})"), expectedHeader, retArray3, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{2,}?)"), expectedHeader, retArray6, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{5,})"), expectedHeader, new String[] {}, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{5,}?)"), expectedHeader, new String[] {}, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{,1})"), expectedHeader, retArray5, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{,1}?)"), expectedHeader, retArray2, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{1,1})"), expectedHeader, retArray4, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{1,1}?)"), expectedHeader, retArray4, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{1})"), expectedHeader, retArray4, DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "PATTERN (B{1}?)"), expectedHeader, retArray4, DATABASE_NAME);
  }
}
