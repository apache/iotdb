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
  public void testOutputMode() {
    String[] expectedHeader = new String[] {"match", "price", "label"};
    String[] retArray1 =
        new String[] {
          "1,null,null,", "2,80.0,B,", "2,70.0,B,", "3,null,null,",
        };
    String[] retArray2 =
        new String[] {
          "1,80.0,B,", "1,70.0,B,",
        };

    String sql =
        "SELECT m.match, m.price, m.label "
            + "FROM t1 "
            + "MATCH_RECOGNIZE ( "
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
        format(sql, "ALL ROWS PER MATCH", "PATTERN (B*)"),
        expectedHeader,
        retArray1,
        DATABASE_NAME);

    tableResultSetEqualTest(
        format(sql, "ALL ROWS PER MATCH", "PATTERN (B+)"),
        expectedHeader,
        retArray2,
        DATABASE_NAME);
  }
}
