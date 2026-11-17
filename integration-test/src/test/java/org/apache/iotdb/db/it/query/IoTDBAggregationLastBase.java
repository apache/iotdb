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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class IoTDBAggregationLastBase {

  private static final String DATABASE_NAME = "test_null_time_aggs_all_types_last";
  private static final String LAST_BY_DATABASE_NAME = "test_last_by_agg";
  private static final String LAST_IN_GROUP_DATABASE_NAME = "test_grouped_last_agg";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table_a("
            + "device STRING TAG, "
            + "s_int INT32 FIELD, "
            + "s_long INT64 FIELD, "
            + "s_float FLOAT FIELD, "
            + "s_double DOUBLE FIELD, "
            + "s_bool BOOLEAN FIELD, "
            + "s_string STRING FIELD, "
            + "time_type TIMESTAMP FIELD)",
        "CREATE TABLE table_b(" + "time TIMESTAMP TIME, " + "s_back INT32 FIELD)",

        // batch insert：d1(5 rows) + d2(3 rows)
        "INSERT INTO table_a(time, device, s_int, s_long, s_float, s_double, s_bool, s_string, time_type) VALUES "
            // --- Device d1 ---
            + "(-50,  'd1', -50,  -50,  -50.0,  -50.0,  false, '-50s',  NULL),"
            + "(-10,  'd1', -10,  -10,  -10.0,  -10.0,  true,  '-10s',  -10),"
            + "(-100, 'd1', -100, -100, -100.0, -100.0, true, '-100s', -100),"
            + "(50,   'd1', 50,   50,   50.0,   50.0,   false, '50s',   NULL),"
            + "(-5,   'd1', -5,   -5,   -5.0,   -5.0,   false, '-5s',   -5),"

            // --- Device d2 ---
            + "(-80,  'd2', -80,  -80,  -80.0,  -80.0,  true,  '-80s',  NULL),"
            + "(-5,   'd2', NULL, -5,   -5.0,   NULL,   false, NULL,    -5),"
            + "(-40,  'd2', -40,  -40,  -40.0,  -40.0,  false, '-40s',  -40),"
            + "(20,   'd2', 20,   20,   20.0,   20.0,   true,  '20s',   NULL),"
            + "(-4,   'd2', -4,   NULL, NULL,   -4.0,   NULL,  '-4s',   -4),"

            // --- Device d3 (Pure NULL test) ---
            // d3: all time_type are NULL
            + "(-200, 'd3', null, null, null, null, null, null, -200),"
            + "(-150, 'd3', null, null, null, null, null, null, -150)"
      };

  private static final String[] lastByCreateSqls =
      new String[] {
        "CREATE DATABASE " + LAST_BY_DATABASE_NAME,
        "USE " + LAST_BY_DATABASE_NAME,
        "CREATE TABLE table_a(device STRING TAG, s_int INT32 FIELD, s_long INT64 FIELD, s_float FLOAT FIELD, s_double DOUBLE FIELD, s_bool BOOLEAN FIELD, s_string STRING FIELD, time_type TIMESTAMP FIELD, y_criteria INT32 FIELD)",
        "CREATE TABLE table_b(time TIMESTAMP TIME, s_back INT32 FIELD)",
        "INSERT INTO table_a(time, device, s_int, s_long, s_float, s_double, s_bool, s_string, time_type, y_criteria) VALUES "
            + "(99, 'd1', 99, 99, 99.0, 99.0, true, '99s', NULL, 99),"
            + "(-200, 'd1', -200, -200, -200.0, -200.0, true, '-200s', -200, -200),"
            + "(-100, 'd1', -100, -100, -100.0, -100.0, true, '-100s', -100, -100),"
            + "(-10, 'd1', -10, -10, -10.0, -10.0, true, '-10s', -10, -10),"
            + "(-5, 'd1', -5, -5, -5.0, -5.0, false, '-5s', -5, -5),"
            + "(88, 'd2', 88, 88, 88.0, 88.0, true, '2s', NULL, 88),"
            + "(-5, 'd2', -5, -5, -5.0, -5.0, false, '-5s', -5, NULL),"
            + "(-8, 'd2', -8, -8, -8.0, -8.0, false, '-8s', -8, NULL),"
            + "(-10, 'd2', -10, -10, -10.0, -10.0, true, '-10s', -10, -10),"
            + "(-20, 'd2', -20, -20, -20.0, -20.0, true, '-20s', -20, -20),"
            + "(3, 'd3', 77, 77, 77.0, 77.0, true, '77s', NULL, 77),"
            + "(-5, 'd3', -5, -5, NULL, NULL, NULL, NULL, -5, -5),"
            + "(-10, 'd3', -10, -10, -10.0, -10.0, true, '-10s', -10, NULL),"
            + "(-20, 'd3', -20, -20, -20.0, -20.0, true, '-20s', -20, -20),"
            + "(4, 'd4', 66, 66, 66.0, 66.0, true, '66s', NULL, NULL),"
            + "(-5, 'd4', -5, -5, -5.0, -5.0, false, '-5s', -5, NULL),"
            + "(-10, 'd4', -10, -10, -10.0, -10.0, true, '-10s', -10, NULL),"
            + "(-20, 'd4', -20, -20, -20.0, -20.0, true, '-20s', -20, NULL),"
            + "(1, 'd5', 10, 10, 10.0, 10.0, true, '10s', NULL, NULL),"
            + "(2, 'd5', 50, 50, 50.0, 50.0, false, '50s', NULL, 50)"
      };

  private static final String[] lastInGroupCreateSqls =
      new String[] {
        "CREATE DATABASE " + LAST_IN_GROUP_DATABASE_NAME,
        "USE " + LAST_IN_GROUP_DATABASE_NAME,
        "CREATE TABLE table_a(s_int INT32 FIELD, s_long INT64 FIELD, s_float FLOAT FIELD, s_double DOUBLE FIELD, s_bool BOOLEAN FIELD, s_string STRING FIELD, time_type TIMESTAMP FIELD, partition STRING FIELD)",
        "CREATE TABLE table_b(time TIMESTAMP TIME, s_back INT32 FIELD)",
        "INSERT INTO table_a(time, s_int, s_long, s_float, s_double, s_bool, s_string, time_type, partition) VALUES "
            + "(1, -40, -40, -40.0, -40.0, false, '-40s', -40, 'p1'),"
            + "(2, 20, 20, 20.0, 20.0, true, '20s', NULL, 'p1'),"
            + "(-1, -5, -5, -5.0, -5.0, false, '-5s', -5, 'p1'),"
            + "(-2, -4, -4, -4.0, -4.0, true, '-4s', NULL, 'p1'),"
            + "(-100, -80, -80, -80.0, -80.0, true, '-80s', -80, 'p1'),"
            + "(11, -80, -80, -80.0, -80.0, true, '-80s', NULL, 'p2'),"
            + "(-21, -40, -40, -40.0, -40.0, false, '-40s', -40, 'p2'),"
            + "(-102, 20, 20, 20.0, 20.0, true, '20s', NULL, 'p2'),"
            + "(33, NULL, -5, -5.0, NULL, false, NULL, -5, 'p2'),"
            + "(68, -80, -80, -80.0, -80.0, true, '-80s', NULL, 'p3'),"
            + "(288, -40, -40, -40.0, -40.0, false, '-40s', NULL, 'p3')"
      };

  protected static void insertLastData() {
    prepareTableData(createSqls);
    prepareTableData(lastByCreateSqls);
    prepareTableData(lastInGroupCreateSqls);
  }

  @Test
  public void TestPriority() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] retArray = {"50,50,50.0,50.0,false,50s,"};
    tableResultSetEqualTest(
        "select "
            + "last(s_int), "
            + "last(s_long), "
            + "last(s_float), "
            + "last(s_double), "
            + "last(s_bool), "
            + "last(s_string) "
            + "from table_a "
            + "where table_a.device='d1' ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testNoTimeStamp() {

    String sql =
        "select "
            + "last(s_int)"
            + " from ("
            + "  select "
            + "    s_int,"
            + "    s_long,"
            + "    s_float"
            + "  from table_a"
            + ") AS t";
    try (Connection connection =
        EnvFactory.getEnv()
            .getConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + DATABASE_NAME);
        statement.executeQuery(sql);
      }
      fail("Missing valid time column, the query should fail");
    } catch (SQLException e) {
      assertEquals(
          "701: Missing valid time column. The table must contain either a column with the TIME category or at least one TIMESTAMP column.",
          e.getMessage());
    }
  }

  // Checks LAST across types when a device has no null ordering values; all results select -5.
  @Test
  public void testLastAggregation() {

    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};

    //  Expected Result:
    String[] retArray = {"-5,-5,-5.0,-5.0,false,-5s,"};

    tableResultSetEqualTest(
        "select "
            + "last(s_int, time), "
            + "last(s_long, time), "
            + "last(s_float, time), "
            + "last(s_double, time), "
            + "last(s_bool, time), "
            + "last(s_string, time) "
            + "from "
            // subQuery: project all the column needed and rename the time_type to the time
            + "(select "
            + "  time_type as time, "
            + "  s_int, s_long, s_float, s_double, s_bool, s_string "
            + "from table_a "
            + "left join table_b on table_a.time=table_b.time "
            + "where table_a.device='d1') ",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "last(s_int), "
            + "last(s_long), "
            + "last(s_float), "
            + "last(s_double), "
            + "last(s_bool), "
            + "last(s_string) "
            + "from "
            + "(select "
            + "  s_int, s_long, s_float, s_double, s_bool, s_string, time_type "
            + "from table_a "
            + "left join table_b on table_a.time=table_b.time "
            + "where table_a.device='d1') ",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "last(s_int, time_type), "
            + "last(s_long, time_type), "
            + "last(s_float, time_type), "
            + "last(s_double, time_type), "
            + "last(s_bool, time_type), "
            + "last(s_string, time_type) "
            + "from table_a "
            + "where table_a.device='d1' ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // Checks LAST skips null ordering values and returns the expected latest value for device d2.
  @Test
  public void testLastAggregationWithNullValue() {

    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};

    //  Expected Result:
    String[] retArray = {"-4,-5,-5.0,-4.0,false,-4s,"};

    tableResultSetEqualTest(
        "select "
            + "last(s_int, time), "
            + "last(s_long, time), "
            + "last(s_float, time), "
            + "last(s_double, time), "
            + "last(s_bool, time), "
            + "last(s_string, time) "
            + "from "
            // subQuery: project all the column needed and rename the time_type to the time
            + "(select "
            + "  time_type as time, "
            + "  s_int, s_long, s_float, s_double, s_bool, s_string "
            + "from table_a "
            + "left join table_b on table_a.time=table_b.time "
            + "where table_a.device='d2') ",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "last(s_int), "
            + "last(s_long), "
            + "last(s_float), "
            + "last(s_double), "
            + "last(s_bool), "
            + "last(s_string) "
            + "from "
            + "(select "
            + "  s_int, s_long, s_float, s_double, s_bool, s_string, time_type "
            + "from table_a "
            + "left join table_b on table_a.time=table_b.time "
            + "where table_a.device='d2') ",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "last(s_int, time_type), "
            + "last(s_long, time_type), "
            + "last(s_float, time_type), "
            + "last(s_double, time_type), "
            + "last(s_bool, time_type), "
            + "last(s_string, time_type) "
            + "from table_a "
            + "where table_a.device='d2'",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // Checks LAST returns null across all types when every ordering value for device d3 is null.
  @Test
  public void testLastAggregationWithAllNull() {

    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};

    //  Expected Result:
    String[] retArray = {"null,null,null,null,null,null,"};

    tableResultSetEqualTest(
        "select "
            + "last(s_int, time), "
            + "last(s_long, time), "
            + "last(s_float, time), "
            + "last(s_double, time), "
            + "last(s_bool, time), "
            + "last(s_string, time) "
            + "from "
            // subQuery: project all the column needed and rename the time_type to the time
            + "(select "
            + "  time_type as time, "
            + "  s_int, s_long, s_float, s_double, s_bool, s_string "
            + "from table_a "
            + "left join table_b on table_a.time=table_b.time "
            + "where table_a.device='d3') ",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "last(s_int), "
            + "last(s_long), "
            + "last(s_float), "
            + "last(s_double), "
            + "last(s_bool), "
            + "last(s_string) "
            + "from "
            + "(select "
            + "  time_type, "
            + "  s_int, s_long, s_float, s_double, s_bool, s_string "
            + "from table_a "
            + "left join table_b on table_a.time=table_b.time "
            + "where table_a.device='d3') ",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 3. through field column
    tableResultSetEqualTest(
        "select "
            + "last(s_int, time_type), "
            + "last(s_long, time_type), "
            + "last(s_float, time_type), "
            + "last(s_double, time_type), "
            + "last(s_bool, time_type), "
            + "last(s_string, time_type) "
            + "from table_a "
            + "where table_a.device='d3'",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // Checks LAST_BY uses the latest physical row even when its criterion is null.
  @Test
  public void testLastByPriority() {
    tableResultSetEqualTest(
        "select last_by(s_int, y_criteria), last_by(s_long, y_criteria), last_by(s_float, y_criteria), last_by(s_double, y_criteria), last_by(s_bool, y_criteria), last_by(s_string, y_criteria) from table_a where table_a.device='d1'",
        new String[] {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"},
        new String[] {"99,99,99.0,99.0,true,99s,"},
        LAST_BY_DATABASE_NAME);
  }

  // Confirms LAST_BY fails when a projected subquery removes all valid time columns.
  @Test
  public void testLastByNoTimeStamp() {
    String sql =
        "select last_by(s_int, y_criteria) from (select s_int, s_long, s_float, y_criteria from table_a) AS t";
    try (Connection connection =
        EnvFactory.getEnv()
            .getConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + LAST_BY_DATABASE_NAME);
        statement.executeQuery(sql);
      }
      fail("Missing valid time column, the query should fail");
    } catch (SQLException e) {
      assertEquals(
          "701: Missing valid time column. The table must contain either a column with the TIME category or at least one TIMESTAMP column.",
          e.getMessage());
    }
  }

  // Checks LAST_BY chooses the latest qualifying row when criteria are non-null for device d1.
  @Test
  public void testLastByD1NoNulls() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"-5,-5,-5.0,-5.0,false,-5s,"};
    runLastByTest("d1", expectedHeader, expectedRows);
    runLastByTest2("d1", expectedHeader, expectedRows);
    runLastByTest3("d1", expectedHeader, expectedRows);
  }

  // Checks LAST_BY backtracks past null criteria and selects the later qualifying row for d2.
  @Test
  public void testLastByD2Backtracking() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"-10,-10,-10.0,-10.0,true,-10s,"};
    runLastByTest("d2", expectedHeader, expectedRows);
    runLastByTest2("d2", expectedHeader, expectedRows);
    runLastByTest3("d2", expectedHeader, expectedRows);
  }

  // Checks LAST_BY returns null-valued columns when the selected row has null source values.
  @Test
  public void testLastByD3TargetNull() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"-5,-5,null,null,null,null,"};
    runLastByTest("d3", expectedHeader, expectedRows);
    runLastByTest2("d3", expectedHeader, expectedRows);
    runLastByTest3("d3", expectedHeader, expectedRows);
  }

  // Checks LAST_BY returns null when every criterion is null for device d4.
  @Test
  public void testLastByD4AllNullCriteria() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"null,null,null,null,null,null,"};
    runLastByTest("d4", expectedHeader, expectedRows);
    runLastByTest2("d4", expectedHeader, expectedRows);
    runLastByTest3("d4", expectedHeader, expectedRows);
  }

  // Checks LAST_BY skips null times and criteria when a later qualifying row exists for d5.
  @Test
  public void testLastByD5AllTimeNull() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"50,50,50.0,50.0,false,50s,"};
    runLastByTest("d5", expectedHeader, expectedRows);
    runLastByTest2("d5", expectedHeader, expectedRows);
    runLastByTest3("d5", expectedHeader, expectedRows);
  }

  private void runLastByTest(String deviceId, String[] expectedHeader, String[] expectedRows) {
    tableResultSetEqualTest(
        "select last_by(s_int, y_criteria, time), last_by(s_long, y_criteria, time), last_by(s_float, y_criteria, time), last_by(s_double, y_criteria, time), last_by(s_bool, y_criteria, time), last_by(s_string, y_criteria, time) from (select time_type as time, s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria from table_a left join table_b on table_a.time=table_b.time where table_a.device='"
            + deviceId
            + "')",
        expectedHeader,
        expectedRows,
        LAST_BY_DATABASE_NAME);
  }

  private void runLastByTest2(String deviceId, String[] expectedHeader, String[] expectedRows) {
    tableResultSetEqualTest(
        "select last_by(s_int, y_criteria), last_by(s_long, y_criteria), last_by(s_float, y_criteria), last_by(s_double, y_criteria), last_by(s_bool, y_criteria), last_by(s_string, y_criteria) from (select s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria, time_type from table_a left join table_b on table_a.time=table_b.time where table_a.device='"
            + deviceId
            + "')",
        expectedHeader,
        expectedRows,
        LAST_BY_DATABASE_NAME);
  }

  private void runLastByTest3(String deviceId, String[] expectedHeader, String[] expectedRows) {
    tableResultSetEqualTest(
        "select last_by(s_int, y_criteria, time_type), last_by(s_long, y_criteria, time_type), last_by(s_float, y_criteria, time_type), last_by(s_double, y_criteria, time_type), last_by(s_bool, y_criteria, time_type), last_by(s_string, y_criteria, time_type) from table_a where table_a.device='"
            + deviceId
            + "'",
        expectedHeader,
        expectedRows,
        LAST_BY_DATABASE_NAME);
  }

  // Checks grouped LAST on partitions with mixed, partial, and entirely NULL ordering values.
  @Test
  public void testGroupedLastAggregation() {
    String[] expectedHeader = {"partition", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6"};
    String[] expectedRows = {
      "p1,-5,-5,-5.0,-5.0,false,-5s,",
      "p2,-40,-5,-5.0,-40.0,false,-40s,",
      "p3,-80,-80,-80.0,-80.0,true,-80s,"
    };
    tableResultSetEqualTest(
        "select partition, last(s_int, time), last(s_long, time), last(s_float, time), last(s_double, time), last(s_bool, time), last(s_string, time) from (select time_type as time, partition, s_int, s_long, s_float, s_double, s_bool, s_string from table_a left join table_b on table_a.time=table_b.time) group by partition order by partition",
        expectedHeader,
        expectedRows,
        LAST_IN_GROUP_DATABASE_NAME);
    tableResultSetEqualTest(
        "select partition, last(s_int), last(s_long), last(s_float), last(s_double), last(s_bool), last(s_string) from (select time_type, partition, s_int, s_long, s_float, s_double, s_bool, s_string from table_a left join table_b on table_a.time=table_b.time) group by partition order by partition",
        expectedHeader,
        expectedRows,
        LAST_IN_GROUP_DATABASE_NAME);
    tableResultSetEqualTest(
        "select partition, last(s_int, time_type), last(s_long, time_type), last(s_float, time_type), last(s_double, time_type), last(s_bool, time_type), last(s_string, time_type) from table_a group by partition order by partition",
        expectedHeader,
        expectedRows,
        LAST_IN_GROUP_DATABASE_NAME);
  }
}
