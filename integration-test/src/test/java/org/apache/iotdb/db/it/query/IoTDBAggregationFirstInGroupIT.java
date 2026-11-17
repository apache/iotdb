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
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAggregationFirstInGroupIT extends IoTDBAggregationLastBase {

  private static final String DATABASE_NAME = "test_grouped_first_agg";
  private static final String FIRST_BY_DATABASE_NAME = "test_first_by_in_group_agg";
  private static final String FIRST_DATABASE_NAME = "test_null_time_aggs_all_types_first";
  private static final String FIRST_BY_CLASS_DATABASE_NAME = "test_first_by_agg";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table_a("
            + "s_int INT32 FIELD, "
            + "s_long INT64 FIELD, "
            + "s_float FLOAT FIELD, "
            + "s_double DOUBLE FIELD, "
            + "s_bool BOOLEAN FIELD, "
            + "s_string STRING FIELD, "
            + "time_type TIMESTAMP FIELD, "
            + "partition STRING FIELD)",
        "CREATE TABLE table_b(" + "time TIMESTAMP TIME, " + "s_back INT32 FIELD)",

        // Batch Insert: Split into 3 partitions (using positive numbers)
        // Physical 'time' column is kept distinct and out-of-order as requested.
        "INSERT INTO table_a(time, s_int, s_long, s_float, s_double, s_bool, s_string, time_type, partition) VALUES "

            // Partition 'p1': Standard Mixed Scenario
            // Valid time_types: 40, 5, 80.
            // Logic: Min Valid Time is 5.
            + "(1,    40,   40,   40.0,   40.0,   false, '40s',   40,   'p1')," // Valid
            + "(2,    20,   20,   20.0,   20.0,   true,  '20s',   NULL, 'p1')," // Null Time
            + "(-1,   5,    5,    5.0,    5.0,    false, '5s',    5,    'p1')," // Min Valid Time
            // (5)
            + "(-2,   4,    4,    4.0,    4.0,    true,  '4s',    NULL, 'p1')," // Null Time
            + "(-100, 80,   80,   80.0,   80.0,   true,  '80s',   80,   'p1')," // Valid

            // Partition 'p2': Mixed Null Values Scenario
            + "(11,   80,   80,   80.0,   80.0,   true,  '80s',   NULL, 'p2')," // Null Time
            + "(-21,  40,   40,   40.0,   40.0,   false, '40s',   40,   'p2')," // Previous Valid
            + "(-102, 20,   20,   20.0,   20.0,   true,  '20s',   NULL, 'p2')," // Null Time
            + "(33,   NULL, 5,    5.0,    NULL,   false,  NULL,   5,    'p2'),"
            + "(100,  100,  100,  100.0,  100,    true,   100,    100,  'p2'),"

            // Partition 'p3': Only Null Time Scenario
            // Logic: No Valid Time. "First Null Wins" strategy applies.
            + "(68,   80,   80,   80.0,   80.0,   true,  '80s',   NULL, 'p3'),"
            + "(288,  40,   40,   40.0,   40.0,   false, '40s',   NULL, 'p3')"
      };

  private static final String[] firstCreateSqls =
      new String[] {
        "CREATE DATABASE " + FIRST_DATABASE_NAME,
        "USE " + FIRST_DATABASE_NAME,
        "CREATE TABLE table_a(device STRING TAG, s_int INT32 FIELD, s_long INT64 FIELD, s_float FLOAT FIELD, s_double DOUBLE FIELD, s_bool BOOLEAN FIELD, s_string STRING FIELD, time_type TIMESTAMP FIELD)",
        "CREATE TABLE table_b(time TIMESTAMP TIME, s_back INT32 FIELD)",
        "INSERT INTO table_a(time, device, s_int, s_long, s_float, s_double, s_bool, s_string, time_type) VALUES "
            + "(100, 'd1', 100, 100, 100.0, 100.0, true, '100s', 100),"
            + "(50, 'd1', 50, 50, 50.0, 50.0, false, '50s', NULL),"
            + "(10, 'd1', 10, 10, 10.0, 10.0, true, '10s', 10),"
            + "(5, 'd1', 5, 5, 5.0, 5.0, false, '5s', 5),"
            + "(-50, 'd1', -50, -50, -50.0, -50.0, false, '-50s', NULL),"
            + "(80, 'd2', 80, 80, 80.0, 80.0, true, '80s', NULL),"
            + "(9, 'd2', NULL, 9, 9.0, NULL, false, NULL, 9),"
            + "(40, 'd2', 40, 40, 40.0, 40.0, false, '40s', 40),"
            + "(-20, 'd2', -20, -20, -20.0, -20.0, true, '-20s', NULL),"
            + "(10, 'd2', 10, NULL, NULL, 10.0, NULL, '10s', 10),"
            + "(200, 'd3', null, null, null, null, null, null, 200),"
            + "(150, 'd3', null, null, null, null, null, null, 150)"
      };

  private static final String[] firstByClassCreateSqls =
      new String[] {
        "CREATE DATABASE " + FIRST_BY_CLASS_DATABASE_NAME,
        "USE " + FIRST_BY_CLASS_DATABASE_NAME,
        "CREATE TABLE table_a(device STRING TAG, s_int INT32 FIELD, s_long INT64 FIELD, s_float FLOAT FIELD, s_double DOUBLE FIELD, s_bool BOOLEAN FIELD, s_string STRING FIELD, time_type TIMESTAMP FIELD, y_criteria INT32 FIELD)",
        "CREATE TABLE table_b(time TIMESTAMP TIME, s_back INT32 FIELD)",
        "INSERT INTO table_a(time, device, s_int, s_long, s_float, s_double, s_bool, s_string, time_type, y_criteria) VALUES "
            + "(1, 'd1', 1, 1, 1.0, 1.0, true, '1s', NULL, 1),"
            + "(200, 'd1', 200, 200, 200.0, 200.0, true, '200s', 200, 200),"
            + "(100, 'd1', 100, 100, 100.0, 100.0, true, '100s', 100, 100),"
            + "(10, 'd1', 10, 10, 10.0, 10.0, true, '10s', 10, 10),"
            + "(5, 'd1', 5, 5, 5.0, 5.0, false, '5s', 5, 5),"
            + "(2, 'd2', 2, 2, 2.0, 2.0, true, '2s', NULL, 2),"
            + "(5, 'd2', 5, 5, 5.0, 5.0, false, '5s', 5, NULL),"
            + "(8, 'd2', 8, 8, 8.0, 8.0, false, '8s', 8, NULL),"
            + "(10, 'd2', 10, 10, 10.0, 10.0, true, '10s', 10, 10),"
            + "(20, 'd2', 20, 20, 20.0, 20.0, true, '20s', 20, 20),"
            + "(3, 'd3', 3, 3, 3.0, 3.0, true, '3s', NULL, 3),"
            + "(5, 'd3', 5, 5, NULL, NULL, NULL, NULL, 5, 5),"
            + "(10, 'd3', 10, 10, 10.0, 10.0, true, '10s', 10, NULL),"
            + "(20, 'd3', 20, 20, 20.0, 20.0, true, '20s', 20, 20),"
            + "(4, 'd4', 66, 66, 66.0, 66.0, true, '66s', NULL, NULL),"
            + "(5, 'd4', 5, 5, 5.0, 5.0, false, '5s', 5, NULL),"
            + "(10, 'd4', 10, 10, 10.0, 10.0, true, '10s', 10, NULL),"
            + "(20, 'd4', 20, 20, 20.0, 20.0, true, '20s', 20, NULL),"
            + "(1, 'd5', 10, 10, 10.0, 10.0, true, '10s', NULL, NULL),"
            + "(2, 'd5', 50, 50, 50.0, 50.0, false, '50s', NULL, 50)"
      };

  private static final String[] firstByCreateSqls =
      new String[] {
        "CREATE DATABASE " + FIRST_BY_DATABASE_NAME,
        "USE " + FIRST_BY_DATABASE_NAME,
        "CREATE TABLE table_a("
            + "s_int INT32 FIELD, "
            + "s_long INT64 FIELD, "
            + "s_float FLOAT FIELD, "
            + "s_double DOUBLE FIELD, "
            + "s_bool BOOLEAN FIELD, "
            + "s_string STRING FIELD, "
            + "time_type TIMESTAMP FIELD, "
            + "y_criteria INT32 FIELD, " // Acts as the FIRST_BY criterion.
            + "partition STRING FIELD)",
        "CREATE TABLE table_b(" + "time TIMESTAMP TIME, " + "s_back INT32 FIELD)",
        "INSERT INTO table_a(time, s_int, s_long, s_float, s_double, s_bool, s_string, time_type, y_criteria, partition) VALUES "
            // p1 has valid criteria at every non-null time; the earliest time wins.
            + "(1, 99, 99, 99.0, 99.0, true, '99s', NULL, 99, 'p1'),"
            + "(2, 200, 200, 200.0, 200.0, true, '200s', 200, 200, 'p1'),"
            + "(3, 100, 100, 100.0, 100.0, true, '100s', 100, 100, 'p1'),"
            + "(4, 10, 10, 10.0, 10.0, true, '10s', 10, 10, 'p1'),"
            + "(5, 5, 5, 5.0, 5.0, false, '5s', 5, 5, 'p1'),"
            // p2 skips null criteria and selects the first later qualifying row.
            + "(6, 88, 88, 88.0, 88.0, true, '88s', NULL, 88, 'p2'),"
            + "(7, 5, 5, 5.0, 5.0, false, '5s', 5, NULL, 'p2'),"
            + "(8, 8, 8, 8.0, 8.0, false, '8s', 8, NULL, 'p2'),"
            + "(9, 10, 10, 10.0, 10.0, true, '10s', 10, 10, 'p2'),"
            + "(10, 20, 20, 20.0, 20.0, true, '20s', 20, 20, 'p2'),"
            // p3's first qualifying row has null values; p4 has no qualifying row.
            + "(11, 77, 77, 77.0, 77.0, true, '77s', NULL, 77, 'p3'),"
            + "(12, NULL, NULL, 5.0, 5.0, NULL, NULL, 5, 5, 'p3'),"
            + "(13, 10, 10, 10.0, 10.0, true, '10s', 10, NULL, 'p3'),"
            + "(14, 20, 20, 20.0, 20.0, true, '20s', 20, 20, 'p3'),"
            + "(15, 66, 66, 66.0, 66.0, true, '66s', NULL, NULL, 'p4'),"
            + "(16, 5, 5, 5.0, 5.0, false, '5s', 5, NULL, 'p4'),"
            + "(17, 10, 10, 10.0, 10.0, true, '10s', 10, NULL, 'p4'),"
            + "(18, 20, 20, 20.0, 20.0, true, '20s', 20, NULL, 'p4'),"
            // p5 has only null ordering values, so the criterion still selects row 20.
            + "(19, 10, 10, 10.0, 10.0, true, '10s', NULL, NULL, 'p5'),"
            + "(20, 50, 50, 50.0, 50.0, false, '50s', NULL, 50, 'p5')"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
    prepareTableData(firstByCreateSqls);
    prepareTableData(firstCreateSqls);
    prepareTableData(firstByClassCreateSqls);
    // The Last fixtures use separate databases and the same topology, so load them into this
    // cluster.
    IoTDBAggregationLastBase.insertLastData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // Verifies grouped FIRST with null ordering columns and the first-row fallback for an all-null
  // group.
  @Test
  public void testGroupedFirstAggregation() {

    // Expected Header: partition column + 6 aggregation results
    String[] expectedHeader = {"partition", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6"};

    // Expected Results:
    String[] retArray = {
      // p1
      "p1,5,5,5.0,5.0,false,5s,",
      // p2
      "p2,40,5,5.0,40.0,false,40s,",
      // p3
      "p3,80,80,80.0,80.0,true,80s,"
    };

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "first(s_int, time), "
            + "first(s_long, time), "
            + "first(s_float, time), "
            + "first(s_double, time), "
            + "first(s_bool, time), "
            + "first(s_string, time) "
            + "from "
            // SubQuery: Rename time_type to 'ts' to avoid ambiguity with physical 'time'
            + "(select "
            + "  time_type as time, "
            + "  partition, "
            + "  s_int, s_long, s_float, s_double, s_bool, s_string "
            + "from table_a "
            + "left join table_b on table_a.time=table_b.time) "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "first(s_int), "
            + "first(s_long), "
            + "first(s_float), "
            + "first(s_double), "
            + "first(s_bool), "
            + "first(s_string) "
            + "from "
            + "(select "
            + "  time_type, "
            + "  partition, "
            + "  s_int, s_long, s_float, s_double, s_bool, s_string "
            + "from table_a "
            + "left join table_b on table_a.time=table_b.time) "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "first(s_int, time_type), "
            + "first(s_long, time_type), "
            + "first(s_float, time_type), "
            + "first(s_double, time_type), "
            + "first(s_bool, time_type), "
            + "first(s_string, time_type) "
            + "from table_a "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // Verifies grouped FIRST_BY backtracks past null criteria and returns null when no row qualifies.
  @Test
  public void testGroupedFirstByAggregation() {
    String[] expectedHeader = {"partition", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6"};
    String[] retArray = {
      "p1,5,5,5.0,5.0,false,5s,",
      "p2,10,10,10.0,10.0,true,10s,",
      "p3,null,null,5.0,5.0,null,null,",
      "p4,null,null,null,null,null,null,",
      "p5,50,50,50.0,50.0,false,50s,"
    };

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "first_by(s_int, y_criteria, time), "
            + "first_by(s_long, y_criteria, time), "
            + "first_by(s_float, y_criteria, time), "
            + "first_by(s_double, y_criteria, time), "
            + "first_by(s_bool, y_criteria, time), "
            + "first_by(s_string, y_criteria, time) "
            + "from "
            + "(select time_type as time, s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria, partition "
            + "from table_a left join table_b on table_a.time=table_b.time) "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        FIRST_BY_DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "first_by(s_int, y_criteria), "
            + "first_by(s_long, y_criteria), "
            + "first_by(s_float, y_criteria), "
            + "first_by(s_double, y_criteria), "
            + "first_by(s_bool, y_criteria), "
            + "first_by(s_string, y_criteria) "
            + "from "
            + "(select s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria, partition, time_type "
            + "from table_a left join table_b on table_a.time=table_b.time) "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        FIRST_BY_DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "first_by(s_int, y_criteria, time_type), "
            + "first_by(s_long, y_criteria, time_type), "
            + "first_by(s_float, y_criteria, time_type), "
            + "first_by(s_double, y_criteria, time_type), "
            + "first_by(s_bool, y_criteria, time_type), "
            + "first_by(s_string, y_criteria, time_type) "
            + "from table_a "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        FIRST_BY_DATABASE_NAME);
  }

  // Checks FIRST selects the earliest time_type for a device when the physical time is reordered.
  @Test
  public void testPriority() {
    tableResultSetEqualTest(
        "select first(s_int), first(s_long), first(s_float), first(s_double), first(s_bool), first(s_string) from table_a where table_a.device='d1'",
        new String[] {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"},
        new String[] {"-50,-50,-50.0,-50.0,false,-50s,"},
        FIRST_DATABASE_NAME);
  }

  // Confirms FIRST fails when a projected subquery removes all valid time columns.
  @Test
  public void testFirstNoTimeStamp() {
    String sql = "select first(s_int) from (select s_int, s_long, s_float from table_a) AS t";
    try (Connection connection =
        EnvFactory.getEnv()
            .getConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + FIRST_DATABASE_NAME);
        statement.executeQuery(sql);
      }
      fail("Missing valid time column, the query should fail");
    } catch (SQLException e) {
      assertEquals(
          "701: Missing valid time column. The table must contain either a column with the TIME category or at least one TIMESTAMP column.",
          e.getMessage());
    }
  }

  // Checks FIRST with explicit and inferred time columns after joining away the physical time.
  @Test
  public void testAggregation() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"5,5,5.0,5.0,false,5s,"};
    tableResultSetEqualTest(
        "select first(s_int, time), first(s_long, time), first(s_float, time), first(s_double, time), first(s_bool, time), first(s_string, time) from (select time_type as time, s_int, s_long, s_float, s_double, s_bool, s_string from table_a left join table_b on table_a.time=table_b.time where table_a.device='d1')",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
    tableResultSetEqualTest(
        "select first(s_int), first(s_long), first(s_float), first(s_double), first(s_bool), first(s_string) from (select time_type, s_int, s_long, s_float, s_double, s_bool, s_string from table_a left join table_b on table_a.time=table_b.time where table_a.device='d1')",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
    tableResultSetEqualTest(
        "select first(s_int, time_type), first(s_long, time_type), first(s_float, time_type), first(s_double, time_type), first(s_bool, time_type), first(s_string, time_type) from table_a where table_a.device='d1'",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
  }

  // Checks FIRST skips null ordering values and preserves nulls in the selected row's columns.
  @Test
  public void testAggregationWithNullValue() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"10,9,9.0,10.0,false,10s,"};
    tableResultSetEqualTest(
        "select first(s_int, time), first(s_long, time), first(s_float, time), first(s_double, time), first(s_bool, time), first(s_string, time) from (select time_type as time, s_int, s_long, s_float, s_double, s_bool, s_string from table_a left join table_b on table_a.time=table_b.time where table_a.device='d2')",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
    tableResultSetEqualTest(
        "select first(s_int), first(s_long), first(s_float), first(s_double), first(s_bool), first(s_string) from (select time_type, s_int, s_long, s_float, s_double, s_bool, s_string from table_a left join table_b on table_a.time=table_b.time where table_a.device='d2')",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
    tableResultSetEqualTest(
        "select first(s_int, time_type), first(s_long, time_type), first(s_float, time_type), first(s_double, time_type), first(s_bool, time_type), first(s_string, time_type) from table_a where table_a.device='d2'",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
  }

  // Checks FIRST returns null for every value type when all grouped candidates are null.
  @Test
  public void testAggregationWithAllNull() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"null,null,null,null,null,null,"};
    tableResultSetEqualTest(
        "select first(s_int, time), first(s_long, time), first(s_float, time), first(s_double, time), first(s_bool, time), first(s_string, time) from (select time_type as time, s_int, s_long, s_float, s_double, s_bool, s_string from table_a left join table_b on table_a.time=table_b.time where table_a.device='d3')",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
    tableResultSetEqualTest(
        "select first(s_int), first(s_long), first(s_float), first(s_double), first(s_bool), first(s_string) from (select time_type, s_int, s_long, s_float, s_double, s_bool, s_string from table_a left join table_b on table_a.time=table_b.time where table_a.device='d3')",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
    tableResultSetEqualTest(
        "select first(s_int, time_type), first(s_long, time_type), first(s_float, time_type), first(s_double, time_type), first(s_bool, time_type), first(s_string, time_type) from table_a where table_a.device='d3'",
        expectedHeader,
        expectedRows,
        FIRST_DATABASE_NAME);
  }

  // Checks FIRST_BY uses the earliest physical row even when its criterion is null.
  @Test
  public void testFirstByPriority() {
    tableResultSetEqualTest(
        "select first_by(s_int, y_criteria), first_by(s_long, y_criteria), first_by(s_float, y_criteria), first_by(s_double, y_criteria), first_by(s_bool, y_criteria), first_by(s_string, y_criteria) from table_a where table_a.device='d1'",
        new String[] {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"},
        new String[] {"1,1,1.0,1.0,true,1s,"},
        FIRST_BY_CLASS_DATABASE_NAME);
  }

  // Confirms FIRST_BY fails when a projected subquery removes all valid time columns.
  @Test
  public void testFirstByNoTimeStamp() {
    String sql =
        "select first_by(s_int, y_criteria) from (select s_int, s_long, s_float, y_criteria from table_a) AS t";
    try (Connection connection =
        EnvFactory.getEnv()
            .getConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + FIRST_BY_CLASS_DATABASE_NAME);
        statement.executeQuery(sql);
      }
      fail("Missing valid time column, the query should fail");
    } catch (SQLException e) {
      assertEquals(
          "701: Missing valid time column. The table must contain either a column with the TIME category or at least one TIMESTAMP column.",
          e.getMessage());
    }
  }

  // Checks FIRST_BY chooses the first qualifying row when criteria are non-null for device d1.
  @Test
  public void testFirstByD1NoNulls() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"5,5,5.0,5.0,false,5s,"};
    runFirstByTest("d1", expectedHeader, expectedRows);
    runFirstByTest2("d1", expectedHeader, expectedRows);
    runFirstByTest3("d1", expectedHeader, expectedRows);
  }

  // Checks FIRST_BY skips null criteria and selects the later qualifying row for device d2.
  @Test
  public void testFirstByD2ForwardTracking() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"10,10,10.0,10.0,true,10s,"};
    runFirstByTest("d2", expectedHeader, expectedRows);
    runFirstByTest2("d2", expectedHeader, expectedRows);
    runFirstByTest3("d2", expectedHeader, expectedRows);
  }

  // Checks FIRST_BY returns null-valued columns when the selected row has null source values.
  @Test
  public void testFirstByD3TargetNull() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"5,5,null,null,null,null,"};
    runFirstByTest("d3", expectedHeader, expectedRows);
    runFirstByTest2("d3", expectedHeader, expectedRows);
    runFirstByTest3("d3", expectedHeader, expectedRows);
  }

  // Checks FIRST_BY returns null when every criterion is null for device d4.
  @Test
  public void testFirstByD4AllNullCriteria() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"null,null,null,null,null,null,"};
    runFirstByTest("d4", expectedHeader, expectedRows);
    runFirstByTest2("d4", expectedHeader, expectedRows);
    runFirstByTest3("d4", expectedHeader, expectedRows);
  }

  // Checks FIRST_BY skips null times and criteria when a later qualifying row exists for d5.
  @Test
  public void testFirstByD5AllTimeNull() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] expectedRows = {"50,50,50.0,50.0,false,50s,"};
    runFirstByTest("d5", expectedHeader, expectedRows);
    runFirstByTest2("d5", expectedHeader, expectedRows);
    runFirstByTest3("d5", expectedHeader, expectedRows);
  }

  private void runFirstByTest(String deviceId, String[] expectedHeader, String[] expectedRows) {
    tableResultSetEqualTest(
        "select first_by(s_int, y_criteria, time), first_by(s_long, y_criteria, time), first_by(s_float, y_criteria, time), first_by(s_double, y_criteria, time), first_by(s_bool, y_criteria, time), first_by(s_string, y_criteria, time) from (select time_type as time, s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria from table_a left join table_b on table_a.time=table_b.time where table_a.device='"
            + deviceId
            + "')",
        expectedHeader,
        expectedRows,
        FIRST_BY_CLASS_DATABASE_NAME);
  }

  private void runFirstByTest2(String deviceId, String[] expectedHeader, String[] expectedRows) {
    tableResultSetEqualTest(
        "select first_by(s_int, y_criteria), first_by(s_long, y_criteria), first_by(s_float, y_criteria), first_by(s_double, y_criteria), first_by(s_bool, y_criteria), first_by(s_string, y_criteria) from (select s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria, time_type from table_a left join table_b on table_a.time=table_b.time where table_a.device='"
            + deviceId
            + "')",
        expectedHeader,
        expectedRows,
        FIRST_BY_CLASS_DATABASE_NAME);
  }

  private void runFirstByTest3(String deviceId, String[] expectedHeader, String[] expectedRows) {
    tableResultSetEqualTest(
        "select first_by(s_int, y_criteria, time_type), first_by(s_long, y_criteria, time_type), first_by(s_float, y_criteria, time_type), first_by(s_double, y_criteria, time_type), first_by(s_bool, y_criteria, time_type), first_by(s_string, y_criteria, time_type) from table_a where table_a.device='"
            + deviceId
            + "'",
        expectedHeader,
        expectedRows,
        FIRST_BY_CLASS_DATABASE_NAME);
  }
}
