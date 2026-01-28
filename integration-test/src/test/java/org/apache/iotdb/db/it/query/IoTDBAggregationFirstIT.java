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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAggregationFirstIT {

  private static final String DATABASE_NAME = "test_null_time_aggs_all_types_first";

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

        // batch insert：d1(5 rows) + d2(4 rows) + d3(2 rows)
        // Values are changed to positive numbers
        "INSERT INTO table_a(time, device, s_int, s_long, s_float, s_double, s_bool, s_string, time_type) VALUES "

            // --- Device d1 ---
            + "(100, 'd1', 100, 100, 100.0, 100.0, true, '100s', 100),"
            + "(50,  'd1', 50,  50,  50.0,  50.0,  false, '50s',  NULL),"
            + "(10,  'd1', 10,  10,  10.0,  10.0,  true,  '10s',  10),"
            + "(5,   'd1', 5,   5,   5.0,   5.0,   false, '5s',   5),"
            + "(-50, 'd1', -50, -50, -50.0, -50.0, false, '-50s', NULL),"

            // --- Device d2 ---
            + "(80,  'd2', 80,  80,  80.0,  80.0,  true,  '80s',  NULL),"
            + "(9,   'd2', NULL, 9,  9.0,   NULL,  false,  NULL,  9),"
            + "(40,  'd2', 40,  40,  40.0,  40.0,  false, '40s',  40),"
            + "(-20, 'd2', -20, -20, -20.0, -20.0, true,  '-20s', NULL),"
            + "(10,  'd2', 10,  NULL, NULL, 10.0,  NULL,  '10s',  10),"

            // --- Device d3 (Pure NULL test) ---
            // d3: all time_type are values, but data cols are NULL (to test if first returns null
            // value correctly)
            + "(200, 'd3', null, null, null, null, null, null, 200),"
            + "(150, 'd3', null, null, null, null, null, null, 150)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAggregation() {

    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};

    // Expected Result for d1:
    // Valid time_types: 100, 10, 5.
    // FIRST(..., time) looks for MIN(time_type).
    // MIN(time_type) is 5.
    // Corresponding values at time_type=5: s_int=5, s_long=5, ..., s_bool=false, s_string='5s'
    String[] retArray = {"5,5,5.0,5.0,false,5s,"};

    tableResultSetEqualTest(
        "select "
            + "first(s_int, time), "
            + "first(s_long, time), "
            + "first(s_float, time), "
            + "first(s_double, time), "
            + "first(s_bool, time), "
            + "first(s_string, time) "
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
  }

  @Test
  public void testAggregationWithNullValue() {

    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};

    // Expected Result for d2:
    // Valid time_types: 40, 5, 4.
    // MIN(time_type) is 4.
    // Row at time_type=4:
    // s_int=4, s_long=NULL, s_float=NULL, s_double=4.0, s_bool=NULL, s_string='4s'
    String[] retArray = {"10,9,9.0,10.0,false,10s,"};

    tableResultSetEqualTest(
        "select "
            + "first(s_int, time), "
            + "first(s_long, time), "
            + "first(s_float, time), "
            + "first(s_double, time), "
            + "first(s_bool, time), "
            + "first(s_string, time) "
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
  }

  @Test
  public void testAggregationWithAllNull() {

    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] retArray = {"null,null,null,null,null,null,"};

    tableResultSetEqualTest(
        "select "
            + "first(s_int, time), "
            + "first(s_long, time), "
            + "first(s_float, time), "
            + "first(s_double, time), "
            + "first(s_bool, time), "
            + "first(s_string, time) "
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
  }
}
