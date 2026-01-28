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
public class IoTDBAggregationFirstByIT {
  private static final String DATABASE_NAME = "test_first_by_agg";

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
            + "time_type TIMESTAMP FIELD, "
            + "y_criteria INT32 FIELD)", // Acts as s2
        "CREATE TABLE table_b(" + "time TIMESTAMP TIME, " + "s_back INT32 FIELD)",

        // Batch Insert
        "INSERT INTO table_a(time, device, s_int, s_long, s_float, s_double, s_bool, s_string, time_type, y_criteria) VALUES "

            // Case 1: s2 (y_criteria) has NO NULLs in Valid Times.
            // Device: d1
            + "(1,    'd1',  1,    1,    1.0,    1.0,   true,  '1s',   NULL, 1),"
            + "(200,  'd1', 200,  200,  200.0,  200.0,  true,  '200s', 200,  200),"
            + "(100,  'd1', 100,  100,  100.0,  100.0,  true,  '100s', 100,  100),"
            + "(10,   'd1', 10,   10,   10.0,   10.0,   true,  '10s',  10,   10),"
            + "(5,    'd1', 5,    5,    5.0,    5.0,    false, '5s',   5,    5)," // Target

            // Case 2: s2 (y_criteria) has NULLs
            // Device: d2
            + "(2,    'd2', 2,    2,    2.0,    2.0,    true,  '2s',   NULL,  2),"
            + "(5,    'd2', 5,    5,    5.0,    5.0,    false, '5s',   5,    NULL),"
            + "(8,    'd2', 8,    8,    8.0,    8.0,    false, '8s',   8,    NULL),"
            + "(10,   'd2', 10,   10,   10.0,   10.0,   true,  '10s',  10,   10)," // Target
            + "(20,   'd2', 20,   20,   20.0,   20.0,   true,  '20s',  20,   20),"

            // Case 3: s1 (value) has NULLs.
            // Device: d3
            + "(3,    'd3', 3,    3,    3.0,    3.0,    true,  '3s',  NULL,  3),"
            + "(5,    'd3', 5,    5,    NULL,   NULL,   NULL,  NULL,   5,    5)," // Target
            + "(10,   'd3', 10,   10,   10.0,   10.0,   true,  '10s',  10,   NULL),"
            + "(20,   'd3', 20,   20,   20.0,   20.0,   true,  '20s',  20,   20),"

            // Case 4: s2 (y_criteria) is ALL NULLs.
            // Device: d4
            + "(4,    'd4', 66,   66,   66.0,   66.0,   true,  '66s',  NULL, NULL),"
            + "(5,    'd4', 5,    5,    5.0,    5.0,    false, '5s',   5,    NULL),"
            + "(10,   'd4', 10,   10,   10.0,   10.0,   true,  '10s',  10,   NULL),"
            + "(20,   'd4', 20,   20,   20.0,   20.0,   true,  '20s',  20,   NULL),"

            // Case 5: All time_type are NULL.
            // Device: d5
            + "(1,    'd5', 10,   10,   10.0,   10.0,   true,  '10s',  NULL, NULL),"
            + "(2,    'd5', 50,   50,   50.0,   50.0,   false, '50s',  NULL, 50)" // target
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
  public void testFirstBy_d1_NoNulls() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] retArray = {"5,5,5.0,5.0,false,5s,"};
    runTest("d1", expectedHeader, retArray);
  }

  @Test
  public void testFirstBy_d2_ForwardTracking() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] retArray = {"10,10,10.0,10.0,true,10s,"};
    runTest("d2", expectedHeader, retArray);
  }

  @Test
  public void testFirstBy_d3_TargetNull() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    String[] retArray = {"5,5,null,null,null,null,"};
    runTest("d3", expectedHeader, retArray);
  }

  @Test
  public void testFirstBy_d4_AllNullCriteria() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    // Expected: No valid s2 found.
    String[] retArray = {"null,null,null,null,null,null,"};
    runTest("d4", expectedHeader, retArray);
  }

  @Test
  public void testFirstBy_d5_AllTimeNull() {
    String[] expectedHeader = {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5"};
    // Expected: The row with y_criteria=NULL is skipped. The row with y_criteria=50 is picked.
    String[] retArray = {"50,50,50.0,50.0,false,50s,"};
    runTest("d5", expectedHeader, retArray);
  }

  private void runTest(String deviceId, String[] expectedHeader, String[] retArray) {
    tableResultSetEqualTest(
        "select "
            + "first_by(s_int, y_criteria, time), "
            + "first_by(s_long, y_criteria, time), "
            + "first_by(s_float, y_criteria, time), "
            + "first_by(s_double, y_criteria, time), "
            + "first_by(s_bool, y_criteria, time), "
            + "first_by(s_string, y_criteria, time) "
            + "from "
            + "(select time_type as time, s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria "
            + "from table_a left join table_b on table_a.time=table_b.time "
            + "where table_a.device='"
            + deviceId
            + "') ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
