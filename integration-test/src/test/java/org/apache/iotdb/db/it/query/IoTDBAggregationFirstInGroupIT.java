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
public class IoTDBAggregationFirstInGroupIT {

  private static final String DATABASE_NAME = "test_grouped_first_agg";

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
            + "(100,  100,  100,  100.0,  100,    100,    100,    100,  'p2'),"

            // Partition 'p3': Only Null Time Scenario
            // Logic: No Valid Time. "First Null Wins" strategy applies.
            + "(68,   80,   80,   80.0,   80.0,   true,  '80s',   NULL, 'p3'),"
            + "(288,  40,   40,   40.0,   40.0,   false, '40s',   NULL, 'p3')"
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
  }
}
