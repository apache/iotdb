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
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAggregationLastByInGroupIT {
  private static final String DATABASE_NAME = "test_last_by_in_group_agg";

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
            + "y_criteria INT32 FIELD, " // Acts as s2
            + "partition STRING FIELD)",
        "CREATE TABLE table_b(" + "time TIMESTAMP TIME, " + "s_back INT32 FIELD)",

        // Batch Insert: 5 Partitions (p1-p5) using negative values
        "INSERT INTO table_a(time, s_int, s_long, s_float, s_double, s_bool, s_string, time_type, y_criteria, partition) VALUES "

            // Partition p1: s2 has NO NULLs in Valid Times.
            + "(1,    -99,   -99,   -99.0,   -99.0,   true,  '-99s',  NULL, -99,  'p1'),"
            + "(2,    -200,  -200,  -200.0,  -200.0,  true,  '-200s', -200, -200, 'p1'),"
            + "(3,    -100,  -100,  -100.0,  -100.0,  true,  '-100s', -100, -100, 'p1'),"
            + "(4,    -10,   -10,   -10.0,   -10.0,   true,  '-10s',  -10,  -10,  'p1'),"
            + "(5,    -5,    -5,    -5.0,    -5.0,    false, '-5s',   -5,   -5,   'p1')," // Target

            // Partition p2: s2 has NULLs (Backtracking).
            + "(6,    -88,   -88,   -88.0,   -88.0,   true,  '-88s',  NULL, -88,  'p2'),"
            + "(7,    -5,    -5,    -5.0,    -5.0,    false, '-5s',   -5,   NULL, 'p2'),"
            + "(8,    -8,    -8,    -8.0,    -8.0,    false, '-8s',   -8,   NULL, 'p2'),"
            + "(9,    -10,   -10,   -10.0,   -10.0,   true,  '-10s',  -10,  -10,  'p2')," // Target
            + "(10,   -20,   -20,   -20.0,   -20.0,   true,  '-20s',  -20,  -20,  'p2'),"

            // Partition p3: s1 (value) has NULLs.
            + "(11,   -77,   -77,   -77.0,   -77.0,   true,  '-77s',  NULL, -77,  'p3'),"
            + "(12,   NULL,  NULL,  -5.0,    -5.0,    NULL,   NULL,   -5,   -5,   'p3')," // Target
            + "(13,   -10,   -10,   -10.0,   -10.0,   true,  '-10s',  -10,  NULL, 'p3'),"
            + "(14,   -20,   -20,   -20.0,   -20.0,   true,  '-20s',  -20,  -20,  'p3'),"

            // Partition p4: s2 is ALL NULLs.
            + "(15,   -66,   -66,   -66.0,   -66.0,   true,  '-66s',  NULL, NULL, 'p4'),"
            + "(16,   -5,    -5,    -5.0,    -5.0,    false, '-5s',   -5,   NULL, 'p4'),"
            + "(17,   -10,   -10,   -10.0,   -10.0,   true,  '-10s',  -10,  NULL, 'p4'),"
            + "(18,   -20,   -20,   -20.0,   -20.0,   true,  '-20s',  -20,  NULL, 'p4'),"

            // Partition p5: All time_type are NULL.
            + "(19,   -10,   -10,   -10.0,   -10.0,   true,  '-10s',  NULL,  NULL, 'p5'),"
            + "(20,   -50,   -50,   -50.0,   -50.0,   false, '-50s',  NULL, -50, 'p5')" // Target
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
  public void testGroupedLastByAggregation() {

    // Expected Header: partition column + 6 aggregation results
    String[] expectedHeader = {"partition", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6"};

    // Expected Results:
    String[] retArray = {
      "p1,-5,-5,-5.0,-5.0,false,-5s,",
      "p2,-10,-10,-10.0,-10.0,true,-10s,",
      "p3,null,null,-5.0,-5.0,null,null,",
      "p4,null,null,null,null,null,null,",
      "p5,-50,-50,-50.0,-50.0,false,-50s,"
    };

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "last_by(s_int, y_criteria, time), "
            + "last_by(s_long, y_criteria, time), "
            + "last_by(s_float, y_criteria, time), "
            + "last_by(s_double, y_criteria, time), "
            + "last_by(s_bool, y_criteria, time), "
            + "last_by(s_string, y_criteria, time) "
            + "from "
            // SubQuery: Rename time_type to 'ts' to avoid ambiguity
            + "(select time_type as time, s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria, partition "
            + "from table_a left join table_b on table_a.time=table_b.time) "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "last_by(s_int, y_criteria), "
            + "last_by(s_long, y_criteria), "
            + "last_by(s_float, y_criteria), "
            + "last_by(s_double, y_criteria), "
            + "last_by(s_bool, y_criteria), "
            + "last_by(s_string, y_criteria) "
            + "from "
            + "(select time_type, s_int, s_long, s_float, s_double, s_bool, s_string, y_criteria, partition "
            + "from table_a left join table_b on table_a.time=table_b.time) "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select "
            + "partition, "
            + "last_by(s_int, y_criteria, time_type), "
            + "last_by(s_long, y_criteria, time_type), "
            + "last_by(s_float, y_criteria, time_type), "
            + "last_by(s_double, y_criteria, time_type), "
            + "last_by(s_bool, y_criteria, time_type), "
            + "last_by(s_string, y_criteria, time_type) "
            + "from table_a "
            + "group by partition "
            + "order by partition",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
