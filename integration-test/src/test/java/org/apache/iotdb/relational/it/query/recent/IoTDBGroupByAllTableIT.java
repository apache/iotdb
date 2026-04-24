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

package org.apache.iotdb.relational.it.query.recent;

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
public class IoTDBGroupByAllTableIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE t1(device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD, s3 DOUBLE FIELD)",
        "INSERT INTO t1(time, device_id, s1, s2, s3) VALUES (1, 'a', 10, 100, 1.0)",
        "INSERT INTO t1(time, device_id, s1, s2, s3) VALUES (2, 'a', 20, 200, 2.0)",
        "INSERT INTO t1(time, device_id, s1, s2, s3) VALUES (3, 'a', 30, 300, 3.0)",
        "INSERT INTO t1(time, device_id, s1, s2, s3) VALUES (4, 'b', 40, 400, 4.0)",
        "INSERT INTO t1(time, device_id, s1, s2, s3) VALUES (5, 'b', 50, 500, 5.0)",
        "INSERT INTO t1(time, device_id, s1, s2, s3) VALUES (6, 'c', 60, 600, 6.0)",
        "FLUSH",
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
  public void groupByAllSingleColumnTest() {
    // GROUP BY ALL should infer device_id as the grouping key
    String[] expectedHeader = new String[] {"device_id", "_col1"};
    String[] retArray =
        new String[] {
          "a,3,", "b,2,", "c,1,",
        };
    tableResultSetEqualTest(
        "SELECT device_id, count(s1) FROM t1 GROUP BY ALL ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllMultipleColumnsTest() {
    // GROUP BY ALL should infer device_id and s1 as grouping keys
    String[] expectedHeader = new String[] {"device_id", "s1", "_col2"};
    String[] retArray =
        new String[] {
          "a,10,100,", "a,20,200,", "a,30,300,", "b,40,400,", "b,50,500,", "c,60,600,",
        };
    tableResultSetEqualTest(
        "SELECT device_id, s1, sum(s2) FROM t1 GROUP BY ALL ORDER BY device_id, s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllEquivalenceTest() {
    // GROUP BY ALL and explicit GROUP BY should produce the same results
    String[] expectedHeader = new String[] {"device_id", "_col1"};
    String[] retArray =
        new String[] {
          "a,60,", "b,90,", "c,60,",
        };
    tableResultSetEqualTest(
        "SELECT device_id, sum(s1) FROM t1 GROUP BY ALL ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT device_id, sum(s1) FROM t1 GROUP BY device_id ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllGlobalAggregationTest() {
    // All SELECT items are aggregates, GROUP BY ALL => global aggregation
    String[] expectedHeader = new String[] {"_col0", "_col1"};
    String[] retArray = new String[] {"6,2100,"};
    tableResultSetEqualTest(
        "SELECT count(s1), sum(s2) FROM t1 GROUP BY ALL",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllWithExpressionTest() {
    // GROUP BY ALL with a computed expression (s1 + 1)
    String[] expectedHeader = new String[] {"_col0", "_col1"};
    String[] retArray =
        new String[] {
          "11,100,", "21,200,", "31,300,", "41,400,", "51,500,", "61,600,",
        };
    tableResultSetEqualTest(
        "SELECT s1 + 1, sum(s2) FROM t1 GROUP BY ALL ORDER BY s1 + 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllMixedExpressionTest() {
    // SELECT s1 + avg(s2) FROM t1 GROUP BY ALL
    // should be equivalent to GROUP BY s1
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray =
        new String[] {
          "110.0,", "220.0,", "330.0,", "440.0,", "550.0,", "660.0,",
        };
    tableResultSetEqualTest(
        "SELECT s1 + avg(s2) FROM t1 GROUP BY ALL ORDER BY s1 + avg(s2)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllMixedExpressionEquivalenceTest() {
    // GROUP BY ALL with s1 + avg(s2) should equal GROUP BY s1
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray =
        new String[] {
          "110.0,", "220.0,", "330.0,", "440.0,", "550.0,", "660.0,",
        };
    tableResultSetEqualTest(
        "SELECT s1 + avg(s2) FROM t1 GROUP BY ALL ORDER BY s1 + avg(s2)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT s1 + avg(s2) FROM t1 GROUP BY s1 ORDER BY s1 + avg(s2)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllMixedWithPureColumnTest() {
    // SELECT device_id, s1 + avg(s2) FROM t1 GROUP BY ALL
    // should be equivalent to GROUP BY device_id, s1
    String[] expectedHeader = new String[] {"device_id", "_col1"};
    String[] retArray =
        new String[] {
          "a,110.0,", "a,220.0,", "a,330.0,", "b,440.0,", "b,550.0,", "c,660.0,",
        };
    tableResultSetEqualTest(
        "SELECT device_id, s1 + avg(s2) FROM t1 GROUP BY ALL ORDER BY device_id, s1 + avg(s2)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT device_id, s1 + avg(s2) FROM t1 GROUP BY device_id, s1 ORDER BY device_id, s1 + avg(s2)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllMixedMultipleSubExpressionsTest() {
    // SELECT s1 + s3 + avg(s2) FROM t1 GROUP BY ALL
    // should be equivalent to GROUP BY s1, s3
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray =
        new String[] {
          "111.0,", "222.0,", "333.0,", "444.0,", "555.0,", "666.0,",
        };
    tableResultSetEqualTest(
        "SELECT s1 + s3 + avg(s2) FROM t1 GROUP BY ALL ORDER BY s1 + s3 + avg(s2)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT s1 + s3 + avg(s2) FROM t1 GROUP BY s1, s3 ORDER BY s1 + s3 + avg(s2)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllWithHavingTest() {
    // GROUP BY ALL with HAVING clause
    String[] expectedHeader = new String[] {"device_id", "_col1"};
    String[] retArray =
        new String[] {
          "a,3,",
        };
    tableResultSetEqualTest(
        "SELECT device_id, count(s1) FROM t1 GROUP BY ALL HAVING count(s1) >= 3 ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllWithWhereTest() {
    // GROUP BY ALL with WHERE clause
    String[] expectedHeader = new String[] {"device_id", "_col1"};
    String[] retArray =
        new String[] {
          "a,2,", "b,2,",
        };
    tableResultSetEqualTest(
        "SELECT device_id, count(s1) FROM t1 WHERE s1 >= 20 GROUP BY ALL ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllQuantifierBackwardCompatibilityTest() {
    // GROUP BY ALL s1 (ALL as set-quantifier, not GROUP BY ALL feature)
    String[] expectedHeader = new String[] {"s1", "_col1"};
    String[] retArray =
        new String[] {
          "10,100,", "20,200,", "30,300,", "40,400,", "50,500,", "60,600,",
        };
    tableResultSetEqualTest(
        "SELECT s1, sum(s2) FROM t1 GROUP BY ALL s1 ORDER BY s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void groupByAllMultipleAggregatesTest() {
    // Multiple aggregation functions with GROUP BY ALL
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3"};
    String[] retArray =
        new String[] {
          "a,3,60,200.0,", "b,2,90,450.0,", "c,1,60,600.0,",
        };
    tableResultSetEqualTest(
        "SELECT device_id, count(s1), sum(s1), avg(s2) FROM t1 GROUP BY ALL ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
