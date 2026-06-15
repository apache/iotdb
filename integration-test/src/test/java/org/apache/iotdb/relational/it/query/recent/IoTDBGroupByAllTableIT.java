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
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBGroupByAllTableIT {
  protected static final String DATABASE_NAME = "test";

  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(device_id STRING TAG, s1 DOUBLE FIELD, x INT32 FIELD, y INT32 FIELD)",
        "INSERT INTO table1(time, device_id, s1, x, y) VALUES (1, 'd1', 20.0, 1, 10)",
        "INSERT INTO table1(time, device_id, s1, x, y) VALUES (2, 'd1', 30.0, 2, 20)",
        "INSERT INTO table1(time, device_id, s1, x, y) VALUES (3, 'd2', 20.0, 1, 30)",
        "INSERT INTO table1(time, device_id, s1, x, y) VALUES (4, 'd2', 40.0, 3, 40)",
        "FLUSH"
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
  public void testBasicInference() {
    String[] expectedHeader = new String[] {"device_id", "_col1"};
    String[] retArray = new String[] {"d1,25.0,", "d2,30.0,"};

    tableResultSetEqualTest(
        "SELECT device_id, avg(s1) FROM table1 GROUP BY ALL ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"d", "avg_s1"};
    retArray = new String[] {"d1,25.0,", "d2,30.0,"};
    tableResultSetEqualTest(
        "SELECT device_id AS d, avg(s1) AS avg_s1 FROM table1 GROUP BY ALL ORDER BY d",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testExpressionAndConstantInference() {
    String[] expectedHeader = new String[] {"_col0", "_col1"};
    String[] retArray = new String[] {"false,2,", "true,2,"};

    tableResultSetEqualTest(
        "SELECT s1 > 25, count(*) FROM table1 GROUP BY ALL ORDER BY 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray = new String[] {"factory_01,4,"};
    tableResultSetEqualTest(
        "SELECT 'factory_01', count(*) FROM table1 GROUP BY ALL",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray = new String[] {"100,4,"};
    tableResultSetEqualTest(
        "SELECT 100, count(*) FROM table1 GROUP BY ALL", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    retArray = new String[] {"d1,1,25.0,", "d2,1,30.0,"};
    tableResultSetEqualTest(
        "SELECT device_id, 1, avg(s1) FROM table1 GROUP BY ALL ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testAggregateExpressionsAreSkipped() {
    String[] expectedHeader = new String[] {"_col0", "_col1"};
    String[] retArray = new String[] {"7.0,7.0,"};

    tableResultSetEqualTest(
        "SELECT sum(x), abs(sum(x)) FROM table1 GROUP BY ALL",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"27.5,"};
    tableResultSetEqualTest(
        "SELECT avg(s1) FROM table1 GROUP BY ALL", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testMixedAggregateExpressionValidation() {
    tableAssertTestFail(
        "SELECT s1 + avg(y) FROM table1 GROUP BY ALL",
        "must be an aggregate expression or appear in GROUP BY clause",
        DATABASE_NAME);

    String[] expectedHeader = new String[] {"s1", "_col1"};
    String[] retArray = new String[] {"20.0,40.0,", "30.0,50.0,", "40.0,80.0,"};

    tableResultSetEqualTest(
        "SELECT s1, s1 + avg(y) FROM table1 GROUP BY ALL ORDER BY s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSelectAllAfterExpansion() {
    String[] expectedHeader = new String[] {"time", "device_id", "s1", "x", "y", "_col5"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,20.0,1,10,1,",
          "1970-01-01T00:00:00.002Z,d1,30.0,2,20,1,",
          "1970-01-01T00:00:00.003Z,d2,20.0,1,30,1,",
          "1970-01-01T00:00:00.004Z,d2,40.0,3,40,1,"
        };

    tableResultSetEqualTest(
        "SELECT *, count(*) FROM table1 GROUP BY ALL ORDER BY time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testWindowFunctionValidation() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    String[] retArray = new String[] {"d1,1,25.0,", "d2,1,30.0,"};

    tableResultSetEqualTest(
        "SELECT device_id, count(*) OVER (PARTITION BY device_id), avg(s1) FROM table1 GROUP BY ALL ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "s1"};
    retArray = new String[] {"d1,20.0,", "d2,20.0,", "d1,30.0,", "d2,40.0,"};
    tableResultSetEqualTest(
        "SELECT device_id, s1 FROM table1 GROUP BY ALL ORDER BY rank() OVER (ORDER BY s1), device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT device_id, rank() OVER (ORDER BY s1), avg(s1) FROM table1 GROUP BY ALL",
        "ORDER BY expression 's1' must be an aggregate expression or appear in GROUP BY clause",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT device_id, count(*) OVER (PARTITION BY s1), avg(s1) FROM table1 GROUP BY ALL",
        "PARTITION BY expression 's1' must be an aggregate expression or appear in GROUP BY clause",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT device_id, avg(s1) FROM table1 GROUP BY ALL ORDER BY rank() OVER (ORDER BY s1)",
        "ORDER BY expression 's1' must be an aggregate expression or appear in GROUP BY clause",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT sum(count(*) OVER (PARTITION BY device_id)) FROM table1 GROUP BY ALL",
        "Cannot nest window functions inside aggregation",
        DATABASE_NAME);
  }

  @Test
  public void testWindowFrameValidation() {
    tableAssertTestFail(
        "SELECT device_id, count(*) OVER (PARTITION BY device_id ORDER BY device_id ROWS BETWEEN x PRECEDING AND CURRENT ROW), avg(s1) FROM table1 GROUP BY ALL",
        "Window frame start must be an aggregate expression or appear in GROUP BY clause",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT device_id, count(*) OVER (PARTITION BY device_id ORDER BY device_id ROWS BETWEEN CURRENT ROW AND x FOLLOWING), avg(s1) FROM table1 GROUP BY ALL",
        "Window frame end must be an aggregate expression or appear in GROUP BY clause",
        DATABASE_NAME);
  }

  @Test
  public void testIllegalAutoGroupByCombination() {
    tableAssertTestFail(
        "SELECT device_id, count(*) FROM table1 GROUP BY ALL, device_id",
        "GROUP BY ALL cannot be combined with explicit grouping elements",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT device_id, count(*) FROM table1 GROUP BY ALL, ROLLUP(device_id)",
        "GROUP BY ALL cannot be combined with explicit grouping elements",
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT device_id, count(*) FROM table1 GROUP BY CUBE(device_id), ALL",
        "GROUP BY ALL cannot be combined with explicit grouping elements",
        DATABASE_NAME);
  }
}
