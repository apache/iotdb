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

package org.apache.iotdb.relational.it.query.recent.subquery.uncorrelated;

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
import static org.apache.iotdb.relational.it.query.recent.subquery.SubqueryDataSetUtils.CREATE_SQLS;
import static org.apache.iotdb.relational.it.query.recent.subquery.SubqueryDataSetUtils.DATABASE_NAME;
import static org.apache.iotdb.relational.it.query.recent.subquery.SubqueryDataSetUtils.NUMERIC_MEASUREMENTS;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBUncorrelatedQuantifiedComparisonIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(CREATE_SQLS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAnyAndSomeComparisonInWhereClauseWithoutNull() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: where s > any (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > any (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s > some (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > some (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s >= any (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s >= any (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s >= some (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s >= some (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s < any (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < any (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where < some (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < some (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s <= any (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s <= any (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s <= some (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s <= some (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s = any (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s = any (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s = some (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s = some (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s != any (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s != any (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s != some (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s != some (SELECT %s FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testAllComparisonInWhereClauseWithoutNull() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: where s > all (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > all (SELECT %s FROM table3 WHERE device_id = 'd01')";
    retArray = new String[] {"50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s >= all (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s >= all (SELECT %s FROM table3 WHERE device_id = 'd01')";
    retArray = new String[] {"40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s < all (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < all (SELECT %s FROM table3 WHERE device_id = 'd01')";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s <= all (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s <= all (SELECT %s FROM table3 WHERE device_id = 'd01')";
    retArray = new String[] {"30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s = all (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s = all (SELECT %s FROM table3 WHERE device_id = 'd01')";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s != all (subquery), s does not contain null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s != all (SELECT %s FROM table3 WHERE device_id = 'd01')";
    retArray = new String[] {"50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testAnyAndSomeComparisonInWhereClauseWithNull() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: where s1 > any (subquery), s1 in table3 contains null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > any (SELECT s1 FROM table3)";
    retArray = new String[] {"40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 > some (subquery), s1 in table3 contains null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > some (SELECT s1 FROM table3)";
    retArray = new String[] {"40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 >= any (subquery), s1 in table3 contains null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s >= any (SELECT s1 FROM table3)";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 >= some (subquery), s1 in table3 contains null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s >= some (SELECT s1 FROM table3)";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 < any (subquery), s1 in table3 contains null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < any (SELECT s1 FROM table3)";
    retArray = new String[] {"30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 < some (subquery), s1 in table3 contains null value
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < some (SELECT s1 FROM table3)";
    retArray = new String[] {"30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testAllComparisonInWhereClauseWithNull() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: where s1 > all (subquery), s1 in table3 contains null value.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > all (SELECT s1 FROM table3)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 >= all (subquery), s1 in table3 contains null value.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s >= all (SELECT s1 FROM table3)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 < all (subquery), s1 in table3 contains null value.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < all (SELECT s1 FROM table3)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 <= all (subquery), s1 in table3 contains null value.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s <= all (SELECT s1 FROM table3)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 = all (subquery), s1 in table3 contains null value.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s = all (SELECT s1 FROM table3)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s1 != all (subquery), s1 in table3 contains null value.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and cast(%s as INT32) != all (SELECT s1 FROM table3)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testQuantifiedComparisonInWhereWithExpression() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    sql =
        "SELECT cast(%s + 10 AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s + 10 > any (SELECT %s + 10 FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"50,", "60,", "70,", "80,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    sql =
        "SELECT cast(%s + 10 AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s + 10 > some (SELECT %s + 10 FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"50,", "60,", "70,", "80,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    sql =
        "SELECT cast(%s + 10 AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s + 10 >= all (SELECT %s + 10 FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"80,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testQuantifiedComparisonInHavingClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: having s >= any(subquery)
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having count(*) + 25 >= any(SELECT cast(s1 as INT64) from table3 where device_id = 'd01')";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray =
        new String[] {
          "d01,5,", "d03,5,", "d05,5,", "d07,5,", "d09,5,", "d11,5,", "d13,5,", "d15,5,"
        };
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: having s >= some(subquery)
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having count(*) + 25 >= some(SELECT cast(s1 as INT64) from table3 where device_id = 'd01')";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray =
        new String[] {
          "d01,5,", "d03,5,", "d05,5,", "d07,5,", "d09,5,", "d11,5,", "d13,5,", "d15,5,"
        };
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: having s >= all(subquery)
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having count(*) + 35 >= all(SELECT cast(s1 as INT64) from table3 where device_id = 'd01')";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray =
        new String[] {
          "d01,5,", "d03,5,", "d05,5,", "d07,5,", "d09,5,", "d11,5,", "d13,5,", "d15,5,"
        };
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  public void testQuantifiedComparisonInSelectClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: select s > any(subquery)
    sql =
        "SELECT %s > any(SELECT (%s) from table3 WHERE device_id = 'd01') from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"false,", "true,", "true,", "true,", "true,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s > some(subquery)
    sql =
        "SELECT %s > some(SELECT (%s) from table3 WHERE device_id = 'd01') from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"false,", "true,", "true,", "true,", "true,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s > all(subquery)
    sql =
        "SELECT %s > all(SELECT (%s) from table3 WHERE device_id = 'd01') from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"false,", "false,", "false,", "false,", "false,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s < any(subquery), subquery contains null value
    sql = "SELECT %s < any(SELECT (%s) from table3) from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"null,", "null,", "null,", "null,", "null,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s < some(subquery), subquery contains null value
    sql = "SELECT %s < some(SELECT (%s) from table3) from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"null,", "null,", "null,", "null,", "null,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s <= any(subquery), subquery contains null value
    sql = "SELECT %s <= any(SELECT (%s) from table3) from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"true,", "null,", "null,", "null,", "null,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s <= some(subquery), subquery contains null value
    sql = "SELECT %s <= some(SELECT (%s) from table3) from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"true,", "null,", "null,", "null,", "null,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s != all(subquery), subquery contains null value
    sql = "SELECT %s != all(SELECT (%s) from table3) from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"false,", "false,", "null,", "null,", "null,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s != all(subquery), subquery result contains null value and s not in
    // non-null
    // value result set
    sql =
        "SELECT %s != all(SELECT (%s) from table3 where device_id = 'd_null') from table1 where device_id = 'd02' and %s != 30";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"null,", "null,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }
  }

  @Test
  public void testQuantifiedComparisonLegalityCheck() {
    // Legality check: only support any/some/all quantifier
    tableAssertTestFail(
        "select s1 from table1 where s1 > any_value (select s1 from table3)",
        "mismatched input",
        DATABASE_NAME);
  }
}
