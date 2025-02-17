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
public class IoTDBUncorrelatedInPredicateSubqueryIT {
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
  public void testInPredicateSubqueryInWhereClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: where s in (subquery)
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s in (SELECT (%s) from table3 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s not in (subquery)
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s not in (SELECT (%s) FROM table3 WHERE device_id = 'd01')";
    retArray = new String[] {"50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s in (subquery), subquery returns empty set
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s in (SELECT (%s) FROM table3 WHERE device_id = 'd_empty')";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s not in (subquery), subquery returns empty set. Should return all rows
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s not in (SELECT (%s) FROM table3 WHERE device_id = 'd_empty')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s in (subquery), subquery contains scalar subquery.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s in (SELECT min(%s) from table3 WHERE device_id = 'd01')";
    retArray = new String[] {"30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s in (subquery), subquery contains scalar subquery.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s in (SELECT (%s) from table3 WHERE device_id = 'd01' and (%s) > (select avg(%s) from table3 where device_id = 'd01'))";
    retArray = new String[] {"40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(
              sql, measurement, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s in (subquery), subquery contains expression.
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and cast(%s AS INT32) in (SELECT cast(((%s) + 30) AS INT32) from table3 WHERE device_id = 'd01')";
    retArray = new String[] {"60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: where s in (subquery), s contains null value
    sql = "SELECT s1 FROM table3 WHERE device_id = 'd_null' and s1 in (SELECT s1 from table3)";
    expectedHeader = new String[] {"s1"};
    retArray = new String[] {"30,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // Test case: where s not in (subquery), s contains null value, the resutl should be empty
    sql = "SELECT s1 FROM table3 WHERE device_id = 'd_null' and s1 not in (SELECT s1 from table3)";
    expectedHeader = new String[] {"s1"};
    retArray = new String[] {};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testInPredicateSubqueryInHavingClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: having s in (subquery)
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having count(*) + 25 in (SELECT cast(s1 as INT64) from table3 where device_id = 'd01')";
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

    // Test case: having s not in (subquery)
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having count(*) + 30 not in (SELECT cast(s1 as INT64) from table3 where device_id = 'd01') and count(*) > 3";
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

    // Test case: having s in (subquery), subquery returns empty set
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having count(*) + 25 in (SELECT cast(s1 as INT64) from table3 where device_id = 'd010')";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: having s not in (subquery), subquery returns empty set, should return all rows
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having count(*) + 25 not in (SELECT cast(s1 as INT64) from table3 where device_id = 'd11') and count(*) > 3";
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

  @Test
  public void testInPredicateSubqueryInSelectClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: select s in (subquery)
    sql =
        "SELECT %s in (SELECT (%s) from table3 WHERE device_id = 'd01') from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"true,", "true,", "false,", "false,", "false,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s not in (subquery)
    sql =
        "SELECT %s not in (SELECT (%s) from table3 WHERE device_id = 'd01') from table1 where device_id = 'd01'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"false,", "false,", "true,", "true,", "true,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: select s in (subquery), s contains null value. The result should also be null when
    // s is null.
    sql = "SELECT s1 in (SELECT s1 from table3) from table3 where device_id = 'd_null'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"true,", "null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // Test case: select s in (subquery), s contains null value. The result should also be null when
    // s is null.
    sql = "SELECT s1 not in (SELECT s1 from table3) from table3 where device_id = 'd_null'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"false,", "null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // Test case: select s in (subquery), s contains null value. 36 not in(30, 40, null) returns
    // null
    sql = "SELECT s1 not in (SELECT s1 from table3) from table1 where device_id = 'd02'";
    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"null,", "false,", "null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testInPredicateSubqueryLegalityCheck() {
    // Legality check: Multiple parentheses around subquery, this behaves the same as Trino
    tableAssertTestFail(
        "select s1 from table1 where device_id = 'd01' and s1 not in ((select s1 from table3 where device_id = 'd01'))",
        "701: Scalar sub-query has returned multiple rows.",
        DATABASE_NAME);

    // Legality check: Join key type mismatch.(left key is int and right key is double)
    tableAssertTestFail(
        "select s1 from table1 where device_id = 'd01' and s1 in (select s1 + 30.0 from table3 where device_id = 'd01')",
        "701: Join key type mismatch",
        DATABASE_NAME);

    // Legality check: Row Type is not supported for now.
    tableAssertTestFail(
        "select s1, s2 in (select (s1, s2) from table1) from table1",
        "701: Subquery must return only one column for now. Row Type is not supported for now.",
        DATABASE_NAME);

    // Legality check: Row Type is not supported for now.
    tableAssertTestFail(
        "select (s1, s2) in (select (s1, s2) from table1) from table1",
        "701: Subquery must return only one column for now. Row Type is not supported for now.",
        DATABASE_NAME);

    // Legality check: subquery can not be parsed(without parentheses)
    tableAssertTestFail(
        "select s1 from table1 where s1 in select s1 from table1",
        "mismatched input",
        DATABASE_NAME);

    // Legality check: subquery can not be parsed
    tableAssertTestFail(
        "select s1 from table1 where s1 in (select s1 from)", "mismatched input", DATABASE_NAME);
  }
}
