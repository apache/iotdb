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

package org.apache.iotdb.relational.it.query.recent.subquery;

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
public class IoTDBUncorrelatedScalarSubqueryIT {

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

  // region test in where clause
  @Test
  public void testScalarSubqueryAfterComparisonInOneTable() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: s equals to the maximum value of s in table1
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s = (SELECT max(%s) from table1 WHERE device_id = 'd01')";
    retArray = new String[] {"70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: s not equals to the maximum value of s in table1
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s != ((SELECT max(%s) FROM table1 WHERE device_id = 'd01'))";
    retArray = new String[] {"30,", "40,", "50,", "60,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: s greater than the average value of s in table1
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s >= ((SELECT AVG(%s) FROM table1 WHERE device_id = 'd01'))";
    retArray = new String[] {"50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: s greater than the max value of s in table1
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > ((SELECT max(%s) FROM table1 WHERE device_id = 'd01'))";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: s is less than the maximum value of s in table1 and greater than the minimum value
    // of s in table1
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < (SELECT max(%s) from table1 WHERE device_id = 'd01') and %s > (SELECT min(%s) from table1 WHERE device_id = 'd01') ";
    retArray = new String[] {"40,", "50,", "60,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(
              sql, measurement, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: s greater than the avg value of s in table1 and s5 = true
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > ((SELECT avg(%s) FROM table1 WHERE device_id = 'd01' and s5 = true))";
    retArray = new String[] {"60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: s greater than the count value of s in table1
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > (SELECT count(%s) FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: s less than the sum value of s in table1
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < (SELECT sum(%s) FROM table1 WHERE device_id = 'd01')";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: subquery is not aggregation but returns exactly one row
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s = (SELECT %s FROM table1 WHERE device_id = 'd01' and %s = 30)";
    retArray = new String[] {"30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(
              sql, measurement, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testScalarSubqueryAfterComparisonInDifferentTables() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: s greater than the count value of s in table2
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s > (SELECT count(%s) from table2)";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: s less than the max value of s in table2 * the count value of s in table2 * 10
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s < ((SELECT max(%s) from table2) * (SELECT count(%s) from table2)) * 10";
    retArray = new String[] {"30,", "40,", "50,", "60,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testNestedScalarSubqueryAfterComparison() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: nested scalar subquery in where clause
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s = (SELECT max(%s) from table1 where %s = (SELECT max(%s) from table1 WHERE device_id = 'd01'))";
    retArray = new String[] {"70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(
              sql, measurement, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: nested scalar subquery with table subquery
    sql =
        "SELECT %s from (SELECT cast(%s AS INT32) as %s FROM table1 WHERE device_id = 'd01' and %s = (SELECT max(%s) from table1 where %s = (SELECT max(%s) from table1 WHERE device_id = 'd01')))";
    retArray = new String[] {"70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(
              sql,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testScalarSubqueryAfterComparisonLegalityCheck() {
    // Legality check: subquery returns multiple rows (should fail)
    tableAssertTestFail(
        "select s1 from table1 where s1 = (select s1 from table1)",
        "301: Scalar sub-query has returned multiple rows.",
        DATABASE_NAME);

    // Legality check: subquery can not be parsed
    tableAssertTestFail(
        "select s1 from table1 where s1 = (select s1 from)", "mismatched input", DATABASE_NAME);

    // Legality check: subquery can not be parsed(without parentheses)
    tableAssertTestFail(
        "select s1 from table1 where s1 = select s1 from table1",
        "mismatched input",
        DATABASE_NAME);

    // Legality check: Main query can not be parsed
    tableAssertTestFail(
        "select s1 from table1 where s1 = (select max(s1) from table1) and",
        "mismatched input",
        DATABASE_NAME);
  }

  // endregion

  // region test in select clause
  @Test
  public void testScalarSubqueryInSelectClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: select scalar subquery as one constant column
    sql =
        "SELECT cast((SELECT max(%s) from table1 where device_id = 'd01') AS INT32) as %s from table1 where device_id = 'd01'";
    retArray = new String[] {"70,", "70,", "70,", "70,", "70,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: scalar subquery in arithmetic expression and function expression
    sql =
        "SELECT cast((s1 + (SELECT max(%s) from table1 where device_id = 'd01')) AS INT32) as %s from table1 where device_id = 'd01'";
    retArray = new String[] {"100,", "110,", "120,", "130,", "140,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    sql =
        "SELECT  cast(((SELECT max(%s) from table1 where device_id = 'd01') + (SELECT max(%s) from table1 where device_id = 'd01')) AS INT32) as %s from table1 where device_id = 'd01'";
    retArray = new String[] {"140,", "140,", "140,", "140,", "140,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: scalar subquery in udf expression
    sql =
        "SELECT cast(floor(sin((SELECT max(%s) from table1 where device_id = 'd01'))) AS INT32) as %s from table1 where device_id = 'd01'";
    retArray = new String[] {"0,", "0,", "0,", "0,", "0,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: scalar subquery in aggregation function
    sql = "SELECT count((select count(s1) from table1)) as %s from table1";
    retArray = new String[] {"64,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  // endregion

  // region test in having clause
  @Test
  public void testScalarSubqueryInHavingClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: scalar subquery in having clause
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having count(*) > 3 + (SELECT count(*) from table2 where device_id = 'd01')";
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
  // endregion
}
