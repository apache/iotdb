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

package org.apache.iotdb.relational.it.query.recent.subquery.correlated;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

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
public class IoTDBCorrelatedExistsSubqueryIT {
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
  public void testCorrelatedExistsSubqueryInWhereClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: exists and other filter
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: only exists
    sql = "SELECT s1 FROM table3 t3 WHERE exists(SELECT s1 from table1 t1 WHERE t1.s1 = t3.s1)";
    retArray = new String[] {"30,", "30,", "40,"};
    expectedHeader = new String[] {"s1"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // Test case: exists with distinct
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: aggregation in exists
    sql =
        "SELECT cast(min(%s) as INT32) as %s FROM table1 t1 WHERE exists(SELECT avg(%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s)";
    retArray = new String[] {"30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: aggregation with group by in exists(subquery returns empty result with having
    // clause)
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE exists(SELECT count(*) from table3 t3 where t1.%s = t3.%s group by device_id having count(*) > 5)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: limit 1 in exists
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s limit 1)";
    retArray = new String[] {"30,", "40,"};
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
  public void testNestedExistsSubquery() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: Nested exists
    sql =
        "select distinct s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 = t3.s1 and exists(select s1 from table2 t2 where t2.s1 = t3.s1 - 25))";
    retArray = new String[] {"30,"};
    expectedHeader = new String[] {"s1"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // Test case: Nested exists with not(64 - 4 = 60 rows remain)
    sql =
        "select count(*) as cnt from table1 t1 where not exists(select s1 from table3 t3 where t1.s1 = t3.s1 and exists(select s1 from table2 t2 where t2.s1 = t3.s1 - 25))";
    retArray = new String[] {"60,"};
    expectedHeader = new String[] {"cnt"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testMultipleExistsSubquery() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: multiple exists
    sql =
        "select distinct s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 = t3.s1) and exists(select s1 from table2 t2 where t2.s1 = t1.s1 - 25)";
    retArray = new String[] {"30,"};
    expectedHeader = new String[] {"s1"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // Test case: multiple exists with not
    sql =
        "select distinct s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 = t3.s1) and not exists(select s1 from table2 t2 where t2.s1 = t1.s1)";
    retArray = new String[] {"30,", "40,"};
    expectedHeader = new String[] {"s1"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testCorrelatedExistsSubqueryInWhereClauseWithOtherUncorrelatedSubquery() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: with InPredicate
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s) and %s in (select %s from table3)";
    retArray = new String[] {"30,", "40,"};
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

    // Test case: with InPredicate in exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s and %s not in (select %s from table2 where %s is not null))";
    retArray = new String[] {"30,", "40,"};
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
              measurement,
              measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: with Scalar Subquery
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s) and s1 > (select min(%s) from table3)";
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

    // Test case: with Scalar Subquery in exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s and s1 = (select min(%s) from table3))";
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

    // Test case: with Quantified Comparison
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s) and s1 != any(select %s from table2)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(
              sql, measurement, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: with Quantified Comparison in exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(SELECT (%s) from table3 t3 WHERE device_id = 'd01' and t1.%s = t3.%s and %s = any (select %s from table3))";
    retArray = new String[] {"30,", "40,"};
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
  public void testCorrelatedExistsSubqueryWithMultipleCorrelation() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: Multiple correlation in exists
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(SELECT (%s) from table3 t3 WHERE t1.%s = t3.%s and t1.s1 = t3.s1 and t1.s2 = t3.s2)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    sql =
        "SELECT cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and not exists(SELECT (%s) from table3 t3 WHERE t1.%s = t3.%s and t1.s1 = t3.s1 and t1.s2 = t3.s2)";
    retArray = new String[] {"50,", "60,", "70,"};
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
  public void testCorrelatedExistsSubqueryInHavingClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: exists in having
    sql =
        "SELECT device_id, count(*) from table1 t1 group by device_id having exists(SELECT 1 from table3 t3 where t3.device_id = t1.device_id)";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray = new String[] {"d01,5,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    sql =
        "SELECT device_id, count(*) from table1 t1 group by device_id having exists(SELECT 1 from table3 t3 where t3.device_id != 'd01' and t3.device_id = t1.device_id)";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: not exists in having
    sql =
        "SELECT device_id, count(*) from table1 t1 group by device_id having count(*) = 5 and not exists(SELECT 1 from table3 t3 where t3.device_id = t1.device_id)";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray = new String[] {"d03,5,", "d05,5,", "d07,5,", "d09,5,", "d11,5,", "d13,5,", "d15,5,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement), expectedHeader, retArray, DATABASE_NAME);
    }
  }

  @Test
  public void testCorrelatedExistsSubqueryInSelectClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: exists in Select clause
    sql =
        "select exists(select s1 from table1 t1 where t1.s1 = t3.s1) from table3 t3 where exists(select s1 from table2 t2 where t2.s1 = t3.s1 - 25)";
    retArray = new String[] {"true,", "true,"};
    expectedHeader = new String[] {"_col0"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql = "select exists(select s1 from table1 t1 where t1.s1 = t3.s1) from table3 t3";
    retArray = new String[] {"true,", "true,", "true,", "false,"};
    expectedHeader = new String[] {"_col0"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "select not exists(select s1 from table1 t1 where t1.s1 = t3.s1) from table3 t3 where exists(select s1 from table2 t2 where t2.s1 = t3.s1 - 25)";
    retArray = new String[] {"false,", "false,"};
    expectedHeader = new String[] {"_col0"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql = "select not exists(select s1 from table1 t1 where t1.s1 = t3.s1) from table3 t3";
    retArray = new String[] {"false,", "false,", "false,", "true,"};
    expectedHeader = new String[] {"_col0"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testNonComparisonFilterInCorrelatedExistsSubquery() {
    String errMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode() + ": " + "Unsupported Join creteria";
    tableAssertTestFail(
        "select s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 > t3.s1)",
        errMsg,
        DATABASE_NAME);
    tableAssertTestFail(
        "select s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 >= t3.s1)",
        errMsg,
        DATABASE_NAME);
    tableAssertTestFail(
        "select s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 < t3.s1)",
        errMsg,
        DATABASE_NAME);
    tableAssertTestFail(
        "select s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 <= t3.s1)",
        errMsg,
        DATABASE_NAME);
    tableAssertTestFail(
        "select s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 != t3.s1)",
        errMsg,
        DATABASE_NAME);
  }

  @Test
  public void testCorrelatedExistsLegalityCheck() {
    // Legality check: Correlated subqueries can only access columns from the immediately outer
    // scope and cannot access columns from the further outer queries.
    tableAssertTestFail(
        "select s1 from table1 t1 where exists(select s1 from table3 t3 where t1.s1 = t3.s1 and exists(select s1 from table2 t2 where t2.s1 = t1.s1))",
        "701: Given correlated subquery is not supported",
        DATABASE_NAME);

    // Legality check: Correlated subqueries with limit clause and limit count greater than 1 is not
    // supported for now
    tableAssertTestFail(
        "select s1 from table3 t3 where 30 = t3.s1 and exists(select s1 from table2 t2 where t2.s1 = t3.s1 limit 2)",
        "Decorrelation for LIMIT with row count greater than 1 is not supported yet",
        DATABASE_NAME);
  }
}
