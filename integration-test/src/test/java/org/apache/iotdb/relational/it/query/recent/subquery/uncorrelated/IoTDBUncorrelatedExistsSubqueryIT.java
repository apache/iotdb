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
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.relational.it.query.recent.subquery.SubqueryDataSetUtils.CREATE_SQLS;
import static org.apache.iotdb.relational.it.query.recent.subquery.SubqueryDataSetUtils.DATABASE_NAME;
import static org.apache.iotdb.relational.it.query.recent.subquery.SubqueryDataSetUtils.NUMERIC_MEASUREMENTS;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBUncorrelatedExistsSubqueryIT {
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
  public void testUnCorrelatedExistsSubqueryInWhereClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: aggregation in exist subquery
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table3 t3 WHERE device_id = 'd01' and exists(select max(s1) from table1)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: group by in exist subquery
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table3 t3 WHERE device_id = 'd01' and exists(select max(s1) from table1 group by device_id)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: expression in select clause with exist subquery
    sql =
        "SELECT cast(%s AS INT32) + 1 as %s FROM table3 t3 WHERE device_id = 'd01' and exists(select max(s1) from table1 group by device_id)";
    retArray = new String[] {"31,", "41,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: exists with other conjuncts
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table3 t3 WHERE %s = 30 and exists(select max(s1) from table1)";
    retArray = new String[] {"30,", "30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: false exist
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table3 t3 WHERE %s = 30 and exists(select s1 from table1 where s1 < 0)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: limit in exist subquery
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table3 t3 WHERE %s is not null and exists(select s1 from table1 limit 10)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: Nested exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table3 t3 WHERE %s is not null and exists(select s1 from table1 where exists(select s1 from table2))";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: Multiple exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table3 t3 WHERE %s is not null and exists(select avg(s1) from table1 limit 1) and exists(select s1 from table1 where exists(select s1 from table2))";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testUnCorrelatedNotExistsSubqueryInWhereClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: aggregation in exist subquery
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table3 t3 WHERE device_id = 'd01' and not exists(select max(s1) from table1)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: limit in exist subquery
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table3 t3 WHERE not exists(select s1 from table1 limit 1)";
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: subquery has output with not exists
    sql =
        "SELECT cast(%s AS INT32) as %s FROM table3 t3 WHERE %s = 30 and not exists(select s1 from table1 where s1 < 0)";
    retArray = new String[] {"30,", "30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: with distinct
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table3 t3 WHERE %s is not null and not exists(select s1 from table1 where s1 < 0)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: Nested exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table3 t3 WHERE %s is not null and exists(select s1 from table1 where not exists(select s1 from table1 where s1 < 0))";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: Multiple exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table3 t3 WHERE %s is not null and not exists(select s1 from table1 where s1 < 0) and exists(select s1 from table1 where not exists(select s1 from table1 where s1 < 0))";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testUnCorrelatedExistsSubqueryInWhereClauseWithOtherSubquery() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: with InPredicate
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(select max(s1) from table1) and s1 in (select s1 from table3)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: with InPredicate in exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table3 t3 WHERE device_id = 'd01' and exists(select max(s1) from table1 where s1 in (select s1 from table3))";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: with Scalar Subquery
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(select s1 from table1) and s1 = (select min(s1) from table3)";
    retArray = new String[] {"30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: with Scalar Subquery in exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and %s < 50 and exists(select s1 from table1 where s1 = (select min(s1) from table3))";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: with Quantified Comparison
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and exists(select s1 from table1) and s1 = any(select s1 from table3)";
    retArray = new String[] {"30,", "40,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }

    // Test case: with Quantified Comparison in exists
    sql =
        "SELECT distinct cast(%s AS INT32) as %s FROM table1 t1 WHERE device_id = 'd01' and %s = 30 and exists(select s1 from table1 where s1 = any (select s1 from table3))";
    retArray = new String[] {"30,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement, measurement),
          expectedHeader,
          retArray,
          DATABASE_NAME);
    }
  }

  @Test
  public void testUnCorrelatedExistsSubqueryInHavingClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: exists in having
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having exists(SELECT %s from table1) and count(*) = 5";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray =
        new String[] {
          "d01,5,", "d03,5,", "d05,5,", "d07,5,", "d09,5,", "d11,5,", "d13,5,", "d15,5,"
        };
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    sql =
        "SELECT device_id, count(*) from table1 group by device_id having exists(SELECT %s from table1 where s1 < 0)";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    // Test case: not exists in having
    sql =
        "SELECT device_id, count(*) from table1 group by device_id having not exists(SELECT %s from table1 where s1 < 0) and count(*) = 5";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray =
        new String[] {
          "d01,5,", "d03,5,", "d05,5,", "d07,5,", "d09,5,", "d11,5,", "d13,5,", "d15,5,"
        };
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    sql =
        "SELECT device_id, count(*) from table1 group by device_id having not exists(SELECT %s from table1) and count(*) = 5";
    expectedHeader = new String[] {"device_id", "_col1"};
    retArray = new String[] {};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      tableResultSetEqualTest(
          String.format(sql, measurement), expectedHeader, retArray, DATABASE_NAME);
    }
  }

  @Test
  public void testUnCorrelatedExistsSubqueryInSelectClause() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // Test case: exists in Select clause
    sql =
        "SELECT exists(select max(s1) from table1) as %s FROM table3 t3 WHERE %s is not null and device_id = 'd01'";
    retArray = new String[] {"true,", "true,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }

    sql =
        "SELECT not exists(select max(s1) from table1) as %s FROM table3 t3 WHERE %s is not null and device_id = 'd01'";
    retArray = new String[] {"false,", "false,"};
    for (String measurement : NUMERIC_MEASUREMENTS) {
      expectedHeader = new String[] {measurement};
      tableResultSetEqualTest(
          String.format(sql, measurement, measurement), expectedHeader, retArray, DATABASE_NAME);
    }
  }
}
