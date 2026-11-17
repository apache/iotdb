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
public class IoTDBUnionTableIT {
  protected static final String DATABASE_NAME = "test";
  private static final String INTERSECT_DATABASE_NAME = "test_intersect_table";
  private static final String EXCEPT_DATABASE_NAME = "test_except_table";
  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table table1(device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD)",
        "insert into table1 values (1, 'd1', 1, 1)",
        "insert into table1 values (2, 'd1', 2, 2)",
        "create table table2(device STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD)",
        "insert into table2 values (1, 'd1', 1, 1.0)",
        "insert into table2 values (3, 'd1', 3, 3.0)",
        "create table table3(device STRING TAG, s1_testName INT64 FIELD, s2_testName DOUBLE FIELD)",
        "insert into table3 values (1, 'd1', 1, 1.0)",
        "insert into table3 values (3, 'd1', 3, 3.0)",
        "create table table4(device STRING TAG, s1 TEXT FIELD, s2 DOUBLE FIELD)"
      };

  private static final String[] intersectCreateSqls =
      new String[] {
        "CREATE DATABASE " + INTERSECT_DATABASE_NAME,
        "USE " + INTERSECT_DATABASE_NAME,
        "create table table1(device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD)",
        "insert into table1 values (1, 'd1', 1, 1)",
        "insert into table1 values (2, 'd1', 1, 1)",
        "insert into table1 values (3, 'd1', 2, 2)",
        "create table table2(device STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD)",
        "insert into table2 values (1, 'd1', 1, 1.0)",
        "insert into table2 values (2, 'd1', 1, 1.0)",
        "insert into table2 values (3, 'd1', 1, 1.0)",
        "insert into table2 values (4, 'd1', 3, 3.0)",
        "create table table3(device STRING TAG, s1_testName INT64 FIELD, s2_testName DOUBLE FIELD)",
        "insert into table3 values (1, 'd1', 1, 1.0)",
        "insert into table3 values (2, 'd1', 1, 1.0)",
        "insert into table3 values (3, 'd1', 1, 1.0)",
        "insert into table3 values (4, 'd1', 3, 3.0)",
        "create table table4(device STRING TAG, s1 TEXT FIELD, s2 DOUBLE FIELD)"
      };

  private static final String[] exceptCreateSqls =
      new String[] {
        "CREATE DATABASE " + EXCEPT_DATABASE_NAME,
        "USE " + EXCEPT_DATABASE_NAME,
        "create table table1(device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD)",
        "insert into table1 values (1, 'd1', 1, 1)",
        "insert into table1 values (2, 'd1', 1, 1)",
        "insert into table1 values (3, 'd1', 1, 1)",
        "insert into table1 values (4, 'd1', 2, 2)",
        "create table table2(device STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD)",
        "insert into table2 values (1, 'd1', 1, 1.0)",
        "insert into table2 values (2, 'd1', 3, 3.0)",
        "create table table3(device STRING TAG, s1_testName INT64 FIELD, s2_testName DOUBLE FIELD)",
        "insert into table3 values (1, 'd1', 1, 1.0)",
        "insert into table3 values (2, 'd1', 3, 3.0)",
        "create table table4(device STRING TAG, s1 TEXT FIELD, s2 DOUBLE FIELD)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
    prepareTableData(intersectCreateSqls);
    prepareTableData(exceptCreateSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void normalTest() {
    String[] expectedHeader = new String[] {"time", "device", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,1.0,",
          "1970-01-01T00:00:00.002Z,d1,2,2.0,",
          "1970-01-01T00:00:00.003Z,d1,3,3.0,"
        };
    tableResultSetEqualTest(
        "(select * from table1) union (select * from table2) order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "(select * from table1) union distinct (select * from table2) order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,1.0,",
          "1970-01-01T00:00:00.001Z,d1,1,1.0,",
          "1970-01-01T00:00:00.002Z,d1,2,2.0,",
          "1970-01-01T00:00:00.003Z,d1,3,3.0,"
        };
    tableResultSetEqualTest(
        "(select * from table1) union all (select * from table2) order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "(select * from table1) union all (select * from table3) order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // result correction test for union with predicate
    retArray =
        new String[] {"1970-01-01T00:00:00.002Z,d1,2,2.0,", "1970-01-01T00:00:00.003Z,d1,3,3.0,"};
    tableResultSetEqualTest(
        "select * from ((select * from table1) union all (select * from table2)) where s1>1 order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void mappingTest() {
    String[] expectedHeader = new String[] {"col_a"};
    String[] retArray = new String[] {"1.0,", "2.0,", "3.0,"};
    tableResultSetEqualTest(
        "select col_a from ((select s1 as col_a, device as col_b from table1) union (select s2 as col_a, device as col_b from table2)) order by col_a",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray = new String[] {"1.0,", "1.0,", "2.0,", "3.0,"};
    tableResultSetEqualTest(
        "select col_a from ((select s1 as col_a, device as col_b from table1) union all (select s2 as col_a, device as col_b from table2)) order by col_a",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void exceptionTest() {
    tableAssertTestFail(
        "(select * from table1) union all (select * from table4)",
        "has incompatible types: INT32, TEXT",
        DATABASE_NAME);
    tableAssertTestFail(
        "(select * from table1) union all (select time from table4)",
        "UNION query has different number of fields: 4, 1",
        DATABASE_NAME);
  }

  // Checks INTERSECT DISTINCT and ALL preserve duplicate counts and support differing source types.
  @Test
  public void intersectNormalTest() {
    String[] expectedHeader = new String[] {"device", "s1", "s2"};
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 intersect select device, s1, s2 from table2",
        expectedHeader,
        new String[] {"d1,1,1.0,"},
        INTERSECT_DATABASE_NAME);
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 intersect distinct select device, s1, s2 from table2",
        expectedHeader,
        new String[] {"d1,1,1.0,"},
        INTERSECT_DATABASE_NAME);
    String[] allRows = {"d1,1,1.0,", "d1,1,1.0,"};
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 intersect all select device, s1, s2 from table2",
        expectedHeader,
        allRows,
        INTERSECT_DATABASE_NAME);
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 intersect all select device, s1_testName, s2_testName from table3",
        expectedHeader,
        allRows,
        INTERSECT_DATABASE_NAME);
  }

  // Checks INTERSECT aliases preserve column mapping for distinct and duplicate-preserving forms.
  @Test
  public void intersectMappingTest() {
    String[] expectedHeader = {"col_a"};
    tableResultSetEqualTest(
        "select col_a from ((select s1 as col_a, device as col_b from table1) intersect (select s2 as col_a, device as col_b from table2)) order by col_a",
        expectedHeader,
        new String[] {"1.0,"},
        INTERSECT_DATABASE_NAME);
    tableResultSetEqualTest(
        "select col_a from ((select s1 as col_a, device as col_b from table1) intersect all (select s2 as col_a, device as col_b from table2)) order by col_a",
        expectedHeader,
        new String[] {"1.0,", "1.0,"},
        INTERSECT_DATABASE_NAME);
  }

  // Rejects INTERSECT when input types conflict or the input relations have different column
  // counts.
  @Test
  public void intersectExceptionTest() {
    tableAssertTestFail(
        "(select * from table1) intersect all (select * from table4)",
        "has incompatible types: INT32, TEXT",
        INTERSECT_DATABASE_NAME);
    tableAssertTestFail(
        "(select * from table1) intersect all (select time from table4)",
        "INTERSECT query has different number of fields: 4, 1",
        INTERSECT_DATABASE_NAME);
  }

  // Checks EXCEPT DISTINCT and ALL subtract duplicate counts and support differing source types.
  @Test
  public void exceptNormalTest() {
    String[] expectedHeader = new String[] {"device", "s1", "s2"};
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 except select device, s1, s2 from table2",
        expectedHeader,
        new String[] {"d1,2,2.0,"},
        EXCEPT_DATABASE_NAME);
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 except distinct select device, s1, s2 from table2",
        expectedHeader,
        new String[] {"d1,2,2.0,"},
        EXCEPT_DATABASE_NAME);
    String[] allRows = {"d1,1,1.0,", "d1,1,1.0,", "d1,2,2.0,"};
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 except all select device, s1, s2 from table2",
        expectedHeader,
        allRows,
        EXCEPT_DATABASE_NAME);
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 except all select device, s1_testName, s2_testName from table3",
        expectedHeader,
        allRows,
        EXCEPT_DATABASE_NAME);
  }

  // Checks EXCEPT aliases preserve mapping for distinct and duplicate-preserving forms.
  @Test
  public void exceptMappingTest() {
    String[] expectedHeader = {"col_a"};
    tableResultSetEqualTest(
        "select col_a from ((select s1 as col_a, device as col_b from table1) except (select s2, device from table2)) order by col_a",
        expectedHeader,
        new String[] {"2.0,"},
        EXCEPT_DATABASE_NAME);
    tableResultSetEqualTest(
        "select col_a from ((select s1 as col_a, device as col_b from table1) except all (select s2, device from table2)) order by col_a",
        expectedHeader,
        new String[] {"1.0,", "1.0,", "2.0,"},
        EXCEPT_DATABASE_NAME);
  }

  // Rejects EXCEPT when input types conflict or the input relations have different column counts.
  @Test
  public void exceptExceptionTest() {
    tableAssertTestFail(
        "(select * from table1) except all (select * from table4)",
        "has incompatible types: INT32, TEXT",
        EXCEPT_DATABASE_NAME);
    tableAssertTestFail(
        "(select * from table1) except all (select time from table4)",
        "EXCEPT query has different number of fields: 4, 1",
        EXCEPT_DATABASE_NAME);
  }
}
