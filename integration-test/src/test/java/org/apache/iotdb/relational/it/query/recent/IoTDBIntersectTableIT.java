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
public class IoTDBIntersectTableIT {
  protected static final String DATABASE_NAME = "test";
  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        // table1:  ('d1', 1, 1) * 2, ('d1', 2, 2) *1
        "create table table1(device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD)",
        "insert into table1 values (1, 'd1', 1, 1)",
        "insert into table1 values (2, 'd1', 1, 1)",
        "insert into table1 values (3, 'd1', 2, 2)",
        // table2: ('d1', 1, 1.0) * 3, ('d1', 3, 3.0) *1
        "create table table2(device STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD)",
        "insert into table2 values (1, 'd1', 1, 1.0)",
        "insert into table2 values (2, 'd1', 1, 1.0)",
        "insert into table2 values (3, 'd1', 1, 1.0)",
        "insert into table2 values (4, 'd1', 3, 3.0)",
        // table3: use for testing alias
        "create table table3(device STRING TAG, s1_testName INT64 FIELD, s2_testName DOUBLE FIELD)",
        "insert into table3 values (1, 'd1', 1, 1.0)",
        "insert into table3 values (2, 'd1', 1, 1.0)",
        "insert into table3 values (3, 'd1', 1, 1.0)",
        "insert into table3 values (4, 'd1', 3, 3.0)",
        // table4: test type compatible
        "create table table4(device STRING TAG, s1 TEXT FIELD, s2 DOUBLE FIELD)"
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
  public void normalTest() {
    String[] expectedHeader = new String[] {"device", "s1", "s2"};

    // --- INTERSECT (DISTINCT) ---
    // table1 and table2, expected one tuple : ('d1', 1, 1.0)
    String[] retArray =
        new String[] {
          "d1,1,1.0,",
        };
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 intersect select device, s1, s2 from table2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 intersect distinct select device, s1, s2 from table2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // --- INTERSECT ALL ---
    // (1, 1.0) shows twice in table1, shows three times in table2
    // expected: min(2, 3) = 2 tuple
    retArray = new String[] {"d1,1,1.0,", "d1,1,1.0,"};
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 intersect all select device, s1, s2 from table2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    // test table3, the column name is different
    tableResultSetEqualTest(
        "select device, s1, s2 from table1 intersect all select device, s1_testName, s2_testName from table3",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void mappingTest() {
    // table1 (aliased): (s1 as col_a) -> (1), (1), (2)
    // table2 (aliased): (s2 as col_a) -> (1.0), (1.0), (1.0), (3.0)
    // common value: (1.0)

    String[] expectedHeader = new String[] {"col_a"};

    // --- INTERSECT (DISTINCT) with alias ---
    String[] retArray = new String[] {"1.0,"};
    tableResultSetEqualTest(
        "select col_a from ((select s1 as col_a, device as col_b from table1) intersect (select s2 as col_a, device as col_b from table2)) order by col_a",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // --- INTERSECT ALL with alias ---
    retArray = new String[] {"1.0,", "1.0,"};
    tableResultSetEqualTest(
        "select col_a from ((select s1 as col_a, device as col_b from table1) intersect all (select s2 as col_a, device as col_b from table2)) order by col_a",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void exceptionTest() {
    // type is incompatible (INT32 vs TEXT)
    tableAssertTestFail(
        "(select * from table1) intersect all (select * from table4)",
        "has incompatible types: INT32, TEXT",
        DATABASE_NAME);

    tableAssertTestFail(
        "(select * from table1) intersect all (select time from table4)",
        "INTERSECT query has different number of fields: 4, 1",
        DATABASE_NAME);
  }
}
