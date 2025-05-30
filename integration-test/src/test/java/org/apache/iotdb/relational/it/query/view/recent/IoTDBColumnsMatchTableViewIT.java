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

package org.apache.iotdb.relational.it.query.view.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBColumnsMatchTableViewIT {
  private static final String TREE_DB_NAME = "root.test";
  private static final String DATABASE_NAME = "test";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + TREE_DB_NAME,
        "CREATE ALIGNED TIMESERIES root.test.d1(s1 INT32, s2 INT64)",
        "CREATE ALIGNED TIMESERIES root.test.d2(s1 INT32, s2 INT64)",
        "CREATE ALIGNED TIMESERIES root.test.d3(s1 INT32, s2 INT64)",
        "INSERT INTO root.test.d1(time,s1,s2) aligned values(1,1,10)",
        "INSERT INTO root.test.d2(time,s1,s2) aligned values(2,2,20)",
        "INSERT INTO root.test.d3(time,s1) aligned values(3,3)",
      };
  private static final String[] createTableViewSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE VIEW table1(device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD) as root.test.**",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(createSqls);
    prepareTableData(createTableViewSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void columnsTest() {

    // case 1: match all columns
    String[] expectedHeader = new String[] {"time", "device_id", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,10,",
          "1970-01-01T00:00:00.002Z,d2,2,20,",
          "1970-01-01T00:00:00.003Z,d3,3,null,"
        };
    tableResultSetEqualTest(
        "SELECT COLUMNS(*) FROM table1 order by time", expectedHeader, retArray, DATABASE_NAME);

    // case 2: match columns which name starts with 's'
    expectedHeader = new String[] {"s1", "s2"};
    retArray = new String[] {"1,10,", "2,20,", "3,null,"};
    tableResultSetEqualTest(
        "SELECT COLUMNS('^s.*') FROM table1 order by s1", expectedHeader, retArray, DATABASE_NAME);

    // case 3: cannot match any column
    tableAssertTestFail(
        "SELECT COLUMNS('^b.*') FROM table1",
        "701: No matching columns found that match regex '^b.*'",
        DATABASE_NAME);

    // case 4: invalid input of regex
    tableAssertTestFail(
        "SELECT COLUMNS('*b') FROM table1 order by s1", "701: Invalid regex '*b'", DATABASE_NAME);
  }

  @Test
  public void aliasTest() {
    // case 1: normal test
    String[] expectedHeader = new String[] {"series1", "series2"};
    String[] retArray = new String[] {"1,10,", "2,20,", "3,null,"};
    tableResultSetEqualTest(
        "SELECT COLUMNS('^s(.*)') AS \"series$1\" FROM table1 order by series1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"s", "s", "s", "s"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,10,",
          "1970-01-01T00:00:00.002Z,d2,2,20,",
          "1970-01-01T00:00:00.003Z,d3,3,null,"
        };
    tableResultSetEqualTest(
        "SELECT COLUMNS(*) AS s FROM table1 order by 1", expectedHeader, retArray, DATABASE_NAME);

    // case 2: no according reference group
    tableAssertTestFail(
        "SELECT COLUMNS('^s.*') AS \"series$1\" FROM table1", "701: No group 1", DATABASE_NAME);

    // case 3: alias grammar error
    tableAssertTestFail(
        "SELECT COLUMNS('^s.*') AS 'series$1' FROM table1",
        "700: line 1:27: mismatched input ''series$1''. Expecting: <identifier>",
        DATABASE_NAME);
  }

  @Test
  public void usedWithExpressionTest1() {
    // case 1: select min value for each column
    String[] expectedHeader =
        new String[] {"_col0_time", "_col1_device_id", "_col2_s1", "_col3_s2"};
    String[] retArray = new String[] {"1970-01-01T00:00:00.001Z,d1,1,10,"};
    tableResultSetEqualTest(
        "SELECT min(COLUMNS(*)) FROM table1", expectedHeader, retArray, DATABASE_NAME);

    // case 2: multi columns() in different selectItem,
    expectedHeader =
        new String[] {
          "_col0_time", "_col1_device_id", "_col2_s1", "_col3_s2", "_col4_s1", "_col5_s2"
        };
    retArray = new String[] {"1970-01-01T00:00:00.001Z,d1,1,10,1,10,"};
    tableResultSetEqualTest(
        "SELECT min(COLUMNS(*)), min(COLUMNS('^s.*')) FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 3: multi columns() in one expression, columns() are same
    expectedHeader = new String[] {"_col0_s1", "_col1_s2"};
    retArray = new String[] {"4,30,"};
    tableResultSetEqualTest(
        "SELECT min(COLUMNS('^s.*')) + max(COLUMNS('^s.*')) FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 4: multi columns() in one expression, columns() are different
    tableAssertTestFail(
        "SELECT min(COLUMNS('^s.*')) + max(COLUMNS('^t.*')) FROM table1",
        "701: Multiple different COLUMNS in the same expression are not supported",
        DATABASE_NAME);

    // case 5: get last row of Data
    expectedHeader =
        new String[] {"_col0", "_col1_time", "_col2_device_id", "_col3_s1", "_col4_s2"};
    retArray = new String[] {"1970-01-01T00:00:00.003Z,1970-01-01T00:00:00.003Z,d3,3,null,"};
    tableResultSetEqualTest(
        "SELECT last(time), last_by(COLUMNS(*), time) FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 6: get last non-null value of each column, and according time
    expectedHeader =
        new String[] {
          "_col0_time",
          "_col1_device_id",
          "_col2_s1",
          "_col3_s2",
          "_col4_time",
          "_col5_device_id",
          "_col6_s1",
          "_col7_s2"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d3,3,20,1970-01-01T00:00:00.003Z,1970-01-01T00:00:00.003Z,1970-01-01T00:00:00.003Z,1970-01-01T00:00:00.002Z,"
        };
    tableResultSetEqualTest(
        "SELECT last(COLUMNS(*)), last_by(time,COLUMNS(*)) FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "test", "test", "test", "test", "_col4_time", "_col5_device_id", "_col6_s1", "_col7_s2"
        };
    tableResultSetEqualTest(
        "SELECT last(COLUMNS(*)) as test, last_by(time,COLUMNS(*)) FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // used with all supported Expressions
  @Test
  public void usedWithExpressionTest2() {
    String[] expectedHeader =
        new String[] {
          "_col0_s1",
          "_col1_s2",
          "_col2_s1",
          "_col3_s2",
          "_col4_s1",
          "_col5_s2",
          "_col6_s1",
          "_col7_s2"
        };
    String[] retArray =
        new String[] {
          "2,20,-1,-10,true,false,false,true,",
          "4,40,-2,-20,true,false,true,true,",
          "6,null,-3,null,false,null,true,false,"
        };
    tableResultSetEqualTest(
        "select columns('s.*')+columns('s.*'), -columns('s.*'),columns('s.*') between 1 and 2,if(columns('s.*')>1,true,false) from table1 order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "select columns(*).s1 from table1",
        "701: Columns are not supported in DereferenceExpression",
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s1", "s2"};
    retArray = new String[] {"1970-01-01T00:00:00.002Z,d2,2,20,"};
    tableResultSetEqualTest(
        "select * from table1 where columns('s.*') > any(select s1 from table1) order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "_col0_s1",
          "_col1_s2",
          "_col2_s1",
          "_col3_time",
          "_col4_device_id",
          "_col5_s1",
          "_col6_s2",
          "_col7_time",
          "_col8_device_id",
          "_col9_s1",
          "_col10_s2",
          "_col11_time",
          "_col12_device_id",
          "_col13_s1",
          "_col14_s2"
        };
    retArray =
        new String[] {
          "1,10,1,true,true,true,true,true,true,true,true,false,false,false,false,",
          "2,20,2,true,true,true,true,true,true,true,true,false,false,false,false,",
          "3,null,3,true,true,true,null,true,true,true,false,false,false,false,true,"
        };
    tableResultSetEqualTest(
        "select trim(cast(columns('s.*') as String)),COALESCE(columns('s1.*'),1), columns(*) in (columns(*)),columns(*) is not null, columns(*) is null from table1 order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"_col0_device_id", "_col1_time", "_col2_device_id", "_col3_s1", "_col4_s2"};
    retArray =
        new String[] {
          "true,true,true,true,true,", "true,true,true,true,true,", "true,true,true,true,null,"
        };
    tableResultSetEqualTest(
        "select columns('d.*') like 'd_',columns(*)=columns(*) from table1 order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "SELECT CASE columns('s.*') WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END from table1",
        "701: CASE operand type does not match WHEN clause operand type: INT64 vs INT32",
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0_s1"};
    retArray = new String[] {"one,", "two,", "many,"};
    tableResultSetEqualTest(
        "SELECT CASE columns('s1.*') WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END from table1 order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0_s1", "_col1_s2"};
    retArray = new String[] {"one,many,", "two,many,", "many,many,"};
    tableResultSetEqualTest(
        "select case when columns('s.*') = 1 then 'one' when columns('s.*')=2 then 'two' else 'many' end from table1 order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void usedInWhereTest() {
    // case 1: normal test
    String[] expectedHeader = new String[] {"time", "device_id", "s1", "s2"};
    String[] retArray = new String[] {"1970-01-01T00:00:00.002Z,d2,2,20,"};
    tableResultSetEqualTest(
        "SELECT * FROM table1 WHERE COLUMNS('^s.*') > 1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s1", "s2"};
    retArray =
        new String[] {"1970-01-01T00:00:00.002Z,d2,2,20,", "1970-01-01T00:00:00.003Z,d3,3,null,"};
    tableResultSetEqualTest(
        "SELECT * FROM table1 WHERE COLUMNS('^s1.*') > 1 order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 3: cannot match any column
    tableAssertTestFail(
        "SELECT * FROM table1 WHERE COLUMNS('^b.*') > 1",
        "701: No matching columns found that match regex '^b.*'",
        DATABASE_NAME);

    // case 4: invalid input of regex
    tableAssertTestFail(
        "SELECT * FROM table1 WHERE COLUMNS('*b') > 1", "701: Invalid regex '*b'", DATABASE_NAME);
  }

  @Test
  public void otherExceptionTest() {
    // cannot be used in HAVING clause
    tableAssertTestFail(
        "SELECT device_id, count(s1) FROM table1 HAVING COLUMNS('device_id') > 1",
        "701: Columns only support to be used in SELECT and WHERE clause",
        DATABASE_NAME);

    // cannot be used for columns without name
    tableAssertTestFail(
        "SELECT COLUMNS(*) FROM (SELECT s1,s1+1 from table1)",
        "701: Unknown ColumnName",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT COLUMNS('s1') FROM (SELECT s1,s1+1 from table1)",
        "701: Unknown ColumnName",
        DATABASE_NAME);
  }
}
