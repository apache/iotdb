/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBIfExpressionTableIT {
  private static final String DATABASE = "testDatabase";

  private static final String[] expectedHeader = {"_col0"};

  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE " + DATABASE,
        "Use " + DATABASE,
        "CREATE table table1 (device_id STRING TAG, s1 INT32 FIELD, s5 BOOLEAN FIELD, s6 TEXT FIELD, s7 INT64 FIELD)",
        "CREATE table table2 (device_id STRING TAG, s3 FLOAT FIELD, s4 DOUBLE FIELD)",
        "CREATE table table3 (device_id STRING TAG, s2 INT64 FIELD)",
        "INSERT INTO table1(time, device_id, s1, s6) values(100, 'd1',  0, 'text100')",
        "INSERT INTO table1(time, device_id, s1, s6) values(200, 'd1', 11, 'text200')",
        "INSERT INTO table1(time, device_id, s1, s6) values(300, 'd1', 22, 'text300')",
        "INSERT INTO table1(time, device_id, s1, s6) values(400, 'd1', 33, 'text400')",
        "INSERT INTO table2(time, device_id, s3) values(100, 'd1',  0)",
        "INSERT INTO table2(time, device_id, s3) values(200, 'd1', 11)",
        "INSERT INTO table2(time, device_id, s3) values(300, 'd1', 22)",
        "INSERT INTO table2(time, device_id, s3) values(400, 'd1', 33)",
        "INSERT INTO table2(time, device_id, s4) values(100, 'd1', 44)",
        "INSERT INTO table2(time, device_id, s4) values(200, 'd1', 55)",
        "INSERT INTO table2(time, device_id, s4) values(300, 'd1', 66)",
        "INSERT INTO table2(time, device_id, s4) values(400, 'd1', 77)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(SQLs);
    List<String> moreSQLs = new ArrayList<>();
    moreSQLs.add("use " + DATABASE);
    for (int i = 0; i < 100; i++) {
      moreSQLs.add(
          String.format("INSERT INTO table3(time,device_id,s2) values(%d, 'd1', %d)", i, i));
    }
    prepareTableData(moreSQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testKind1Basic() {
    String[] retArray = new String[] {"99,", "9999,", "9999,", "999,"};
    tableResultSetEqualTest(
        "select if(s1=0, 99, if(s1>22, 999, 9999)) from table1",
        expectedHeader,
        retArray,
        DATABASE);

    // without false value
    retArray = new String[] {"99,", "null,", "null,", "999,"};
    tableResultSetEqualTest(
        "select if(s1=0, 99, if(s1>22, 999)) from table1", expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testKind2Basic() {
    String sql = "select if(s3=0.0, s4, if(s3=22.0, 999.0, 9999.0)) from table2";
    String[] retArray = new String[] {"44.0,", "9999.0,", "999.0,", "9999.0,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // without false value
    sql = "select if(s3=0.0, s4, if(s3=22.0, 999.0)) from table2";
    retArray = new String[] {"44.0,", "null,", "999.0,", "null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testShortCircuitEvaluation() {
    String[] retArray = new String[] {"0,", "11,", "22,", "33,"};
    tableResultSetEqualTest(
        "select if(1=0, s1/0, if(1!=0, s1)) from table1", expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testKind1InputTypeRestrict() {
    // IF condition must return BOOLEAN
    String sql = "select if(s1+1, 20, 22) from table1";
    String msg = "701: IF condition must evaluate to a BOOLEAN (actual: INT32)";
    tableAssertTestFail(sql, msg, DATABASE);
  }

  @Test
  public void testKind2InputTypeRestrict() {
    // the expression in IF condition must be able to be equated with the expression in IF condition
    String sql = "select if(s1='1', 20, 22) from table1";
    String msg = "701: Cannot apply operator: INT32 = STRING";
    tableAssertTestFail(sql, msg, DATABASE);
  }

  @Test
  public void testKind1OutputTypeRestrict() {
    // BOOLEAN and other types cannot exist at the same time
    String[] retArray = new String[] {"true,", "false,", "true,", "true,"};
    // success
    tableResultSetEqualTest(
        "select if(s1<=0, true, if(s1=11, false, true)) from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select if(s1<=0, true, 22) from table1",
        "701: Result types for IF must be the same: BOOLEAN vs INT32",
        DATABASE);

    // INT32 and INT64 cases
    retArray = new String[] {"0,", "100,", "22,", "33,"};
    // success for int32 value and int32 filed
    tableResultSetEqualTest(
        "select if(s1=11, 100, s1) from table1", expectedHeader, retArray, DATABASE);

    // fail for int32 value and int64 filed
    tableAssertTestFail(
        "select if(s1=11, 100, s7) from table1",
        "701: Result types for IF must be the same: INT32 vs INT64",
        DATABASE);

    // TEXT and other types cannot exist at the same time
    retArray = new String[] {"good,", "bad,", "okk,", "okk,"};
    // success
    tableResultSetEqualTest(
        "select if(s1<=0, 'good', if(s1=11, 'bad', 'okk')) from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select if(s1<=0, 'good', 22) from table1",
        "701: Result types for IF must be the same: STRING vs INT32",
        DATABASE);

    // 4 numerical types(INT LONG FLOAT DOUBLE) cases
    // fail for int32 and int64 values
    tableAssertTestFail(
        "select if(s1=0, 99, " + Long.MAX_VALUE + ") from table1",
        "701: Result types for IF must be the same: INT32 vs INT64",
        DATABASE);

    // fail for int32 and int64 fields
    tableAssertTestFail(
        "select if(s1=11, s1, s7) from table1",
        "701: Result types for IF must be the same: INT32 vs INT64",
        DATABASE);

    // success for float and double values
    String maxValueOfDouble = Double.MAX_VALUE + ",";
    retArray = new String[] {"1.2,", maxValueOfDouble, maxValueOfDouble, maxValueOfDouble};
    tableResultSetEqualTest(
        "select if(s1=0, 1.2, " + Double.MAX_VALUE + ") from table1",
        expectedHeader,
        retArray,
        DATABASE);

    // fail for float and double fields
    tableAssertTestFail(
        "select if(s3=11, s3, s4) from table2",
        "701: Result types for IF must be the same: FLOAT vs DOUBLE",
        DATABASE);

    // fail for int32 and float/double
    tableAssertTestFail(
        "select if(s1=0, 99, 1.2) from table1",
        "701: Result types for IF must be the same: INT32 vs DOUBLE",
        DATABASE);

    // fail for int32, int64, float and double
    tableAssertTestFail(
        "select if(s1=0, 99, if(s1=11, 99.9, if(s1=22, 8589934588, if(s1=33, 999.9999999999, "
            + Long.MAX_VALUE
            + ")))) from table1",
        "701: Result types for IF must be the same: DOUBLE vs INT64",
        DATABASE);
  }

  @Test
  public void testKind1LargeNumberBranches() {
    StringBuilder sqlBuilder = new StringBuilder();
    List<String> retList = new ArrayList<>();
    sqlBuilder.append("select ");
    for (int i = 0; i < 100; i++) {
      sqlBuilder.append(String.format("if(s2=%d, s2*%d ", i, i * 100));
      if (i != 99) {
        sqlBuilder.append(", ");
      }
      retList.add(String.format("%d,", i * i * 100));
    }
    for (int i = 0; i < 100; i++) {
      sqlBuilder.append(")");
    }
    sqlBuilder.append(" from table3");
    tableResultSetEqualTest(
        sqlBuilder.toString(), expectedHeader, retList.toArray(new String[] {}), DATABASE);
  }

  @Test
  public void testKind1UsedInOtherOperation() {
    String sql;
    String[] retArray;

    // use in scalar operation
    // multiply
    sql = "select 2 * if(s1=0, 99, if(s1=22.0, 999, 9999)) from table1";
    retArray = new String[] {"198,", "19998,", "1998,", "19998,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // add
    sql = "select if(s1=0, 99, if(s1=22.0, 999, 9999)) + if(s1=11, 99, 9999) from table1";
    retArray =
        new String[] {
          "10098,", "10098,", "10998,", "19998,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    //  built-in scalar function
    sql = "select diff(if(s1=0, 99, if(s1>22, 999, 9999))) from table1";
    retArray = new String[] {"null,", "9900.0,", "0.0,", "-9000.0,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // use in aggregation operation
    // avg
    sql = "select avg(if(s1=0, 100, s1)) from table1";
    retArray = new String[] {"41.5,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // max
    sql = "select max(if(s1=0, 100, s1)) from table1";
    retArray = new String[] {"100,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // avg × max
    sql = "select avg(if(s1=0, 100, s1)) * max(if(s1=0, 100, s1)) from table1";
    retArray = new String[] {"4150.0,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // UDF
  }

  @Test
  public void testKind1UseOtherOperation() {
    // use scalar function
    String sql = "select if(sin(s1)>=0, '>=0', '<0') from table1";
    String[] retArray =
        new String[] {
          ">=0,", "<0,", "<0,", ">=0,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // true_value and false_value use scalar function
    sql = "select if(sin(s1)>=0, abs(s1), abs(s1+1)) from table1";
    retArray =
        new String[] {
          "0,", "12,", "23,", "33,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // use aggregation function
    sql = "select if(max(s1)>=44, max(s1), min(s1)) from table1";
    retArray = new String[] {"0,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // UDF
  }

  @Test
  public void testKind1UseInWhereClause() {
    String sql = "select s4 from table2 where if(s3=0, s4>44, if(s3=22, s4>0, if(time>300, true)))";
    String[] retArray = new String[] {"66.0,", "77.0,"};
    tableResultSetEqualTest(sql, new String[] {"s4"}, retArray, DATABASE);

    sql = "select if(s3=0, s4>44, if(s3=22, s4>0, if(time>300, true))) from table2";
    retArray =
        new String[] {
          "false,", "null,", "true,", "true,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // CASE time
    sql = "select s4 from table2 where if(time=0, false, if(time=300, true, if(time=200, true)))";
    retArray = new String[] {"55.0,", "66.0,"};
    tableResultSetEqualTest(sql, new String[] {"s4"}, retArray, DATABASE);
  }

  @Test
  public void testKind1CaseInIf() {
    String sql =
        "select if(s1=0 OR s1=22, cast(case when s1=0 then 99 when s1>22 then 999 end as STRING), 'xxx') from table1";
    String[] retArray =
        new String[] {
          "99,", "xxx,", "null,", "xxx,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testKind1IfInIf() {
    String sql =
        "select if(s1=0 OR s1=22, cast(if(s1=0, 99, if(s1>22, 999)) as STRING), 'xxx') from table1";
    String[] retArray =
        new String[] {
          "99,", "xxx,", "null,", "xxx,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testKind1Logic() {
    String sql =
        "select if(s3 >= 0 and s3 < 20 and s4 >= 50 and s4 < 60, 'value1', if(s3 >= 20 and s3 < 40 and s4 >= 70 and s4 < 80, 'value2')) from table2";
    String[] retArray = new String[] {"null,", "value1,", "null,", "value2,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testReturnValueWithBinarySameConstant() {
    String sql = "select if(true, 200 + (s1 - 200)) as result FROM table1";
    String[] expectedHeader = new String[] {"result"};
    String[] retArray = new String[] {"0,", "11,", "22,", "33,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testIfWithText() {
    String sql = "select if(s6 like 'text%', 'true', 'false') FROM table1";
    String[] retArray = new String[] {"true,", "true,", "true,", "true,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    sql = "select if(s6 is not null, 'true', 'false') FROM table1";
    retArray = new String[] {"true,", "true,", "true,", "true,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    sql = "select if(s6 in ('text100', 'text200'), 'true', 'false') FROM table1";
    retArray = new String[] {"true,", "true,", "false,", "false,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // REGEXP does not work yet
  }
}
