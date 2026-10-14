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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

// TODO:fix the different type exception
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBCaseWhenThenTableIT {

  private static final String DATABASE = "test";

  private static final String[] expectedHeader = {"_col0"};

  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE " + DATABASE,
        "Use " + DATABASE,
        "CREATE table table1 (device_id STRING TAG, s1 INT32 FIELD, s5 BOOLEAN FIELD, s6 TEXT FIELD)",
        "CREATE table table2 (device_id STRING TAG, s3 FLOAT FIELD, s4 DOUBLE FIELD)",
        "CREATE table table3 (device_id STRING TAG, s2 INT64 FIELD)",
        "INSERT INTO table1(time, device_id, s1) values(100, 'd1', 0)",
        "INSERT INTO table1(time, device_id, s1) values(200, 'd1', 11)",
        "INSERT INTO table1(time, device_id, s1) values(300, 'd1', 22)",
        "INSERT INTO table1(time, device_id, s1) values(400, 'd1', 33)",
        "INSERT INTO table2(time, device_id, s3) values(100, 'd1', 0)",
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
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockLineNumber(1);
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
  public void testIfWithCastedDefaultType() {
    String[] retArray = new String[] {"0,", "0,", "2,", "3,"};
    tableResultSetEqualTest(
        "select if(s2 > 1, s2, cast(0 as int64)) from table3 limit 4",
        expectedHeader,
        retArray,
        DATABASE);
  }

  @Test
  public void testKind1Basic() {
    String[] retArray = new String[] {"99,", "9999,", "9999,", "999,"};
    tableResultSetEqualTest(
        "select case when s1=0 then 99 when s1>22 then 999 else 9999 end from table1",
        expectedHeader,
        retArray,
        DATABASE);

    // without ELSE clause
    retArray = new String[] {"99,", "null,", "null,", "999,"};
    tableResultSetEqualTest(
        "select case when s1=0 then 99 when s1>22 then 999 end from table1",
        expectedHeader,
        retArray,
        DATABASE);
  }

  @Test
  public void testKind2Basic() {
    String sql = "select case s1 when 0 then 99 when 22 then 999 else 9999 end from table1";
    String[] retArray = new String[] {"99,", "9999,", "999,", "9999,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // without ELSE clause
    sql = "select case s1 when 0 then 99 when 22 then 999 end from table1";
    retArray = new String[] {"99,", "null,", "999,", "null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testShortCircuitEvaluation() {
    String[] retArray = new String[] {"0,", "11,", "22,", "33,"};
    tableResultSetEqualTest(
        "select case when 1=0 then s1/0 when 1!=0 then s1 end from table1",
        expectedHeader,
        retArray,
        DATABASE);
  }

  @Test
  public void testKind1InputTypeRestrict() {
    // WHEN clause must return BOOLEAN
    String sql = "select case when s1+1 then 20 else 22 end from table1";
    String msg = "701: CASE WHEN clause must evaluate to a BOOLEAN (actual: INT32)";
    tableAssertTestFail(sql, msg, DATABASE);
  }

  @Test
  public void testKind2InputTypeRestrict() {
    // the expression in CASE clause must be able to be equated with the expression in WHEN clause
    String sql = "select case s1 when '1' then 20 else 22 end from table1";
    String msg = "701: CASE operand type does not match WHEN clause operand type: INT32 vs STRING";
    tableAssertTestFail(sql, msg, DATABASE);
  }

  @Test
  public void testKind1OutputTypeRestrict() {
    // BOOLEAN and other types cannot exist at the same time
    String[] retArray = new String[] {"true,", "false,", "true,", "true,"};
    // success
    tableResultSetEqualTest(
        "select case when s1<=0 then true when s1=11 then false else true end from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select case when s1<=0 then true else 22 end from table1",
        "701: All CASE results must be the same type or coercible to a common type. Cannot find common type between BOOLEAN and INT32, all types (without duplicates): [BOOLEAN, INT32]",
        DATABASE);
    tableAssertTestFail(
        "select case when s1<=0 then 0 when s1>1 then null end from table1",
        "701: All result types and default result type must be the same:",
        DATABASE);

    // TEXT and other types cannot exist at the same time
    retArray = new String[] {"good,", "bad,", "okok,", "okok,"};
    // success
    tableResultSetEqualTest(
        "select case when s1<=0 then 'good' when s1=11 then 'bad' else 'okok' end from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select case when s1<=0 then 'good' else 22 end from table1",
        "701: All CASE results must be the same type or coercible to a common type. Cannot find common type between STRING and INT32, all types (without duplicates): [STRING, INT32]",
        DATABASE);

    // 4 numerical types(INT LONG FLOAT DOUBLE) can exist at the same time
    //    retArray = new String[] {"99.0,", "99.9,", "8.589934588E9,", "999.9999999999,"};
    //    tableResultSetEqualTest(
    //        "select case when s1=0 then 99 when s1=11 then 99.9 when s1=22 then 8589934588 when
    // s1=33 then 999.9999999999 else 10086 end from table1",
    //        expectedHeader,
    //        retArray,
    //        DATABASE);
  }

  @Test
  public void testKind2OutputTypeRestrict() {
    // BOOLEAN and other types cannot exist at the same time
    String[] retArray =
        new String[] {
          "true,", "false,", "true,", "true,",
        };
    // success
    tableResultSetEqualTest(
        "select case s1 when 0 then true when 11 then false else true end from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select case s1 when 0 then true else 22 end from table1",
        "701: All CASE results must be the same type or coercible to a common type. Cannot find common type between BOOLEAN and INT32, all types (without duplicates): [BOOLEAN, INT32]",
        DATABASE);

    // TEXT and other types cannot exist at the same time
    retArray = new String[] {"good,", "bad,", "okok,", "okok,"};
    // success
    tableResultSetEqualTest(
        "select case s1 when 0 then 'good' when 11 then 'bad' else 'okok' end from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select case s1 when 0 then 'good' else 22 end from table1",
        "701: All CASE results must be the same type or coercible to a common type. Cannot find common type between STRING and INT32, all types (without duplicates): [STRING, INT32]",
        DATABASE);

    // 4 numerical types(INT LONG FLOAT DOUBLE) can exist at the same time
    //    tableAssertTestFail(
    //        "select case s1 when 0 then 99 when 11 then 99.9 when 22 then 8589934588 when 33 then
    // 999.9999999999 else 10086 end from table1",
    //        "All result types must be the same",
    //        DATABASE);
    //    retArray = new String[] {"99.0,", "99.9,", "8.589934588E9,", "999.9999999999,"};
    //    tableResultSetEqualTest(
    //        "select case s1 when 0 then 99 when 11 then 99.9 when 22 then 8589934588 when 33 then
    // 999.9999999999 else 10086 end from table1",
    //        expectedHeader,
    //        retArray,
    //        DATABASE);
  }

  /** 100 branches */
  @Test
  public void testKind1LargeNumberBranches() {
    StringBuilder sqlBuilder = new StringBuilder();
    List<String> retList = new ArrayList<>();
    sqlBuilder.append("select case ");
    for (int i = 0; i < 100; i++) {
      sqlBuilder.append(String.format("when s2=%d then s2*%d ", i, i * 100));
      retList.add(String.format("%d,", i * i * 100));
    }
    sqlBuilder.append("end from table3");
    tableResultSetEqualTest(
        sqlBuilder.toString(), expectedHeader, retList.toArray(new String[] {}), DATABASE);
  }

  /** 100 branches */
  @Ignore
  @Test
  public void testKind2LargeNumberBranches() {
    StringBuilder sqlBuilder = new StringBuilder();
    List<String> retList = new ArrayList<>();
    sqlBuilder.append("select case s2 ");
    for (int i = 0; i < 100; i++) {
      sqlBuilder.append(String.format("when %d then s2*%d ", i, i * 100));
      retList.add(String.format("%d.0,", i * i * 100));
    }
    sqlBuilder.append("end from table1");
    tableResultSetEqualTest(
        sqlBuilder.toString(), expectedHeader, retList.toArray(new String[] {}), DATABASE);
  }

  @Test
  public void testKind1UsedInOtherOperation() {
    String sql;
    String[] retArray;

    // use in scalar operation

    // multiply
    sql = "select 2 * case when s1=0 then 99 when s1=22.0 then 999 else 9999 end from table1";
    retArray = new String[] {"198,", "19998,", "1998,", "19998,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // add
    sql =
        "select "
            + "case when s1=0 then 99 when s1=22.0 then 999 else 9999 end "
            + "+"
            + "case when s1=11 then 99 else 9999 end "
            + "from table1";
    retArray =
        new String[] {
          "10098,", "10098,", "10998,", "19998,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // function
    sql = "select diff(case when s1=0 then 99 when s1>22 then 999 else 9999 end) from table1";
    retArray = new String[] {"null,", "9900.0,", "0.0,", "-9000.0,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // use in aggregation operation

    // avg
    // not support yet

    // max
    // not support yet

    // avg × max_value
    // not support yet

    // UDF is not allowed
  }

  @Test
  public void testKind2UsedInOtherOperation() {
    String sql;
    String[] retArray;

    // use in scalar operation

    // multiply
    sql = "select 2 * case s1 when 0 then 99 when 22 then 999 else 9999 end from table1";
    retArray = new String[] {"198,", "19998,", "1998,", "19998,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // add
    //    sql =
    //        "select "
    //            + "case s1 when 0 then 99 when 22 then 999 else 9999 end "
    //            + "+"
    //            + "case s4 when 55.0 then 99 else 9999 end "
    //            + "from table1, table2";
    //
    //    retArray =
    //        new String[] {
    //          "10098,", "10098,", "10998,", "19998,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // function
    sql = "select diff(case s1 when 0 then 99 when 22 then 999 else 9999 end) from table1";
    retArray =
        new String[] {
          "null,", "9900.0,", "-9000.0,", "9000.0,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // use in aggregation operation

    // avg
    // not support yet

    // max_value
    // not support yet

    // avg × max_value
    // not support yet

    // UDF is not allowed
  }

  @Test
  public void testKind1UseOtherOperation() {
    // WHEN-clause use scalar function
    String sql = "select case when sin(s1)>=0 then '>0' else '<0' end from table1";
    String[] retArray =
        new String[] {
          ">0,", "<0,", "<0,", ">0,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // THEN-clause and ELSE-clause use scalar function

    // TODO: align by is not supported.

    //    sql =
    //        "select case when s1<=11 then CAST(diff(s1) as TEXT) else CAST(s1-1 as TEXT) end from
    // table1 align by device";
    //
    //    retArray =
    //        new String[] {
    //          "0,table1,null,",
    //          "1000000,table1,11.0,",
    //          "20000000,table1,21.0,",
    //          "210000000,table1,32.0,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testKind2UseOtherOperation() {
    // CASE-clause use scalar function
    String sql =
        "select case round(sin(s1)) when 0 then '=0' when -1 then '<0' else '>0' end from table1";

    tableAssertTestFail(
        sql,
        "701: CASE operand type does not match WHEN clause operand type: DOUBLE vs INT32",
        DATABASE);
    //    String[] retArray =
    //        new String[] {
    //          "=0,", "<0,", ">0,", ">0,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // WHEN-clause use scalar function
    sql = "select case 0 when sin(s1) then '=0' else '!=0' end from table1";
    tableAssertTestFail(
        sql,
        "701: CASE operand type does not match WHEN clause operand type: INT32 vs DOUBLE",
        DATABASE);
    //    retArray =
    //        new String[] {
    //          "=0,", "!=0,", "!=0,", "!=0,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // THEN-clause and ELSE-clause use scalar function
    //    sql =
    //        "select case s1 when 11 then CAST(diff(s1) as TEXT) else CAST(s1-1 as TEXT) end from
    // table1 align by device";
    //
    //    retArray =
    //        new String[] {
    //          "table1,-1.0,", "table1,11.0,", "table1,21.0,", "table1,32.0,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // UDF is not allowed
    //    sql = "select case s1 when 0 then change_points(s1) end from table1";
    //    String msg = "301: CASE expression cannot be used with non-mappable UDF";
    //    tableAssertTestFail(sql, msg, DATABASE);
  }

  @Test
  @Ignore
  public void testKind1Wildcard() {
    String sql = "select case when *=* then * else * end from root.sg.d2";
    String[] expectedHeaders = new String[16];
    for (int i = 0; i < expectedHeaders.length; i++) {
      expectedHeaders[i] = "_col" + i;
    }
    String[] retArray = {
      "0.0,0.0,44.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,0.0,44.0,44.0,",
      "11.0,11.0,55.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,11.0,55.0,55.0,",
      "22.0,22.0,66.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,22.0,66.0,66.0,",
      "33.0,33.0,77.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,33.0,77.0,77.0,",
    };
    tableResultSetEqualTest(sql, expectedHeaders, retArray, DATABASE);
  }

  @Test
  @Ignore
  public void testKind2Wildcard() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("select ");
    String[] caseSQL =
        new String[] {
          "CASE WHEN s3 = s3 THEN s3 ELSE s3 END,",
          "CASE WHEN s3 = s3 THEN s3 ELSE s4 END,",
          "CASE WHEN s3 = s3 THEN s4 ELSE s3 END,",
          "CASE WHEN s3 = s3 THEN s4 ELSE s4 END,",
          "CASE WHEN s3 = s4 THEN s3 ELSE s3 END,",
          "CASE WHEN s3 = s4 THEN s3 ELSE s4 END,",
          "CASE WHEN s3 = s4 THEN s4 ELSE s3 END,",
          "CASE WHEN s3 = s4 THEN s4 ELSE s4 END,",
          "CASE WHEN s4 = s3 THEN s3 ELSE s3 END,",
          "CASE WHEN s4 = s3 THEN s3 ELSE s4 END,",
          "CASE WHEN s4 = s3 THEN s4 ELSE s3 END,",
          "CASE WHEN s4 = s3 THEN s4 ELSE s4 END,",
          "CASE WHEN s4 = s4 THEN s3 ELSE s3 END,",
          "CASE WHEN s4 = s4 THEN s3 ELSE s4 END,",
          "CASE WHEN s4 = s4 THEN s4 ELSE s3 END,",
          "CASE WHEN s4 = s4 THEN s4 ELSE s4 END",
        };
    for (String string : caseSQL) {
      sqlBuilder.append(string);
    }
    sqlBuilder.append(" from table2");

    String[] expectedHeaders = new String[16];
    for (int i = 0; i < 16; i++) {
      expectedHeaders[i] = "_col" + i;
    }

    String[] retArray = {
      "0,0.0,0.0,44.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,0.0,44.0,44.0,",
      "1000000,11.0,11.0,55.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,11.0,55.0,55.0,",
      "20000000,22.0,22.0,66.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,22.0,66.0,66.0,",
      "210000000,33.0,33.0,77.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,33.0,77.0,77.0,",
    };
    tableResultSetEqualTest(sqlBuilder.toString(), expectedHeaders, retArray, DATABASE);
  }

  @Ignore
  @Test
  public void testKind1AlignedByDevice() {
    // from different devices, result should be empty
    String sql = "select case when s1<=11 then s3 else s4 end from table1, table2 align by device";
    String[] retArray = {};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    //     from same device
    sql = "select case when s3<=11 then s3 else s4 end from table1, root.sg.d2 align by device";
    retArray =
        new String[] {
          "root.sg.d2,0.0,", "root.sg.d2,11.0,", "root.sg.d2,66.0,", "root.sg.d2,77.0,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    //     from same device, two result column
    sql =
        "select "
            + "case when s1<=11 then s1 else s1*2 end, "
            + "case when s3<=11 then s3 else s4 end "
            + "from table1, table2 align by device";

    retArray =
        new String[] {
          "table1,0.0,null,",
          "table1,11.0,null,",
          "table1,44.0,null,",
          "table1,66.0,null,",
          "root.sg.d2,null,0.0,",
          "root.sg.d2,null,11.0,",
          "root.sg.d2,null,66.0,",
          "root.sg.d2,null,77.0,",
        };
    tableResultSetEqualTest(sql, new String[] {"_col0", "_col1"}, retArray, DATABASE);
  }

  @Ignore
  @Test
  public void testKind2AlignedByDevice() {
    // from different devices, result should be empty
    String sql = "select case s1 when 11 then s3 else s4 end from table1, table2 align by device";
    String[] retArray = {};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // from same device
    sql = "select case s3 when 11 then s3 else s4 end from table1, root.sg.d2 align by device";
    retArray =
        new String[] {
          "table2,44.0,", "table2,11.0,", "table2,66.0,", "table2,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // from same device, two result column
    sql =
        "select "
            + "case s1 when 11 then s1 else s1*2 end, "
            + "case s3 when 11 then s3 else s4 end "
            + "from table1, root.sg.d2 align by device";
    retArray =
        new String[] {
          "0,table1,0.0,null,",
          "1000000,table1,11.0,null,",
          "20000000,table1,44.0,null,",
          "210000000,table1,66.0,null,",
          "0,root.sg.d2,null,44.0,",
          "1000000,root.sg.d2,null,11.0,",
          "20000000,root.sg.d2,null,66.0,",
          "210000000,root.sg.d2,null,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Ignore
  @Test
  public void testKind1MultipleTimeseries() {
    // time stamp is aligned
    String sql = "select s1*s1, case when s1<=11 then s3 else s4 end from table1, table2";
    String[] retArray =
        new String[] {
          "0.0,0.0,", "121.0,11.0,", "484.0,66.0,", "1089.0,77.0,",
        };
    tableResultSetEqualTest(sql, new String[] {"_col0,_col1"}, retArray, DATABASE);

    // time stamp is not aligned
    sql =
        "select "
            + "case when s2%2==1 then s2 else s2/2 end, "
            + "case when s1<=11 then s3 else s4 end "
            + "from table1, table2 limit 5 offset 98";

    retArray =
        new String[] {
          "98,49.0,null,",
          "99,99.0,null,",
          "1000000,null,11.0,",
          "20000000,null,66.0,",
          "210000000,null,77.0,",
        };
    tableResultSetEqualTest(sql, new String[] {"_col0", "_col1"}, retArray, DATABASE);
  }

  @Ignore
  @Test
  public void testKind2MultipleTimeseries() {
    // time stamp is aligned
    String sql = "select s1*s1, case s1 when 11 then s3 else s4 end from table1, table2";
    String[] retArray =
        new String[] {
          "0.0,44.0,", "121.0,11.0,", "484.0,66.0,", "1089.0,77.0,",
        };
    tableResultSetEqualTest(sql, new String[] {"_col0", "_col1"}, retArray, DATABASE);

    // time stamp is not aligned
    sql =
        "select "
            + "case s2%2 when 1 then s2 else s2/2 end, "
            + "case s3 when 11 then s3 else s4 end "
            + "from table1, root.sg.d2 limit 5 offset 98";
    retArray =
        new String[] {
          "49.0,null,", "99.0,null,", "null,11.0,", "null,66.0,", "null,77.0,",
        };
    resultSetEqualTest(sql, new String[] {"_col0", "_col1"}, retArray);
  }

  @Test
  public void testKind1UseInWhereClause() {
    String sql =
        "select s4 from table2 where case when s3=0 then s4>44 when s3=22 then s4>0 when time>300 then true end";
    String[] retArray = new String[] {"66.0,", "77.0,"};
    tableResultSetEqualTest(sql, new String[] {"s4"}, retArray, DATABASE);

    sql =
        "select case when s3=0 then s4>44 when s3=22 then s4>0 when time>300 then true end from table2";
    retArray =
        new String[] {
          "false,", "null,", "true,", "true,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Ignore
  @Test
  public void testKind2UseInWhereClause() {
    String sql = "select s4 from table2 where case s3 when 0 then s4>44 when 22 then s4>0 end";
    String[] retArray = new String[] {"66.0,"};
    tableResultSetEqualTest(sql, new String[] {"s4"}, retArray, DATABASE);

    // CASE time
    sql =
        "select s4 from table2 where case time when 0 then false when 300 then true when 200 then true end";
    retArray = new String[] {"55.0,", "66.0,"};
    tableResultSetEqualTest(sql, new String[] {"s4"}, retArray, DATABASE);
  }

  @Test
  public void testKind1CaseInCase() {
    String sql =
        "select case when s1=0 OR s1=22 then cast(case when s1=0 then 99 when s1>22 then 999 end as STRING) else 'xxx' end from table1";
    String[] retArray =
        new String[] {
          "99,", "xxx,", "null,", "xxx,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testKind2CaseInCase() {
    String sql =
        "select case s1 when 0 then cast(case when s1=0 then 99 when s1>22 then 999 end as STRING) when 22 then cast(case when s1=0 then 99 when s1>22 then 999 end as STRING) else 'xxx' end from table1";
    String[] retArray =
        new String[] {
          "99,", "xxx,", "null,", "xxx,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testKind1Logic() {
    String sql =
        "select case when s3 >= 0 and s3 < 20 and s4 >= 50 and s4 < 60 then 'just so so~~~' when s3 >= 20 and s3 < 40 and s4 >= 70 and s4 < 80 then 'very well~~~' end from table2";
    String[] retArray = new String[] {"null,", "just so so~~~,", "null,", "very well~~~,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testThenWithBinarySameConstant() {
    String sql = "SELECT CASE WHEN true THEN 200 + (s1 - 200) END AS result FROM table1";
    String[] expectedHeader = new String[] {"result"};
    String[] retArray = new String[] {"0,", "11,", "22,", "33,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  @Test
  public void testCaseWhenReusesAggregationAcrossBlocks() {
    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "CREATE TABLE case_aggregation_cache (dev STRING TAG, c DOUBLE FIELD, q INT32 FIELD)",
          "INSERT INTO case_aggregation_cache(time, dev, c, q) VALUES "
              + "(0, 'd0', 1.0, 0), (0, 'd1', 1.0, 0), (3600000, 'd1', 1.0, 0)"
        });

    // The non-pushdown predicate and the different device/window shapes make the aggregation
    // produce multiple blocks. CASE clears the shared aggregate before the next output uses it.
    String suffix = " FROM case_aggregation_cache WHERE q % 2 < 1 GROUP BY dev, date_bin(1h, time)";
    for (String projection :
        new String[] {
          "CASE WHEN sum(c) > 0 THEN 1.0 END AS case_result, CAST(sum(c) AS DOUBLE) AS total",
          "CASE WHEN sum(c) > 0 THEN 1.0 END AS case_result, sum(c) + 0 AS total",
          "CASE WHEN sum(c) > 0 THEN CAST(sum(c) AS DOUBLE) / CAST(sum(c) AS DOUBLE) END "
              + "AS case_result, CAST(sum(c) AS DOUBLE) AS total"
        }) {
      tableResultSetEqualTest(
          "SELECT " + projection + suffix,
          new String[] {"case_result", "total"},
          new String[] {"1.0,1.0,", "1.0,1.0,", "1.0,1.0,"},
          DATABASE);
    }
  }

  @Test
  public void testCaseOperandsAndMultiArgumentFunctions() {
    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "CREATE TABLE case_operands (dev STRING TAG, x DOUBLE FIELD)",
          "INSERT INTO case_operands(time, dev, x) VALUES (0, 'd0', 1.0), (1, 'd0', -2.0)"
        });
    String caseWhen = "CASE WHEN x > 0 THEN 2.0 ELSE 3.0 END";
    String[] expressions = {
      "x + " + caseWhen,
      "CASE WHEN x > -100 THEN coalesce(x, 0.0) END",
      "greatest(x, " + caseWhen + ")",
      "CASE WHEN x BETWEEN -5.0 AND " + caseWhen + " THEN 1.0 ELSE 0.0 END",
      "CASE WHEN x > -100 THEN CASE WHEN x BETWEEN -5.0 AND "
          + caseWhen
          + " THEN 1.0 ELSE 0.0 END END",
      "coalesce(x, " + caseWhen + ")",
      "CASE WHEN x > -100 THEN coalesce(x, " + caseWhen + ") END"
    };
    String[][] results = {
      {"3.0,", "1.0,"},
      {"1.0,", "-2.0,"},
      {"2.0,", "3.0,"},
      {"1.0,", "1.0,"},
      {"1.0,", "1.0,"},
      {"1.0,", "-2.0,"},
      {"1.0,", "-2.0,"}
    };
    for (int i = 0; i < expressions.length; i++) {
      tableResultSetEqualTest(
          "SELECT " + expressions[i] + " AS result FROM case_operands ORDER BY time",
          new String[] {"result"},
          results[i],
          DATABASE);
    }
    tableResultSetEqualTest(
        "SELECT coalesce(x, 0.0) AS result FROM case_operands "
            + "WHERE CASE WHEN coalesce(x, 0.0) > 0 THEN 1.0 ELSE 0.0 END > 0",
        new String[] {"result"},
        new String[] {"1.0,"},
        DATABASE);
  }

  @Test
  public void testCommonCastRespectsCaseFilter() {
    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "CREATE TABLE guarded_common_cast (dev STRING TAG, src STRING FIELD)",
          "INSERT INTO guarded_common_cast(time, dev, src) VALUES "
              + "(0, 'd0', 'n/a'), (1, 'd0', '5')"
        });
    String query =
        "SELECT CAST(src AS INT32) + 1 AS result FROM guarded_common_cast "
            + "WHERE CASE WHEN src <> 'n/a' THEN CAST(src AS INT32) > 10 ELSE false END";
    tableResultSetEqualTest(query, new String[] {"result"}, new String[0], DATABASE);

    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "INSERT INTO guarded_common_cast(time, dev, src) VALUES (2, 'd0', '50')"
        });
    tableResultSetEqualTest(query, new String[] {"result"}, new String[] {"51,"}, DATABASE);
  }

  @Test
  public void testReusingCasePreservesPendingSiblingColumn() {
    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "CREATE TABLE case_sibling_cache (dev STRING TAG, x DOUBLE FIELD)",
          "INSERT INTO case_sibling_cache(time, dev, x) VALUES "
              + "(0, 'd0', -3.0), (1, 'd0', 0.0), (2, 'd0', 2.0)"
        });

    String sharedCase = "CASE WHEN x > 0 THEN 1.0 ELSE 0.0 END";
    tableResultSetEqualTest(
        "SELECT "
            + sharedCase
            + " AS a, x + CASE WHEN ("
            + sharedCase
            + ") > 0 THEN 2.0 ELSE 3.0 END AS b FROM case_sibling_cache ORDER BY time",
        new String[] {"a", "b"},
        new String[] {"0.0,0.0,", "0.0,3.0,", "1.0,4.0,"},
        DATABASE);
  }

  @Test
  public void testSharedDiffAcrossCaseFilterAndProjection() {
    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "CREATE TABLE case_shared_diff (dev STRING TAG, x DOUBLE FIELD)",
          "INSERT INTO case_shared_diff(time, dev, x) VALUES "
              + "(1, 'd0', 1.0), (2, 'd0', 3.0), (3, 'd0', 6.0)",
          "FLUSH",
          "INSERT INTO case_shared_diff(time, dev, x) VALUES (4, 'd0', 10.0), (5, 'd0', 11.0)",
          "FLUSH"
        });
    tableResultSetEqualTest(
        "SELECT time, diff(x) AS d FROM case_shared_diff "
            + "WHERE CASE WHEN diff(x) > 1.0 THEN 1.0 ELSE 0.0 END > 0.0",
        new String[] {"time", "d"},
        new String[] {
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
          "1970-01-01T00:00:00.004Z,4.0,"
        },
        DATABASE);
    tableResultSetEqualTest(
        "SELECT diff(x) AS d FROM case_shared_diff "
            + "WHERE CASE WHEN diff(x) > 1.0 THEN true ELSE true END",
        new String[] {"d"},
        new String[] {"null,", "2.0,", "3.0,", "4.0,", "1.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "SELECT CASE WHEN x > 5 THEN diff(x) ELSE 100.0 END AS a, "
            + "CASE WHEN x > 5 THEN 200.0 ELSE diff(x) END AS b, diff(x) AS d "
            + "FROM case_shared_diff",
        new String[] {"a", "b", "d"},
        new String[] {
          "100.0,null,null,", "100.0,2.0,2.0,", "3.0,200.0,3.0,", "4.0,200.0,4.0,", "1.0,200.0,1.0,"
        },
        DATABASE);
  }
}
