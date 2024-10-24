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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCaseWhenThenIT {
  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d2.s3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d2.s4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "INSERT INTO root.sg.d1(timestamp,s1) values(0,         0) ",
        "INSERT INTO root.sg.d1(timestamp,s1) values(1000000,   11)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(20000000,  22)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(210000000, 33)",
        "INSERT INTO root.sg.d2(timestamp,s3) values(0,         0) ",
        "INSERT INTO root.sg.d2(timestamp,s3) values(1000000,   11)",
        "INSERT INTO root.sg.d2(timestamp,s3) values(20000000,  22)",
        "INSERT INTO root.sg.d2(timestamp,s3) values(210000000, 33)",
        "INSERT INTO root.sg.d2(timestamp,s4) values(0,         44)",
        "INSERT INTO root.sg.d2(timestamp,s4) values(1000000,   55)",
        "INSERT INTO root.sg.d2(timestamp,s4) values(20000000,  66)",
        "INSERT INTO root.sg.d2(timestamp,s4) values(210000000, 77)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
    List<String> moreSQLs = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      moreSQLs.add(String.format("INSERT INTO root.sg.d1(timestamp,s2) values(%d, %d)", i, i));
    }
    prepareData(moreSQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testKind1Basic() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 ELSE 9999 END"
        };
    String[] retArray =
        new String[] {"0,99.0,", "1000000,9999.0,", "20000000,9999.0,", "210000000,999.0,"};
    resultSetEqualTest(
        "select case when s1=0 then 99 when s1>22 then 999 else 9999 end from root.sg.d1",
        expectedHeader,
        retArray);

    // without ELSE clause
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 END"
        };
    retArray = new String[] {"0,99.0,", "1000000,null,", "20000000,null,", "210000000,999.0,"};
    resultSetEqualTest(
        "select case when s1=0 then 99 when s1>22 then 999 end from root.sg.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testKind2Basic() {
    String sql = "select case s1 when 0 then 99 when 22 then 999 else 9999 end from root.sg.d1";
    String[] expectedHeader =
        new String[] {
          "Time",
          "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22 THEN 999 ELSE 9999 END",
        };
    String[] retArray =
        new String[] {
          "0,99.0,", "1000000,9999.0,", "20000000,999.0,", "210000000,9999.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // without ELSE clause
    sql = "select case s1 when 0 then 99 when 22 then 999 end from root.sg.d1";
    expectedHeader =
        new String[] {
          "Time", "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22 THEN 999 END",
        };
    retArray =
        new String[] {
          "0,99.0,", "1000000,null,", "20000000,999.0,", "210000000,null,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testShortCircuitEvaluation() {
    String[] retArray =
        new String[] {"0,0.0,", "1000000,11.0,", "20000000,22.0,", "210000000,33.0,"};
    String[] expectedHeader =
        new String[] {
          "Time", "CASE WHEN 1 = 0 THEN root.sg.d1.s1 / 0 WHEN 1 != 0 THEN root.sg.d1.s1 END",
        };
    resultSetEqualTest(
        "select case when 1=0 then s1/0 when 1!=0 then s1 end from root.sg.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testKind1InputTypeRestrict() {
    // WHEN clause must return BOOLEAN
    String sql = "select case when s1+1 then 20 else 22 end from root.sg.d1";
    String msg =
        "701: The expression in the WHEN clause must return BOOLEAN. expression: root.sg.d1.s1 + 1, actual data type: DOUBLE.";
    assertTestFail(sql, msg);
  }

  @Test
  public void testKind2InputTypeRestrict() {
    // the expression in CASE clause must be able to be equated with the expression in WHEN clause
    String sql = "select case s1 when \"1\" then 20 else 22 end from root.sg.d1";
    String msg =
        "701: Invalid input expression data type. expression: root.sg.d1.s1, actual data type: INT32, expected data type(s): [TEXT, STRING].";
    assertTestFail(sql, msg);
  }

  @Test
  public void testKind1OutputTypeRestrict() {
    // BOOLEAN and other types cannot exist at the same time
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 <= 0 THEN true WHEN root.sg.d1.s1 = 11 THEN false ELSE true END"
        };
    String[] retArray =
        new String[] {"0,true,", "1000000,false,", "20000000,true,", "210000000,true,"};
    // success
    resultSetEqualTest(
        "select case when s1<=0 then true when s1=11 then false else true end from root.sg.d1",
        expectedHeader,
        retArray);
    // fail
    assertTestFail(
        "select case when s1<=0 then true else 22 end from root.sg.d1",
        "701: CASE expression: BOOLEAN and other types cannot exist at the same time");

    // TEXT and other types cannot exist at the same time
    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 <= 0 THEN \"good\" WHEN root.sg.d1.s1 = 11 THEN \"bad\" ELSE \"okok\" END"
        };
    retArray = new String[] {"0,good,", "1000000,bad,", "20000000,okok,", "210000000,okok,"};
    // success
    resultSetEqualTest(
        "select case when s1<=0 then \"good\" when s1=11 then \"bad\" else \"okok\" end from root.sg.d1",
        expectedHeader,
        retArray);
    // fail
    assertTestFail(
        "select case when s1<=0 then \"good\" else 22 end from root.sg.d1",
        "701: CASE expression: TEXT and other types cannot exist at the same time");

    // 4 numerical types(INT LONG FLOAT DOUBLE) can exist at the same time
    expectedHeader = new String[] {TIMESTAMP_STR, "result"};
    retArray =
        new String[] {
          "0,99.0,", "1000000,99.9,", "20000000,8.589934588E9,", "210000000,999.9999999999,"
        };
    resultSetEqualTest(
        "select case when s1=0 then 99 when s1=11 then 99.9 when s1=22 then 8589934588 when s1=33 then 999.9999999999 else 10086 end as `result` from root.sg.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testKind2OutputTypeRestrict() {
    // BOOLEAN and other types cannot exist at the same time
    String[] expectedHeader =
        new String[] {
          "Time",
          "CASE WHEN root.sg.d1.s1 = 0 THEN true WHEN root.sg.d1.s1 = 11 THEN false ELSE true END",
        };
    String[] retArray =
        new String[] {
          "0,true,", "1000000,false,", "20000000,true,", "210000000,true,",
        };
    // success
    resultSetEqualTest(
        "select case s1 when 0 then true when 11 then false else true end from root.sg.d1",
        expectedHeader,
        retArray);
    // fail
    assertTestFail(
        "select case s1 when 0 then true else 22 end from root.sg.d1",
        "701: CASE expression: BOOLEAN and other types cannot exist at the same time");

    // TEXT and other types cannot exist at the same time
    expectedHeader =
        new String[] {
          "Time",
          "CASE WHEN root.sg.d1.s1 = 0 THEN \"good\" WHEN root.sg.d1.s1 = 11 THEN \"bad\" ELSE \"okok\" END",
        };
    retArray = new String[] {"0,good,", "1000000,bad,", "20000000,okok,", "210000000,okok,"};
    // success
    resultSetEqualTest(
        "select case s1 when 0 then \"good\" when 11 then \"bad\" else \"okok\" end from root.sg.d1",
        expectedHeader,
        retArray);
    // fail
    assertTestFail(
        "select case s1 when 0 then \"good\" else 22 end from root.sg.d1",
        "701: CASE expression: TEXT and other types cannot exist at the same time");

    // 4 numerical types(INT LONG FLOAT DOUBLE) can exist at the same time
    expectedHeader = new String[] {TIMESTAMP_STR, "result"};
    retArray =
        new String[] {
          "0,99.0,", "1000000,99.9,", "20000000,8.589934588E9,", "210000000,999.9999999999,"
        };
    resultSetEqualTest(
        "select case s1 when 0 then 99 when 11 then 99.9 when 22 then 8589934588 when 33 then 999.9999999999 else 10086 end as `result` from root.sg.d1",
        expectedHeader,
        retArray);
  }

  /** 100 branches */
  @Test
  public void testKind1LargeNumberBranches() {
    StringBuilder sqlBuilder = new StringBuilder(), expectedHeaderBuilder = new StringBuilder();
    List<String> retList = new ArrayList<>();
    sqlBuilder.append("select case ");
    expectedHeaderBuilder.append("CASE ");
    for (int i = 0; i < 100; i++) {
      sqlBuilder.append(String.format("when s2=%d then s2*%d ", i, i * 100));
      expectedHeaderBuilder.append(
          String.format("WHEN root.sg.d1.s2 = %d THEN root.sg.d1.s2 * %d ", i, i * 100));
      retList.add(String.format("%d,%d.0,", i, i * i * 100));
    }
    sqlBuilder.append("end from root.sg.d1");
    expectedHeaderBuilder.append("END");
    String[] expectedHeader = new String[] {TIMESTAMP_STR, expectedHeaderBuilder.toString()};
    resultSetEqualTest(sqlBuilder.toString(), expectedHeader, retList.toArray(new String[] {}));
  }

  /** 100 branches */
  @Test
  public void testKind2LargeNumberBranches() {
    StringBuilder sqlBuilder = new StringBuilder(), expectedHeaderBuilder = new StringBuilder();
    List<String> retList = new ArrayList<>();
    sqlBuilder.append("select case s2 ");
    expectedHeaderBuilder.append("CASE ");
    for (int i = 0; i < 100; i++) {
      sqlBuilder.append(String.format("when %d then s2*%d ", i, i * 100));
      expectedHeaderBuilder.append(
          String.format("WHEN root.sg.d1.s2 = %d THEN root.sg.d1.s2 * %d ", i, i * 100));
      retList.add(String.format("%d,%d.0,", i, i * i * 100));
    }
    sqlBuilder.append("end from root.sg.d1");
    expectedHeaderBuilder.append("END");
    String[] expectedHeader = new String[] {TIMESTAMP_STR, expectedHeaderBuilder.toString()};
    resultSetEqualTest(sqlBuilder.toString(), expectedHeader, retList.toArray(new String[] {}));
  }

  @Test
  public void testKind1UsedInOtherOperation() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // use in scalar operation

    // multiply
    sql = "select 2 * case when s1=0 then 99 when s1=22.0 then 999 else 9999 end from root.sg.d1";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "2 * CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22.0 THEN 999 ELSE 9999 END"
        };
    retArray =
        new String[] {"0,198.0,", "1000000,19998.0,", "20000000,1998.0,", "210000000,19998.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // add
    sql =
        "select "
            + "case when s1=0 then 99 when s1=22.0 then 999 else 9999 end "
            + "+"
            + "case when s1=11 then 99 else 9999 end "
            + "from root.sg.d1, root.sg.d2";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22.0 THEN 999 ELSE 9999 END + CASE WHEN root.sg.d1.s1 = 11 THEN 99 ELSE 9999 END"
        };
    retArray =
        new String[] {
          "0,10098.0,", "1000000,10098.0,", "20000000,10998.0,", "210000000,19998.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // function
    sql =
        "select diff(case when s1=0 then 99 when s1>22 then 999 else 9999 end) as `result` from root.sg.d1";
    expectedHeader = new String[] {TIMESTAMP_STR, "result"};
    retArray = new String[] {"0,null,", "1000000,9900.0,", "20000000,0.0,", "210000000,-9000.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // use in aggregation operation

    // avg
    sql =
        "select avg(case when s1=0 then 99 when s1>22 then 999 else 9999 end) as `result` from root.sg.d1";
    expectedHeader = new String[] {"result"};
    retArray = new String[] {"5274.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // max_value
    sql =
        "select max_value(case when s1=0 then 99 when s1>22 then 999 else 9999 end) as `result` from root.sg.d1";
    expectedHeader = new String[] {"result"};
    retArray = new String[] {"9999.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // avg × max_value
    resultSetEqualTest(
        "select avg(case when s1=0 then 99 when s1>22 then 999 else 9999 end) * max_value(case when s1=0 then 99 when s1>22 then 999 else 9999 end) from root.sg.d1",
        new String[] {
          "avg(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 ELSE 9999 END) * max_value(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 ELSE 9999 END)"
        },
        new String[] {"5.2734726E7,"});

    // UDF is not allowed

    sql = "select change_points(case when s1=0 then 99 end) from root.sg.d1";
    String msg = "301: CASE expression cannot be used with non-mappable UDF";
    assertTestFail(sql, msg);

    sql = "select change_points(s1) + case when s1=0 then 99 end from root.sg.d1";
    msg = "301: CASE expression cannot be used with non-mappable UDF";
    assertTestFail(sql, msg);
  }

  @Test
  public void testKind2UsedInOtherOperation() {
    String sql;
    String[] expectedHeader;
    String[] retArray;

    // use in scalar operation

    // multiply
    sql = "select 2 * case s1 when 0 then 99 when 22.0 then 999 else 9999 end from root.sg.d1";
    expectedHeader =
        new String[] {
          "Time",
          "2 * CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22.0 THEN 999 ELSE 9999 END",
        };
    retArray =
        new String[] {"0,198.0,", "1000000,19998.0,", "20000000,1998.0,", "210000000,19998.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // add
    sql =
        "select "
            + "case s1 when 0 then 99 when 22.0 then 999 else 9999 end "
            + "+"
            + "case s3 when 11 then 99 else 9999 end "
            + "from root.sg.d1, root.sg.d2";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22.0 THEN 999 ELSE 9999 END + CASE WHEN root.sg.d2.s3 = 11 THEN 99 ELSE 9999 END"
        };
    retArray =
        new String[] {
          "0,10098.0,", "1000000,10098.0,", "20000000,10998.0,", "210000000,19998.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // function
    sql =
        "select diff(case s1 when 0 then 99 when 22 then 999 else 9999 end) as `result` from root.sg.d1";
    expectedHeader = new String[] {TIMESTAMP_STR, "result"};
    retArray =
        new String[] {
          "0,null,", "1000000,9900.0,", "20000000,-9000.0,", "210000000,9000.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // use in aggregation operation

    // avg
    sql =
        "select avg(case s1 when 0 then 99 when 22 then 999 else 9999 end) as `result` from root.sg.d1";
    expectedHeader = new String[] {"result"};
    retArray = new String[] {"5274.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // max_value
    sql =
        "select max_value(case s1 when 0 then 99 when 22 then 999 else 9999 end) as `result` from root.sg.d1";
    expectedHeader = new String[] {"result"};
    retArray = new String[] {"9999.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // avg × max_value
    resultSetEqualTest(
        "select avg(case s1 when 0 then 99 when 22 then 999 else 9999 end) * max_value(case s1 when 0 then 99 when 22 then 999 else 9999 end) from root.sg.d1",
        new String[] {
          "avg(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22 THEN 999 ELSE 9999 END) * max_value(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22 THEN 999 ELSE 9999 END)"
        },
        new String[] {"5.2734726E7,"});

    // UDF is not allowed

    sql = "select change_points(case s1 when 0 then 99 end) from root.sg.d1";
    String msg = "301: CASE expression cannot be used with non-mappable UDF";
    assertTestFail(sql, msg);

    sql = "select change_points(s1) + case when s1=0 then 99 end from root.sg.d1";
    msg = "301: CASE expression cannot be used with non-mappable UDF";
    assertTestFail(sql, msg);
  }

  @Test
  public void testKind1UseOtherOperation() {
    // WHEN-clause use scalar function
    String sql = "select case when sin(s1)>=0 then \">0\" else \"<0\" end from root.sg.d1";
    String[] expectHeader =
        new String[] {
          TIMESTAMP_STR, "CASE WHEN sin(root.sg.d1.s1) >= 0 THEN \">0\" ELSE \"<0\" END",
        };
    String[] retArray =
        new String[] {
          "0,>0,", "1000000,<0,", "20000000,<0,", "210000000,>0,",
        };
    resultSetEqualTest(sql, expectHeader, retArray);

    // THEN-clause and ELSE-clause use scalar function
    sql =
        "select case when s1<=11 then CAST(diff(s1) as TEXT) else CAST(s1-1 as TEXT) end from root.sg.d1 align by device";
    expectHeader =
        new String[] {
          TIMESTAMP_STR,
          DEVICE,
          "CASE WHEN s1 <= 11 THEN CAST(diff(s1) AS TEXT) ELSE CAST(s1 - 1 AS TEXT) END",
        };
    retArray =
        new String[] {
          "0,root.sg.d1,null,",
          "1000000,root.sg.d1,11.0,",
          "20000000,root.sg.d1,21.0,",
          "210000000,root.sg.d1,32.0,",
        };
    resultSetEqualTest(sql, expectHeader, retArray);

    // UDF is not allowed
    sql = "select case when s1=0 then change_points(s1) end from root.sg.d1";
    String msg = "301: CASE expression cannot be used with non-mappable UDF";
    assertTestFail(sql, msg);
  }

  @Test
  public void testKind2UseOtherOperation() {
    // CASE-clause use scalar function
    String sql =
        "select case round(sin(s1)) when 0 then \"=0\" when -1 then \"<0\" else \">0\" end from root.sg.d1";
    String[] expectHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN ROUND(sin(root.sg.d1.s1)) = 0 THEN \"=0\" WHEN ROUND(sin(root.sg.d1.s1)) = -1 THEN \"<0\" ELSE \">0\" END",
        };
    String[] retArray =
        new String[] {
          "0,=0,", "1000000,<0,", "20000000,>0,", "210000000,>0,",
        };
    resultSetEqualTest(sql, expectHeader, retArray);

    // WHEN-clause use scalar function
    sql = "select case 0 when sin(s1) then \"=0\" else \"!=0\" end from root.sg.d1";
    expectHeader =
        new String[] {
          TIMESTAMP_STR, "CASE WHEN 0 = sin(root.sg.d1.s1) THEN \"=0\" ELSE \"!=0\" END",
        };
    retArray =
        new String[] {
          "0,=0,", "1000000,!=0,", "20000000,!=0,", "210000000,!=0,",
        };
    resultSetEqualTest(sql, expectHeader, retArray);

    // THEN-clause and ELSE-clause use scalar function
    sql =
        "select case s1 when 11 then CAST(diff(s1) as TEXT) else CAST(s1-1 as TEXT) end from root.sg.d1 align by device";
    expectHeader =
        new String[] {
          "Time",
          "Device",
          "CASE WHEN s1 = 11 THEN CAST(diff(s1) AS TEXT) ELSE CAST(s1 - 1 AS TEXT) END",
        };
    retArray =
        new String[] {
          "0,root.sg.d1,-1.0,",
          "1000000,root.sg.d1,11.0,",
          "20000000,root.sg.d1,21.0,",
          "210000000,root.sg.d1,32.0,",
        };
    resultSetEqualTest(sql, expectHeader, retArray);

    // UDF is not allowed
    sql = "select case s1 when 0 then change_points(s1) end from root.sg.d1";
    String msg = "301: CASE expression cannot be used with non-mappable UDF";
    assertTestFail(sql, msg);
  }

  @Test
  public void testKind1Wildcard() {
    String sql = "select case when *=* then * else * end from root.sg.d2";
    String[] expectedHeaders =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s4 END",
        };
    String[] retArray = {
      "0,0.0,0.0,44.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,0.0,44.0,44.0,",
      "1000000,11.0,11.0,55.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,11.0,55.0,55.0,",
      "20000000,22.0,22.0,66.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,22.0,66.0,66.0,",
      "210000000,33.0,33.0,77.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,33.0,77.0,77.0,",
    };
    resultSetEqualTest(sql, expectedHeaders, retArray);
  }

  @Test
  public void testKind2Wildcard() {
    String sql = "select case * when * then * else * end from root.sg.d2";
    String[] expectedHeaders =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s3 END",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s4 END",
        };
    String[] retArray = {
      "0,0.0,0.0,44.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,0.0,44.0,44.0,",
      "1000000,11.0,11.0,55.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,11.0,55.0,55.0,",
      "20000000,22.0,22.0,66.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,22.0,66.0,66.0,",
      "210000000,33.0,33.0,77.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,33.0,77.0,77.0,",
    };
    resultSetEqualTest(sql, expectedHeaders, retArray);
  }

  @Test
  public void testKind1AlignedByDevice() {
    // from different devices, result should be empty
    String sql =
        "select case when s1<=11 then s3 else s4 end from root.sg.d1, root.sg.d2 align by device";
    String[] expectedHeader = new String[] {TIMESTAMP_STR};
    String[] retArray = {};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // from same device
    sql = "select case when s3<=11 then s3 else s4 end from root.sg.d1, root.sg.d2 align by device";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, DEVICE, "CASE WHEN s3 <= 11 THEN s3 ELSE s4 END",
        };
    retArray =
        new String[] {
          "0,root.sg.d2,0.0,",
          "1000000,root.sg.d2,11.0,",
          "20000000,root.sg.d2,66.0,",
          "210000000,root.sg.d2,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // from same device, two result column
    sql =
        "select "
            + "case when s1<=11 then s1 else s1*2 end, "
            + "case when s3<=11 then s3 else s4 end "
            + "from root.sg.d1, root.sg.d2 align by device";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          DEVICE,
          "CASE WHEN s1 <= 11 THEN s1 ELSE s1 * 2 END",
          "CASE WHEN s3 <= 11 THEN s3 ELSE s4 END",
        };
    retArray =
        new String[] {
          "0,root.sg.d1,0.0,null,",
          "1000000,root.sg.d1,11.0,null,",
          "20000000,root.sg.d1,44.0,null,",
          "210000000,root.sg.d1,66.0,null,",
          "0,root.sg.d2,null,0.0,",
          "1000000,root.sg.d2,null,11.0,",
          "20000000,root.sg.d2,null,66.0,",
          "210000000,root.sg.d2,null,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testKind2AlignedByDevice() {
    // from different devices, result should be empty
    String sql =
        "select case s1 when 11 then s3 else s4 end from root.sg.d1, root.sg.d2 align by device";
    String[] expectedHeader = new String[] {TIMESTAMP_STR};
    String[] retArray = {};
    resultSetEqualTest(sql, expectedHeader, retArray);

    // from same device
    sql = "select case s3 when 11 then s3 else s4 end from root.sg.d1, root.sg.d2 align by device";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, DEVICE, "CASE WHEN s3 = 11 THEN s3 ELSE s4 END",
        };
    retArray =
        new String[] {
          "0,root.sg.d2,44.0,",
          "1000000,root.sg.d2,11.0,",
          "20000000,root.sg.d2,66.0,",
          "210000000,root.sg.d2,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // from same device, two result column
    sql =
        "select "
            + "case s1 when 11 then s1 else s1*2 end, "
            + "case s3 when 11 then s3 else s4 end "
            + "from root.sg.d1, root.sg.d2 align by device";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          DEVICE,
          "CASE WHEN s1 = 11 THEN s1 ELSE s1 * 2 END",
          "CASE WHEN s3 = 11 THEN s3 ELSE s4 END",
        };
    retArray =
        new String[] {
          "0,root.sg.d1,0.0,null,",
          "1000000,root.sg.d1,11.0,null,",
          "20000000,root.sg.d1,44.0,null,",
          "210000000,root.sg.d1,66.0,null,",
          "0,root.sg.d2,null,44.0,",
          "1000000,root.sg.d2,null,11.0,",
          "20000000,root.sg.d2,null,66.0,",
          "210000000,root.sg.d2,null,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testKind1MultipleTimeseries() {
    // time stamp is aligned
    String sql = "select s1*s1, case when s1<=11 then s3 else s4 end from root.sg.d1, root.sg.d2";
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "root.sg.d1.s1 * root.sg.d1.s1",
          "CASE WHEN root.sg.d1.s1 <= 11 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
        };
    String[] retArray =
        new String[] {
          "0,0.0,0.0,", "1000000,121.0,11.0,", "20000000,484.0,66.0,", "210000000,1089.0,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // time stamp is not aligned
    sql =
        "select "
            + "case when s2%2==1 then s2 else s2/2 end, "
            + "case when s1<=11 then s3 else s4 end "
            + "from root.sg.d1, root.sg.d2 limit 5 offset 98";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s2 % 2 = 1 THEN root.sg.d1.s2 ELSE root.sg.d1.s2 / 2 END",
          "CASE WHEN root.sg.d1.s1 <= 11 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
        };
    retArray =
        new String[] {
          "98,49.0,null,",
          "99,99.0,null,",
          "1000000,null,11.0,",
          "20000000,null,66.0,",
          "210000000,null,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testKind2MultipleTimeseries() {
    // time stamp is aligned
    String sql = "select s1*s1, case s1 when 11 then s3 else s4 end from root.sg.d1, root.sg.d2";
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "root.sg.d1.s1 * root.sg.d1.s1",
          "CASE WHEN root.sg.d1.s1 = 11 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
        };
    String[] retArray =
        new String[] {
          "0,0.0,44.0,", "1000000,121.0,11.0,", "20000000,484.0,66.0,", "210000000,1089.0,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // time stamp is not aligned
    sql =
        "select "
            + "case s2%2 when 1 then s2 else s2/2 end, "
            + "case s3 when 11 then s3 else s4 end "
            + "from root.sg.d1, root.sg.d2 limit 5 offset 98";
    expectedHeader =
        new String[] {
          "Time",
          "CASE WHEN root.sg.d1.s2 % 2 = 1 THEN root.sg.d1.s2 ELSE root.sg.d1.s2 / 2 END",
          "CASE WHEN root.sg.d2.s3 = 11 THEN root.sg.d2.s3 ELSE root.sg.d2.s4 END",
        };
    retArray =
        new String[] {
          "98,49.0,null,",
          "99,99.0,null,",
          "1000000,null,11.0,",
          "20000000,null,66.0,",
          "210000000,null,77.0,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testKind1UseInWhereClause() {
    String sql =
        "select s4 from root.sg.d2 where case when s3=0 then s4>44 when s3=22 then s4>0 when time>200000000 then true end";
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "root.sg.d2.s4",
        };
    String[] retArray = new String[] {"20000000,66.0,", "210000000,77.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    sql =
        "select case when s3=0 then s4>44 when s3=22 then s4>0 when time>200000000 then true end as result from root.sg.d2";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, "result",
        };
    retArray =
        new String[] {
          "0,false,", "1000000,null,", "20000000,true,", "210000000,true,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testKind2UseInWhereClause() {
    String sql = "select s4 from root.sg.d2 where case s3 when 0 then s4>44 when 22 then s4>0 end";
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "root.sg.d2.s4",
        };
    String[] retArray = new String[] {"20000000,66.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);

    sql = "select case s3 when 0 then s4>44 when 22 then s4>0 end as result from root.sg.d2";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, "result",
        };
    retArray =
        new String[] {
          "0,false,", "1000000,null,", "20000000,true,", "210000000,null,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);

    // CASE time
    sql =
        "select s4 from root.sg.d2 where case time when 0 then false when 20000000 then true when 1000000 then true end";
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, "root.sg.d2.s4",
        };
    retArray = new String[] {"1000000,55.0,", "20000000,66.0,"};
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testKind1CaseInCase() {
    String sql =
        "select case when s1=0 || s1=22 then cast(case when s1=0 then 99 when s1>22 then 999 end as TEXT) else \"xxx\" end from root.sg.d1";
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 = 0 | root.sg.d1.s1 = 22 THEN CAST(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 END AS TEXT) ELSE \"xxx\" END"
        };
    String[] retArray =
        new String[] {
          "0,99.0,", "1000000,xxx,", "20000000,null,", "210000000,xxx,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testKind2CaseInCase() {
    String sql =
        "select case s1 when 0 then cast(case when s1=0 then 99 when s1>22 then 999 end as TEXT) when 22 then cast(case when s1=0 then 99 when s1>22 then 999 end as TEXT) else \"xxx\" end from root.sg.d1";
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 = 0 THEN CAST(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 END AS TEXT) WHEN root.sg.d1.s1 = 22 THEN CAST(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 END AS TEXT) ELSE \"xxx\" END"
        };
    String[] retArray =
        new String[] {
          "0,99.0,", "1000000,xxx,", "20000000,null,", "210000000,xxx,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testKind1Logic() {
    String sql =
        "select case when s3 >= 0 and s3 < 20 and s4 >= 50 and s4 < 60 then 'just so so~~~' when s3 >= 20 and s3 < 40 and s4 >= 70 and s4 < 80 then 'very well~~~' end as result from root.sg.d2";
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "result"};
    String[] retArray =
        new String[] {
          "0,null,", "1000000,just so so~~~,", "20000000,null,", "210000000,very well~~~,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }

  @Test
  public void testMultipleSatisfyCase() {
    // Test the result when two when clause are satisfied
    String sql =
        "select case when s3 < 20 or s4 > 60 then \"just so so~~~\" when s3 > 20 or s4 < 60 then \"very well~~~\" end from root.sg.d2";
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d2.s3 < 20 | root.sg.d2.s4 > 60 THEN \"just so so~~~\" WHEN root.sg.d2.s3 > 20 | root.sg.d2.s4 < 60 THEN \"very well~~~\" END"
        };
    String[] retArray =
        new String[] {
          "0,just so so~~~,",
          "1000000,just so so~~~,",
          "20000000,just so so~~~,",
          "210000000,just so so~~~,",
        };
    resultSetEqualTest(sql, expectedHeader, retArray);
  }
}
