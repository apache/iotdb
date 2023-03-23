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
        "INSERT INTO root.sg.d1(timestamp,s1) values(0, 0)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(1, 11)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(2, 22)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(3, 33)",
        "INSERT INTO root.sg.d2(timestamp,s3) values(0, 0)",
        "INSERT INTO root.sg.d2(timestamp,s3) values(1, 11)",
        "INSERT INTO root.sg.d2(timestamp,s3) values(2, 22)",
        "INSERT INTO root.sg.d2(timestamp,s3) values(3, 33)",
        "INSERT INTO root.sg.d2(timestamp,s4) values(0, 44)",
        "INSERT INTO root.sg.d2(timestamp,s4) values(1, 55)",
        "INSERT INTO root.sg.d2(timestamp,s4) values(2, 66)",
        "INSERT INTO root.sg.d2(timestamp,s4) values(3, 77)",
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
  public void testKind1Easy() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 ELSE 9999"
        };
    String[] retArray = new String[] {"0,99.0,", "1,9999.0,", "2,9999.0,", "3,999.0,"};
    resultSetEqualTest(
        "select case when s1=0 then 99 when s1>22 then 999 else 9999 end from root.sg.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testKind2Easy() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22.0 THEN 999 ELSE 9999"
        };
    String[] retArray = new String[] {"0,99.0,", "1,9999.0,", "2,999.0,", "3,9999.0,"};
    resultSetEqualTest(
        "select case s1 when 0 then 99 when 22.0 then 999 else 9999 end from root.sg.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testBoolean() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 <= 0 THEN true WHEN root.sg.d1.s1 = 11 THEN false ELSE true"
        };
    String[] retArray = new String[] {"0,true,", "1,false,", "2,true,", "3,true,"};
    resultSetEqualTest(
        "select case when s1<=0 then true when s1=11 then false else true end from root.sg.d1",
        expectedHeader,
        retArray);
    assertTestFail(
        "select case when s1<=0 then true else 22 end from root.sg.d1",
        "701: CASE expression: BOOLEAN and other types cannot exist at same time");
  }

  @Test
  public void testString() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 <= 0 THEN \"good\" WHEN root.sg.d1.s1 = 11 THEN \"bad\" ELSE \"okok\""
        };
    String[] retArray = new String[] {"0,good,", "1,bad,", "2,okok,", "3,okok,"};
    resultSetEqualTest(
        "select case when s1<=0 then \"good\" when s1=11 then \"bad\" else \"okok\" end from root.sg.d1",
        expectedHeader,
        retArray);

    assertTestFail(
        "select case when s1<=0 then \"good\" else 22 end from root.sg.d1",
        "701: CASE expression: TEXT and other types cannot exist at same time");
  }

  @Test
  public void testDigit() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 11 THEN 99.9 WHEN root.sg.d1.s1 = 22 THEN 8589934588 WHEN root.sg.d1.s1 = 33 THEN 999.9999999999 ELSE 10086"
        };
    String[] retArray =
        new String[] {"0,99.0,", "1,99.9000015258789,", "2,8.589934588E9,", "3,1000.0,"};
    resultSetEqualTest(
        "select case s1 when 0 then 99 when 11 then 99.9 when 22 then 8589934588 when 33 then 999.9999999999 else 10086 end from root.sg.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testWithoutElse() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 "
        };
    String[] retArray = new String[] {"0,99.0,", "1,null,", "2,null,", "3,999.0,"};
    resultSetEqualTest(
        "select case when s1=0 then 99 when s1>22 then 999 end from root.sg.d1",
        expectedHeader,
        retArray);
  }

  /** 100 branches */
  @Test
  public void testLargeNumberBranches() {
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
    String[] expectedHeader = new String[] {TIMESTAMP_STR, expectedHeaderBuilder.toString()};
    resultSetEqualTest(sqlBuilder.toString(), expectedHeader, retList.toArray(new String[] {}));
  }

  @Test
  public void testCalculate() {
    String[] expectedHeader =
        new String[] {
            TIMESTAMP_STR,
            "2 * CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 = 22.0 THEN 999 ELSE 9999"
        };
    String[] retArray = new String[] {"0,198.0,", "1,19998.0,", "2,1998.0,", "3,19998.0,"};
    resultSetEqualTest(
        "select 2 * case s1 when 0 then 99 when 22.0 then 999 else 9999 end from root.sg.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testWildcard() {
    String sql = "select case * when * then * else * end from root.sg.d2";
    String[] expectedHeaders =
        new String[] {
          TIMESTAMP_STR,
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s3",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s4",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s3",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s4",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s3",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s4",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s3",
          "CASE WHEN root.sg.d2.s3 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s4",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s3",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s3 ELSE root.sg.d2.s4",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s3",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s3 THEN root.sg.d2.s4 ELSE root.sg.d2.s4",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s3",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s3 ELSE root.sg.d2.s4",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s3",
          "CASE WHEN root.sg.d2.s4 = root.sg.d2.s4 THEN root.sg.d2.s4 ELSE root.sg.d2.s4",
        };
    String[] retArray = {
      "0,0.0,0.0,44.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,44.0,0.0,0.0,44.0,44.0,",
      "1,11.0,11.0,55.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,55.0,11.0,11.0,55.0,55.0,",
      "2,22.0,22.0,66.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,66.0,22.0,22.0,66.0,66.0,",
      "3,33.0,33.0,77.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,77.0,33.0,33.0,77.0,77.0,",
    };
    resultSetEqualTest(sql, expectedHeaders, retArray);
  }

  @Test
  public void testWithAggregation() {
    String[] expectedHeader =
        new String[] {
            "avg(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 ELSE 9999)"
        };
    String[] retArray = new String[] {"5274.0,"};
    resultSetEqualTest(
        "select avg(case when s1=0 then 99 when s1>22 then 999 else 9999 end) from root.sg.d1",
        expectedHeader,
        retArray);

    resultSetEqualTest(
        "select max_value(case when s1=0 then 99 when s1>22 then 999 else 9999 end) from root.sg.d1",
        new String[] {"max_value(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 ELSE 9999)"},
        new String[] {"9999.0,"}
    );

    resultSetEqualTest(
        "select avg(case when s1=0 then 99 when s1>22 then 999 else 9999 end) * max_value(case when s1=0 then 99 when s1>22 then 999 else 9999 end) from root.sg.d1",
        new String[] {"avg(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 ELSE 9999) * max_value(CASE WHEN root.sg.d1.s1 = 0 THEN 99 WHEN root.sg.d1.s1 > 22 THEN 999 ELSE 9999)"},
        new String[] {"5.2734726E7,"}
    );
  }
}
