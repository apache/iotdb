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

package org.apache.iotdb.relational.it.db.it.udf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBUserDefinedTableFunctionIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE vehicle (device_id string tag, s1 INT32 field, s2 INT64 field)",
        "insert into vehicle(time, device_id, s1, s2) values (1, 'd0', 1, 1)",
        "insert into vehicle(time, device_id, s1, s2) values (2, 'd0', null, 2)",
        "insert into vehicle(time, device_id, s1, s2) values (3, 'd0', 3, 3)",
        "insert into vehicle(time, device_id, s1) values (5, 'd1', 4)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void dropFunction() {
    SQLFunctionUtils.dropAllUDF();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        System.out.println(sql + ";");
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @Test
  public void testMySplit() {
    // split: table function without table parameter
    SQLFunctionUtils.createUDF("split", "org.apache.iotdb.db.query.udf.example.relational.MySplit");
    String[] expectedHeader = new String[] {"output"};
    String[] retArray =
        new String[] {
          "1,", "2,", "3,", "4,", "5,",
        };
    tableResultSetEqualTest(
        "select * from split('1,2,3,4,5')", expectedHeader, retArray, DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from table(split('1,2,3,4,5'))", expectedHeader, retArray, DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from split('1+2+3+4+5','\\+')", expectedHeader, retArray, DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from table(split('1+2+3+4+5','\\+'))", expectedHeader, retArray, DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from split(INPUT=>'1-2-3-4-5',split=>'-')",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from table(split(INPUT=>'1-2-3-4-5',split=>'-'))",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"o1", "o2"};
    retArray =
        new String[] {
          "2,2,", "4,4,",
        };
    tableResultSetEqualTest(
        "select * from TABLE(SPLIT('1,2,4,5')) a(o1) join TABLE(SPLIT('2、3、4', '、')) b(o2) on a.o1=b.o2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from SPLIT('1,2,4,5') a(o1) join TABLE(SPLIT('2、3、4', '、')) b(o2) on a.o1=b.o2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testMyRepeat() {
    // repeat1: row semantic, pass through, with proper column
    // repeat2: row semantic, pass through, without proper column
    SQLFunctionUtils.createUDF(
        "repeat1", "org.apache.iotdb.db.query.udf.example.relational.MyRepeatWithIndex");
    SQLFunctionUtils.createUDF(
        "repeat2", "org.apache.iotdb.db.query.udf.example.relational.MyRepeatWithoutIndex");
    String[] expectedHeader = new String[] {"repeat_index", "time", "device_id", "s1", "s2"};
    String[] retArray =
        new String[] {
          "0,1970-01-01T00:00:00.001Z,d0,1,1,",
          "1,1970-01-01T00:00:00.001Z,d0,1,1,",
          "0,1970-01-01T00:00:00.002Z,d0,null,2,",
          "1,1970-01-01T00:00:00.002Z,d0,null,2,",
          "0,1970-01-01T00:00:00.003Z,d0,3,3,",
          "1,1970-01-01T00:00:00.003Z,d0,3,3,",
          "0,1970-01-01T00:00:00.005Z,d1,4,null,",
          "1,1970-01-01T00:00:00.005Z,d1,4,null,",
        };
    tableResultSetEqualTest(
        "select * from TABLE(repeat1(TABLE(vehicle), 2)) order by time,repeat_index",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from repeat1(vehicle, 2) order by time,repeat_index",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    expectedHeader = new String[] {"time", "device_id", "s1"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,1,",
          "1970-01-01T00:00:00.001Z,d0,1,",
          "1970-01-01T00:00:00.002Z,d0,null,",
          "1970-01-01T00:00:00.002Z,d0,null,",
          "1970-01-01T00:00:00.003Z,d0,3,",
          "1970-01-01T00:00:00.003Z,d0,3,",
          "1970-01-01T00:00:00.005Z,d1,4,",
          "1970-01-01T00:00:00.005Z,d1,4,",
        };
    tableResultSetEqualTest(
        "select * from TABLE(repeat2(TABLE(select time, device_id, s1 from vehicle), 2)) order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from repeat2((select time, device_id, s1 from vehicle), 2) order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testHybrid() {
    SQLFunctionUtils.createUDF(
        "repeat", "org.apache.iotdb.db.query.udf.example.relational.MyRepeatWithoutIndex");
    SQLFunctionUtils.createUDF(
        "exclude", "org.apache.iotdb.db.query.udf.example.relational.MyExcludeColumn");
    SQLFunctionUtils.createUDF("split", "org.apache.iotdb.db.query.udf.example.relational.MySplit");
    String[] expectedHeader = new String[] {"time", "device_id", "s1", "output"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,1,1,",
          "1970-01-01T00:00:00.001Z,d0,1,1,",
          "1970-01-01T00:00:00.001Z,d0,1,1,",
          "1970-01-01T00:00:00.005Z,d1,4,4,",
          "1970-01-01T00:00:00.005Z,d1,4,4,",
          "1970-01-01T00:00:00.005Z,d1,4,4,",
        };
    tableResultSetEqualTest(
        "select * from "
            + "TABLE(REPEAT(TABLE(select * from TABLE(EXCLUDE(TABLE(vehicle), 's2'))), 3)) a "
            + "JOIN "
            + "TABLE(SPLIT('1,4,6,8,10')) b "
            + "ON a.s1=CAST(b.output AS INT32)"
            + "ORDER BY time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select * from "
            + "REPEAT((select * from EXCLUDE(vehicle, 's2')), 3) a "
            + "JOIN "
            + "SPLIT('1,4,6,8,10') b "
            + "ON a.s1=CAST(b.output AS INT32)"
            + "ORDER BY time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    expectedHeader = new String[] {"device_id", "sum"};
    retArray =
        new String[] {
          "d0,9.0,", "d1,12.0,",
        };
    tableResultSetEqualTest(
        "select device_id, sum(s1) as sum from "
            + "TABLE(REPEAT(TABLE(select * from vehicle where time>1), 3)) "
            + "GROUP BY device_id "
            + "ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select device_id, sum(s1) as sum from "
            + "REPEAT((select * from vehicle where time>1), 3) "
            + "GROUP BY device_id "
            + "ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPassThroughPartitionColumn() {
    SQLFunctionUtils.createUDF(
        "MY_SELECT", "org.apache.iotdb.db.query.udf.example.relational.MySelectColumn");
    String[] expectedHeader = new String[] {"s1", "device_id"};
    String[] retArray =
        new String[] {
          "1,d0,", "null,d0,", "3,d0,", "4,d1,",
        };
    tableResultSetEqualTest(
        "select * from MY_SELECT(vehicle PARTITION BY device_id, 's1') ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testIllegalInput() {
    SQLFunctionUtils.createUDF(
        "repeat", "org.apache.iotdb.db.query.udf.example.relational.MyRepeatWithoutIndex");
    SQLFunctionUtils.createUDF(
        "error", "org.apache.iotdb.db.query.udf.example.relational.MyErrorTableFunction");
    tableAssertTestFail(
        "SELECT * FROM repeat(N=>2)", "Missing required argument: DATA", DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM repeat(vehicle, N=>2)",
        "All arguments must be passed by name or all must be passed positionally",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM repeat(2, vehicle)",
        "Invalid argument DATA. Expected table argument, got expression",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM repeat(DATA=>vehicle, N=>vehicle)",
        "Invalid argument N. Expected scalar argument, got table",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM repeat(DATA=>vehicle, N=>1, N=>2)",
        "Too many arguments. Expected at most 2 arguments, got 3 arguments",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM repeat(DATA=>vehicle, DATA=>vehicle)",
        "Duplicate argument name: DATA",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM repeat(DATA=>vehicle, NUM=>3)",
        "Unexpected argument name: NUM",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM repeat(DATA=>vehicle PARTITION BY device_id, NUM=>3)",
        "Partitioning can not be specified for table argument with row semantics",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM repeat(DATA=>vehicle ORDER BY time, NUM=>3)",
        "Ordering can not be specified for table argument with row semantics",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM error(vehicle, 0)",
        "does not specify required input columns from table argument",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM error(vehicle, 1)",
        "specifies empty list of required columns from table argument DATA",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM error(vehicle, 2)",
        "specifies negative index of required column from table argument DATA",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM error(vehicle, 3)", "out of bounds for table with 4 columns", DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM error(vehicle, 4)",
        "specifies required columns from table argument TIMECHO which cannot be found",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM not_register(vehicle, 4)", "Unknown function: not_register", DATABASE_NAME);
  }
}
