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

package org.apache.iotdb.relational.it.query.old.builtinfunction.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

public abstract class IoTDBReplaceFunctionTableBase extends IoTDBScalarFunctionTableBase {

  private static final String DATABASE_NAME = "db";
  private static final String[] SQLs =
      new String[] {
        "use " + DATABASE_NAME,
        "create table table1(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD, s5 DOUBLE FIELD, s6 BOOLEAN FIELD, s7 DATE FIELD, s8 TIMESTAMP FIELD, s9 STRING FIELD)",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd')",
        "INSERT INTO table1(time,device_id,s1) values(2, 'd1', 'test\\\\')",
        "INSERT INTO table1(time,device_id,s1) values(3, 'd1', 'abcd\\\\')",
        "INSERT INTO table1(time,device_id,s9) values(2, 'd1', 'test\\\\')",
        "INSERT INTO table1(time,device_id,s9) values(3, 'd1', 'abcd\\\\')",
        "create table substring_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD, s5 DOUBLE FIELD, s6 BOOLEAN FIELD, s7 DATE FIELD, s8 TIMESTAMP FIELD, s9 STRING FIELD, s10 BLOB FIELD)",
        "INSERT INTO substring_table(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', -1, 3, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO substring_table(Time,device_id,s1) values(2, 'd1', 'test')",
        "INSERT INTO substring_table(Time,device_id,s1) values(3, 'd1', 'abcdefg')",
        "INSERT INTO substring_table(Time,device_id,s9) values(2, 'd1', 'test')",
        "INSERT INTO substring_table(Time,device_id,s9) values(3, 'd1', 'abcdefg')",
        "INSERT INTO substring_table(Time,device_id,s2,s3) values(2, 'd1', -1, 10)",
        "INSERT INTO substring_table(Time,device_id,s2,s3) values(3, 'd1', 2, 3)",
        "flush"
      };

  protected static void insertReplaceData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testNewTransformer() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "_col1", "_col2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,ABcd,abcd,",
          "1970-01-01T00:00:00.002Z,test\\\\,testaa,",
          "1970-01-01T00:00:00.003Z,ABcd\\\\,abcdaa,"
        };
    tableResultSetEqualTest(
        "select Time, REPLACE(s1, 'ab', 'AB'), REPLACE(s1, '\\', 'a') from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    String[] retArray2 =
        new String[] {
          "1970-01-01T00:00:00.001Z,ABcd,abcd,",
          "1970-01-01T00:00:00.002Z,test\\\\,testaa,",
          "1970-01-01T00:00:00.003Z,ABcd\\\\,abcdaa,"
        };
    tableResultSetEqualTest(
        "select Time, REPLACE(s9, 'ab', 'AB'), REPLACE(s9, '\\', 'a') from table1",
        expectedHeader,
        retArray2,
        DATABASE_NAME);
  }

  @Test
  public void testWithoutTo() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "_col1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,acd,",
          "1970-01-01T00:00:00.002Z,test\\\\,",
          "1970-01-01T00:00:00.003Z,acd\\\\,"
        };
    tableResultSetEqualTest(
        "select Time, REPLACE(s1, 'b') from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testWrongInputType() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try {
        statement.execute("select REPLACE(s2, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s3, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s4, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s5, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s6, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s7, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s8, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      //      try {
      //        statement.execute("select REPLACE(s10, 'a', 'b') from table1");
      //        fail();
      //      } catch (Exception ignored) {
      //      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  // Verifies SUBSTRING returns the expected slices with literal and column bounds across rows.
  @Test
  public void testSubstringTransformer() {
    String[] expectedHeader = {"time", "s1", "_col2", "_col3", "s9", "_col5", "_col6"};
    String[] expectedRows = {
      "1970-01-01T00:00:00.001Z,abcd,abcd,abc,abcd,abcd,abc,",
      "1970-01-01T00:00:00.002Z,test,test,tes,test,test,tes,",
      "1970-01-01T00:00:00.003Z,abcdefg,abcdefg,abc,abcdefg,abcdefg,abc,",
    };
    tableResultSetEqualTest(
        "select time,s1,SUBSTRING(s1 FROM 1),SUBSTRING(s1 FROM 1 FOR 3),s9,SUBSTRING(s9 from 1),SUBSTRING(s9 from 1 for 3) from substring_table",
        expectedHeader,
        expectedRows,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "s1", "s9", "s2", "_col4", "_col5"};
    expectedRows =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,abcd,-1,abcd,abcd,",
          "1970-01-01T00:00:00.002Z,test,test,-1,test,test,",
          "1970-01-01T00:00:00.003Z,abcdefg,abcdefg,2,bcdefg,bcdefg,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,s2,SUBSTRING(s1 from s2),SUBSTRING(s1 from s2) from substring_table",
        expectedHeader,
        expectedRows,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "s1", "s9", "s2", "s3", "_col5", "_col6"};
    expectedRows =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,abcd,-1,3,a,a,",
          "1970-01-01T00:00:00.002Z,test,test,-1,10,test,test,",
          "1970-01-01T00:00:00.003Z,abcdefg,abcdefg,2,3,bcd,bcd,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,s2,s3,SUBSTRING(s1 from s2 for s3),SUBSTRING(s1 from s2 for s3) from substring_table",
        expectedHeader,
        expectedRows,
        DATABASE_NAME);
  }

  // Verifies SUBSTRING rejects non-text inputs, non-integral bounds, negative lengths, and
  // out-of-range starts.
  @Test
  public void testSubstringInvalidArguments() {
    String semanticError =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]";
    for (String column : new String[] {"s2", "s3", "s4", "s5", "s6", "s7", "s8", "s10"}) {
      tableAssertTestFail(
          "select SUBSTRING(" + column + " FROM 1 FOR 1) from substring_table",
          semanticError,
          DATABASE_NAME);
    }
    tableAssertTestFail(
        "select s1,SUBSTRING(s1) from substring_table", semanticError, DATABASE_NAME);
    tableAssertTestFail(
        "select SUBSTRING(s1 FROM 1.0 FOR 1) from substring_table", semanticError, DATABASE_NAME);
    tableAssertTestFail(
        "select SUBSTRING(s1 FROM 1.0 FOR 1.1) from substring_table", semanticError, DATABASE_NAME);
    tableAssertTestFail(
        "select SUBSTRING(s1 FROM 1 FOR -10) from substring_table",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Argument exception,the scalar function substring length must not be less than 0",
        DATABASE_NAME);
    tableAssertTestFail(
        "select SUBSTRING(s1 FROM 100 FOR 1) from substring_table",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Argument exception,the scalar function substring beginPosition must not be greater than the string length",
        DATABASE_NAME);
  }
}
