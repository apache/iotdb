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
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSubStringFunctionTableIT {

  private static final String DATABASE_NAME = "db";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        "create table table1(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO table1(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO table1(Time,device_id,s1) values(2, 'd1', 'test')",
        "INSERT INTO table1(Time,device_id,s1) values(3, 'd1', 'abcdefg')",
        "INSERT INTO table1(Time,device_id,s9) values(2, 'd1', 'test')",
        "INSERT INTO table1(Time,device_id,s9) values(3, 'd1', 'abcdefg')",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNewTransformer() {
    // Normal
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "_col3"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,abcd,abc,",
          "1970-01-01T00:00:00.002Z,test,test,tes,",
          "1970-01-01T00:00:00.003Z,abcdefg,abcdefg,abc,",
        };
    tableResultSetEqualTest(
        "select time,s1,SUBSTRING(s1 FROM 1),SUBSTRING(s1 FROM 1 FOR 3) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // Param 1 greater than input series length
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,,,",
          "1970-01-01T00:00:00.002Z,test,,,",
          "1970-01-01T00:00:00.003Z,abcdefg,,,",
        };
    tableResultSetEqualTest(
        "select time,s1,SUBSTRING(s1 from 11),SUBSTRING(s1 from 11 for 13) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // Normal
    expectedHeader = new String[] {"time", "s9", "_col2", "_col3"};
    String[] retArray2 =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,abcd,abc,",
          "1970-01-01T00:00:00.002Z,test,test,tes,",
          "1970-01-01T00:00:00.003Z,abcdefg,abcdefg,abc,",
        };
    tableResultSetEqualTest(
        "select time,s9,SUBSTRING(s9 from 1),SUBSTRING(s9 from 1 for 3) from table1",
        expectedHeader,
        retArray2,
        DATABASE_NAME);
  }

  @Test
  public void testRoundBooleanAndText() {
    // Using substring without start and end position.
    tableAssertTestFail(
        "select s1,SUBSTRING(s1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Wrong input type
    tableAssertTestFail(
        "select SUBSTRING(s2 FROM 1 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Wrong input type
    tableAssertTestFail(
        "select SUBSTRING(s3 FROM 1 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Wrong input type
    tableAssertTestFail(
        "select SUBSTRING(s4 FROM 1 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Wrong input type
    tableAssertTestFail(
        "select SUBSTRING(s5 FROM 1 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Wrong input type
    tableAssertTestFail(
        "select SUBSTRING(s6 FROM 1 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Wrong input type
    tableAssertTestFail(
        "select SUBSTRING(s7 FROM 1 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);
    // Wrong input type
    tableAssertTestFail(
        "select SUBSTRING(s8 FROM 1 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Wrong input type
    tableAssertTestFail(
        "select SUBSTRING(s10 FROM 1 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Using substring with float start position
    tableAssertTestFail(
        "select SUBSTRING(s1 FROM 1.0 FOR 1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Using substring with float start and length
    tableAssertTestFail(
        "select SUBSTRING(s1 FROM 1.0 FOR 1.1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function substring only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]",
        DATABASE_NAME);

    // Negative characters length
    tableAssertTestFail(
        "select SUBSTRING(s1 FROM 1 FOR -10) from table1",
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()
            + ": Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0",
        DATABASE_NAME);

    // Negative characters begin
    tableAssertTestFail(
        "select SUBSTRING(s1 FROM -1 FOR 10) from table1",
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()
            + ": Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0",
        DATABASE_NAME);
  }
}
