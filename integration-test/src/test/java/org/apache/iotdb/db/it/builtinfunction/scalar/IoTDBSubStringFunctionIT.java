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

package org.apache.iotdb.db.it.builtinfunction.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSubStringFunctionIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s3 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s5 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s6 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s7 WITH DATATYPE=DATE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s8 WITH DATATYPE=TIMESTAMP, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s9 WITH DATATYPE=STRING, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s10 WITH DATATYPE=BLOB, ENCODING=PLAIN",
        "INSERT INTO root.sg(timestamp,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', 'abcd')",
        "INSERT INTO root.sg(timestamp,s1) values(2, 'test')",
        "INSERT INTO root.sg(timestamp,s1) values(3, 'abcdefg')",
        "INSERT INTO root.sg(timestamp,s9) values(2, 'test')",
        "INSERT INTO root.sg(timestamp,s9) values(3, 'abcdefg')",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
    registerUDF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void registerUDF() {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create function constvalue as 'org.apache.iotdb.db.query.udf.example.ConstValue'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testNewTransformer() {
    // Normal
    String[] expectedHeader =
        new String[] {
          "Time,root.sg.s1,SUBSTRING(root.sg.s1,1),SUBSTRING(root.sg.s1,1,3),SUBSTRING(root.sg.s1 FROM 1),SUBSTRING(root.sg.s1 FROM 1 FOR 3)"
        };
    String[] retArray =
        new String[] {
          "1,abcd,abcd,abc,abcd,abc,",
          "2,test,test,tes,test,tes,",
          "3,abcdefg,abcdefg,abc,abcdefg,abc,",
        };
    resultSetEqualTest(
        "select s1,SUBSTRING(s1,1),SUBSTRING(s1,1,3),SUBSTRING(s1 from 1),SUBSTRING(s1 from 1 for 3) from root.sg",
        expectedHeader,
        retArray);

    // Param 1 greater than input series length
    expectedHeader =
        new String[] {
          "Time,root.sg.s1,SUBSTRING(root.sg.s1,11),SUBSTRING(root.sg.s1,11,13),SUBSTRING(root.sg.s1 FROM 11),SUBSTRING(root.sg.s1 FROM 11 FOR 13)"
        };
    retArray =
        new String[] {
          "1,abcd,,,,,", "2,test,,,,,", "3,abcdefg,,,,,",
        };
    resultSetEqualTest(
        "select s1,SUBSTRING(s1,11),SUBSTRING(s1,11,13),SUBSTRING(s1 from 11),SUBSTRING(s1 from 11 for 13) from root.sg",
        expectedHeader,
        retArray);

    // Normal
    String[] expectedHeader2 =
        new String[] {
          "Time,root.sg.s9,SUBSTRING(root.sg.s9,1),SUBSTRING(root.sg.s9,1,3),SUBSTRING(root.sg.s9 FROM 1),SUBSTRING(root.sg.s9 FROM 1 FOR 3)"
        };
    String[] retArray2 =
        new String[] {
          "1,abcd,abcd,abc,abcd,abc,",
          "2,test,test,tes,test,tes,",
          "3,abcdefg,abcdefg,abc,abcdefg,abc,",
        };
    resultSetEqualTest(
        "select s9,SUBSTRING(s9,1),SUBSTRING(s9,1,3),SUBSTRING(s9 from 1),SUBSTRING(s9 from 1 for 3) from root.sg",
        expectedHeader2,
        retArray2);
  }

  @Test
  public void testOldTransformer() {
    // Normal
    String[] expectedHeader =
        new String[] {
          "Time,root.sg.s1,change_points(root.sg.s1),SUBSTRING(root.sg.s1,1),SUBSTRING(root.sg.s1,1,3),SUBSTRING(root.sg.s1 FROM 1),SUBSTRING(root.sg.s1 FROM 1 FOR 3)"
        };
    String[] retArray =
        new String[] {
          "1,abcd,abcd,abcd,abc,abcd,abc,",
          "2,test,test,test,tes,test,tes,",
          "3,abcdefg,abcdefg,abcdefg,abc,abcdefg,abc,",
        };
    resultSetEqualTest(
        "select s1,change_points(s1),SUBSTRING(s1,1),SUBSTRING(s1,1,3),SUBSTRING(s1 from 1),SUBSTRING(s1 from 1 for 3) from root.sg",
        expectedHeader,
        retArray);

    // Param 1 greater than input series length
    expectedHeader =
        new String[] {
          "Time,root.sg.s1,change_points(root.sg.s1),SUBSTRING(root.sg.s1,11),SUBSTRING(root.sg.s1,11,13),SUBSTRING(root.sg.s1 FROM 11),SUBSTRING(root.sg.s1 FROM 11 FOR 13)"
        };
    retArray =
        new String[] {
          "1,abcd,abcd,,,,,", "2,test,test,,,,,", "3,abcdefg,abcdefg,,,,,",
        };
    resultSetEqualTest(
        "select s1,change_points(s1),SUBSTRING(s1,11),SUBSTRING(s1,11,13),SUBSTRING(s1 from 11),SUBSTRING(s1 from 11 for 13) from root.sg",
        expectedHeader,
        retArray);

    // Normal
    String[] expectedHeader2 =
        new String[] {
          "Time,root.sg.s9,change_points(root.sg.s1),SUBSTRING(root.sg.s9,1),SUBSTRING(root.sg.s9,1,3),SUBSTRING(root.sg.s9 FROM 1),SUBSTRING(root.sg.s9 FROM 1 FOR 3)"
        };
    String[] retArray2 =
        new String[] {
          "1,abcd,abcd,abcd,abc,abcd,abc,",
          "2,test,test,test,tes,test,tes,",
          "3,abcdefg,abcdefg,abcdefg,abc,abcdefg,abc,",
        };
    resultSetEqualTest(
        "select s9,change_points(s1),SUBSTRING(s9,1),SUBSTRING(s9,1,3),SUBSTRING(s9 from 1),SUBSTRING(s9 from 1 for 3) from root.sg",
        expectedHeader2,
        retArray2);
  }

  @Test
  public void testRoundBooleanAndText() {
    // Using substring without start and end position.
    assertTestFail(
        "select s1,SUBSTRING(s1) from root.sg",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Argument exception,the scalar function [SUBSTRING] needs at least one argument,it must be a signed integer");

    // Wrong input type
    assertTestFail(
        "select SUBSTRING(s2,1,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Unsupported data type INT32 for function SUBSTRING.");

    // Wrong input type
    assertTestFail(
        "select SUBSTRING(s3,1,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Unsupported data type INT64 for function SUBSTRING.");

    // Wrong input type
    assertTestFail(
        "select SUBSTRING(s4,1,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Unsupported data type FLOAT for function SUBSTRING.");

    // Wrong input type
    assertTestFail(
        "select SUBSTRING(s5,1,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Unsupported data type DOUBLE for function SUBSTRING.");

    // Wrong input type
    assertTestFail(
        "select SUBSTRING(s6,1,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Unsupported data type BOOLEAN for function SUBSTRING.");

    // Wrong input type
    assertTestFail(
        "select SUBSTRING(s7,1,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Unsupported data type DATE for function SUBSTRING.");
    // Wrong input type
    assertTestFail(
        "select SUBSTRING(s8,1,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Unsupported data type TIMESTAMP for function SUBSTRING.");

    // Wrong input type
    assertTestFail(
        "select SUBSTRING(s10,1,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Unsupported data type BLOB for function SUBSTRING.");

    // Using substring with float start position
    assertTestFail(
        "select SUBSTRING(s1,1.0,1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Argument exception,the scalar function [SUBSTRING] needs at least one argument,it must be a signed integer");

    // Using substring with float start and length
    assertTestFail(
        "select SUBSTRING(s1,1.0,1.1) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Argument exception,the scalar function [SUBSTRING] needs at least one argument,it must be a signed integer");

    // Negative characters length
    assertTestFail(
        "select SUBSTRING(s1,1,-10) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0");

    // Negative characters begin
    assertTestFail(
        "select SUBSTRING(s1,-1,10) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0");

    // Negative characters begin
    assertTestFail(
        "select SUBSTRING(s1 from -1 for 10) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0");

    // Negative characters begin
    assertTestFail(
        "select SUBSTRING(s1,'start'='1','to'='2') from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Syntax error,please check that the parameters of the function are correct");
  }
}
