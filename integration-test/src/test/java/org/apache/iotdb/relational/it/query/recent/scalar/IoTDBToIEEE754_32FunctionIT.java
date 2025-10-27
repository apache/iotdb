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

package org.apache.iotdb.relational.it.query.recent.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

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
public class IoTDBToIEEE754_32FunctionIT {

  private static final String DATABASE_NAME = "test_to_ieee754_32_big_endian";

  // test data: valid and invalid inputs for to_ieee754_32 function
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_float FLOAT, c_int32 INT32, c_text TEXT)",

        // Case 1: 1.25f
        "INSERT INTO table1(time, c_float) VALUES (1, 1.25)",
        // Case 2: -2.5f
        "INSERT INTO table1(time, c_float) VALUES (2, -2.5)",
        // Case 3: 0.0f
        "INSERT INTO table1(time, c_float) VALUES (3, 0.0)",
        // Case 4: 3.1415f
        "INSERT INTO table1(time, c_float) VALUES (4, 3.1415)",
        // Case 5: null
        "INSERT INTO table1(time, c_float) VALUES (5, null)",
        // invalid type inputs for error handling tests
        "INSERT INTO table1(time, c_int32, c_text) VALUES (6, 100, 'invalid_type')",
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
  public void testToIEEE754_32OnValidInputs() {
    String[] expectedHeader = {"time", "to_ieee754_32(c_float)"};
    String[] retArray = {
      "1970-01-01T00:00:00.001Z,0x3fa00000,",
      "1970-01-01T00:00:00.002Z,0xc0200000,",
      "1970-01-01T00:00:00.003Z,0x00000000,",
      "1970-01-01T00:00:00.004Z,0x40490e56,",
      "1970-01-01T00:00:00.005Z,null,"
    };

    tableResultSetEqualTest(
        "SELECT time, to_ieee754_32(c_float) AS \"to_ieee754_32(c_float)\" FROM table1 WHERE time <= 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** test the invalid data input */
  @Test
  public void testToIEEE754_32OnInvalidInputs() {
    String errorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_ieee754_32 only accepts one argument and it must be Float data type.";

    // test with invalid parameter type (INT32)
    tableAssertTestFail(
        "SELECT to_ieee754_32(c_int32) FROM table1 WHERE time = 6", errorMsg, DATABASE_NAME);

    // test with invalid parameter type (TEXT)
    tableAssertTestFail(
        "SELECT to_ieee754_32(c_text) FROM table1 WHERE time = 6", errorMsg, DATABASE_NAME);

    // test with no parameter
    tableAssertTestFail("SELECT to_ieee754_32() FROM table1", errorMsg, DATABASE_NAME);

    // test with two parameters
    tableAssertTestFail(
        "SELECT to_ieee754_32(c_float, c_float) FROM table1 WHERE time = 1",
        errorMsg,
        DATABASE_NAME);
  }
}
