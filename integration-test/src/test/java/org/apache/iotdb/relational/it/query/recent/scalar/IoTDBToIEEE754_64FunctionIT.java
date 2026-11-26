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
public class IoTDBToIEEE754_64FunctionIT {

  private static final String DATABASE_NAME = "test_to_ieee754_64_big_endian";

  // Test data: Insert valid DOUBLE values to verify conversion to big-endian IEEE 754 64-bit BLOBs
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_double DOUBLE, c_int64 INT64, c_text TEXT)",

        // Case 1: 1.25d → IEEE 754 64-bit: 0x3FF4000000000000 (big-endian BLOB)
        "INSERT INTO table1(time, c_double) VALUES (1, 1.25)",
        // Case 2: -2.5d → IEEE 754 64-bit: 0xC004000000000000 (big-endian BLOB)
        "INSERT INTO table1(time, c_double) VALUES (2, -2.5)",
        // Case 3: 0.0d → IEEE 754 64-bit: 0x0000000000000000 (big-endian BLOB)
        "INSERT INTO table1(time, c_double) VALUES (3, 0.0)",
        // Case 4: 3.1415926535d → IEEE 754 64-bit: 0x400921FB54442D18 (big-endian BLOB)
        "INSERT INTO table1(time, c_double) VALUES (4, 3.1415926535)",
        // Case 5: Null input → null output
        "INSERT INTO table1(time, c_double) VALUES (5, null)",
        // Invalid type test data
        "INSERT INTO table1(time, c_int64, c_text) VALUES (6, 1000, 'invalid_type')",
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

  /** Verify valid conversions: big-endian BLOB matches IEEE 754 64-bit standard encoding */
  @Test
  public void testToIEEE754_64OnValidInputs() {
    String[] expectedHeader = {"time", "to_ieee754_64(c_double)"};
    String[] retArray = {
      "1970-01-01T00:00:00.001Z,0x3ff4000000000000,", // 1.25d → 0x3ff4000000000000
      "1970-01-01T00:00:00.002Z,0xc004000000000000,", // -2.5d → 0xc004000000000000
      "1970-01-01T00:00:00.003Z,0x0000000000000000,", // 0.0d → 0x0000000000000000
      "1970-01-01T00:00:00.004Z,0x400921fb54411744,", // 3.1415926535d → 0x400921fb54411744
      "1970-01-01T00:00:00.005Z,null," // Null input → null
    };

    tableResultSetEqualTest(
        "SELECT time, to_ieee754_64(c_double) AS \"to_ieee754_64(c_double)\" FROM table1 WHERE time <= 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Verify error handling for invalid inputs (wrong type/argument count) */
  @Test
  public void testToIEEE754_64OnInvalidInputs() {
    String errorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_ieee754_64 only accepts one argument and it must be Double data type.";

    // Test non-DOUBLE input (INT64)
    tableAssertTestFail(
        "SELECT to_ieee754_64(c_int64) FROM table1 WHERE time = 6", errorMsg, DATABASE_NAME);

    // Test non-DOUBLE input (TEXT)
    tableAssertTestFail(
        "SELECT to_ieee754_64(c_text) FROM table1 WHERE time = 6", errorMsg, DATABASE_NAME);

    // Test no arguments
    tableAssertTestFail("SELECT to_ieee754_64() FROM table1", errorMsg, DATABASE_NAME);

    // Test multiple arguments
    tableAssertTestFail(
        "SELECT to_ieee754_64(c_double, c_double) FROM table1 WHERE time = 1",
        errorMsg,
        DATABASE_NAME);
  }
}
