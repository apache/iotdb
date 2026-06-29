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
public class IoTDBFromIEEE754_64FunctionIT {

  private static final String DATABASE_NAME = "test_from_ieee754_64_big_endian";

  // Test data: Insert big-endian IEEE 754 64-bit BLOBs to verify DOUBLE parsing
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int64 INT64, c_text TEXT)",

        // Case 1: Big-endian BLOB 0x3FF4000000000000 → 1.25d
        "INSERT INTO table1(time, c_blob) VALUES (1, X'3FF4000000000000')",
        // Case 2: Big-endian BLOB 0xC004000000000000 → -2.5d
        "INSERT INTO table1(time, c_blob) VALUES (2, X'C004000000000000')",
        // Case 3: Big-endian BLOB 0x0000000000000000 → 0.0d
        "INSERT INTO table1(time, c_blob) VALUES (3, X'0000000000000000')",
        // Case 4: Big-endian BLOB 0x400921FB54442D18 → ~3.1415926535d
        "INSERT INTO table1(time, c_blob) VALUES (4, X'400921FB54442D18')",
        // Case 5: Big-endian BLOB 0x7FF8000000000000 → NaN (special value)
        "INSERT INTO table1(time, c_blob) VALUES (5, X'7FF8000000000000')",
        // Case 6: Big-endian BLOB 0x7FF0000000000000 → INF (positive infinity)
        "INSERT INTO table1(time, c_blob) VALUES (6, X'7FF0000000000000')",
        // Case 7: Null BLOB → null output
        "INSERT INTO table1(time, c_blob) VALUES (7, null)",
        // Invalid BLOB length (<8 bytes)
        "INSERT INTO table1(time, c_blob) VALUES (8, X'3FF40000')",
        // Invalid BLOB length (>8 bytes)
        "INSERT INTO table1(time, c_blob) VALUES (9, X'3FF400000000000000')",
        // Invalid type test data
        "INSERT INTO table1(time, c_int64, c_text) VALUES (10, 1000, 'invalid_type')",
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

  /**
   * Verify valid big-endian BLOB parsing: correct conversion to DOUBLE (including special values)
   */
  @Test
  public void testFromIEEE754_64OnValidInputs() {
    String[] expectedHeader = {"time", "from_ieee754_64(c_blob)"};
    String[] retArray = {
      "1970-01-01T00:00:00.001Z,1.25,", // X'3FF4000000000000' → 1.25d
      "1970-01-01T00:00:00.002Z,-2.5,", // X'C004000000000000' → -2.5d
      "1970-01-01T00:00:00.003Z,0.0,", // X'0000000000000000' → 0.0d
      "1970-01-01T00:00:00.004Z,3.141592653589793,", // X'400921FB54442D18' → 3.141592653589793
      "1970-01-01T00:00:00.005Z,NaN,", // X'7FF8000000000000' → NaN
      "1970-01-01T00:00:00.006Z,Infinity,", // X'7FF0000000000000' → Infinity
      "1970-01-01T00:00:00.007Z,null," // Null BLOB → null
    };

    tableResultSetEqualTest(
        "SELECT time, from_ieee754_64(c_blob) AS \"from_ieee754_64(c_blob)\" FROM table1 WHERE time <= 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Verify error handling for invalid inputs (wrong type/count, invalid BLOB length) */
  @Test
  public void testFromIEEE754_64OnInvalidInputs() {
    // Error msg for wrong type/argument count
    String typeCountErrorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_ieee754_64 only accepts one argument and it must be BLOB data type.";

    // Test non-BLOB input (INT64)
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_int64) FROM table1 WHERE time = 10",
        typeCountErrorMsg,
        DATABASE_NAME);

    // Test non-BLOB input (TEXT)
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_text) FROM table1 WHERE time = 10",
        typeCountErrorMsg,
        DATABASE_NAME);

    // Test no arguments
    tableAssertTestFail("SELECT from_ieee754_64() FROM table1", typeCountErrorMsg, DATABASE_NAME);

    // Test multiple arguments
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_blob, c_blob) FROM table1 WHERE time = 1",
        typeCountErrorMsg,
        DATABASE_NAME);

    // Error msg for invalid BLOB length
    String lengthErrorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_ieee754_64' due to an invalid input format. "
            + "Problematic value: ";

    // Test BLOB length <8 bytes
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMsg + "0x3ff40000",
        DATABASE_NAME);

    // Test BLOB length >8 bytes
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_blob) FROM table1 WHERE time = 9",
        lengthErrorMsg + "0x3ff400000000000000",
        DATABASE_NAME);
  }
}
