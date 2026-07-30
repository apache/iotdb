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
public class IoTDBFromIEEE754_32FunctionIT {

  private static final String DATABASE_NAME = "test_from_ieee754_32_big_endian";

  // Test data: Insert big-endian IEEE 754 BLOBs to verify FLOAT parsing
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int32 INT32, c_text TEXT)",

        // Case 1: Big-endian BLOB 0x3F400000 → 1.25f
        "INSERT INTO table1(time, c_blob) VALUES (1, X'3FA00000')",
        // Case 2: Big-endian BLOB 0xC0200000 → -2.5f
        "INSERT INTO table1(time, c_blob) VALUES (2, X'C0200000')",
        // Case 3: Big-endian BLOB 0x00000000 → 0.0f
        "INSERT INTO table1(time, c_blob) VALUES (3, X'00000000')",
        // Case 4: Big-endian BLOB 0x40490FDB → ~3.1415f
        "INSERT INTO table1(time, c_blob) VALUES (4, X'40490E56')",
        // Case 5: Big-endian BLOB 0x7FC00000 → NaN (special value)
        "INSERT INTO table1(time, c_blob) VALUES (5, X'7FC00000')",
        // Case 6: Big-endian BLOB 0x7F800000 → INF (positive infinity)
        "INSERT INTO table1(time, c_blob) VALUES (6, X'7F800000')",
        // Case 7: Null BLOB → null output
        "INSERT INTO table1(time, c_blob) VALUES (7, null)",
        // Invalid BLOB length (<4 bytes)
        "INSERT INTO table1(time, c_blob) VALUES (8, X'3F40')",
        // Invalid BLOB length (>4 bytes)
        "INSERT INTO table1(time, c_blob) VALUES (9, X'3F40000000')",
        // Invalid type test data
        "INSERT INTO table1(time, c_int32, c_text) VALUES (10, 100, 'invalid_type')",
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
   * Verify valid big-endian BLOB parsing: correct conversion to FLOAT (including special values)
   */
  @Test
  public void testFromIEEE754_32OnValidInputs() {
    String[] expectedHeader = {"time", "from_ieee754_32(c_blob)"};
    String[] retArray = {
      "1970-01-01T00:00:00.001Z,1.25,", // X'3FA00000' → 1.25f
      "1970-01-01T00:00:00.002Z,-2.5,", // X'C0200000' → -2.5f
      "1970-01-01T00:00:00.003Z,0.0,", // X'00000000' → 0.0f
      "1970-01-01T00:00:00.004Z,3.1415,", // X'40490FDB' → ~3.1415f
      "1970-01-01T00:00:00.005Z,NaN,", // X'7FC00000' → NaN
      "1970-01-01T00:00:00.006Z,Infinity,", // X'7F800000' → Infinity
      "1970-01-01T00:00:00.007Z,null," // Null BLOB → null
    };

    tableResultSetEqualTest(
        "SELECT time, from_ieee754_32(c_blob) AS \"from_ieee754_32(c_blob)\" FROM table1 WHERE time <= 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Verify error handling for invalid inputs (wrong type/count, invalid BLOB length) */
  @Test
  public void testFromIEEE754_32OnInvalidInputs() {
    // Error msg for wrong type/argument count
    String typeCountErrorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_ieee754_32 only accepts one argument and it must be BLOB data type.";

    // Test non-BLOB input (INT32)
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_int32) FROM table1 WHERE time = 10",
        typeCountErrorMsg,
        DATABASE_NAME);

    // Test non-BLOB input (TEXT)
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_text) FROM table1 WHERE time = 10",
        typeCountErrorMsg,
        DATABASE_NAME);

    // Test no arguments
    tableAssertTestFail("SELECT from_ieee754_32() FROM table1", typeCountErrorMsg, DATABASE_NAME);

    // Test multiple arguments
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_blob, c_blob) FROM table1 WHERE time = 1",
        typeCountErrorMsg,
        DATABASE_NAME);

    // Error msg for invalid BLOB length
    String lengthErrorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_ieee754_32' due to an invalid input format. "
            + "Problematic value: ";

    // Test BLOB length <4 bytes
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMsg + "0x3f40",
        DATABASE_NAME);

    // Test BLOB length >4 bytes
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_blob) FROM table1 WHERE time = 9",
        lengthErrorMsg + "0x3f40000000",
        DATABASE_NAME);
  }
}
