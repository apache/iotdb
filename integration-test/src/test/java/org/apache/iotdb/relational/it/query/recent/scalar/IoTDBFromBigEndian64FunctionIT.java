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
public class IoTDBFromBigEndian64FunctionIT {

  private static final String DATABASE_NAME = "test_from_big_endian_64";

  // SQL statements to set up the database and table for testing
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int64 INT64, c_text TEXT)",

        // Case 1: BLOB for a common positive integer (72623859790382856)
        "INSERT INTO table1(time, c_blob) VALUES (1, X'0102030405060708')",

        // Case 2: BLOB for a negative integer (-1)
        "INSERT INTO table1(time, c_blob) VALUES (2, X'FFFFFFFFFFFFFFFF')",

        // Case 3: BLOB for zero
        "INSERT INTO table1(time, c_blob) VALUES (3, X'0000000000000000')",

        // Case 4: BLOB for the maximum INT64 value
        "INSERT INTO table1(time, c_blob) VALUES (4, X'7FFFFFFFFFFFFFFF')",

        // Case 5: BLOB for the minimum INT64 value
        "INSERT INTO table1(time, c_blob) VALUES (5, X'8000000000000000')",

        // Case 6: Null BLOB input
        "INSERT INTO table1(time, c_blob) VALUES (6, null)",

        // Case 7: BLOB with invalid length (< 8 bytes) for error testing
        "INSERT INTO table1(time, c_blob) VALUES (7, X'01020304050607')",

        // Case 8: BLOB with invalid length (> 8 bytes) for error testing
        "INSERT INTO table1(time, c_blob) VALUES (8, X'010203040506070809')",

        // Case 9: Data for invalid type testing
        "INSERT INTO table1(time, c_int64, c_text) VALUES (9, 100, 'some_text')",
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
   * Validates the from_big_endian_64() function on various valid 8-byte BLOB inputs. This test
   * covers positive, negative, zero, min/max, and null values.
   */
  @Test
  public void testFromBigEndian64OnValidInputs() {
    String[] expectedHeader =
        new String[] {
          "time", "from_big_endian_64(c_blob)",
        };
    String[] retArray =
        new String[] {
          // 1. X'0102030405060708' -> 72623859790382856
          "1970-01-01T00:00:00.001Z,72623859790382856,",
          // 2. X'FFFFFFFFFFFFFFFF' -> -1
          "1970-01-01T00:00:00.002Z,-1,",
          // 3. X'0000000000000000' -> 0
          "1970-01-01T00:00:00.003Z,0,",
          // 4. X'7FFFFFFFFFFFFFFF' -> 9223372036854775807
          "1970-01-01T00:00:00.004Z,9223372036854775807,",
          // 5. X'8000000000000000' -> -9223372036854775808
          "1970-01-01T00:00:00.005Z,-9223372036854775808,",
          // 6. Null input -> null output
          "1970-01-01T00:00:00.006Z,null,",
        };

    tableResultSetEqualTest(
        "SELECT time, from_big_endian_64(c_blob) as \"from_big_endian_64(c_blob)\" FROM table1 WHERE time <= 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /**
   * Tests for invalid arguments passed to the from_big_endian_64() function. This includes wrong
   * argument count, wrong data types, and wrong BLOB length.
   */
  @Test
  public void testFromBigEndian64OnInvalidInputs() {
    // Define the expected error message for wrong argument count or type
    String typeAndCountErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_big_endian_64 only accepts one argument and it must be BLOB data type.";

    // Test with invalid parameter type (INT64)
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_int64) FROM table1", typeAndCountErrorMessage, DATABASE_NAME);

    // Test with invalid parameter type (TEXT)
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_text) FROM table1", typeAndCountErrorMessage, DATABASE_NAME);

    // Test with invalid parameter count (0 arguments)
    tableAssertTestFail(
        "SELECT from_big_endian_64() FROM table1", typeAndCountErrorMessage, DATABASE_NAME);

    // Test with invalid parameter count (>1 arguments)
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_blob, c_blob) FROM table1",
        typeAndCountErrorMessage,
        DATABASE_NAME);

    // Define the expected error message for wrong BLOB length
    String lengthErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_big_endian_64' due to an invalid input format. Problematic value: ";

    // Test with BLOB length < 8
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_blob) FROM table1 WHERE time = 7",
        lengthErrorMessage + "0x01020304050607",
        DATABASE_NAME);

    // Test with BLOB length > 8
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMessage + "0x010203040506070809",
        DATABASE_NAME);
  }
}
