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
public class IoTDBToLittleEndian32FunctionIT {

  private static final String DATABASE_NAME = "test_to_little_endian_32";

  // SQL statements to set up the database and table for testing
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_int32 INT32, c_int64 INT64, c_text TEXT)",

        // Case 1: A common positive integer (16909060, which is 0x01020304 in hex)
        "INSERT INTO table1(time, c_int32) VALUES (1, 16909060)",

        // Case 2: A negative integer (-1, which is 0xFFFFFFFF in two's complement)
        "INSERT INTO table1(time, c_int32) VALUES (2, -1)",

        // Case 3: Zero (0x00000000)
        "INSERT INTO table1(time, c_int32) VALUES (3, 0)",

        // Case 4: Maximum INT32 value (2147483647, which is 0x7FFFFFFF)
        "INSERT INTO table1(time, c_int32) VALUES (4, 2147483647)",

        // Case 5: Minimum INT32 value (-2147483648, which is 0x80000000)
        "INSERT INTO table1(time, c_int32) VALUES (5, -2147483648)",

        // Case 6: Null input value, also populate other columns for invalid type testing
        "INSERT INTO table1(time, c_int32, c_int64, c_text) VALUES (6, null, 123, 'some_text')",
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
   * Validates the to_little_endian_32() function on various valid INT32 inputs. This test covers
   * positive, negative, zero, min/max, and null values.
   */
  @Test
  public void testToLittleEndian32OnValidInputs() {
    String[] expectedHeader =
        new String[] {
          "time", "to_little_endian_32(c_int32)",
        };
    String[] retArray =
        new String[] {
          // 1. Positive integer 16909060 (0x01020304) -> 0x04030201
          "1970-01-01T00:00:00.001Z,0x04030201,",
          // 2. Negative integer -1 (0xffffffff) -> 0xffffffff
          "1970-01-01T00:00:00.002Z,0xffffffff,",
          // 3. Zero (0x00000000) -> 0x00000000
          "1970-01-01T00:00:00.003Z,0x00000000,",
          // 4. Max INT32 (0x7fffffff) -> 0xffffff7f
          "1970-01-01T00:00:00.004Z,0xffffff7f,",
          // 5. Min INT32 (0x80000000) -> 0x00000080
          "1970-01-01T00:00:00.005Z,0x00000080,",
          // 6. Null input -> null output
          "1970-01-01T00:00:00.006Z,null,",
        };

    tableResultSetEqualTest(
        "SELECT time, to_little_endian_32(c_int32) as \"to_little_endian_32(c_int32)\" FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /**
   * Tests for invalid arguments passed to the to_little_endian_32() function. This includes wrong
   * argument count and wrong data types.
   */
  @Test
  public void testToLittleEndian32OnInvalidInputs() {
    // Define the expected error message for semantic errors
    String errorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_little_endian_32 only accepts one argument and it must be Int32 data type.";

    // Test with an invalid parameter type (INT64)
    tableAssertTestFail(
        "SELECT to_little_endian_32(c_int64) FROM table1", errorMessage, DATABASE_NAME);

    // Test with an invalid parameter type (TEXT)
    tableAssertTestFail(
        "SELECT to_little_endian_32(c_text) FROM table1", errorMessage, DATABASE_NAME);

    // Test with an invalid parameter count (0 arguments)
    tableAssertTestFail("SELECT to_little_endian_32() FROM table1", errorMessage, DATABASE_NAME);

    // Test with an invalid parameter count (>1 arguments)
    tableAssertTestFail(
        "SELECT to_little_endian_32(c_int32, c_int32) FROM table1", errorMessage, DATABASE_NAME);
  }
}
