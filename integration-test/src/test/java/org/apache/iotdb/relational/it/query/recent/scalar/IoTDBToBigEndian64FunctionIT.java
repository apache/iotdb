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
public class IoTDBToBigEndian64FunctionIT {

  private static final String DATABASE_NAME = "test_to_big_endian_64";

  // SQL statements to set up the database and table for testing
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_int64 INT64, c_int32 INT32, c_text TEXT)",

        // Case 1: A common positive integer (0x0102030405060708)
        "INSERT INTO table1(time, c_int64) VALUES (1, 72623859790382856)",

        // Case 2: A negative integer (-1, which is 0xFFFFFFFFFFFFFFFF in two's complement)
        "INSERT INTO table1(time, c_int64) VALUES (2, -1)",

        // Case 3: Zero (0x0000000000000000)
        "INSERT INTO table1(time, c_int64) VALUES (3, 0)",

        // Case 4: Maximum INT64 value (0x7FFFFFFFFFFFFFFF)
        "INSERT INTO table1(time, c_int64) VALUES (4, 9223372036854775807)",

        // Case 5: Minimum INT64 value (0x8000000000000000)
        "INSERT INTO table1(time, c_int64) VALUES (5, -9223372036854775808)",

        // Case 6: Null input value, also populate other columns for invalid type testing
        "INSERT INTO table1(time, c_int64, c_int32, c_text) VALUES (6, null, 123, 'some_text')",
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
   * Validates the to_big_endian_64() function on various valid INT64 inputs. This test covers
   * positive, negative, zero, min/max, and null values.
   */
  @Test
  public void testToBigEndian64OnValidInputs() {
    String[] expectedHeader =
        new String[] {
          "time", "to_big_endian_64(c_int64)",
        };
    String[] retArray =
        new String[] {
          // 1. Positive integer 72623859790382856 -> 0x0102030405060708
          "1970-01-01T00:00:00.001Z,0x0102030405060708,",
          // 2. Negative integer -1 -> 0xffffffffffffffff
          "1970-01-01T00:00:00.002Z,0xffffffffffffffff,",
          // 3. Zero -> 0x0000000000000000
          "1970-01-01T00:00:00.003Z,0x0000000000000000,",
          // 4. Max INT64 -> 0x7fffffffffffffff
          "1970-01-01T00:00:00.004Z,0x7fffffffffffffff,",
          // 5. Min INT64 -> 0x8000000000000000
          "1970-01-01T00:00:00.005Z,0x8000000000000000,",
          // 6. Null input -> null output
          "1970-01-01T00:00:00.006Z,null,",
        };

    tableResultSetEqualTest(
        "SELECT time, to_big_endian_64(c_int64) as \"to_big_endian_64(c_int64)\" FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /**
   * Tests for invalid arguments passed to the to_big_endian_64() function. This includes wrong
   * argument count and wrong data types.
   */
  @Test
  public void testToBigEndian64OnInvalidInputs() {
    // Define the expected error message for semantic errors
    String errorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_big_endian_64 only accepts one argument and it must be Int64 data type.";

    // Test with an invalid parameter type (INT32)
    tableAssertTestFail(
        "SELECT to_big_endian_64(c_int32) FROM table1", errorMessage, DATABASE_NAME);

    // Test with an invalid parameter type (TEXT)
    tableAssertTestFail("SELECT to_big_endian_64(c_text) FROM table1", errorMessage, DATABASE_NAME);

    // Test with an invalid parameter count (0 arguments)
    tableAssertTestFail("SELECT to_big_endian_64() FROM table1", errorMessage, DATABASE_NAME);

    // Test with an invalid parameter count (>1 arguments)
    tableAssertTestFail(
        "SELECT to_big_endian_64(c_int64, c_int64) FROM table1", errorMessage, DATABASE_NAME);
  }
}
