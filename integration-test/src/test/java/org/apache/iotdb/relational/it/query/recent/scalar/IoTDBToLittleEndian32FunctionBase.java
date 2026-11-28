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

import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

public abstract class IoTDBToLittleEndian32FunctionBase extends IoTDBHashFunctionBase {

  private static final String DATABASE_NAME = "test_to_little_endian_32";
  private static final String TO_BIG_ENDIAN_32_DATABASE_NAME = "test_to_big_endian_32";
  private static final String TO_BIG_ENDIAN_64_DATABASE_NAME = "test_to_big_endian_64";
  private static final String TO_LITTLE_ENDIAN_64_DATABASE_NAME = "test_to_little_endian_64";
  private static final String TO_IEEE754_32_DATABASE_NAME = "test_to_ieee754_32_big_endian";
  private static final String TO_IEEE754_64_DATABASE_NAME = "test_to_ieee754_64_big_endian";

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

  private static final String[] toBigEndian32Sqls =
      new String[] {
        "CREATE DATABASE " + TO_BIG_ENDIAN_32_DATABASE_NAME,
        "USE " + TO_BIG_ENDIAN_32_DATABASE_NAME,
        "CREATE TABLE table1(c_int32 INT32, c_text TEXT, c_boolean BOOLEAN)",
        "INSERT INTO table1(time, c_int32) VALUES (1, 16909060)",
        "INSERT INTO table1(time, c_int32) VALUES (2, -1)",
        "INSERT INTO table1(time, c_int32) VALUES (3, 0)",
        "INSERT INTO table1(time, c_int32) VALUES (4, 2147483647)",
        "INSERT INTO table1(time, c_int32) VALUES (5, -2147483648)",
        "INSERT INTO table1(time, c_int32, c_text, c_boolean) VALUES (6, null, 'text_value', true)"
      };

  private static final String[] toBigEndian64Sqls =
      new String[] {
        "CREATE DATABASE " + TO_BIG_ENDIAN_64_DATABASE_NAME,
        "USE " + TO_BIG_ENDIAN_64_DATABASE_NAME,
        "CREATE TABLE table1(c_int64 INT64, c_int32 INT32, c_text TEXT)",
        "INSERT INTO table1(time, c_int64) VALUES (1, 72623859790382856)",
        "INSERT INTO table1(time, c_int64) VALUES (2, -1)",
        "INSERT INTO table1(time, c_int64) VALUES (3, 0)",
        "INSERT INTO table1(time, c_int64) VALUES (4, 9223372036854775807)",
        "INSERT INTO table1(time, c_int64) VALUES (5, -9223372036854775808)",
        "INSERT INTO table1(time, c_int64, c_int32, c_text) VALUES (6, null, 123, 'some_text')"
      };

  private static final String[] toLittleEndian64Sqls =
      new String[] {
        "CREATE DATABASE " + TO_LITTLE_ENDIAN_64_DATABASE_NAME,
        "USE " + TO_LITTLE_ENDIAN_64_DATABASE_NAME,
        "CREATE TABLE table1(c_int64 INT64, c_int32 INT32, c_text TEXT)",
        "INSERT INTO table1(time, c_int64) VALUES (1, 72623859790382856)",
        "INSERT INTO table1(time, c_int64) VALUES (2, -1)",
        "INSERT INTO table1(time, c_int64) VALUES (3, 0)",
        "INSERT INTO table1(time, c_int64) VALUES (4, 9223372036854775807)",
        "INSERT INTO table1(time, c_int64) VALUES (5, -9223372036854775808)",
        "INSERT INTO table1(time, c_int64, c_int32, c_text) VALUES (6, null, 123, 'some_text')"
      };

  private static final String[] toIEEE754_32Sqls =
      new String[] {
        "CREATE DATABASE " + TO_IEEE754_32_DATABASE_NAME,
        "USE " + TO_IEEE754_32_DATABASE_NAME,
        "CREATE TABLE table1(c_float FLOAT, c_int32 INT32, c_text TEXT)",
        "INSERT INTO table1(time, c_float) VALUES (1, 1.25)",
        "INSERT INTO table1(time, c_float) VALUES (2, -2.5)",
        "INSERT INTO table1(time, c_float) VALUES (3, 0.0)",
        "INSERT INTO table1(time, c_float) VALUES (4, 3.1415)",
        "INSERT INTO table1(time, c_float) VALUES (5, null)",
        "INSERT INTO table1(time, c_int32, c_text) VALUES (6, 100, 'invalid_type')"
      };

  private static final String[] toIEEE754_64Sqls =
      new String[] {
        "CREATE DATABASE " + TO_IEEE754_64_DATABASE_NAME,
        "USE " + TO_IEEE754_64_DATABASE_NAME,
        "CREATE TABLE table1(c_double DOUBLE, c_int64 INT64, c_text TEXT)",
        "INSERT INTO table1(time, c_double) VALUES (1, 1.25)",
        "INSERT INTO table1(time, c_double) VALUES (2, -2.5)",
        "INSERT INTO table1(time, c_double) VALUES (3, 0.0)",
        "INSERT INTO table1(time, c_double) VALUES (4, 3.1415926535)",
        "INSERT INTO table1(time, c_double) VALUES (5, null)",
        "INSERT INTO table1(time, c_int64, c_text) VALUES (6, 1000, 'invalid_type')"
      };

  protected static void insertData() {
    prepareTableData(createSqls);
    prepareTableData(toBigEndian32Sqls);
    prepareTableData(toBigEndian64Sqls);
    prepareTableData(toLittleEndian64Sqls);
    prepareTableData(toIEEE754_32Sqls);
    prepareTableData(toIEEE754_64Sqls);
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

  // Checks big-endian INT32 output for positive, negative, zero, min/max, and NULL values.
  @Test
  public void testToBigEndian32OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, to_big_endian_32(c_int32) as \"to_big_endian_32(c_int32)\" FROM table1",
        new String[] {"time", "to_big_endian_32(c_int32)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,0x01020304,",
          "1970-01-01T00:00:00.002Z,0xffffffff,",
          "1970-01-01T00:00:00.003Z,0x00000000,",
          "1970-01-01T00:00:00.004Z,0x7fffffff,",
          "1970-01-01T00:00:00.005Z,0x80000000,",
          "1970-01-01T00:00:00.006Z,null,"
        },
        TO_BIG_ENDIAN_32_DATABASE_NAME);
  }

  // Rejects big-endian INT32 conversion for TEXT, BOOLEAN, or an invalid argument count.
  @Test
  public void testToBigEndian32OnInvalidArguments() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_big_endian_32 only accepts one argument and it must be Int32 data type.";
    tableAssertTestFail(
        "SELECT to_big_endian_32(c_text) FROM table1",
        expectedErrorMessage,
        TO_BIG_ENDIAN_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_big_endian_32(c_boolean) FROM table1",
        expectedErrorMessage,
        TO_BIG_ENDIAN_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_big_endian_32() FROM table1",
        expectedErrorMessage,
        TO_BIG_ENDIAN_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_big_endian_32(c_int32, c_int32) FROM table1",
        expectedErrorMessage,
        TO_BIG_ENDIAN_32_DATABASE_NAME);
  }

  // Checks big-endian INT64 output for positive, negative, zero, min/max, and NULL values.
  @Test
  public void testToBigEndian64OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, to_big_endian_64(c_int64) as \"to_big_endian_64(c_int64)\" FROM table1",
        new String[] {"time", "to_big_endian_64(c_int64)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,0x0102030405060708,",
          "1970-01-01T00:00:00.002Z,0xffffffffffffffff,",
          "1970-01-01T00:00:00.003Z,0x0000000000000000,",
          "1970-01-01T00:00:00.004Z,0x7fffffffffffffff,",
          "1970-01-01T00:00:00.005Z,0x8000000000000000,",
          "1970-01-01T00:00:00.006Z,null,"
        },
        TO_BIG_ENDIAN_64_DATABASE_NAME);
  }

  // Rejects big-endian INT64 conversion for INT32, TEXT, or an invalid argument count.
  @Test
  public void testToBigEndian64OnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_big_endian_64 only accepts one argument and it must be Int64 data type.";
    tableAssertTestFail(
        "SELECT to_big_endian_64(c_int32) FROM table1",
        expectedErrorMessage,
        TO_BIG_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_big_endian_64(c_text) FROM table1",
        expectedErrorMessage,
        TO_BIG_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_big_endian_64() FROM table1",
        expectedErrorMessage,
        TO_BIG_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_big_endian_64(c_int64, c_int64) FROM table1",
        expectedErrorMessage,
        TO_BIG_ENDIAN_64_DATABASE_NAME);
  }

  // Checks little-endian INT64 output for positive, negative, zero, min/max, and NULL values.
  @Test
  public void testToLittleEndian64OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, to_little_endian_64(c_int64) as \"to_little_endian_64(c_int64)\" FROM table1",
        new String[] {"time", "to_little_endian_64(c_int64)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,0x0807060504030201,",
          "1970-01-01T00:00:00.002Z,0xffffffffffffffff,",
          "1970-01-01T00:00:00.003Z,0x0000000000000000,",
          "1970-01-01T00:00:00.004Z,0xffffffffffffff7f,",
          "1970-01-01T00:00:00.005Z,0x0000000000000080,",
          "1970-01-01T00:00:00.006Z,null,"
        },
        TO_LITTLE_ENDIAN_64_DATABASE_NAME);
  }

  // Rejects little-endian INT64 conversion for INT32, TEXT, or an invalid argument count.
  @Test
  public void testToLittleEndian64OnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_little_endian_64 only accepts one argument and it must be Int64 data type.";
    tableAssertTestFail(
        "SELECT to_little_endian_64(c_int32) FROM table1",
        expectedErrorMessage,
        TO_LITTLE_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_little_endian_64(c_text) FROM table1",
        expectedErrorMessage,
        TO_LITTLE_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_little_endian_64() FROM table1",
        expectedErrorMessage,
        TO_LITTLE_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_little_endian_64(c_int64, c_int64) FROM table1",
        expectedErrorMessage,
        TO_LITTLE_ENDIAN_64_DATABASE_NAME);
  }

  // Checks FLOAT-to-IEEE754 output for finite values, zero, and NULL.
  @Test
  public void testToIEEE754_32OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, to_ieee754_32(c_float) AS \"to_ieee754_32(c_float)\" FROM table1 WHERE time <= 5",
        new String[] {"time", "to_ieee754_32(c_float)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,0x3fa00000,",
          "1970-01-01T00:00:00.002Z,0xc0200000,",
          "1970-01-01T00:00:00.003Z,0x00000000,",
          "1970-01-01T00:00:00.004Z,0x40490e56,",
          "1970-01-01T00:00:00.005Z,null,"
        },
        TO_IEEE754_32_DATABASE_NAME);
  }

  // Rejects FLOAT-to-IEEE754 conversion for INT32, TEXT, or an invalid argument count.
  @Test
  public void testToIEEE754_32OnInvalidInputs() {
    String errorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_ieee754_32 only accepts one argument and it must be Float data type.";
    tableAssertTestFail(
        "SELECT to_ieee754_32(c_int32) FROM table1 WHERE time = 6",
        errorMessage,
        TO_IEEE754_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_ieee754_32(c_text) FROM table1 WHERE time = 6",
        errorMessage,
        TO_IEEE754_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_ieee754_32() FROM table1", errorMessage, TO_IEEE754_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_ieee754_32(c_float, c_float) FROM table1 WHERE time = 1",
        errorMessage,
        TO_IEEE754_32_DATABASE_NAME);
  }

  // Checks DOUBLE-to-IEEE754 output for finite values, zero, and NULL.
  @Test
  public void testToIEEE754_64OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, to_ieee754_64(c_double) AS \"to_ieee754_64(c_double)\" FROM table1 WHERE time <= 5",
        new String[] {"time", "to_ieee754_64(c_double)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,0x3ff4000000000000,",
          "1970-01-01T00:00:00.002Z,0xc004000000000000,",
          "1970-01-01T00:00:00.003Z,0x0000000000000000,",
          "1970-01-01T00:00:00.004Z,0x400921fb54411744,",
          "1970-01-01T00:00:00.005Z,null,"
        },
        TO_IEEE754_64_DATABASE_NAME);
  }

  // Rejects DOUBLE-to-IEEE754 conversion for INT64, TEXT, or an invalid argument count.
  @Test
  public void testToIEEE754_64OnInvalidInputs() {
    String errorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_ieee754_64 only accepts one argument and it must be Double data type.";
    tableAssertTestFail(
        "SELECT to_ieee754_64(c_int64) FROM table1 WHERE time = 6",
        errorMessage,
        TO_IEEE754_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_ieee754_64(c_text) FROM table1 WHERE time = 6",
        errorMessage,
        TO_IEEE754_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_ieee754_64() FROM table1", errorMessage, TO_IEEE754_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_ieee754_64(c_double, c_double) FROM table1 WHERE time = 1",
        errorMessage,
        TO_IEEE754_64_DATABASE_NAME);
  }
}
