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

abstract class IoTDBBinaryEncodingFunctionBase extends IoTDBToBase32ColumnFunctionBase {

  private static final String DATABASE_NAME = "test_from_little_endian_32";
  private static final String FROM_BIG_ENDIAN_32_DATABASE_NAME = "test_from_big_endian_32";
  private static final String FROM_BIG_ENDIAN_64_DATABASE_NAME = "test_from_big_endian_64";
  private static final String FROM_LITTLE_ENDIAN_64_DATABASE_NAME = "test_from_little_endian_64";
  private static final String FROM_IEEE754_32_DATABASE_NAME = "test_from_ieee754_32_big_endian";
  private static final String FROM_IEEE754_64_DATABASE_NAME = "test_from_ieee754_64_big_endian";

  // SQL statements to set up the database and table for testing
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int32 INT32, c_text TEXT)",

        // Case 1: BLOB for a common positive integer (16909060) in little-endian
        "INSERT INTO table1(time, c_blob) VALUES (1, X'04030201')",

        // Case 2: BLOB for a negative integer (-1) in little-endian
        "INSERT INTO table1(time, c_blob) VALUES (2, X'FFFFFFFF')",

        // Case 3: BLOB for zero in little-endian
        "INSERT INTO table1(time, c_blob) VALUES (3, X'00000000')",

        // Case 4: BLOB for the maximum INT32 value in little-endian
        "INSERT INTO table1(time, c_blob) VALUES (4, X'FFFFFF7F')",

        // Case 5: BLOB for the minimum INT32 value in little-endian
        "INSERT INTO table1(time, c_blob) VALUES (5, X'00000080')",

        // Case 6: Null BLOB input
        "INSERT INTO table1(time, c_blob) VALUES (6, null)",

        // Case 7: BLOB with invalid length (< 4 bytes) for error testing
        "INSERT INTO table1(time, c_blob) VALUES (7, X'010203')",

        // Case 8: BLOB with invalid length (> 4 bytes) for error testing
        "INSERT INTO table1(time, c_blob) VALUES (8, X'0102030405')",

        // Case 9: Data for invalid type testing
        "INSERT INTO table1(time, c_int32, c_text) VALUES (9, 100, 'some_text')",
      };

  private static final String[] fromBigEndian32Sqls =
      new String[] {
        "CREATE DATABASE " + FROM_BIG_ENDIAN_32_DATABASE_NAME,
        "USE " + FROM_BIG_ENDIAN_32_DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int32 INT32, c_text TEXT)",
        "INSERT INTO table1(time, c_blob) VALUES (1, X'01020304')",
        "INSERT INTO table1(time, c_blob) VALUES (2, X'FFFFFFFF')",
        "INSERT INTO table1(time, c_blob) VALUES (3, X'00000000')",
        "INSERT INTO table1(time, c_blob) VALUES (4, X'7FFFFFFF')",
        "INSERT INTO table1(time, c_blob) VALUES (5, X'80000000')",
        "INSERT INTO table1(time, c_blob) VALUES (6, null)",
        "INSERT INTO table1(time, c_blob) VALUES (7, X'010203')",
        "INSERT INTO table1(time, c_blob) VALUES (8, X'0102030405')",
        "INSERT INTO table1(time, c_int32, c_text) VALUES (9, 100, 'some_text')"
      };

  private static final String[] fromBigEndian64Sqls =
      new String[] {
        "CREATE DATABASE " + FROM_BIG_ENDIAN_64_DATABASE_NAME,
        "USE " + FROM_BIG_ENDIAN_64_DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int64 INT64, c_text TEXT)",
        "INSERT INTO table1(time, c_blob) VALUES (1, X'0102030405060708')",
        "INSERT INTO table1(time, c_blob) VALUES (2, X'FFFFFFFFFFFFFFFF')",
        "INSERT INTO table1(time, c_blob) VALUES (3, X'0000000000000000')",
        "INSERT INTO table1(time, c_blob) VALUES (4, X'7FFFFFFFFFFFFFFF')",
        "INSERT INTO table1(time, c_blob) VALUES (5, X'8000000000000000')",
        "INSERT INTO table1(time, c_blob) VALUES (6, null)",
        "INSERT INTO table1(time, c_blob) VALUES (7, X'01020304050607')",
        "INSERT INTO table1(time, c_blob) VALUES (8, X'010203040506070809')",
        "INSERT INTO table1(time, c_int64, c_text) VALUES (9, 100, 'some_text')"
      };

  private static final String[] fromLittleEndian64Sqls =
      new String[] {
        "CREATE DATABASE " + FROM_LITTLE_ENDIAN_64_DATABASE_NAME,
        "USE " + FROM_LITTLE_ENDIAN_64_DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int64 INT64, c_text TEXT)",
        "INSERT INTO table1(time, c_blob) VALUES (1, X'0807060504030201')",
        "INSERT INTO table1(time, c_blob) VALUES (2, X'FFFFFFFFFFFFFFFF')",
        "INSERT INTO table1(time, c_blob) VALUES (3, X'0000000000000000')",
        "INSERT INTO table1(time, c_blob) VALUES (4, X'FFFFFFFFFFFFFF7F')",
        "INSERT INTO table1(time, c_blob) VALUES (5, X'0000000000000080')",
        "INSERT INTO table1(time, c_blob) VALUES (6, null)",
        "INSERT INTO table1(time, c_blob) VALUES (7, X'01020304050607')",
        "INSERT INTO table1(time, c_blob) VALUES (8, X'010203040506070809')",
        "INSERT INTO table1(time, c_int64, c_text) VALUES (9, 100, 'some_text')"
      };

  private static final String[] fromIEEE754_32Sqls =
      new String[] {
        "CREATE DATABASE " + FROM_IEEE754_32_DATABASE_NAME,
        "USE " + FROM_IEEE754_32_DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int32 INT32, c_text TEXT)",
        "INSERT INTO table1(time, c_blob) VALUES (1, X'3FA00000')",
        "INSERT INTO table1(time, c_blob) VALUES (2, X'C0200000')",
        "INSERT INTO table1(time, c_blob) VALUES (3, X'00000000')",
        "INSERT INTO table1(time, c_blob) VALUES (4, X'40490E56')",
        "INSERT INTO table1(time, c_blob) VALUES (5, X'7FC00000')",
        "INSERT INTO table1(time, c_blob) VALUES (6, X'7F800000')",
        "INSERT INTO table1(time, c_blob) VALUES (7, null)",
        "INSERT INTO table1(time, c_blob) VALUES (8, X'3F40')",
        "INSERT INTO table1(time, c_blob) VALUES (9, X'3F40000000')",
        "INSERT INTO table1(time, c_int32, c_text) VALUES (10, 100, 'invalid_type')"
      };

  private static final String[] fromIEEE754_64Sqls =
      new String[] {
        "CREATE DATABASE " + FROM_IEEE754_64_DATABASE_NAME,
        "USE " + FROM_IEEE754_64_DATABASE_NAME,
        "CREATE TABLE table1(c_blob BLOB, c_int64 INT64, c_text TEXT)",
        "INSERT INTO table1(time, c_blob) VALUES (1, X'3FF4000000000000')",
        "INSERT INTO table1(time, c_blob) VALUES (2, X'C004000000000000')",
        "INSERT INTO table1(time, c_blob) VALUES (3, X'0000000000000000')",
        "INSERT INTO table1(time, c_blob) VALUES (4, X'400921FB54442D18')",
        "INSERT INTO table1(time, c_blob) VALUES (5, X'7FF8000000000000')",
        "INSERT INTO table1(time, c_blob) VALUES (6, X'7FF0000000000000')",
        "INSERT INTO table1(time, c_blob) VALUES (7, null)",
        "INSERT INTO table1(time, c_blob) VALUES (8, X'3FF40000')",
        "INSERT INTO table1(time, c_blob) VALUES (9, X'3FF400000000000000')",
        "INSERT INTO table1(time, c_int64, c_text) VALUES (10, 1000, 'invalid_type')"
      };

  protected static void insertBinaryEncodingData() {
    insertFromData();
  }

  private static void insertFromData() {
    prepareTableData(createSqls);
    prepareTableData(fromBigEndian32Sqls);
    prepareTableData(fromBigEndian64Sqls);
    prepareTableData(fromLittleEndian64Sqls);
    prepareTableData(fromIEEE754_32Sqls);
    prepareTableData(fromIEEE754_64Sqls);
  }

  /**
   * Validates the from_little_endian_32() function on various valid 4-byte BLOB inputs. This test
   * covers positive, negative, zero, min/max, and null values.
   */
  @Test
  public void testFromLittleEndian32OnValidInputs() {
    String[] expectedHeader =
        new String[] {
          "time", "from_little_endian_32(c_blob)",
        };
    String[] retArray =
        new String[] {
          // 1. X'04030201' -> 16909060
          "1970-01-01T00:00:00.001Z,16909060,",
          // 2. X'FFFFFFFF' -> -1
          "1970-01-01T00:00:00.002Z,-1,",
          // 3. X'00000000' -> 0
          "1970-01-01T00:00:00.003Z,0,",
          // 4. X'FFFFFF7F' -> 2147483647
          "1970-01-01T00:00:00.004Z,2147483647,",
          // 5. X'00000080' -> -2147483648
          "1970-01-01T00:00:00.005Z,-2147483648,",
          // 6. Null input -> null output
          "1970-01-01T00:00:00.006Z,null,",
        };

    tableResultSetEqualTest(
        "SELECT time, from_little_endian_32(c_blob) as \"from_little_endian_32(c_blob)\" FROM table1 WHERE time <= 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /**
   * Tests for invalid arguments passed to the from_little_endian_32() function. This includes wrong
   * argument count, wrong data types, and wrong BLOB length.
   */
  @Test
  public void testFromLittleEndian32OnInvalidInputs() {
    // Define the expected error message for wrong argument count or type
    String typeAndCountErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_little_endian_32 only accepts one argument and it must be BLOB data type.";

    // Test with invalid parameter type (INT32)
    tableAssertTestFail(
        "SELECT from_little_endian_32(c_int32) FROM table1",
        typeAndCountErrorMessage,
        DATABASE_NAME);

    // Test with invalid parameter type (TEXT)
    tableAssertTestFail(
        "SELECT from_little_endian_32(c_text) FROM table1",
        typeAndCountErrorMessage,
        DATABASE_NAME);

    // Test with invalid parameter count (0 arguments)
    tableAssertTestFail(
        "SELECT from_little_endian_32() FROM table1", typeAndCountErrorMessage, DATABASE_NAME);

    // Test with invalid parameter count (>1 arguments)
    tableAssertTestFail(
        "SELECT from_little_endian_32(c_blob, c_blob) FROM table1",
        typeAndCountErrorMessage,
        DATABASE_NAME);

    // Define the expected error message for wrong BLOB length
    String lengthErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_little_endian_32' due to an invalid input format. Problematic value: ";

    // Test with BLOB length < 4
    tableAssertTestFail(
        "SELECT from_little_endian_32(c_blob) FROM table1 WHERE time = 7",
        lengthErrorMessage + "0x010203",
        DATABASE_NAME);

    // Test with BLOB length > 4
    tableAssertTestFail(
        "SELECT from_little_endian_32(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMessage + "0x0102030405",
        DATABASE_NAME);
  }

  // Checks big-endian 4-byte decoding for signed INT32 boundaries, zero, and NULL.
  @Test
  public void testFromBigEndian32OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, from_big_endian_32(c_blob) as \"from_big_endian_32(c_blob)\" FROM table1 WHERE time <= 6",
        new String[] {"time", "from_big_endian_32(c_blob)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,16909060,",
          "1970-01-01T00:00:00.002Z,-1,",
          "1970-01-01T00:00:00.003Z,0,",
          "1970-01-01T00:00:00.004Z,2147483647,",
          "1970-01-01T00:00:00.005Z,-2147483648,",
          "1970-01-01T00:00:00.006Z,null,"
        },
        FROM_BIG_ENDIAN_32_DATABASE_NAME);
  }

  // Rejects big-endian INT32 input with the wrong type, arity, or BLOB length.
  @Test
  public void testFromBigEndian32OnInvalidInputs() {
    String typeAndCountErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_big_endian_32 only accepts one argument and it must be BLOB data type.";
    tableAssertTestFail(
        "SELECT from_big_endian_32(c_int32) FROM table1",
        typeAndCountErrorMessage,
        FROM_BIG_ENDIAN_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_big_endian_32(c_text) FROM table1",
        typeAndCountErrorMessage,
        FROM_BIG_ENDIAN_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_big_endian_32() FROM table1",
        typeAndCountErrorMessage,
        FROM_BIG_ENDIAN_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_big_endian_32(c_blob, c_blob) FROM table1",
        typeAndCountErrorMessage,
        FROM_BIG_ENDIAN_32_DATABASE_NAME);
    String lengthErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_big_endian_32' due to an invalid input format. Problematic value: ";
    tableAssertTestFail(
        "SELECT from_big_endian_32(c_blob) FROM table1 WHERE time = 7",
        lengthErrorMessage + "0x010203",
        FROM_BIG_ENDIAN_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_big_endian_32(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMessage + "0x0102030405",
        FROM_BIG_ENDIAN_32_DATABASE_NAME);
  }

  // Checks big-endian 8-byte decoding for signed INT64 boundaries, zero, and NULL.
  @Test
  public void testFromBigEndian64OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, from_big_endian_64(c_blob) as \"from_big_endian_64(c_blob)\" FROM table1 WHERE time <= 6",
        new String[] {"time", "from_big_endian_64(c_blob)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,72623859790382856,",
          "1970-01-01T00:00:00.002Z,-1,",
          "1970-01-01T00:00:00.003Z,0,",
          "1970-01-01T00:00:00.004Z,9223372036854775807,",
          "1970-01-01T00:00:00.005Z,-9223372036854775808,",
          "1970-01-01T00:00:00.006Z,null,"
        },
        FROM_BIG_ENDIAN_64_DATABASE_NAME);
  }

  // Rejects big-endian INT64 input with the wrong type, arity, or BLOB length.
  @Test
  public void testFromBigEndian64OnInvalidInputs() {
    String typeAndCountErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_big_endian_64 only accepts one argument and it must be BLOB data type.";
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_int64) FROM table1",
        typeAndCountErrorMessage,
        FROM_BIG_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_text) FROM table1",
        typeAndCountErrorMessage,
        FROM_BIG_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_big_endian_64() FROM table1",
        typeAndCountErrorMessage,
        FROM_BIG_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_blob, c_blob) FROM table1",
        typeAndCountErrorMessage,
        FROM_BIG_ENDIAN_64_DATABASE_NAME);
    String lengthErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_big_endian_64' due to an invalid input format. Problematic value: ";
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_blob) FROM table1 WHERE time = 7",
        lengthErrorMessage + "0x01020304050607",
        FROM_BIG_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_big_endian_64(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMessage + "0x010203040506070809",
        FROM_BIG_ENDIAN_64_DATABASE_NAME);
  }

  // Checks little-endian 8-byte decoding for signed INT64 boundaries, zero, and NULL.
  @Test
  public void testFromLittleEndian64OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, from_little_endian_64(c_blob) as \"from_little_endian_64(c_blob)\" FROM table1 WHERE time <= 6",
        new String[] {"time", "from_little_endian_64(c_blob)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,72623859790382856,",
          "1970-01-01T00:00:00.002Z,-1,",
          "1970-01-01T00:00:00.003Z,0,",
          "1970-01-01T00:00:00.004Z,9223372036854775807,",
          "1970-01-01T00:00:00.005Z,-9223372036854775808,",
          "1970-01-01T00:00:00.006Z,null,"
        },
        FROM_LITTLE_ENDIAN_64_DATABASE_NAME);
  }

  // Rejects little-endian INT64 input with the wrong type, arity, or BLOB length.
  @Test
  public void testFromLittleEndian64OnInvalidInputs() {
    String typeAndCountErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_little_endian_64 only accepts one argument and it must be BLOB data type.";
    tableAssertTestFail(
        "SELECT from_little_endian_64(c_int64) FROM table1",
        typeAndCountErrorMessage,
        FROM_LITTLE_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_little_endian_64(c_text) FROM table1",
        typeAndCountErrorMessage,
        FROM_LITTLE_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_little_endian_64() FROM table1",
        typeAndCountErrorMessage,
        FROM_LITTLE_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_little_endian_64(c_blob, c_blob) FROM table1",
        typeAndCountErrorMessage,
        FROM_LITTLE_ENDIAN_64_DATABASE_NAME);
    String lengthErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_little_endian_64' due to an invalid input format. Problematic value: ";
    tableAssertTestFail(
        "SELECT from_little_endian_64(c_blob) FROM table1 WHERE time = 7",
        lengthErrorMessage + "0x01020304050607",
        FROM_LITTLE_ENDIAN_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_little_endian_64(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMessage + "0x010203040506070809",
        FROM_LITTLE_ENDIAN_64_DATABASE_NAME);
  }

  // Checks IEEE754 FLOAT decoding for finite values, NaN, positive infinity, and NULL.
  @Test
  public void testFromIEEE754_32OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, from_ieee754_32(c_blob) AS \"from_ieee754_32(c_blob)\" FROM table1 WHERE time <= 7",
        new String[] {"time", "from_ieee754_32(c_blob)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,1.25,",
          "1970-01-01T00:00:00.002Z,-2.5,",
          "1970-01-01T00:00:00.003Z,0.0,",
          "1970-01-01T00:00:00.004Z,3.1415,",
          "1970-01-01T00:00:00.005Z,NaN,",
          "1970-01-01T00:00:00.006Z,Infinity,",
          "1970-01-01T00:00:00.007Z,null,"
        },
        FROM_IEEE754_32_DATABASE_NAME);
  }

  // Rejects IEEE754 FLOAT decoding with the wrong type, arity, or BLOB length.
  @Test
  public void testFromIEEE754_32OnInvalidInputs() {
    String typeCountErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_ieee754_32 only accepts one argument and it must be BLOB data type.";
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_int32) FROM table1 WHERE time = 10",
        typeCountErrorMessage,
        FROM_IEEE754_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_text) FROM table1 WHERE time = 10",
        typeCountErrorMessage,
        FROM_IEEE754_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_ieee754_32() FROM table1",
        typeCountErrorMessage,
        FROM_IEEE754_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_blob, c_blob) FROM table1 WHERE time = 1",
        typeCountErrorMessage,
        FROM_IEEE754_32_DATABASE_NAME);
    String lengthErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_ieee754_32' due to an invalid input format. Problematic value: ";
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMessage + "0x3f40",
        FROM_IEEE754_32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_ieee754_32(c_blob) FROM table1 WHERE time = 9",
        lengthErrorMessage + "0x3f40000000",
        FROM_IEEE754_32_DATABASE_NAME);
  }

  // Checks IEEE754 DOUBLE decoding for finite values, NaN, positive infinity, and NULL.
  @Test
  public void testFromIEEE754_64OnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, from_ieee754_64(c_blob) AS \"from_ieee754_64(c_blob)\" FROM table1 WHERE time <= 7",
        new String[] {"time", "from_ieee754_64(c_blob)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,1.25,",
          "1970-01-01T00:00:00.002Z,-2.5,",
          "1970-01-01T00:00:00.003Z,0.0,",
          "1970-01-01T00:00:00.004Z,3.141592653589793,",
          "1970-01-01T00:00:00.005Z,NaN,",
          "1970-01-01T00:00:00.006Z,Infinity,",
          "1970-01-01T00:00:00.007Z,null,"
        },
        FROM_IEEE754_64_DATABASE_NAME);
  }

  // Rejects IEEE754 DOUBLE decoding with the wrong type, arity, or BLOB length.
  @Test
  public void testFromIEEE754_64OnInvalidInputs() {
    String typeCountErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_ieee754_64 only accepts one argument and it must be BLOB data type.";
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_int64) FROM table1 WHERE time = 10",
        typeCountErrorMessage,
        FROM_IEEE754_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_text) FROM table1 WHERE time = 10",
        typeCountErrorMessage,
        FROM_IEEE754_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_ieee754_64() FROM table1",
        typeCountErrorMessage,
        FROM_IEEE754_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_blob, c_blob) FROM table1 WHERE time = 1",
        typeCountErrorMessage,
        FROM_IEEE754_64_DATABASE_NAME);
    String lengthErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_ieee754_64' due to an invalid input format. Problematic value: ";
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_blob) FROM table1 WHERE time = 8",
        lengthErrorMessage + "0x3ff40000",
        FROM_IEEE754_64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_ieee754_64(c_blob) FROM table1 WHERE time = 9",
        lengthErrorMessage + "0x3ff400000000000000",
        FROM_IEEE754_64_DATABASE_NAME);
  }
}
