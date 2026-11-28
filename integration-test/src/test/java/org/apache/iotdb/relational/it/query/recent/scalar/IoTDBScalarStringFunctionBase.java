/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

abstract class IoTDBScalarStringFunctionBase extends IoTDBBinaryEncodingFunctionBase {

  private static final String DATABASE_NAME = "test_lpad_function";
  private static final String RPAD_DATABASE_NAME = "test_rpad_function";
  private static final String LENGTH_DATABASE_NAME = "test_length_function";
  private static final String REVERSE_DATABASE_NAME = "test_reverse_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE t1("
            + "id INT32 FIELD, "
            + "c_blob_data BLOB FIELD, "
            + "c_blob_pad BLOB FIELD, "
            + "c_text_data TEXT FIELD, "
            + "c_int_data INT32 FIELD, "
            + "c_int_size INT32 FIELD)",

        // 1. Base data for padding, also has non-blob types for error testing
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad, c_text_data, c_int_data) VALUES (1, 1, x'AABB', x'00', 'text', 123)",
        // 2. Data for multi-byte and truncated padding
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (2, 2, x'FF', x'123456')",
        // 3. Data for truncation
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (3, 3, x'0102030405060708', x'FF')",
        // 4. Data for equal length test
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (4, 4, x'ABCDEF', x'00')",
        // 5. Data for empty source blob test
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (5, 5, x'', x'AB')",
        // 6. Row with NULL blob_data
        "INSERT INTO t1(time, id, c_blob_pad, c_int_size) VALUES (6, 6, x'00', 5)",
        // 7. Row with NULL blob_pad
        "INSERT INTO t1(time, id, c_blob_data, c_int_size) VALUES (7, 7, x'AA', 5)",
        // 8. Row with NULL c_int_size
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (8, 8, x'AA', x'00')",
        // 9. Row with invalid (empty) pad data
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (9, 9, x'AA', x'')"
      };

  private static final String[] lengthCreateSqls =
      new String[] {
        "CREATE DATABASE " + LENGTH_DATABASE_NAME,
        "USE " + LENGTH_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (1, 'hello', 'hello', X'68656C6C6F')",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (2, '你好', '你好', X'e4bda0e5a5bd')",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (3, '', '', X'')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)"
      };

  private static final String[] reverseCreateSqls =
      new String[] {
        "CREATE DATABASE " + REVERSE_DATABASE_NAME,
        "USE " + REVERSE_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '你好, world', '你好, world')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        "INSERT INTO table1(time, c_blob) VALUES (5, x'01020304')",
        "INSERT INTO table1(time, c_blob) VALUES (6, x'')",
        "INSERT INTO table1(time, c_blob) VALUES (7, x'AABBCCDD')"
      };

  private static final String[] rpadCreateSqls =
      new String[] {
        "CREATE DATABASE " + RPAD_DATABASE_NAME,
        "USE " + RPAD_DATABASE_NAME,
        "CREATE TABLE t1("
            + "id INT32 FIELD, "
            + "c_blob_data BLOB FIELD, "
            + "c_blob_pad BLOB FIELD, "
            + "c_text_data TEXT FIELD, "
            + "c_int_data INT32 FIELD, "
            + "c_int_size INT32 FIELD)",
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad, c_text_data, c_int_data) VALUES (1, 1, x'AABB', x'00', 'text', 123)",
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (2, 2, x'FF', x'12345678')",
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (3, 3, x'0102030405060708', x'FF')",
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (4, 4, x'ABCDEF', x'00')",
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (5, 5, x'', x'AB')",
        "INSERT INTO t1(time, id, c_blob_pad, c_int_size) VALUES (6, 6, x'00', 5)",
        "INSERT INTO t1(time, id, c_blob_data, c_int_size) VALUES (7, 7, x'AA', 5)",
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (8, 8, x'AA', x'00')",
        "INSERT INTO t1(time, id, c_blob_data, c_blob_pad) VALUES (9, 9, x'AA', x'')"
      };

  protected static void insertStringFunctionData() {
    prepareTableData(createSqls);
    prepareTableData(rpadCreateSqls);
    prepareTableData(lengthCreateSqls);
    prepareTableData(reverseCreateSqls);
  }

  /** Test cases where the source blob's length is less than the target size, requiring padding. */
  @Test
  public void testPaddingCases() {
    String[] expectedHeader = new String[] {"time", "lpad"};

    // Test simple padding: LPAD(x'AABB', 5, x'00') -> x'000000aabb'
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, 5, c_blob_pad) as \"lpad\" FROM t1 WHERE id = 1",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.001Z,0x000000aabb,"},
        DATABASE_NAME);

    // Test full repetition of pad data: LPAD(x'FF', 7, x'123456') -> x'123456123456ff'
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, 7, c_blob_pad) as \"lpad\" FROM t1 WHERE id = 2",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.002Z,0x123456123456ff,"},
        DATABASE_NAME);

    // Test truncated repetition of pad data: LPAD(x'FF', 6, x'123456') -> x'1234561234ff'
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, 6, c_blob_pad) as \"lpad\" FROM t1 WHERE id = 2",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.002Z,0x1234561234ff,"},
        DATABASE_NAME);

    // Test padding an empty blob: LPAD(x'', 4, x'AB') -> x'abababab'
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, 4, c_blob_pad) as \"lpad\" FROM t1 WHERE id = 5",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.005Z,0xabababab,"},
        DATABASE_NAME);
  }

  /**
   * Test cases where the source blob's length is greater than the target size, requiring
   * truncation.
   */
  @Test
  public void testTruncationCases() {
    String[] expectedHeader = new String[] {"time", "lpad"};

    // Test standard truncation: LPAD(x'0102030405060708', 4, x'FF') -> x'01020304'
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, 4, c_blob_pad) as \"lpad\" FROM t1 WHERE id = 3",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.003Z,0x01020304,"},
        DATABASE_NAME);

    // Test truncation to zero length: LPAD(x'AABB', 0, x'00') -> x''
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, 0, c_blob_pad) as \"lpad\" FROM t1 WHERE id = 1",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.001Z,0x,"},
        DATABASE_NAME);
  }

  /** Test the case where the source blob's length is equal to the target size. */
  @Test
  public void testEqualLengthCase() {
    String[] expectedHeader = new String[] {"time", "lpad"};
    String[] retArray =
        new String[] {
          // LPAD(x'ABCDEF', 3, x'00') -> x'abcdef' (no change)
          "1970-01-01T00:00:00.004Z,0xabcdef,"
        };
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, 3, c_blob_pad) as \"lpad\" FROM t1 where id = 4",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test that if any argument is NULL from the table, the result is NULL. */
  @Test
  public void testNullInputCases() {
    String[] expectedHeader = new String[] {"time", "lpad_result"};

    // Case 1: 'data' argument is NULL. Read from row where id=6.
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, c_int_size, c_blob_pad) as \"lpad_result\" FROM t1 WHERE id = 6",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.006Z,null,"},
        DATABASE_NAME);

    // Case 2: 'paddata' argument is NULL. Read from row where id=7.
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, c_int_size, c_blob_pad) as \"lpad_result\" FROM t1 WHERE id = 7",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.007Z,null,"},
        DATABASE_NAME);

    // Case 3: 'size' argument is NULL. Read from row where id=8.
    tableResultSetEqualTest(
        "SELECT time, LPAD(c_blob_data, c_int_size, c_blob_pad) as \"lpad_result\" FROM t1 WHERE id = 8",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.008Z,null,"},
        DATABASE_NAME);
  }

  /** Test invalid parameter values that should cause the function to fail. */
  @Test
  public void testInvalidParameters() {
    // Test with a negative size
    tableAssertTestFail(
        "SELECT LPAD(c_blob_data, -1, c_blob_pad) FROM t1 WHERE id = 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'Lpad' due to the value 0xaabb corresponding to a invalid target size, the allowed range is [0, 2147483647].",
        DATABASE_NAME);

    // Test with an empty paddata blob
    tableAssertTestFail(
        "SELECT LPAD(c_blob_data, 5, c_blob_pad) FROM t1 WHERE id = 9",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'Lpad' due the value 0xaa corresponding to a empty padding string.",
        DATABASE_NAME);
  }

  /** Test argument type validation for the LPAD function. */
  @Test
  public void testInvalidTypes() {
    String wrongTypeErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lpad only accepts three arguments, first argument must be BlOB type, "
            + "second argument must be int32 or int64 type, third argument must be BLOB type.";

    // First argument is not a BLOB
    tableAssertTestFail(
        "SELECT LPAD(c_text_data, 5, c_blob_pad) FROM t1 WHERE id = 1",
        wrongTypeErrorMessage,
        DATABASE_NAME);

    // Second argument is not an INTEGER
    tableAssertTestFail(
        "SELECT LPAD(c_blob_data, '5', c_blob_pad) FROM t1 WHERE id = 1",
        wrongTypeErrorMessage,
        DATABASE_NAME);

    // Third argument is not a BLOB
    tableAssertTestFail(
        "SELECT LPAD(c_blob_data, 5, c_int_data) FROM t1 WHERE id = 1",
        wrongTypeErrorMessage,
        DATABASE_NAME);
  }

  // Verifies RPAD fills the right side, including repeated and truncated multi-byte padding.
  @Test
  public void testRpadPaddingCases() {
    String[] expectedHeader = new String[] {"time", "rpad"};
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, 5, c_blob_pad) as \"rpad\" FROM t1 WHERE id = 1",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.001Z,0xaabb000000,"},
        RPAD_DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, 7, c_blob_pad) as \"rpad\" FROM t1 WHERE id = 2",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.002Z,0xff123456781234,"},
        RPAD_DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, 6, c_blob_pad) as \"rpad\" FROM t1 WHERE id = 2",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.002Z,0xff1234567812,"},
        RPAD_DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, 4, c_blob_pad) as \"rpad\" FROM t1 WHERE id = 5",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.005Z,0xabababab,"},
        RPAD_DATABASE_NAME);
  }

  // Verifies RPAD truncates values to the target length, including a zero-length result.
  @Test
  public void testRpadTruncationCases() {
    String[] expectedHeader = new String[] {"time", "rpad"};
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, 4, c_blob_pad) as \"rpad\" FROM t1 WHERE id = 3",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.003Z,0x01020304,"},
        RPAD_DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, 0, c_blob_pad) as \"rpad\" FROM t1 WHERE id = 1",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.001Z,0x,"},
        RPAD_DATABASE_NAME);
  }

  // Confirms RPAD leaves a BLOB unchanged when its size already matches the requested length.
  @Test
  public void testRpadEqualLengthCase() {
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, 3, c_blob_pad) as \"rpad\" FROM t1 where id = 4",
        new String[] {"time", "rpad"},
        new String[] {"1970-01-01T00:00:00.004Z,0xabcdef,"},
        RPAD_DATABASE_NAME);
  }

  // Confirms RPAD returns NULL when its source, pad data, or target size is NULL.
  @Test
  public void testRpadNullInputCases() {
    String[] expectedHeader = new String[] {"time", "rpad_result"};
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, c_int_size, c_blob_pad) as \"rpad_result\" FROM t1 WHERE id = 6",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.006Z,null,"},
        RPAD_DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, c_int_size, c_blob_pad) as \"rpad_result\" FROM t1 WHERE id = 7",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.007Z,null,"},
        RPAD_DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, RPAD(c_blob_data, c_int_size, c_blob_pad) as \"rpad_result\" FROM t1 WHERE id = 8",
        expectedHeader,
        new String[] {"1970-01-01T00:00:00.008Z,null,"},
        RPAD_DATABASE_NAME);
  }

  // Rejects RPAD with a negative target size or an empty pad BLOB.
  @Test
  public void testRpadInvalidParameters() {
    tableAssertTestFail(
        "SELECT RPAD(c_blob_data, -1, c_blob_pad) FROM t1 WHERE id = 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'Rpad' due to the value 0xaabb corresponding to a invalid target size, the allowed range is [0, 2147483647].",
        RPAD_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT RPAD(c_blob_data, 5, c_blob_pad) FROM t1 WHERE id = 9",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'Rpad' due the value 0xaa corresponding to a empty padding string.",
        RPAD_DATABASE_NAME);
  }

  // Rejects RPAD when its source, size, or pad argument has the wrong type.
  @Test
  public void testRpadInvalidTypes() {
    String wrongTypeErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function rpad only accepts three arguments, first argument must be BlOB type, "
            + "second argument must be int32 or int64 type, third argument must be BLOB type.";
    tableAssertTestFail(
        "SELECT RPAD(c_text_data, 5, c_blob_pad) FROM t1 WHERE id = 1",
        wrongTypeErrorMessage,
        RPAD_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT RPAD(c_blob_data, '5', c_blob_pad) FROM t1 WHERE id = 1",
        wrongTypeErrorMessage,
        RPAD_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT RPAD(c_blob_data, 5, c_int_data) FROM t1 WHERE id = 1",
        wrongTypeErrorMessage,
        RPAD_DATABASE_NAME);
  }

  // Checks LENGTH counts Unicode characters for TEXT/STRING and returns NULL for missing inputs.
  @Test
  public void testLengthOnTextAndString() {
    tableResultSetEqualTest(
        "SELECT time, length(c_text) as \"length(c_text)\", length(c_string) as \"length(c_string)\" FROM table1",
        new String[] {"time", "length(c_text)", "length(c_string)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,5,5,",
          "1970-01-01T00:00:00.002Z,2,2,",
          "1970-01-01T00:00:00.003Z,0,0,",
          "1970-01-01T00:00:00.004Z,null,null,"
        },
        LENGTH_DATABASE_NAME);
  }

  // Checks LENGTH counts BLOB bytes, including empty and NULL BLOB values.
  @Test
  public void testLengthOnBlob() {
    tableResultSetEqualTest(
        "SELECT time, length(c_blob) as \"length(c_blob)\" FROM table1",
        new String[] {"time", "length(c_blob)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,5,",
          "1970-01-01T00:00:00.002Z,6,",
          "1970-01-01T00:00:00.003Z,0,",
          "1970-01-01T00:00:00.004Z,null,"
        },
        LENGTH_DATABASE_NAME);
  }

  // Rejects LENGTH for a numeric column or with more than one argument.
  @Test
  public void testLengthFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string or blob or object data type.";
    tableAssertTestFail(
        "SELECT length(c_int) FROM table1", expectedErrorMessage, LENGTH_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT length(c_text, 1) FROM table1", expectedErrorMessage, LENGTH_DATABASE_NAME);
  }

  // Checks REVERSE on ASCII and multibyte TEXT/STRING, empty strings, and NULL inputs.
  @Test
  public void testReverseOnTextAndString() {
    tableResultSetEqualTest(
        "SELECT time, REVERSE(c_text) as \"reverse(c_text)\", REVERSE(c_string) as \"reverse(c_string)\" FROM table1 where time < 5",
        new String[] {"time", "reverse(c_text)", "reverse(c_string)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,olleh,olleh,",
          "1970-01-01T00:00:00.002Z,dlrow ,好你,dlrow ,好你,",
          "1970-01-01T00:00:00.003Z,,,",
          "1970-01-01T00:00:00.004Z,null,null,"
        },
        REVERSE_DATABASE_NAME);
  }

  // Checks REVERSE reverses bytes in nonempty and empty BLOB inputs.
  @Test
  public void testReverseOnBlob() {
    tableResultSetEqualTest(
        "SELECT time, REVERSE(c_blob) as \"reverse(c_blob)\" FROM table1 where time >= 5",
        new String[] {"time", "reverse(c_blob)"},
        new String[] {
          "1970-01-01T00:00:00.005Z,0x04030201,",
          "1970-01-01T00:00:00.006Z,0x,",
          "1970-01-01T00:00:00.007Z,0xddccbbaa,"
        },
        REVERSE_DATABASE_NAME);
  }

  // Rejects REVERSE with an INT32 input, no argument, or multiple arguments.
  @Test
  public void testReverseFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function reverse only accepts one argument and it must be TEXT, STRING, or BlOB data type.";
    tableAssertTestFail(
        "SELECT REVERSE(c_int) FROM table1", expectedErrorMessage, REVERSE_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT REVERSE(c_text, 'another_arg') FROM table1",
        expectedErrorMessage,
        REVERSE_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT REVERSE() FROM table1", expectedErrorMessage, REVERSE_DATABASE_NAME);
  }
}
