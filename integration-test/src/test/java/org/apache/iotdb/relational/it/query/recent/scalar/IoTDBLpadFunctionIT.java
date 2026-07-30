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
public class IoTDBLpadFunctionIT {

  private static final String DATABASE_NAME = "test_lpad_function";
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

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
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
}
