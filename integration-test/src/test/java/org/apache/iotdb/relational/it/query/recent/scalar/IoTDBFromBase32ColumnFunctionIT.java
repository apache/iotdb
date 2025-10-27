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
public class IoTDBFromBase32ColumnFunctionIT {

  private static final String DATABASE_NAME = "test_from_base32_function";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT, c_string STRING, c_int INT32)",

        // Case 1: Basic ASCII string
        // 'IoTDB is fun!' -> Base32: JBCGC3LFN5ZGIIDUNBSSA3LSEI====== -> BLOB:
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'JBCGC3LFN5ZGIIDUNBSSA3LSEI======', 'JBCGC3LFN5ZGIIDUNBSSA3LSEI======')",

        // Case 2: UTF-8 string
        // '你好, 世界!' -> Base32: 2W4V625443J56W4S4E73H5BUMM4A====== -> BLOB:
        // X'e4bda0e5a5bd2c20e4b896e7958c21'
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '2W4V625443J56W4S4E73H5BUMM4A======', '2W4V625443J56W4S4E73H5BUMM4A======')",

        // Case 3: Empty string
        // '' -> Base32: '' -> BLOB: X''
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",

        // Case 4: Null value
        "INSERT INTO table1(time, c_int) VALUES (4, 100)",

        // Case 5: Padding scenarios for 1-4 bytes
        // 'f' (0x66) -> Base32: MY======
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'MY======', 'MY======')",
        // 'fo' (0x666f) -> Base32: MZXQ====
        "INSERT INTO table1(time, c_text, c_string) VALUES (6, 'MZXQ====', 'MZXQ====')",
        // 'foo' (0x666f6f) -> Base32: MZXW6===
        "INSERT INTO table1(time, c_text, c_string) VALUES (7, 'MZXW6===', 'MZXW6===')",
        // 'foob' (0x666f6f62) -> Base32: MZXW6YQ=
        "INSERT INTO table1(time, c_text, c_string) VALUES (8, 'MZXW6YQ=', 'MZXW6YQ=')",

        // Case 9: No padding needed (5 bytes)
        // 'fooba' -> Base32: MZXW6YQB
        "INSERT INTO table1(time, c_text, c_string) VALUES (9, 'MZXW6YQB', 'MZXW6YQB')",

        // Case 10: Optional padding test (decoder should handle missing padding)
        // 'foo' (0x666f6f) -> Base32 without padding: MZXW6
        "INSERT INTO table1(time, c_text, c_string) VALUES (10, 'MZXW6', 'MZXW6')",
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

  /** Validates the from_base32() function on various supported and valid inputs. */
  @Test
  public void testFromBase32OnValidInputs() {
    String[] expectedHeader = new String[] {"time", "from_base32(c_text)", "from_base32(c_string)"};
    String[] retArray =
        new String[] {
          // 1. Basic ASCII
          "1970-01-01T00:00:00.001Z,0x4844616d656f726420746865206d7222,0x4844616d656f726420746865206d7222,",
          // 2. UTF-8
          "1970-01-01T00:00:00.002Z,0xd5b95f6bbce6d3df5b92e13fb3f4346338,0xd5b95f6bbce6d3df5b92e13fb3f4346338,",
          // 3. Empty string
          "1970-01-01T00:00:00.003Z,0x,0x,",
          // 4. Null input
          "1970-01-01T00:00:00.004Z,null,null,",
          // 5. 1 byte with padding
          "1970-01-01T00:00:00.005Z,0x66,0x66,",
          // 6. 2 bytes with padding
          "1970-01-01T00:00:00.006Z,0x666f,0x666f,",
          // 7. 3 bytes with padding
          "1970-01-01T00:00:00.007Z,0x666f6f,0x666f6f,",
          // 8. 4 bytes with padding
          "1970-01-01T00:00:00.008Z,0x666f6f62,0x666f6f62,",
          // 9. 5 bytes, no padding
          "1970-01-01T00:00:00.009Z,0x666f6f6201,0x666f6f6201,",
          // 10. Optional padding
          "1970-01-01T00:00:00.010Z,0x666f6f,0x666f6f,",
        };

    tableResultSetEqualTest(
        "SELECT time, from_base32(c_text) as \"from_base32(c_text)\", from_base32(c_string) as \"from_base32(c_string)\" FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Tests for invalid arguments passed to the from_base32() function. */
  @Test
  public void testFromBase32FunctionOnInvalidArguments() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_base32 only accepts one argument and it must be TEXT or STRING data type.";

    // Test with invalid parameter type (INT32)
    tableAssertTestFail(
        "SELECT from_base32(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with invalid parameter count (0)
    tableAssertTestFail("SELECT from_base32() FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with invalid parameter count (>1)
    tableAssertTestFail(
        "SELECT from_base32(c_text, c_string) FROM table1", expectedErrorMessage, DATABASE_NAME);
  }

  /** Validates that from_base32() fails when given incorrectly formatted Base32 strings. */
  @Test
  public void testFromBase32FunctionOnInvalidDataFormat() {
    String baseErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_base32' due to an invalid input format. Problematic value:";

    // Invalid character: '0' (not in Base32 alphabet)
    tableAssertTestFail(
        "SELECT from_base32('JBCGC3LFN5ZGIIDUNBSSA3LSE0==') FROM table1", // [FIXED] Added FROM
        // clause
        baseErrorMessage + " JBCGC3LFN5ZGIIDUNBSSA3LSE0==",
        DATABASE_NAME);

    // Invalid character: '1' (not in Base32 alphabet)
    tableAssertTestFail(
        "SELECT from_base32('JBCGC3LFN5ZGIIDUNBSSA3LSE1==') FROM table1", // [FIXED] Added FROM
        // clause
        baseErrorMessage + " JBCGC3LFN5ZGIIDUNBSSA3LSE1==",
        DATABASE_NAME);

    // Invalid character: '8' (not in Base32 alphabet)
    tableAssertTestFail(
        "SELECT from_base32('JBCGC3LFN5ZGIIDUNBSSA3LSE8==') FROM table1", // [FIXED] Added FROM
        // clause
        baseErrorMessage + " JBCGC3LFN5ZGIIDUNBSSA3LSE8==",
        DATABASE_NAME);

    // Invalid character: '9' (not in Base32 alphabet)
    tableAssertTestFail(
        "SELECT from_base32('JBCGC3LFN5ZGIIDUNBSSA3LSE9==') FROM table1", // [FIXED] Added FROM
        // clause
        baseErrorMessage + " JBCGC3LFN5ZGIIDUNBSSA3LSE9==",
        DATABASE_NAME);

    // Invalid character: '-' (from Base64URL)
    tableAssertTestFail(
        "SELECT from_base32('MZXW6YQ-') FROM table1", // [FIXED] Added FROM clause
        baseErrorMessage + " MZXW6YQ-",
        DATABASE_NAME);

    // Invalid padding: padding character in the middle
    tableAssertTestFail(
        "SELECT from_base32('MZXW6=Q=') FROM table1", // [FIXED] Added FROM clause
        baseErrorMessage + " MZXW6=Q=",
        DATABASE_NAME);
  }
}
