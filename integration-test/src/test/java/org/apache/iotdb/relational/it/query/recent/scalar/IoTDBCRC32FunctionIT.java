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
public class IoTDBCRC32FunctionIT {

  private static final String DATABASE_NAME = "test_crc32_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",
        // 1. Test standard string 'hello'
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        // 2. Test Chinese characters '你好' (UTF-8)
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '你好', '你好')",
        // 3. Test empty string
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        // 4. Test null values
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        // 5. Test special characters and spaces
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'Hello, World!', 'Hello, World!')",
        // 6. Test blob data (hex representation of 'test')
        "INSERT INTO table1(time, c_blob) VALUES (6, x'74657374')",
        // 7. Test standard benchmark string '123456789'
        "INSERT INTO table1(time, c_text) VALUES (7, '123456789')"
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

  /** Validate the CRC32 checksum for TEXT/STRING types */
  @Test
  public void testCrc32OnTextString() {

    String[] expectedHeader = new String[] {"time", "crc32(c_text)", "crc32(c_string)"};
    String[] retArray =
        new String[] {
          // 'hello' -> 907060870
          "1970-01-01T00:00:00.001Z,907060870,907060870,",
          // '你好' (UTF-8) -> 3690463373
          "1970-01-01T00:00:00.002Z,1352841281,1352841281,",
          // '' -> 0
          "1970-01-01T00:00:00.003Z,0,0,",
          // null -> null
          "1970-01-01T00:00:00.004Z,null,null,",
          // 'Hello, World!' -> 4158428615
          "1970-01-01T00:00:00.005Z,3964322768,3964322768,"
        };
    tableResultSetEqualTest(
        "SELECT time, crc32(c_text) as \"crc32(c_text)\", crc32(c_string) as \"crc32(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Validate the CRC32 checksum for BLOB type */
  @Test
  public void testCrc32OnBlob() {
    String[] expectedHeader = new String[] {"time", "crc32(c_blob)"};
    String[] retArray =
        new String[] {
          // blob x'74657374' ('test') -> 3632233996
          "1970-01-01T00:00:00.006Z,3632233996,"
        };
    tableResultSetEqualTest(
        "SELECT time, crc32(c_blob) as \"crc32(c_blob)\" FROM table1 where time = 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Validate against a known industry-standard value */
  @Test
  public void testCrc32KnownValue() {
    String[] expectedHeader = new String[] {"time", "crc32(c_text)"};
    String[] retArray =
        new String[] {
          // '123456789' -> 3421780262
          "1970-01-01T00:00:00.007Z,3421780262,"
        };
    tableResultSetEqualTest(
        "SELECT time, crc32(c_text) as \"crc32(c_text)\" FROM table1 where time = 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test that invalid input types or number of arguments are rejected */
  @Test
  public void testCrc32FunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function crc32 only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // Test with invalid data type (INT32)
    tableAssertTestFail("SELECT crc32(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with multiple arguments
    tableAssertTestFail(
        "SELECT crc32(c_text, 'another_arg') FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT crc32() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
