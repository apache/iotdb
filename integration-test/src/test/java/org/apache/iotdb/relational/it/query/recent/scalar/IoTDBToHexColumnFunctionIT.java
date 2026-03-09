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
public class IoTDBToHexColumnFunctionIT {

  private static final String DATABASE_NAME = "test_tohex_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",

        // Test string 'hello'
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        // Test Chinese characters '你好' (UTF-8)
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '你好', '你好')",
        // Test empty string
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        // Test null values
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        // Test special characters and spaces
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'Hello, World!', 'Hello, World!')",
        // Test blob data (hex representation of 'test')
        "INSERT INTO table1(time, c_blob) VALUES (6, x'74657374')"
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

  /** Validate the normal encoding of to_hex() for TEXT/STRING type */
  @Test
  public void testToHexOnTextString() {
    String[] expectedHeader = new String[] {"time", "to_hex(c_text)", "to_hex(c_string)"};
    String[] retArray =
        new String[] {
          // 'hello' -> '68656c6c6f'
          "1970-01-01T00:00:00.001Z,68656c6c6f,68656c6c6f,",
          // '你好' (UTF-8) -> 'e4bda0e5a5bd'
          "1970-01-01T00:00:00.002Z,e4bda0e5a5bd,e4bda0e5a5bd,",
          // '' -> ''
          "1970-01-01T00:00:00.003Z,,,",
          // null -> null
          "1970-01-01T00:00:00.004Z,null,null,",
          // 'Hello, World!' -> '48656c6c6f2c20576f726c6421'
          "1970-01-01T00:00:00.005Z,48656c6c6f2c20576f726c6421,48656c6c6f2c20576f726c6421,"
        };
    tableResultSetEqualTest(
        "SELECT time, to_hex(c_text) as \"to_hex(c_text)\", to_hex(c_string) as \"to_hex(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Validate the normal encoding of to_hex() for BLOB type */
  @Test
  public void testToHexOnBlob() {
    String[] expectedHeader = new String[] {"time", "to_hex(c_blob)"};
    String[] retArray =
        new String[] {
          // x'74657374' -> '74657374'
          "1970-01-01T00:00:00.006Z,74657374,"
        };
    tableResultSetEqualTest(
        "SELECT time, to_hex(c_blob) as \"to_hex(c_blob)\" FROM table1 where time = 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test roundtrip conversion: from_hex(to_hex(x)) should equal x */
  @Test
  public void testToHexRoundTrip() {
    String[] expectedHeader = new String[] {"time", "roundtrip_result"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,hello,",
          "1970-01-01T00:00:00.002Z,你好,",
          "1970-01-01T00:00:00.003Z,,",
          "1970-01-01T00:00:00.005Z,Hello, World!,"
        };
    tableResultSetEqualTest(
        "SELECT time, CAST(from_hex(to_hex(c_text)) AS TEXT) as roundtrip_result FROM table1 where time in (1, 2, 3, 5)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Invalid input type or number of arguments should be rejected */
  @Test
  public void testToHexFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_hex only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // Test with invalid data type (INT32)
    tableAssertTestFail("SELECT to_hex(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with multiple arguments
    tableAssertTestFail(
        "SELECT to_hex(c_text, 1) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT to_hex() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
