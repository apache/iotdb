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
public class IoTDBToBase64ColumnFunctionIT {

  private static final String DATABASE_NAME = "test_tobase64_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",

        // Test string 'hello'
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        // Test Chinese characters '你好'
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

  /** Validate the normal encoding of to_base64() for TEXT/STRING type */
  @Test
  public void testToBase64OnTextString() {
    String[] expectedHeader = new String[] {"time", "to_base64(c_text)", "to_base64(c_string)"};
    String[] retArray =
        new String[] {
          // 'hello' -> 'aGVsbG8='
          "1970-01-01T00:00:00.001Z,aGVsbG8=,aGVsbG8=,",
          // '你好' -> '5L2g5aW9'
          "1970-01-01T00:00:00.002Z,5L2g5aW9,5L2g5aW9,",
          // '' -> ''
          "1970-01-01T00:00:00.003Z,,,",
          // null -> null
          "1970-01-01T00:00:00.004Z,null,null,",
          // 'Hello, World!' -> 'SGVsbG8sIFdvcmxkIQ=='
          "1970-01-01T00:00:00.005Z,SGVsbG8sIFdvcmxkIQ==,SGVsbG8sIFdvcmxkIQ==,"
        };
    tableResultSetEqualTest(
        "SELECT time, to_base64(c_text) as \"to_base64(c_text)\", to_base64(c_string) as \"to_base64(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Validate the normal encoding of to_base64() for BLOB type */
  @Test
  public void testToBase64OnBlob() {
    String[] expectedHeader = new String[] {"time", "to_base64(c_blob)"};
    String[] retArray =
        new String[] {
          // 0x74657374 ('test' in hex) -> 'dGVzdA=='
          "1970-01-01T00:00:00.006Z,dGVzdA==,"
        };
    tableResultSetEqualTest(
        "SELECT time, to_base64(c_blob) as \"to_base64(c_blob)\" FROM table1 where time = 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test roundtrip conversion: to_base64(from_base64(x)) should equal x */
  @Test
  public void testToBase64RoundTrip() {
    String[] expectedHeader = new String[] {"time", "roundtrip_result"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,hello,",
          "1970-01-01T00:00:00.002Z,你好,",
          "1970-01-01T00:00:00.003Z,,",
          "1970-01-01T00:00:00.005Z,Hello, World!,"
        };
    tableResultSetEqualTest(
        "SELECT time, CAST(from_base64(to_base64(c_text)) AS TEXT) as roundtrip_result FROM table1 where time in (1, 2, 3, 5)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Invalid input type or number of arguments should be rejected */
  @Test
  public void testToBase64FunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_base64 only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // Test with invalid data type (INT32)
    tableAssertTestFail("SELECT to_base64(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with multiple arguments
    tableAssertTestFail(
        "SELECT to_base64(c_text, 1) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT to_base64() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
