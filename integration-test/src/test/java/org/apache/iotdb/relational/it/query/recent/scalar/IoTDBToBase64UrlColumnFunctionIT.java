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
public class IoTDBToBase64UrlColumnFunctionIT {

  private static final String DATABASE_NAME = "test_tobase64url_function";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT, c_string STRING, c_blob BLOB, c_int INT32)",

        // 'hello world' -> Base64URL: aGVsbG8gd29ybGQ
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (1, 'hello world', 'hello world', X'68656c6c6f20776f726c64')",

        // '你好, IoTDB!' -> Base64URL: 5L2g5aW9LCBJT1REQjEh
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (2, '你好, IoTDB!', '你好, IoTDB!', X'e4bda0e5a5bd2c20496f54444221')",

        // important: test cases with characters that are encoded to '+' and '/' in standard Base64
        // Byte array [251, 255, 191] -> Standard Base64: ++/v -> Base64URL: --_v
        "INSERT INTO table1(time, c_blob) VALUES (3, X'fbffbf')",

        // '' -> Base64URL: ''
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (4, '', '', X'')",

        // null values
        "INSERT INTO table1(time, c_int) VALUES (5, 100)",
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

  /** validate the correctness of the to_base64url() function on all supported data types */
  @Test
  public void testToBase64UrlOnAllTypes() {
    String[] expectedHeader =
        new String[] {
          "time", "to_base64url(c_text)", "to_base64url(c_string)", "to_base64url(c_blob)"
        };
    String[] retArray =
        new String[] {
          // 1. 'hello world'
          "1970-01-01T00:00:00.001Z,aGVsbG8gd29ybGQ,aGVsbG8gd29ybGQ,aGVsbG8gd29ybGQ,",
          // 2. '你好, IoTDB!'
          "1970-01-01T00:00:00.002Z,5L2g5aW9LCBJb1REQiE,5L2g5aW9LCBJb1REQiE,5L2g5aW9LCBJb1REQiE,",
          // 3. special characters that are encoded to '+' and '/' in standard Base64
          "1970-01-01T00:00:00.003Z,null,null,-_-_,",
          // 4. empty string
          "1970-01-01T00:00:00.004Z,,,,",
          // null
          "1970-01-01T00:00:00.005Z,null,null,null,"
        };

    tableResultSetEqualTest(
        "SELECT time, to_base64url(c_text) as \"to_base64url(c_text)\", to_base64url(c_string) as \"to_base64url(c_string)\" , to_base64url(c_blob) as \"to_base64url(c_blob)\" FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /**
   * validate that when the to_base64url() function receives invalid parameters (type or number),
   */
  @Test
  public void testToBase64UrlFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_base64url only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // test cases for invalid parameter types
    tableAssertTestFail(
        "SELECT to_base64url(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // test cases for invalid parameter numbers (0)
    tableAssertTestFail("SELECT to_base64url() FROM table1", expectedErrorMessage, DATABASE_NAME);

    // test cases for invalid parameter numbers (>1)
    tableAssertTestFail(
        "SELECT to_base64url(c_text, c_string) FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
