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
public class IoTDBSha256ColumnFunctionIT {

  private static final String DATABASE_NAME = "test_sha256_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",

        // Test with a simple ASCII string
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        // Test with Chinese characters (UTF-8)
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '你好', '你好')",
        // Test with an empty string
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        // Test with null values, which should result in a null hash
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        // Test with a string containing special characters and spaces
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'Hello, World!', 'Hello, World!')",
        // Test with blob data (hex representation of 'test')
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

  /** Test the sha256() function on TEXT and STRING data types with various inputs. */
  @Test
  public void testSha256OnTextAndString() {
    String[] expectedHeader = new String[] {"time", "sha256(c_text)", "sha256(c_string)"};
    String[] retArray =
        new String[] {
          // sha256('hello') -> 0x2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
          "1970-01-01T00:00:00.001Z,0x2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824,0x2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824,",
          // sha256('你好') -> 0x670d9743542cae3ea7ebe36af56bd53648b0a1126162e78d81a32934a711302e
          "1970-01-01T00:00:00.002Z,0x670d9743542cae3ea7ebe36af56bd53648b0a1126162e78d81a32934a711302e,0x670d9743542cae3ea7ebe36af56bd53648b0a1126162e78d81a32934a711302e,",
          // sha256('') -> 0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
          "1970-01-01T00:00:00.003Z,0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855,0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855,",
          // sha256(null) -> null
          "1970-01-01T00:00:00.004Z,null,null,",
          // sha256('Hello, World!') ->
          // 0xdffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f
          "1970-01-01T00:00:00.005Z,0xdffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f,0xdffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f,"
        };
    tableResultSetEqualTest(
        "SELECT time, sha256(c_text) as \"sha256(c_text)\", sha256(c_string) as \"sha256(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test the sha256() function on the BLOB data type. */
  @Test
  public void testSha256OnBlob() {
    String[] expectedHeader = new String[] {"time", "sha256(c_blob)"};
    String[] retArray =
        new String[] {
          // sha256(x'74657374') which is 'test' ->
          // 0x9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
          "1970-01-01T00:00:00.006Z,0x9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08,"
        };
    tableResultSetEqualTest(
        "SELECT time, sha256(c_blob) as \"sha256(c_blob)\" FROM table1 where time = 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test that the sha256() function fails when provided with invalid arguments. */
  @Test
  public void testSha256FunctionOnInvalidInputs() {
    // Construct the expected error message for semantic errors
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sha256 only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // Test with an invalid data type (INT32)
    tableAssertTestFail("SELECT sha256(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with too many arguments
    tableAssertTestFail(
        "SELECT sha256(c_text, 'another_arg') FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT sha256() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
