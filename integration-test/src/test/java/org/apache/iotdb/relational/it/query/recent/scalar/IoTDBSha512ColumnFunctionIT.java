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
public class IoTDBSha512ColumnFunctionIT {

  private static final String DATABASE_NAME = "test_sha512_function";
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

  /** Test the sha512() function on TEXT and STRING data types with various inputs. */
  @Test
  public void testSha512OnTextAndString() {
    String[] expectedHeader = new String[] {"time", "sha512(c_text)", "sha512(c_string)"};
    String[] retArray =
        new String[] {
          // sha512('hello') ->
          "1970-01-01T00:00:00.001Z,0x9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043,0x9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043,",
          // sha512('你好') ->
          "1970-01-01T00:00:00.002Z,0x5232181bc0d9888f5c9746e410b4740eb461706ba5dacfbc93587cecfc8d068bac7737e92870d6745b11a25e9cd78b55f4ffc706f73cfcae5345f1b53fb8f6b5,0x5232181bc0d9888f5c9746e410b4740eb461706ba5dacfbc93587cecfc8d068bac7737e92870d6745b11a25e9cd78b55f4ffc706f73cfcae5345f1b53fb8f6b5,",
          // sha512('') ->
          "1970-01-01T00:00:00.003Z,0xcf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e,0xcf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e,",
          // sha512(null) -> null
          "1970-01-01T00:00:00.004Z,null,null,",
          // sha512('Hello, World!') ->
          "1970-01-01T00:00:00.005Z,0x374d794a95cdcfd8b35993185fef9ba368f160d8daf432d08ba9f1ed1e5abe6cc69291e0fa2fe0006a52570ef18c19def4e617c33ce52ef0a6e5fbe318cb0387,0x374d794a95cdcfd8b35993185fef9ba368f160d8daf432d08ba9f1ed1e5abe6cc69291e0fa2fe0006a52570ef18c19def4e617c33ce52ef0a6e5fbe318cb0387,"
        };
    tableResultSetEqualTest(
        "SELECT time, sha512(c_text) as \"sha512(c_text)\", sha512(c_string) as \"sha512(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test the sha512() function on the BLOB data type. */
  @Test
  public void testSha512OnBlob() {
    String[] expectedHeader = new String[] {"time", "sha512(c_blob)"};
    String[] retArray =
        new String[] {
          // sha512(x'74657374') which is 'test' ->
          "1970-01-01T00:00:00.006Z,0xee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff,"
        };
    tableResultSetEqualTest(
        "SELECT time, sha512(c_blob) as \"sha512(c_blob)\" FROM table1 where time = 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test that the sha512() function fails when provided with invalid arguments. */
  @Test
  public void testSha512FunctionOnInvalidInputs() {
    // Construct the expected error message for semantic errors
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sha512 only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // Test with an invalid data type (INT32)
    tableAssertTestFail("SELECT sha512(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with too many arguments
    tableAssertTestFail(
        "SELECT sha512(c_text, 'another_arg') FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT sha512() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
