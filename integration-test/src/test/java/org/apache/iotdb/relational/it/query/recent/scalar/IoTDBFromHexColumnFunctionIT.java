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
public class IoTDBFromHexColumnFunctionIT {

  private static final String DATABASE_NAME = "test_fromhex_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_int INT32 FIELD)",

        // 'hello' hex: 68656c6c6f
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, '68656c6c6f', '68656c6c6f')",
        // '你好' hex: e4bda0e5a5bd
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, 'e4bda0e5a5bd', 'e4bda0e5a5bd')",
        // '' hex: ''
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        // for null test
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        // invalid hex string (non-hex characters)
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'not_hex', 'not_hex')",
        // invalid hex string (odd length)
        "INSERT INTO table1(time, c_text, c_string) VALUES (6, '123', '123')"
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

  /** validate the normal decoding of from_hex() for TEXT/STRING type */
  @Test
  public void testFromHexOnTextString() {
    String[] expectedHeader = new String[] {"time", "from_hex(c_text)", "from_hex(c_string)"};
    String[] retArray =
        new String[] {
          // 'hello'
          "1970-01-01T00:00:00.001Z,0x68656c6c6f,0x68656c6c6f,",
          // '你好'
          "1970-01-01T00:00:00.002Z,0xe4bda0e5a5bd,0xe4bda0e5a5bd,",
          // ''
          "1970-01-01T00:00:00.003Z,0x,0x,",
          // null
          "1970-01-01T00:00:00.004Z,null,null,"
        };
    tableResultSetEqualTest(
        "SELECT time, from_hex(c_text) as \"from_hex(c_text)\", from_hex(c_string) as \"from_hex(c_string)\" FROM table1 where time < 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** invalid hex string (with non-hex characters) should throw exception */
  @Test
  public void testFromHexOnInvalidHexChars() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_hex' due to an invalid input format. Problematic value: not_hex";
    tableAssertTestFail(
        "SELECT from_hex(c_text) FROM table1 WHERE time = 5", expectedErrorMessage, DATABASE_NAME);
  }

  /** invalid hex string (with odd length) should throw exception */
  @Test
  public void testFromHexOnInvalidHexLength() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_hex' due to an invalid input format. Problematic value: 123";
    tableAssertTestFail(
        "SELECT from_hex(c_text) FROM table1 WHERE time = 6", expectedErrorMessage, DATABASE_NAME);
  }

  /** invalid input type or number of arguments should be rejected */
  @Test
  public void testFromHexFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_hex only accepts one argument and it must be TEXT or STRING data type.";

    // Test with invalid data type
    tableAssertTestFail("SELECT from_hex(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with multiple arguments
    tableAssertTestFail(
        "SELECT from_hex(c_text, 1) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT from_hex() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
