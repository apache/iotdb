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
public class IoTDBFromBase64ColumnFunctionIT {

  private static final String DATABASE_NAME = "test_frombase64_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_int INT32 FIELD)",

        // 'hello' base64: aGVsbG8=
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'aGVsbG8=', 'aGVsbG8=')",
        // '你好' base64: 5L2g5aW9
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '5L2g5aW9', '5L2g5aW9')",
        // '' base64: ''
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        // invalid base64
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'not_base64', 'not_base64')"
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

  /** validate the normal decoding of from_base64() for TEXT/STRING type */
  @Test
  public void testFromBase64OnTextString() {
    String[] expectedHeader = new String[] {"time", "from_base64(c_text)", "from_base64(c_string)"};
    String[] retArray =
        new String[] {
          // 'hello', 'hello'
          "1970-01-01T00:00:00.001Z,0x68656c6c6f,0x68656c6c6f,",
          // '你好', '你好'
          "1970-01-01T00:00:00.002Z,0xe4bda0e5a5bd,0xe4bda0e5a5bd,",
          // '', ''
          "1970-01-01T00:00:00.003Z,0x,0x,",
          // null, null
          "1970-01-01T00:00:00.004Z,null,null,"
        };
    tableResultSetEqualTest(
        "SELECT time, from_base64(c_text) as \"from_base64(c_text)\", from_base64(c_string) as \"from_base64(c_string)\" FROM table1 where time < 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** invalid base64 string should throw exception */
  @Test
  public void testFromBase64OnInvalidBase64() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_base64' due to an invalid input format. Problematic value: not_base64";
    tableAssertTestFail(
        "SELECT from_base64(c_text) FROM table1 WHERE time = 5",
        expectedErrorMessage,
        DATABASE_NAME);
  }

  /** invalid input type or number of arguments should be rejected */
  @Test
  public void testFromBase64FunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_base64 only accepts one argument and it must be TEXT or STRING data type.";

    tableAssertTestFail(
        "SELECT from_base64(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64(c_text, 1) FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
