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
public class IoTDBMurmur3ColumnFunctionIT {

  private static final String DATABASE_NAME = "test_murmur3_function";
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

  /** Test the murmur3() function on TEXT and STRING data types with various inputs. */
  @Test
  public void testMurmur3OnTextAndString() {
    String[] expectedHeader = new String[] {"time", "murmur3(c_text)", "murmur3(c_string)"};
    String[] retArray =
        new String[] {
          // murmur3('hello') -> 5852166b
          "1970-01-01T00:00:00.001Z,0x029bbd41b3a7d8cb191dae486a901e5b,0x029bbd41b3a7d8cb191dae486a901e5b,",
          // murmur3('你好') -> 323a9688
          "1970-01-01T00:00:00.002Z,0x2089985d4eb4023c3022351dc70b520c,0x2089985d4eb4023c3022351dc70b520c,",
          // murmur3('') -> 00000000
          "1970-01-01T00:00:00.003Z,0x00000000000000000000000000000000,0x00000000000000000000000000000000,",
          // murmur3(null) -> null
          "1970-01-01T00:00:00.004Z,null,null,",
          // murmur3('Hello, World!') -> 06717d9f
          "1970-01-01T00:00:00.005Z,0xc0a1b86f7365bc93bfae71678c2843aa,0xc0a1b86f7365bc93bfae71678c2843aa,"
        };
    tableResultSetEqualTest(
        "SELECT time, murmur3(c_text) as \"murmur3(c_text)\", murmur3(c_string) as \"murmur3(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test the murmur3() function on the BLOB data type. */
  @Test
  public void testMurmur3OnBlob() {
    String[] expectedHeader = new String[] {"time", "murmur3(c_blob)"};
    String[] retArray =
        new String[] {
          // murmur3(x'74657374')
          "1970-01-01T00:00:00.006Z,0x9de1bd74cc287dac824dbdf93182129a,"
        };
    tableResultSetEqualTest(
        "SELECT time, murmur3(c_blob) as \"murmur3(c_blob)\" FROM table1 where time = 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test that the murmur3() function fails when provided with invalid arguments. */
  @Test
  public void testMurmur3FunctionOnInvalidInputs() {
    // Construct the expected error message for semantic errors
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function murmur3 only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // Test with an invalid data type (INT32)
    tableAssertTestFail("SELECT murmur3(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with too many arguments
    tableAssertTestFail(
        "SELECT murmur3(c_text, 'another_arg') FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT murmur3() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
