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
public class IoTDBMd5ColumnFunctionIT {

  private static final String DATABASE_NAME = "test_md5_function";
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

  /** Test the md5() function on TEXT and STRING data types with various inputs. */
  @Test
  public void testMd5OnTextAndString() {
    String[] expectedHeader = new String[] {"time", "md5(c_text)", "md5(c_string)"};
    String[] retArray =
        new String[] {
          // md5('hello')
          "1970-01-01T00:00:00.001Z,0x5d41402abc4b2a76b9719d911017c592,0x5d41402abc4b2a76b9719d911017c592,",
          // md5('你好')
          "1970-01-01T00:00:00.002Z,0x7eca689f0d3389d9dea66ae112e5cfd7,0x7eca689f0d3389d9dea66ae112e5cfd7,",
          // md5('')
          "1970-01-01T00:00:00.003Z,0xd41d8cd98f00b204e9800998ecf8427e,0xd41d8cd98f00b204e9800998ecf8427e,",
          // md5(null)
          "1970-01-01T00:00:00.004Z,null,null,",
          // md5('Hello, World!')
          "1970-01-01T00:00:00.005Z,0x65a8e27d8879283831b664bd8b7f0ad4,0x65a8e27d8879283831b664bd8b7f0ad4,"
        };
    tableResultSetEqualTest(
        "SELECT time, md5(c_text) as \"md5(c_text)\", md5(c_string) as \"md5(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test the md5() function on the BLOB data type. */
  @Test
  public void testMd5OnBlob() {
    String[] expectedHeader = new String[] {"time", "md5(c_blob)"};
    String[] retArray =
        new String[] {
          // md5(x'74657374')
          "1970-01-01T00:00:00.006Z,0x098f6bcd4621d373cade4e832627b4f6,"
        };
    tableResultSetEqualTest(
        "SELECT time, md5(c_blob) as \"md5(c_blob)\" FROM table1 where time = 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test that the md5() function fails when provided with invalid arguments. */
  @Test
  public void testMd5FunctionOnInvalidInputs() {
    // Construct the expected error message for semantic errors
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function md5 only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // Test with an invalid data type (INT32)
    tableAssertTestFail("SELECT md5(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with too many arguments
    tableAssertTestFail(
        "SELECT md5(c_text, 'another_arg') FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT md5() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
