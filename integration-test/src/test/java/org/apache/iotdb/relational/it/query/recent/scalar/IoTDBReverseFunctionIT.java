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
public class IoTDBReverseFunctionIT {

  private static final String DATABASE_NAME = "test_reverse_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",

        // 1. Test with a simple ASCII string
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        // 2. Test with multi-byte UTF-8 characters and mixed content
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '你好, world', '你好, world')",
        // 3. Test with an empty string
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        // 4. Test with null values, which should result in a null output
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        // 5. Test with blob data for byte-wise reversal
        "INSERT INTO table1(time, c_blob) VALUES (5, x'01020304')",
        // 6. Test with an empty blob
        "INSERT INTO table1(time, c_blob) VALUES (6, x'')",
        // 7. Test with a more complex blob
        "INSERT INTO table1(time, c_blob) VALUES (7, x'AABBCCDD')"
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

  /** Test the REVERSE() function on TEXT and STRING data types for character-wise reversal. */
  @Test
  public void testReverseOnTextAndString() {
    String[] expectedHeader = new String[] {"time", "reverse(c_text)", "reverse(c_string)"};
    String[] retArray =
        new String[] {
          // REVERSE('hello') -> 'olleh'
          "1970-01-01T00:00:00.001Z,olleh,olleh,",
          // REVERSE('你好, world') -> 'dlrow ,好你'
          "1970-01-01T00:00:00.002Z,dlrow ,好你,dlrow ,好你,",
          // REVERSE('') -> ''
          "1970-01-01T00:00:00.003Z,,,",
          // REVERSE(null) -> null
          "1970-01-01T00:00:00.004Z,null,null,"
        };
    tableResultSetEqualTest(
        "SELECT time, REVERSE(c_text) as \"reverse(c_text)\", REVERSE(c_string) as \"reverse(c_string)\" FROM table1 where time < 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test the REVERSE() function on the BLOB data type for byte-wise reversal. */
  @Test
  public void testReverseOnBlob() {
    String[] expectedHeader = new String[] {"time", "reverse(c_blob)"};
    String[] retArray =
        new String[] {
          // REVERSE(x'01020304') -> 0x04030201
          "1970-01-01T00:00:00.005Z,0x04030201,",
          // REVERSE(x'') -> 0x
          "1970-01-01T00:00:00.006Z,0x,",
          // REVERSE(x'AABBCCDD') -> 0xDDCCBBAA
          "1970-01-01T00:00:00.007Z,0xddccbbaa,"
        };
    tableResultSetEqualTest(
        "SELECT time, REVERSE(c_blob) as \"reverse(c_blob)\" FROM table1 where time >= 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Test that the REVERSE() function fails when provided with invalid arguments. */
  @Test
  public void testReverseFunctionOnInvalidInputs() {
    // Construct the expected error message for semantic errors
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function reverse only accepts one argument and it must be TEXT, STRING, or BlOB data type.";

    // Test with an invalid data type (INT32)
    tableAssertTestFail("SELECT REVERSE(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with too many arguments
    tableAssertTestFail(
        "SELECT REVERSE(c_text, 'another_arg') FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with no arguments
    tableAssertTestFail("SELECT REVERSE() FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
