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

package org.apache.iotdb.relational.it.query.recent;

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
public class IoTDBLengthFunctionIT {

  private static final String DATABASE_NAME = "test_length_function";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (1, 'hello', 'hello', X'68656C6C6F')",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (2, '你好', '你好', X'e4bda0e5a5bd')",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (3, '', '', X'')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)"
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

  /** validate LENGTH() for TEXT and STRING types, correctly calculate the character count */
  @Test
  public void testLengthOnTextAndString() {
    String[] expectedHeader = new String[] {"time", "length(c_text)", "length(c_string)"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,5,5,",
          "1970-01-01T00:00:00.002Z,2,2,",
          "1970-01-01T00:00:00.003Z,0,0,",
          "1970-01-01T00:00:00.004Z,null,null,"
        };

    tableResultSetEqualTest(
        "SELECT time, length(c_text) as \"length(c_text)\", length(c_string) as \"length(c_string)\" FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** validate LENGTH() for BLOB type, correctly calculate the number of bytes */
  @Test
  public void testLengthOnBlob() {
    String[] expectedHeader = new String[] {"time", "length(c_blob)"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,5,",
          "1970-01-01T00:00:00.002Z,6,",
          "1970-01-01T00:00:00.003Z,0,",
          "1970-01-01T00:00:00.004Z,null,"
        };

    tableResultSetEqualTest(
        "SELECT time, length(c_blob) as \"length(c_blob)\" FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /**
   * Test the error handling behavior of the LENGTH() function when it receives invalid parameters.
   */
  @Test
  public void testLengthFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string or blob or object data type.";

    // Exception 1: Using LENGTH() on non-TEXT/BLOB/STRING types
    tableAssertTestFail("SELECT length(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Exception 2: Incorrect number of arguments passed to the LENGTH() function
    tableAssertTestFail(
        "SELECT length(c_text, 1) FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
