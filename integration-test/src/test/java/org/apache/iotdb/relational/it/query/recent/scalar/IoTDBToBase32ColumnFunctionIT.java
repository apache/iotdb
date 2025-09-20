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
public class IoTDBToBase32ColumnFunctionIT {

  private static final String DATABASE_NAME = "test_to_base32_function";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT, c_string STRING, c_blob BLOB, c_int INT32)",

        // Case 1: Basic ASCII string for TEXT, STRING and equivalent BLOB
        // 'Hello, IoTDB!' -> Hex: 0x48656c6c6f2c20496f54444221
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (1, 'Hello, IoTDB!', 'Hello, IoTDB!', X'48656c6c6f2c20496f54444221')",

        // Case 2: UTF-8 string for TEXT, STRING and equivalent BLOB
        // '你好,世界' -> Hex: 0xe4bda0e5a5bd2ce4b896e7958c
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (2, '你好,世界', '你好,世界', X'e4bda0e5a5bd2ce4b896e7958c')",

        // Case 3: Empty string and empty BLOB
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (3, '', '', X'')",

        // Case 4: Null values for all supported types
        "INSERT INTO table1(time, c_int) VALUES (4, 100)",

        // Case 5: BLOB padding scenarios for 1-5 bytes
        // 'f' (0x66) -> Base32: MY======
        "INSERT INTO table1(time, c_blob) VALUES (5, X'66')",
        // 'fo' (0x666f) -> Base32: MZXQ====
        "INSERT INTO table1(time, c_blob) VALUES (6, X'666f')",
        // 'foo' (0x666f6f) -> Base32: MZXW6===
        "INSERT INTO table1(time, c_blob) VALUES (7, X'666f6f')",
        // 'foob' (0x666f6f62) -> Base32: MZXW6YQ=
        "INSERT INTO table1(time, c_blob) VALUES (8, X'666f6f62')",
        // 'fooba' (0x666f6f6261) -> Base32: MZXW6YTB
        "INSERT INTO table1(time, c_blob) VALUES (9, X'666f6f6261')",
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

  /** Validates the to_base32() function on various supported and valid inputs. */
  @Test
  public void testToBase32OnValidInputs() {
    String[] expectedHeader =
        new String[] {
          "time", "to_base32(c_text)", "to_base32(c_string)", "to_base32(c_blob)",
        };
    String[] retArray =
        new String[] {
          // 1. Basic ASCII
          "1970-01-01T00:00:00.001Z,JBSWY3DPFQQES32UIRBCC===,JBSWY3DPFQQES32UIRBCC===,JBSWY3DPFQQES32UIRBCC===,",
          // 2. UTF-8
          "1970-01-01T00:00:00.002Z,4S62BZNFXUWOJOEW46KYY===,4S62BZNFXUWOJOEW46KYY===,4S62BZNFXUWOJOEW46KYY===,",
          // 3. Empty string/blob
          "1970-01-01T00:00:00.003Z,,,,",
          // 4. Null input
          "1970-01-01T00:00:00.004Z,null,null,null,",
          // 5. 1 byte with padding
          "1970-01-01T00:00:00.005Z,null,null,MY======,",
          // 6. 2 bytes with padding
          "1970-01-01T00:00:00.006Z,null,null,MZXQ====,",
          // 7. 3 bytes with padding
          "1970-01-01T00:00:00.007Z,null,null,MZXW6===,",
          // 8. 4 bytes with padding
          "1970-01-01T00:00:00.008Z,null,null,MZXW6YQ=,",
          // 9. 5 bytes with padding
          "1970-01-01T00:00:00.009Z,null,null,MZXW6YTB,",
        };

    tableResultSetEqualTest(
        "SELECT time, to_base32(c_text) as \"to_base32(c_text)\", to_base32(c_string) as \"to_base32(c_string)\", to_base32(c_blob) as \"to_base32(c_blob)\" FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** Tests for invalid arguments passed to the to_base32() function. */
  @Test
  public void testToBase32FunctionOnInvalidArguments() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_base32 only accepts one argument and it must be TEXT, STRING, or BLOB data type.";

    // Test with invalid parameter type (INT32)
    tableAssertTestFail("SELECT to_base32(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with invalid parameter count (0)
    tableAssertTestFail("SELECT to_base32() FROM table1", expectedErrorMessage, DATABASE_NAME);

    // Test with invalid parameter count (>1)
    tableAssertTestFail(
        "SELECT to_base32(c_text, c_blob) FROM table1", expectedErrorMessage, DATABASE_NAME);
  }
}
