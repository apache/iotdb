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
public class IoTDBHmacMd5FunctionIT {

  private static final String DATABASE_NAME = "test_hmac_md5_function";

  // Comprehensive data for testing the HMAC_MD5 function
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE HmacTestTable("
            + "data_string STRING, "
            + "data_text TEXT, "
            + "data_blob BLOB, "
            + "key_string STRING, "
            + "key_text TEXT, "
            + "key_blob BLOB, "
            + "int_col INT32"
            + ")",
        // 1. Standard STRING data and STRING key
        "INSERT INTO HmacTestTable(time, data_string, key_string) VALUES(1, 'Hello IoTDB', 'secret_key')",
        // 2. Standard TEXT data and TEXT key
        "INSERT INTO HmacTestTable(time, data_text, key_text) VALUES(2, 'Another test message', 'another_key')",
        // 3. BLOB data
        "INSERT INTO HmacTestTable(time, data_blob) VALUES(3, X'48656C6C6F20496F544442')", // 'Hello
        // IoTDB'
        // 4. Unicode data and key
        "INSERT INTO HmacTestTable(time, data_text, key_text) VALUES(4, '你好世界', '这是一个密钥')",
        // 5. Empty string data (valid case)
        "INSERT INTO HmacTestTable(time, data_string, key_string) VALUES(5, '', 'some_key')",
        // 6. Empty string key (invalid case, for failure testing)
        "INSERT INTO HmacTestTable(time, data_string, key_string) VALUES(6, 'some_data', '')",
        // 7. Null data (valid case)
        "INSERT INTO HmacTestTable(time, data_string, key_string) VALUES(7, null, 'some_key')",
        // 8. Null key (valid case)
        "INSERT INTO HmacTestTable(time, data_string, key_string) VALUES(8, 'some_data', null)",
        // 9. Data for invalid type testing
        "INSERT INTO HmacTestTable(time, int_col, key_string, key_blob) VALUES (9, 123, 'key_for_int', X'deadbeef')",
        "INSERT INTO HmacTestTable(time, data_string, int_col) VALUES (10, 'data_for_int_key', 456)",
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

  /** Test hmac_md5 where inputs are column references. */
  @Test
  public void testHmacMd5WithColumnInputs() {
    // TC-P1: STRING data, STRING key
    String[] expectedHeader1 = {"time", "hmac_md5(data_string, key_string)"};
    String[] retArray1 = {"1970-01-01T00:00:00.001Z,0x39cc932e8ee74450ca31422ea48858c7,"};
    tableResultSetEqualTest(
        "SELECT time, hmac_md5(data_string, key_string) as \"hmac_md5(data_string, key_string)\" FROM HmacTestTable WHERE time = 1",
        expectedHeader1,
        retArray1,
        DATABASE_NAME);

    // TC-P2: TEXT data, TEXT key
    String[] expectedHeader2 = {"time", "hmac_md5(data_text, key_text)"};
    String[] retArray2 = {"1970-01-01T00:00:00.002Z,0x43ddac9f669670c3554f742e4c8b6279,"};
    tableResultSetEqualTest(
        "SELECT time, hmac_md5(data_text, key_text) as \"hmac_md5(data_text, key_text)\" FROM HmacTestTable WHERE time = 2",
        expectedHeader2,
        retArray2,
        DATABASE_NAME);

    // TC-P3: Unicode TEXT data, Unicode TEXT key
    String[] retArray3 = {"1970-01-01T00:00:00.004Z,0xc6b5824c2d9846e89a8f1d340a560df4,"};
    tableResultSetEqualTest(
        "SELECT time, hmac_md5(data_text, key_text) as \"hmac_md5(data_text, key_text)\" FROM HmacTestTable WHERE time = 4",
        expectedHeader2, // Reusing header as the alias format is the same
        retArray3,
        DATABASE_NAME);
  }

  /** Test hmac_md5 where inputs are literals (constants). */
  @Test
  public void testHmacMd5WithLiteralInputs() {
    // TC-L1: STRING literal data, STRING literal key
    String[] expectedHeader1 = {"hmac_md5('Hello IoTDB', 'secret_key')"};
    String[] retArray1 = {"0x39cc932e8ee74450ca31422ea48858c7,"};
    tableResultSetEqualTest(
        "SELECT hmac_md5('Hello IoTDB', 'secret_key') as \"hmac_md5('Hello IoTDB', 'secret_key')\" from HmacTestTable where time = 1",
        expectedHeader1,
        retArray1,
        DATABASE_NAME);

    // TC-L2: BLOB data (from column), STRING literal key
    String[] expectedHeader2 = {"hmac_md5(data_blob, 'secret_key')"};
    String[] retArray2 = {"0x39cc932e8ee74450ca31422ea48858c7,"};
    tableResultSetEqualTest(
        "SELECT hmac_md5(data_blob, 'secret_key') as \"hmac_md5(data_blob, 'secret_key')\" FROM HmacTestTable WHERE time = 3",
        expectedHeader2,
        retArray2,
        DATABASE_NAME);
  }

  /** Test hmac_md5 on edge cases like empty data strings and NULL inputs. */
  @Test
  public void testHmacMd5OnEdgeCases() {
    String[] expectedHeader = {"time", "hmac_md5(data_string, key_string)"};
    String[] retArray = {
      // time=5, data='', key='some_key' -> VALID
      "1970-01-01T00:00:00.005Z,0x3668b7560bcd8a938bdb83de73d6f76b,",
      // time=7, data=null, key='some_key' -> NULL
      "1970-01-01T00:00:00.007Z,null,",
      // time=8, data='some_data', key=null -> NULL
      "1970-01-01T00:00:00.008Z,null,",
    };
    tableResultSetEqualTest(
        "SELECT time, hmac_md5(data_string, key_string) as \"hmac_md5(data_string, key_string)\" FROM HmacTestTable WHERE time IN (5, 7, 8) ORDER BY time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /**
   * Verify error handling for invalid inputs, such as wrong argument count, incorrect data types,
   * or an empty string key.
   */
  @Test
  public void testHmacMd5OnInvalidInputs() {
    String generalErrorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function hmac_md5 only accepts two arguments, first argument must be TEXT, STRING, or BlOB type, second argument must be STRING OR TEXT type.";

    String emptyKeyErrorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'hmac_md5' due to an invalid input format. the value 'some_data' corresponding to a empty key, the empty key is not allowed in HMAC operation.";

    String emptyLiteralKeyErrorMsg =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function hmac_md5 due to an invalid input format, the empty key is not allowed in HMAC operation";

    // Case 1: Wrong argument count
    tableAssertTestFail("SELECT hmac_md5() from HmacTestTable ", generalErrorMsg, DATABASE_NAME);
    tableAssertTestFail(
        "SELECT hmac_md5(data_string) from HmacTestTable", generalErrorMsg, DATABASE_NAME);
    tableAssertTestFail(
        "SELECT hmac_md5(data_string, key_string, 'extra') from HmacTestTable",
        generalErrorMsg,
        DATABASE_NAME);

    // Case 2: Invalid data types
    tableAssertTestFail(
        "SELECT hmac_md5(int_col, key_string) FROM HmacTestTable WHERE time = 9",
        generalErrorMsg,
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT hmac_md5(data_string, int_col) FROM HmacTestTable WHERE time = 10",
        generalErrorMsg,
        DATABASE_NAME);

    // Case 3: CRITICAL - Empty string key is invalid
    tableAssertTestFail(
        "SELECT hmac_md5(data_string, key_string) FROM HmacTestTable WHERE time = 6",
        emptyKeyErrorMsg,
        DATABASE_NAME);

    // Also test with an empty literal key
    tableAssertTestFail(
        "SELECT hmac_md5('some_data', '') FROM HmacTestTable",
        emptyLiteralKeyErrorMsg,
        DATABASE_NAME);
  }
}
