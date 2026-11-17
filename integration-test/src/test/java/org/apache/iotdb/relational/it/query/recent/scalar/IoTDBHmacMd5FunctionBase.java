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

import org.apache.iotdb.relational.it.query.old.builtinfunction.scalar.IoTDBReplaceFunctionTableBase;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

public abstract class IoTDBHmacMd5FunctionBase extends IoTDBReplaceFunctionTableBase {

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

  protected static void insertData() {
    prepareTableData(createSqls);
  }

  private static void assertHmacColumnInputs(
      String functionName, String stringHash, String textHash, String unicodeHash) {
    String[] stringHeader = {"time", functionName + "(data_string, key_string)"};
    tableResultSetEqualTest(
        "SELECT time, "
            + functionName
            + "(data_string, key_string) as \""
            + functionName
            + "(data_string, key_string)\" FROM HmacTestTable WHERE time = 1",
        stringHeader,
        new String[] {"1970-01-01T00:00:00.001Z," + stringHash + ","},
        DATABASE_NAME);

    String[] textHeader = {"time", functionName + "(data_text, key_text)"};
    tableResultSetEqualTest(
        "SELECT time, "
            + functionName
            + "(data_text, key_text) as \""
            + functionName
            + "(data_text, key_text)\" FROM HmacTestTable WHERE time = 2",
        textHeader,
        new String[] {"1970-01-01T00:00:00.002Z," + textHash + ","},
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, "
            + functionName
            + "(data_text, key_text) as \""
            + functionName
            + "(data_text, key_text)\" FROM HmacTestTable WHERE time = 4",
        textHeader,
        new String[] {"1970-01-01T00:00:00.004Z," + unicodeHash + ","},
        DATABASE_NAME);
  }

  private static void assertHmacLiteralInputs(String functionName, String expectedHash) {
    tableResultSetEqualTest(
        "SELECT "
            + functionName
            + "('Hello IoTDB', 'secret_key') as \""
            + functionName
            + "('Hello IoTDB', 'secret_key')\" from HmacTestTable where time = 1",
        new String[] {functionName + "('Hello IoTDB', 'secret_key')"},
        new String[] {expectedHash + ","},
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT "
            + functionName
            + "(data_blob, 'secret_key') as \""
            + functionName
            + "(data_blob, 'secret_key')\" FROM HmacTestTable WHERE time = 3",
        new String[] {functionName + "(data_blob, 'secret_key')"},
        new String[] {expectedHash + ","},
        DATABASE_NAME);
  }

  private static void assertHmacEdgeCases(String functionName, String emptyDataHash) {
    tableResultSetEqualTest(
        "SELECT time, "
            + functionName
            + "(data_string, key_string) as \""
            + functionName
            + "(data_string, key_string)\" FROM HmacTestTable WHERE time IN (5, 7, 8) ORDER BY time",
        new String[] {"time", functionName + "(data_string, key_string)"},
        new String[] {
          "1970-01-01T00:00:00.005Z," + emptyDataHash + ",",
          "1970-01-01T00:00:00.007Z,null,",
          "1970-01-01T00:00:00.008Z,null,"
        },
        DATABASE_NAME);
  }

  private static void assertHmacInvalidInputs(String functionName) {
    String generalErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function "
            + functionName
            + " only accepts two arguments, first argument must be TEXT, STRING, or BlOB type, second argument must be STRING OR TEXT type.";
    String emptyKeyErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function '"
            + functionName
            + "' due to an invalid input format. the value 'some_data' corresponding to a empty key, the empty key is not allowed in HMAC operation.";
    String emptyLiteralKeyErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function "
            + functionName
            + " due to an invalid input format, the empty key is not allowed in HMAC operation";

    tableAssertTestFail(
        "SELECT " + functionName + "() from HmacTestTable ", generalErrorMessage, DATABASE_NAME);
    tableAssertTestFail(
        "SELECT " + functionName + "(data_string) from HmacTestTable",
        generalErrorMessage,
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT " + functionName + "(data_string, key_string, 'extra') from HmacTestTable",
        generalErrorMessage,
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT " + functionName + "(int_col, key_string) FROM HmacTestTable WHERE time = 9",
        generalErrorMessage,
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT " + functionName + "(data_string, int_col) FROM HmacTestTable WHERE time = 10",
        generalErrorMessage,
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT " + functionName + "(data_string, key_string) FROM HmacTestTable WHERE time = 6",
        emptyKeyErrorMessage,
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT " + functionName + "('some_data', '') FROM HmacTestTable",
        emptyLiteralKeyErrorMessage,
        DATABASE_NAME);
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

  // Verifies SHA-1 HMAC for STRING, TEXT, and Unicode column inputs.
  @Test
  public void testHmacSha1WithColumnInputs() {
    assertHmacColumnInputs(
        "hmac_sha1",
        "0xd307d46d57a553db1b59c28d2d683a155a16eca3",
        "0x2cdd76b9e5784223528c892bc28ffb4c3175438e",
        "0x0660d7047fa2e1a09d3b285e308439e285d2fd3d");
  }

  // Verifies SHA-1 HMAC for a literal and a BLOB input using the literal key.
  @Test
  public void testHmacSha1WithLiteralInputs() {
    assertHmacLiteralInputs("hmac_sha1", "0xd307d46d57a553db1b59c28d2d683a155a16eca3");
  }

  // Verifies SHA-1 HMAC handles empty data and NULL data or keys as defined.
  @Test
  public void testHmacSha1OnEdgeCases() {
    assertHmacEdgeCases("hmac_sha1", "0xe2b59121f73f8695441d7862f191bd58602d797a");
  }

  // Rejects wrong SHA-1 HMAC arity/types and empty keys with semantic errors.
  @Test
  public void testHmacSha1OnInvalidInputs() {
    assertHmacInvalidInputs("hmac_sha1");
  }

  // Verifies SHA-256 HMAC for STRING, TEXT, and Unicode column inputs.
  @Test
  public void testHmacSha256WithColumnInputs() {
    assertHmacColumnInputs(
        "hmac_sha256",
        "0x28c30efa3fd5c6a086dfb3d69a2c993a4febdb98143ed6fa337c80d74616b552",
        "0xbcd034e2fcd3c2817d22aead831192c2b3510a4ddb8ff426f1b046019668fc0c",
        "0x97bcb4378afbbe9b7aafe895152d9a2e7e29f32a294aea397f691d2990d81702");
  }

  // Verifies SHA-256 HMAC for a literal and a BLOB input using the literal key.
  @Test
  public void testHmacSha256WithLiteralInputs() {
    assertHmacLiteralInputs(
        "hmac_sha256", "0x28c30efa3fd5c6a086dfb3d69a2c993a4febdb98143ed6fa337c80d74616b552");
  }

  // Verifies SHA-256 HMAC handles empty data and NULL data or keys as defined.
  @Test
  public void testHmacSha256OnEdgeCases() {
    assertHmacEdgeCases(
        "hmac_sha256", "0xf0c70ec541a25501f5bc61da93d421490cc1278fb9c97f00d704b16e1501c71f");
  }

  // Rejects wrong SHA-256 HMAC arity/types and empty keys with semantic errors.
  @Test
  public void testHmacSha256OnInvalidInputs() {
    assertHmacInvalidInputs("hmac_sha256");
  }

  // Verifies SHA-512 HMAC for STRING, TEXT, and Unicode column inputs.
  @Test
  public void testHmacSha512WithColumnInputs() {
    assertHmacColumnInputs(
        "hmac_sha512",
        "0xc8cf98e50eb168d0a5de5daeb3020b1bc63b1602884f1291b4a6ee42bee4a52c83bb423e8d024e501c5c285af5925056b9f3d1a07546b54755b31e03295ffd04",
        "0x10f2cc84ee40f95a91711b6ed2f1c7c3c7461c26ea29c80b2727ada19f3a77593c1e8b6381416a37ef912be44ac608693ec4005b3b1f89bef455c1c597ed886c",
        "0x99e703ffa1c4cc600b72bd1bbe0a53e2d8012d06e22cbb5cb2de4bba65354e1f8a4be34c49509677098953a9b6cb5bc4b56eff5e9093f88e5247315f32e40d77");
  }

  // Verifies SHA-512 HMAC for a literal and a BLOB input using the literal key.
  @Test
  public void testHmacSha512WithLiteralInputs() {
    assertHmacLiteralInputs(
        "hmac_sha512",
        "0xc8cf98e50eb168d0a5de5daeb3020b1bc63b1602884f1291b4a6ee42bee4a52c83bb423e8d024e501c5c285af5925056b9f3d1a07546b54755b31e03295ffd04");
  }

  // Verifies SHA-512 HMAC handles empty data and NULL data or keys as defined.
  @Test
  public void testHmacSha512OnEdgeCases() {
    assertHmacEdgeCases(
        "hmac_sha512",
        "0xbde90d601f55fc6ddb6faa70f6f20af1a639fa017f4e97db303bd7e70cc6d82797d98b3e38306667e3d00b4909eb3deb135185db6ac2fd5c005914729e172bf4");
  }

  // Rejects wrong SHA-512 HMAC arity/types and empty keys with semantic errors.
  @Test
  public void testHmacSha512OnInvalidInputs() {
    assertHmacInvalidInputs("hmac_sha512");
  }
}
