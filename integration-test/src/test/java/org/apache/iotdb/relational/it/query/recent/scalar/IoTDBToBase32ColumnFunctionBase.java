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

import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

public abstract class IoTDBToBase32ColumnFunctionBase extends IoTDBToLittleEndian32FunctionBase {

  private static final String DATABASE_NAME = "test_to_base32_function";
  private static final String TO_BASE64_DATABASE_NAME = "test_tobase64_function";
  private static final String TO_BASE64URL_DATABASE_NAME = "test_tobase64url_function";
  private static final String TO_HEX_DATABASE_NAME = "test_tohex_function";
  private static final String FROM_BASE32_DATABASE_NAME = "test_from_base32_function";
  private static final String FROM_BASE64_DATABASE_NAME = "test_frombase64_function";
  private static final String FROM_BASE64URL_DATABASE_NAME = "test_frombase64url_function_v2";
  private static final String FROM_HEX_DATABASE_NAME = "test_fromhex_function";

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

  private static final String[] toBase64CreateSqls =
      new String[] {
        "CREATE DATABASE " + TO_BASE64_DATABASE_NAME,
        "USE " + TO_BASE64_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '你好', '你好')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'Hello, World!', 'Hello, World!')",
        "INSERT INTO table1(time, c_blob) VALUES (6, x'74657374')"
      };

  private static final String[] toBase64UrlCreateSqls =
      new String[] {
        "CREATE DATABASE " + TO_BASE64URL_DATABASE_NAME,
        "USE " + TO_BASE64URL_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT, c_string STRING, c_blob BLOB, c_int INT32)",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (1, 'hello world', 'hello world', X'68656c6c6f20776f726c64')",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (2, '你好, IoTDB!', '你好, IoTDB!', X'e4bda0e5a5bd2c20496f54444221')",
        "INSERT INTO table1(time, c_blob) VALUES (3, X'fbffbf')",
        "INSERT INTO table1(time, c_text, c_string, c_blob) VALUES (4, '', '', X'')",
        "INSERT INTO table1(time, c_int) VALUES (5, 100)"
      };

  private static final String[] toHexCreateSqls =
      new String[] {
        "CREATE DATABASE " + TO_HEX_DATABASE_NAME,
        "USE " + TO_HEX_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '你好', '你好')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'Hello, World!', 'Hello, World!')",
        "INSERT INTO table1(time, c_blob) VALUES (6, x'74657374')"
      };

  private static final String[] fromBase32CreateSqls =
      new String[] {
        "CREATE DATABASE " + FROM_BASE32_DATABASE_NAME,
        "USE " + FROM_BASE32_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT, c_string STRING, c_int INT32)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'JBCGC3LFN5ZGIIDUNBSSA3LSEI======', 'JBCGC3LFN5ZGIIDUNBSSA3LSEI======')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '2W4V625443J56W4S4E73H5BUMM4A======', '2W4V625443J56W4S4E73H5BUMM4A======')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (4, 100)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'MY======', 'MY======')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (6, 'MZXQ====', 'MZXQ====')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (7, 'MZXW6===', 'MZXW6===')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (8, 'MZXW6YQ=', 'MZXW6YQ=')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (9, 'MZXW6YQB', 'MZXW6YQB')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (10, 'MZXW6', 'MZXW6')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (11, 'mzxw6', 'mzxw6')"
      };

  private static final String[] fromBase64CreateSqls =
      new String[] {
        "CREATE DATABASE " + FROM_BASE64_DATABASE_NAME,
        "USE " + FROM_BASE64_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_int INT32 FIELD)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'aGVsbG8=', 'aGVsbG8=')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '5L2g5aW9', '5L2g5aW9')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'not_base64', 'not_base64')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (6, 'abc', 'abc')"
      };

  private static final String[] fromBase64UrlCreateSqls =
      new String[] {
        "CREATE DATABASE " + FROM_BASE64URL_DATABASE_NAME,
        "USE " + FROM_BASE64URL_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT, c_string STRING, c_int INT32)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'SW9UREIgaXMgZnVuIQ', 'SW9UREIgaXMgZnVuIQ')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '5L2g5aW9LCBll-OCll-OCoSE', '5L2g5aW9LCBll-OCll-OCoSE')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '-_-_', '-_-_')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (4, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (5, 100)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (6, 'YQ', 'YQ')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (7, 'YWI', 'YWI')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (8, 'YWJj', 'YWJj')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (9, 'QXBhY2hlIElvVERCIGlzIGFuIElvVCBuYXRpdmUgZGF0YWJhc2Ugd2l0aCBoaWdoIHBlcmZvcm1hbmNlIGZvciBkYXRhIG1hbmFnZW1lbnQgYW5kIGFuYWx5c2lzLg', 'QXBhY2hlIElvVERCIGlzIGFuIElvVCBuYXRpdmUgZGF0YWJhc2Ugd2l0aCBoaWdoIHBlcmZvcm1hbmNlIGZvciBkYXRhIG1hbmFnZW1lbnQgYW5kIGFuYWx5c2lzLg')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (10, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_', 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_')"
      };

  private static final String[] fromHexCreateSqls =
      new String[] {
        "CREATE DATABASE " + FROM_HEX_DATABASE_NAME,
        "USE " + FROM_HEX_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_int INT32 FIELD)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, '68656c6c6f', '68656c6c6f')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, 'e4bda0e5a5bd', 'e4bda0e5a5bd')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'not_hex', 'not_hex')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (6, '123', '123')"
      };

  protected static void insertData() {
    prepareTableData(createSqls);
    prepareTableData(toBase64CreateSqls);
    prepareTableData(toBase64UrlCreateSqls);
    prepareTableData(toHexCreateSqls);
    prepareTableData(fromBase32CreateSqls);
    prepareTableData(fromBase64CreateSqls);
    prepareTableData(fromBase64UrlCreateSqls);
    prepareTableData(fromHexCreateSqls);
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

  // Verifies standard Base64 for ASCII, UTF-8, empty, NULL, and punctuation-containing text.
  @Test
  public void testToBase64OnTextString() {
    String[] expectedHeader = new String[] {"time", "to_base64(c_text)", "to_base64(c_string)"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,aGVsbG8=,aGVsbG8=,",
          "1970-01-01T00:00:00.002Z,5L2g5aW9,5L2g5aW9,",
          "1970-01-01T00:00:00.003Z,,,",
          "1970-01-01T00:00:00.004Z,null,null,",
          "1970-01-01T00:00:00.005Z,SGVsbG8sIFdvcmxkIQ==,SGVsbG8sIFdvcmxkIQ==,"
        };
    tableResultSetEqualTest(
        "SELECT time, to_base64(c_text) as \"to_base64(c_text)\", to_base64(c_string) as \"to_base64(c_string)\" FROM table1 where time < 6",
        new String[] {"time", "to_base64(c_text)", "to_base64(c_string)"},
        retArray,
        TO_BASE64_DATABASE_NAME);
  }

  // Verifies standard Base64 encodes the raw bytes of a BLOB containing "test".
  @Test
  public void testToBase64OnBlob() {
    tableResultSetEqualTest(
        "SELECT time, to_base64(c_blob) as \"to_base64(c_blob)\" FROM table1 where time = 6",
        new String[] {"time", "to_base64(c_blob)"},
        new String[] {"1970-01-01T00:00:00.006Z,dGVzdA==,"},
        TO_BASE64_DATABASE_NAME);
  }

  // Confirms Base64 encode/decode round trips preserve ASCII, UTF-8, empty, and punctuation text.
  @Test
  public void testToBase64RoundTrip() {
    tableResultSetEqualTest(
        "SELECT time, CAST(from_base64(to_base64(c_text)) AS TEXT) as roundtrip_result FROM table1 where time in (1, 2, 3, 5)",
        new String[] {"time", "roundtrip_result"},
        new String[] {
          "1970-01-01T00:00:00.001Z,hello,",
          "1970-01-01T00:00:00.002Z,你好,",
          "1970-01-01T00:00:00.003Z,,",
          "1970-01-01T00:00:00.005Z,Hello, World!,"
        },
        TO_BASE64_DATABASE_NAME);
  }

  // Rejects Base64 calls with a numeric input, no argument, or multiple arguments.
  @Test
  public void testToBase64FunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_base64 only accepts one argument and it must be TEXT, STRING, or BLOB data type.";
    tableAssertTestFail(
        "SELECT to_base64(c_int) FROM table1", expectedErrorMessage, TO_BASE64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64(c_text, 1) FROM table1", expectedErrorMessage, TO_BASE64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64() FROM table1", expectedErrorMessage, TO_BASE64_DATABASE_NAME);
  }

  // Verifies Base64URL encoding for TEXT, STRING, BLOB, URL-safe symbols, empty, and NULL values.
  @Test
  public void testToBase64UrlOnAllTypes() {
    String[] expectedHeader =
        new String[] {
          "time", "to_base64url(c_text)", "to_base64url(c_string)", "to_base64url(c_blob)"
        };
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,aGVsbG8gd29ybGQ,aGVsbG8gd29ybGQ,aGVsbG8gd29ybGQ,",
          "1970-01-01T00:00:00.002Z,5L2g5aW9LCBJb1REQiE,5L2g5aW9LCBJb1REQiE,5L2g5aW9LCBJb1REQiE,",
          "1970-01-01T00:00:00.003Z,null,null,-_-_,",
          "1970-01-01T00:00:00.004Z,,,,",
          "1970-01-01T00:00:00.005Z,null,null,null,"
        };
    tableResultSetEqualTest(
        "SELECT time, to_base64url(c_text) as \"to_base64url(c_text)\", to_base64url(c_string) as \"to_base64url(c_string)\" , to_base64url(c_blob) as \"to_base64url(c_blob)\" FROM table1",
        expectedHeader,
        retArray,
        TO_BASE64URL_DATABASE_NAME);
  }

  // Rejects Base64URL calls with a numeric input, no argument, or multiple arguments.
  @Test
  public void testToBase64UrlFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_base64url only accepts one argument and it must be TEXT, STRING, or BLOB data type.";
    tableAssertTestFail(
        "SELECT to_base64url(c_int) FROM table1", expectedErrorMessage, TO_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64url() FROM table1", expectedErrorMessage, TO_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64url(c_text, c_string) FROM table1",
        expectedErrorMessage,
        TO_BASE64URL_DATABASE_NAME);
  }

  // Verifies lowercase hexadecimal output for ASCII, UTF-8, empty, NULL, and punctuation text.
  @Test
  public void testToHexOnTextString() {
    String[] expectedHeader = new String[] {"time", "to_hex(c_text)", "to_hex(c_string)"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,68656c6c6f,68656c6c6f,",
          "1970-01-01T00:00:00.002Z,e4bda0e5a5bd,e4bda0e5a5bd,",
          "1970-01-01T00:00:00.003Z,,,",
          "1970-01-01T00:00:00.004Z,null,null,",
          "1970-01-01T00:00:00.005Z,48656c6c6f2c20576f726c6421,48656c6c6f2c20576f726c6421,"
        };
    tableResultSetEqualTest(
        "SELECT time, to_hex(c_text) as \"to_hex(c_text)\", to_hex(c_string) as \"to_hex(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        retArray,
        TO_HEX_DATABASE_NAME);
  }

  // Verifies hexadecimal output for the raw bytes of the BLOB containing "test".
  @Test
  public void testToHexOnBlob() {
    tableResultSetEqualTest(
        "SELECT time, to_hex(c_blob) as \"to_hex(c_blob)\" FROM table1 where time = 6",
        new String[] {"time", "to_hex(c_blob)"},
        new String[] {"1970-01-01T00:00:00.006Z,74657374,"},
        TO_HEX_DATABASE_NAME);
  }

  // Confirms hexadecimal encode/decode round trips preserve ASCII, UTF-8, empty, and punctuation
  // text.
  @Test
  public void testToHexRoundTrip() {
    tableResultSetEqualTest(
        "SELECT time, CAST(from_hex(to_hex(c_text)) AS TEXT) as roundtrip_result FROM table1 where time in (1, 2, 3, 5)",
        new String[] {"time", "roundtrip_result"},
        new String[] {
          "1970-01-01T00:00:00.001Z,hello,",
          "1970-01-01T00:00:00.002Z,你好,",
          "1970-01-01T00:00:00.003Z,,",
          "1970-01-01T00:00:00.005Z,Hello, World!,"
        },
        TO_HEX_DATABASE_NAME);
  }

  // Rejects hexadecimal encoding calls with a numeric input, no argument, or multiple arguments.
  @Test
  public void testToHexFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function to_hex only accepts one argument and it must be TEXT, STRING, or BLOB data type.";
    tableAssertTestFail(
        "SELECT to_hex(c_int) FROM table1", expectedErrorMessage, TO_HEX_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_hex(c_text, 1) FROM table1", expectedErrorMessage, TO_HEX_DATABASE_NAME);
    tableAssertTestFail("SELECT to_hex() FROM table1", expectedErrorMessage, TO_HEX_DATABASE_NAME);
  }

  // Verifies Base32 decoding for ASCII, UTF-8 bytes, empty and NULL values, padding, and case
  // folding.
  @Test
  public void testFromBase32OnValidInputs() {
    String[] expectedHeader = new String[] {"time", "from_base32(c_text)", "from_base32(c_string)"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0x4844616d656f726420746865206d7222,0x4844616d656f726420746865206d7222,",
          "1970-01-01T00:00:00.002Z,0xd5b95f6bbce6d3df5b92e13fb3f4346338,0xd5b95f6bbce6d3df5b92e13fb3f4346338,",
          "1970-01-01T00:00:00.003Z,0x,0x,",
          "1970-01-01T00:00:00.004Z,null,null,",
          "1970-01-01T00:00:00.005Z,0x66,0x66,",
          "1970-01-01T00:00:00.006Z,0x666f,0x666f,",
          "1970-01-01T00:00:00.007Z,0x666f6f,0x666f6f,",
          "1970-01-01T00:00:00.008Z,0x666f6f62,0x666f6f62,",
          "1970-01-01T00:00:00.009Z,0x666f6f6201,0x666f6f6201,",
          "1970-01-01T00:00:00.010Z,0x666f6f,0x666f6f,",
          "1970-01-01T00:00:00.011Z,0x666f6f,0x666f6f,"
        };
    tableResultSetEqualTest(
        "SELECT time, from_base32(c_text) as \"from_base32(c_text)\", from_base32(c_string) as \"from_base32(c_string)\" FROM table1",
        expectedHeader,
        retArray,
        FROM_BASE32_DATABASE_NAME);
  }

  // Rejects Base32 decoder calls with a numeric input, no argument, or multiple arguments.
  @Test
  public void testFromBase32FunctionOnInvalidArguments() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_base32 only accepts one argument and it must be TEXT or STRING data type.";
    tableAssertTestFail(
        "SELECT from_base32(c_int) FROM table1", expectedErrorMessage, FROM_BASE32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base32() FROM table1", expectedErrorMessage, FROM_BASE32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base32(c_text, c_string) FROM table1",
        expectedErrorMessage,
        FROM_BASE32_DATABASE_NAME);
  }

  // Rejects Base32 strings containing non-alphabet characters or invalid padding placement.
  @Test
  public void testFromBase32FunctionOnInvalidDataFormat() {
    String baseErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_base32' due to an invalid input format. Problematic value:";
    tableAssertTestFail(
        "SELECT from_base32('JBCGC3LFN5ZGIIDUNBSSA3LSE0==') FROM table1",
        baseErrorMessage + " JBCGC3LFN5ZGIIDUNBSSA3LSE0==",
        FROM_BASE32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base32('JBCGC3LFN5ZGIIDUNBSSA3LSE1==') FROM table1",
        baseErrorMessage + " JBCGC3LFN5ZGIIDUNBSSA3LSE1==",
        FROM_BASE32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base32('JBCGC3LFN5ZGIIDUNBSSA3LSE8==') FROM table1",
        baseErrorMessage + " JBCGC3LFN5ZGIIDUNBSSA3LSE8==",
        FROM_BASE32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base32('JBCGC3LFN5ZGIIDUNBSSA3LSE9==') FROM table1",
        baseErrorMessage + " JBCGC3LFN5ZGIIDUNBSSA3LSE9==",
        FROM_BASE32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base32('MZXW6YQ-') FROM table1",
        baseErrorMessage + " MZXW6YQ-",
        FROM_BASE32_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base32('MZXW6=Q=') FROM table1",
        baseErrorMessage + " MZXW6=Q=",
        FROM_BASE32_DATABASE_NAME);
  }

  // Verifies Base64 decoding returns the expected bytes for ASCII, UTF-8, empty, and NULL inputs.
  @Test
  public void testFromBase64OnTextString() {
    tableResultSetEqualTest(
        "SELECT time, from_base64(c_text) as \"from_base64(c_text)\", from_base64(c_string) as \"from_base64(c_string)\" FROM table1 where time < 5",
        new String[] {"time", "from_base64(c_text)", "from_base64(c_string)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,0x68656c6c6f,0x68656c6c6f,",
          "1970-01-01T00:00:00.002Z,0xe4bda0e5a5bd,0xe4bda0e5a5bd,",
          "1970-01-01T00:00:00.003Z,0x,0x,",
          "1970-01-01T00:00:00.004Z,null,null,"
        },
        FROM_BASE64_DATABASE_NAME);
  }

  // Rejects malformed Base64 data for both non-alphabet text and invalid encoding length.
  @Test
  public void testFromBase64OnInvalidBase64() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_base64' due to an invalid input format. Problematic value: ";
    tableAssertTestFail(
        "SELECT from_base64(c_text) FROM table1 WHERE time = 5",
        expectedErrorMessage + "not_base64",
        FROM_BASE64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64(c_text) FROM table1 WHERE time = 6",
        expectedErrorMessage + "abc",
        FROM_BASE64_DATABASE_NAME);
  }

  // Rejects Base64 decoder calls with a numeric input or multiple arguments.
  @Test
  public void testFromBase64FunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_base64 only accepts one argument and it must be TEXT or STRING data type.";
    tableAssertTestFail(
        "SELECT from_base64(c_int) FROM table1", expectedErrorMessage, FROM_BASE64_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64(c_text, 1) FROM table1",
        expectedErrorMessage,
        FROM_BASE64_DATABASE_NAME);
  }

  // Verifies Base64URL decoding for URL-safe symbols, long payloads, all alphabet characters,
  // empty, and NULL.
  @Test
  public void testFromBase64UrlOnValidInputs() {
    tableResultSetEqualTest(
        "SELECT time, from_base64url(c_text) as \"from_base64url(c_text)\", from_base64url(c_string) as \"from_base64url(c_string)\" FROM table1",
        new String[] {"time", "from_base64url(c_text)", "from_base64url(c_string)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,0x496f5444422069732066756e21,0x496f5444422069732066756e21,",
          "1970-01-01T00:00:00.002Z,0xe4bda0e5a5bd2c206597e382965f8e0a8484,0xe4bda0e5a5bd2c206597e382965f8e0a8484,",
          "1970-01-01T00:00:00.003Z,0xfbffbf,0xfbffbf,",
          "1970-01-01T00:00:00.004Z,0x,0x,",
          "1970-01-01T00:00:00.005Z,null,null,",
          "1970-01-01T00:00:00.006Z,0x61,0x61,",
          "1970-01-01T00:00:00.007Z,0x6162,0x6162,",
          "1970-01-01T00:00:00.008Z,0x616263,0x616263,",
          "1970-01-01T00:00:00.009Z,0x41706163686520496f54444220697320616e20496f54206e61746976652064617461626173652077697468206869676820706572666f726d616e636520666f722064617461206d616e6167656d656e7420616e6420616e616c797369732e,0x41706163686520496f54444220697320616e20496f54206e61746976652064617461626173652077697468206869676820706572666f726d616e636520666f722064617461206d616e6167656d656e7420616e6420616e616c797369732e,",
          "1970-01-01T00:00:00.010Z,0x00108310518720928b30d38f41149351559761969b71d79f8218a39259a7a29aabb2dbafc31cb3d35db7e39ebbf3dfbf,0x00108310518720928b30d38f41149351559761969b71d79f8218a39259a7a29aabb2dbafc31cb3d35db7e39ebbf3dfbf,"
        },
        FROM_BASE64URL_DATABASE_NAME);
  }

  // Rejects Base64URL decoder calls with a numeric input, no argument, or multiple arguments.
  @Test
  public void testFromBase64UrlFunctionOnInvalidArguments() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_base64url only accepts one argument and it must be TEXT or STRING data type.";
    tableAssertTestFail(
        "SELECT from_base64url(c_int) FROM table1",
        expectedErrorMessage,
        FROM_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64url() FROM table1", expectedErrorMessage, FROM_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64url(c_text, c_string) FROM table1",
        expectedErrorMessage,
        FROM_BASE64URL_DATABASE_NAME);
  }

  // Rejects Base64URL inputs containing standard-Base64 symbols, padding, invalid characters, or
  // invalid lengths.
  @Test
  public void testFromBase64UrlFunctionOnInvalidDataFormat() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_base64url' due to an invalid input format. Problematic value:";
    tableAssertTestFail(
        "SELECT from_base64url('aGVsbG8gd29ybG+') FROM table1",
        expectedErrorMessage + " aGVsbG8gd29ybG+",
        FROM_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64url('aGVsbG8gd29ybG/') FROM table1",
        expectedErrorMessage + " aGVsbG8gd29ybG/",
        FROM_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64url('YWJj=') FROM table1",
        expectedErrorMessage + " YWJj=",
        FROM_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64url('aGVsbG8gd29ybG$') FROM table1",
        expectedErrorMessage + " aGVsbG8gd29ybG$",
        FROM_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64url('a.b') FROM table1",
        expectedErrorMessage + " a.b",
        FROM_BASE64URL_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_base64url('YWJjY') FROM table1",
        expectedErrorMessage + " YWJjY",
        FROM_BASE64URL_DATABASE_NAME);
  }

  // Verifies hexadecimal decoding for ASCII, UTF-8 bytes, empty, and NULL text inputs.
  @Test
  public void testFromHexOnTextString() {
    tableResultSetEqualTest(
        "SELECT time, from_hex(c_text) as \"from_hex(c_text)\", from_hex(c_string) as \"from_hex(c_string)\" FROM table1 where time < 5",
        new String[] {"time", "from_hex(c_text)", "from_hex(c_string)"},
        new String[] {
          "1970-01-01T00:00:00.001Z,0x68656c6c6f,0x68656c6c6f,",
          "1970-01-01T00:00:00.002Z,0xe4bda0e5a5bd,0xe4bda0e5a5bd,",
          "1970-01-01T00:00:00.003Z,0x,0x,",
          "1970-01-01T00:00:00.004Z,null,null,"
        },
        FROM_HEX_DATABASE_NAME);
  }

  // Rejects hexadecimal input with non-hexadecimal characters and checks the reported value.
  @Test
  public void testFromHexOnInvalidHexChars() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_hex' due to an invalid input format. Problematic value: not_hex";
    tableAssertTestFail(
        "SELECT from_hex(c_text) FROM table1 WHERE time = 5",
        expectedErrorMessage,
        FROM_HEX_DATABASE_NAME);
  }

  // Rejects odd-length hexadecimal input and checks the reported value.
  @Test
  public void testFromHexOnInvalidHexLength() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_hex' due to an invalid input format. Problematic value: 123";
    tableAssertTestFail(
        "SELECT from_hex(c_text) FROM table1 WHERE time = 6",
        expectedErrorMessage,
        FROM_HEX_DATABASE_NAME);
  }

  // Rejects hexadecimal decoder calls with a numeric input, no argument, or multiple arguments.
  @Test
  public void testFromHexFunctionOnInvalidInputs() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_hex only accepts one argument and it must be TEXT or STRING data type.";
    tableAssertTestFail(
        "SELECT from_hex(c_int) FROM table1", expectedErrorMessage, FROM_HEX_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_hex(c_text, 1) FROM table1", expectedErrorMessage, FROM_HEX_DATABASE_NAME);
    tableAssertTestFail(
        "SELECT from_hex() FROM table1", expectedErrorMessage, FROM_HEX_DATABASE_NAME);
  }
}
