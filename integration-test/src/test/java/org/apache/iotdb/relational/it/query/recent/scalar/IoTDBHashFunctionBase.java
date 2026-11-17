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

abstract class IoTDBHashFunctionBase extends IoTDBHmacMd5FunctionBase {

  private static final String DATABASE_NAME = "test_md5_function";
  private static final String UTF8_DATABASE_NAME = "test_utf8_hash_functions";
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
        "INSERT INTO table1(time, c_blob) VALUES (6, x'74657374')",
        "INSERT INTO table1(time, c_text) VALUES (7, '123456789')"
      };

  private static final String[] utf8CreateSqls =
      new String[] {
        "CREATE DATABASE " + UTF8_DATABASE_NAME,
        "USE " + UTF8_DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT FIELD, c_string STRING FIELD, c_blob BLOB FIELD, c_int INT32 FIELD)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'hello', 'hello')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '你好', '你好')",
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (4, 404)",
        "INSERT INTO table1(time, c_text, c_string) VALUES (5, 'Hello, World!', 'Hello, World!')",
        "INSERT INTO table1(time, c_blob) VALUES (6, x'74657374')",
        "INSERT INTO table1(time, c_text) VALUES (7, '123456789')"
      };

  protected static void insertHashData() {
    prepareTableData(createSqls);
    prepareTableData(utf8CreateSqls);
  }

  private static void assertTextAndStringHash(
      String functionName, String databaseName, String[] expectedHashes) {
    String[] expectedHeader = {"time", functionName + "(c_text)", functionName + "(c_string)"};
    String[] expectedRows = new String[expectedHashes.length];
    for (int rowIndex = 0; rowIndex < expectedHashes.length; rowIndex++) {
      String timestamp = "1970-01-01T00:00:00.00" + (rowIndex + 1) + "Z";
      String hash = expectedHashes[rowIndex];
      expectedRows[rowIndex] = timestamp + "," + hash + "," + hash + ",";
    }

    tableResultSetEqualTest(
        "SELECT time, "
            + functionName
            + "(c_text) as \""
            + functionName
            + "(c_text)\", "
            + functionName
            + "(c_string) as \""
            + functionName
            + "(c_string)\" FROM table1 where time < 6",
        expectedHeader,
        expectedRows,
        databaseName);
  }

  private static void assertBlobHash(
      String functionName, String databaseName, String expectedHash) {
    tableResultSetEqualTest(
        "SELECT time, "
            + functionName
            + "(c_blob) as \""
            + functionName
            + "(c_blob)\" FROM table1 where time = 6",
        new String[] {"time", functionName + "(c_blob)"},
        new String[] {"1970-01-01T00:00:00.006Z," + expectedHash + ","},
        databaseName);
  }

  private static void assertKnownValueHash(
      String functionName, String databaseName, String expectedHash) {
    tableResultSetEqualTest(
        "SELECT time, "
            + functionName
            + "(c_text) as \""
            + functionName
            + "(c_text)\" FROM table1 where time = 7",
        new String[] {"time", functionName + "(c_text)"},
        new String[] {"1970-01-01T00:00:00.007Z," + expectedHash + ","},
        databaseName);
  }

  private static void assertInvalidHashArguments(String functionName, String databaseName) {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function "
            + functionName
            + " only accepts one argument and it must be TEXT, STRING, or BLOB data type.";
    tableAssertTestFail(
        "SELECT " + functionName + "(c_int) FROM table1", expectedErrorMessage, databaseName);
    tableAssertTestFail(
        "SELECT " + functionName + "(c_text, 'another_arg') FROM table1",
        expectedErrorMessage,
        databaseName);
    tableAssertTestFail(
        "SELECT " + functionName + "() FROM table1", expectedErrorMessage, databaseName);
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

  // Validates SHA-1 for text, string, BLOB, null, and malformed argument inputs.
  @Test
  public void testSha1OnTextAndString() {
    assertTextAndStringHash(
        "sha1",
        DATABASE_NAME,
        new String[] {
          "0xaaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
          "0x440ee0853ad1e99f962b63e459ef992d7c211722",
          "0xda39a3ee5e6b4b0d3255bfef95601890afd80709",
          "null",
          "0x0a0a9f2a6772942557ab5355d76af442f8f65e01"
        });
  }

  // Confirms SHA-1 hashes the raw bytes of the BLOB value.
  @Test
  public void testSha1OnBlob() {
    assertBlobHash("sha1", DATABASE_NAME, "0xa94a8fe5ccb19ba61c4c0873d391e987982fbbd3");
  }

  // Rejects non-string input and incorrect SHA-1 argument counts with the semantic status.
  @Test
  public void testSha1FunctionOnInvalidInputs() {
    assertInvalidHashArguments("sha1", DATABASE_NAME);
  }

  // Validates SHA-256 across ASCII, UTF-8 text, empty, null, and punctuation-containing values.
  @Test
  public void testSha256OnTextAndString() {
    assertTextAndStringHash(
        "sha256",
        DATABASE_NAME,
        new String[] {
          "0x2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
          "0x670d9743542cae3ea7ebe36af56bd53648b0a1126162e78d81a32934a711302e",
          "0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          "null",
          "0xdffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
        });
  }

  // Confirms SHA-256 hashes the raw bytes of the BLOB value.
  @Test
  public void testSha256OnBlob() {
    assertBlobHash(
        "sha256",
        DATABASE_NAME,
        "0x9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08");
  }

  // Rejects non-string input and incorrect SHA-256 argument counts with the semantic status.
  @Test
  public void testSha256FunctionOnInvalidInputs() {
    assertInvalidHashArguments("sha256", DATABASE_NAME);
  }

  // Validates SHA-512 across ASCII, UTF-8 text, empty, null, and punctuation-containing values.
  @Test
  public void testSha512OnTextAndString() {
    assertTextAndStringHash(
        "sha512",
        DATABASE_NAME,
        new String[] {
          "0x9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
          "0x5232181bc0d9888f5c9746e410b4740eb461706ba5dacfbc93587cecfc8d068bac7737e92870d6745b11a25e9cd78b55f4ffc706f73cfcae5345f1b53fb8f6b5",
          "0xcf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
          "null",
          "0x374d794a95cdcfd8b35993185fef9ba368f160d8daf432d08ba9f1ed1e5abe6cc69291e0fa2fe0006a52570ef18c19def4e617c33ce52ef0a6e5fbe318cb0387"
        });
  }

  // Confirms SHA-512 hashes the raw bytes of the BLOB value.
  @Test
  public void testSha512OnBlob() {
    assertBlobHash(
        "sha512",
        DATABASE_NAME,
        "0xee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff");
  }

  // Rejects non-string input and incorrect SHA-512 argument counts with the semantic status.
  @Test
  public void testSha512FunctionOnInvalidInputs() {
    assertInvalidHashArguments("sha512", DATABASE_NAME);
  }

  // Validates CRC32 for text, string, BLOB, null, and punctuation-containing values.
  @Test
  public void testCrc32OnTextString() {
    assertTextAndStringHash(
        "crc32",
        DATABASE_NAME,
        new String[] {"907060870", "1352841281", "0", "null", "3964322768"});
  }

  // Confirms CRC32 returns the expected unsigned checksum for the BLOB bytes.
  @Test
  public void testCrc32OnBlob() {
    assertBlobHash("crc32", DATABASE_NAME, "3632233996");
  }

  // Checks CRC32 against the standard 123456789 test vector.
  @Test
  public void testCrc32KnownValue() {
    assertKnownValueHash("crc32", DATABASE_NAME, "3421780262");
  }

  // Rejects non-string input and incorrect CRC32 argument counts with the semantic status.
  @Test
  public void testCrc32FunctionOnInvalidInputs() {
    assertInvalidHashArguments("crc32", DATABASE_NAME);
  }

  // Validates XXHash64 for text, string, BLOB, null, and punctuation-containing values.
  @Test
  public void testXxhash64OnTextAndString() {
    assertTextAndStringHash(
        "xxhash64",
        DATABASE_NAME,
        new String[] {
          "0x26c7827d889f6da3",
          "0x8b7c90cd33d92633",
          "0xef46db3751d8e999",
          "null",
          "0xc49aacf8080fe47f"
        });
  }

  // Confirms XXHash64 hashes the raw bytes of the BLOB value.
  @Test
  public void testXxhash64OnBlob() {
    assertBlobHash("xxhash64", DATABASE_NAME, "0x4fdcca5ddb678139");
  }

  // Rejects non-string input and incorrect XXHash64 argument counts with the semantic status.
  @Test
  public void testXxhash64FunctionOnInvalidInputs() {
    assertInvalidHashArguments("xxhash64", DATABASE_NAME);
  }

  // Validates Murmur3 on actual UTF-8 Chinese text, empty text, null, and punctuation.
  @Test
  public void testMurmur3OnTextAndString() {
    assertTextAndStringHash(
        "murmur3",
        UTF8_DATABASE_NAME,
        new String[] {
          "0x029bbd41b3a7d8cb191dae486a901e5b",
          "0x2089985d4eb4023c3022351dc70b520c",
          "0x00000000000000000000000000000000",
          "null",
          "0xc0a1b86f7365bc93bfae71678c2843aa"
        });
  }

  // Confirms Murmur3 hashes the raw bytes of the BLOB value.
  @Test
  public void testMurmur3OnBlob() {
    assertBlobHash("murmur3", UTF8_DATABASE_NAME, "0x9de1bd74cc287dac824dbdf93182129a");
  }

  // Rejects non-string input and incorrect Murmur3 argument counts with the semantic status.
  @Test
  public void testMurmur3FunctionOnInvalidInputs() {
    assertInvalidHashArguments("murmur3", UTF8_DATABASE_NAME);
  }

  // Validates the 32-bit SpookyHash output for UTF-8 text, BLOB, and the standard test vector.
  @Test
  public void testSpookyHashV232OnTextString() {
    assertTextAndStringHash(
        "spooky_hash_v2_32",
        UTF8_DATABASE_NAME,
        new String[] {"0xd382e6ca", "0x38c1de3e", "0x6bf50919", "null", "0x4a0db65a"});
  }

  // Confirms 32-bit SpookyHash hashes the raw bytes of the BLOB value.
  @Test
  public void testSpookyHashV232OnBlob() {
    assertBlobHash("spooky_hash_v2_32", UTF8_DATABASE_NAME, "0xec0d8b75");
  }

  // Checks 32-bit SpookyHash against the standard 123456789 test vector.
  @Test
  public void testSpookyHashV232KnownValue() {
    assertKnownValueHash("spooky_hash_v2_32", UTF8_DATABASE_NAME, "0x98b9cd87");
  }

  // Rejects non-string input and incorrect 32-bit SpookyHash argument counts.
  @Test
  public void testSpookyHashV232FunctionOnInvalidInputs() {
    assertInvalidHashArguments("spooky_hash_v2_32", UTF8_DATABASE_NAME);
  }

  // Validates the 64-bit SpookyHash output for UTF-8 text, BLOB, and the standard test vector.
  @Test
  public void testSpookyHashV264OnTextString() {
    assertTextAndStringHash(
        "spooky_hash_v2_64",
        UTF8_DATABASE_NAME,
        new String[] {
          "0x3768826ad382e6ca",
          "0x116dbe1a38c1de3e",
          "0x232706fc6bf50919",
          "null",
          "0x9c7ad9cc4a0db65a"
        });
  }

  // Confirms 64-bit SpookyHash hashes the raw bytes of the BLOB value.
  @Test
  public void testSpookyHashV264OnBlob() {
    assertBlobHash("spooky_hash_v2_64", UTF8_DATABASE_NAME, "0x7b01e8bcec0d8b75");
  }

  // Checks 64-bit SpookyHash against the standard 123456789 test vector.
  @Test
  public void testSpookyHashV264KnownValue() {
    assertKnownValueHash("spooky_hash_v2_64", UTF8_DATABASE_NAME, "0xb52b501c98b9cd87");
  }

  // Rejects non-string input and incorrect 64-bit SpookyHash argument counts.
  @Test
  public void testSpookyHashV264FunctionOnInvalidInputs() {
    assertInvalidHashArguments("spooky_hash_v2_64", UTF8_DATABASE_NAME);
  }
}
