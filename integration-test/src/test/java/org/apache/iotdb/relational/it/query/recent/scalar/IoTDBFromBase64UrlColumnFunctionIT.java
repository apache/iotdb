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

/** Integration tests for the from_base64url() scalar function. */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFromBase64UrlColumnFunctionIT {

  private static final String DATABASE_NAME = "test_frombase64url_function_v2";

  // 重新设计的、更全面的测试数据集
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(c_text TEXT, c_string STRING, c_int INT32)",

        // 'IoTDB is fun!' -> Base64URL: SW9UREIgaXMgZnVuIQ -> BLOB: X'496f5444422069732066756e21'
        "INSERT INTO table1(time, c_text, c_string) VALUES (1, 'SW9UREIgaXMgZnVuIQ', 'SW9UREIgaXMgZnVuIQ')",

        // '你好, 世界!' -> Base64URL: 5L2g5aW9LCBll-OCll-OCoSE -> BLOB:
        // X'e4bda0e5a5bd2c20e4b896e7958c21'
        "INSERT INTO table1(time, c_text, c_string) VALUES (2, '5L2g5aW9LCBll-OCll-OCoSE', '5L2g5aW9LCBll-OCll-OCoSE')",

        // Bytes [251, 255, 191] (0xfbffbf) -> Standard Base64: +/+/ -> Base64URL: -_-_
        "INSERT INTO table1(time, c_text, c_string) VALUES (3, '-_-_', '-_-_')",

        // '' -> Base64URL: '' -> BLOB: X''
        "INSERT INTO table1(time, c_text, c_string) VALUES (4, '', '')",
        "INSERT INTO table1(time, c_int) VALUES (5, 100)",

        // 'a' (0x61) -> Base64URL: YQ
        "INSERT INTO table1(time, c_text, c_string) VALUES (6, 'YQ', 'YQ')",

        // 'ab' (0x6162) -> Base64URL: YWI
        "INSERT INTO table1(time, c_text, c_string) VALUES (7, 'YWI', 'YWI')",

        // 'abc' (0x616263) -> Base64URL: YWJj
        "INSERT INTO table1(time, c_text, c_string) VALUES (8, 'YWJj', 'YWJj')",

        // 'Apache IoTDB is an IoT native database with high performance for data management and
        // analysis.'
        // Base64URL:
        // QXBhY2hlIElvVERCIGlzIGFuIElvVCBuYXRpdmUgZGF0YWJhc2Ugd2l0aCBoaWdoIHBlcmZvcm1hbmNlIGZvciBkYXRhIG1hbmFnZW1lbnQgYW5kIGFuYWx5c2lzLg
        "INSERT INTO table1(time, c_text, c_string) VALUES (9, 'QXBhY2hlIElvVERCIGlzIGFuIElvVCBuYXRpdmUgZGF0YWJhc2Ugd2l0aCBoaWdoIHBlcmZvcm1hbmNlIGZvciBkYXRhIG1hbmFnZW1lbnQgYW5kIGFuYWx5c2lzLg', 'QXBhY2hlIElvVERCIGlzIGFuIElvVCBuYXRpdmUgZGF0YWJhc2Ugd2l0aCBoaWdoIHBlcmZvcm1hbmNlIGZvciBkYXRhIG1hbmFnZW1lbnQgYW5kIGFuYWx5c2lzLg')",

        //  All Base64URL characters (A-Z, a-z, 0-9, -, _)
        // Bytes [0..63] -> Base64URL:
        // ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_
        "INSERT INTO table1(time, c_text, c_string) VALUES (10, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_', 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_')"
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

  /** validate the from_base64url() function on valid inputs. */
  @Test
  public void testFromBase64UrlOnValidInputs() {
    String[] expectedHeader =
        new String[] {"time", "from_base64url(c_text)", "from_base64url(c_string)"};
    String[] retArray =
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
        };

    tableResultSetEqualTest(
        "SELECT time, from_base64url(c_text) as \"from_base64url(c_text)\", from_base64url(c_string) as \"from_base64url(c_string)\" FROM table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  /** test the invalid arguments for from_base64url() function. */
  @Test
  public void testFromBase64UrlFunctionOnInvalidArguments() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function from_base64url only accepts one argument and it must be TEXT or STRING data type.";

    // test the invalid parameter type (INT32)
    tableAssertTestFail(
        "SELECT from_base64url(c_int) FROM table1", expectedErrorMessage, DATABASE_NAME);

    // test the invalid parameter type (no parameter)
    tableAssertTestFail("SELECT from_base64url() FROM table1", expectedErrorMessage, DATABASE_NAME);

    // test the invalid parameter type (two parameters)
    tableAssertTestFail(
        "SELECT from_base64url(c_text, c_string) FROM table1", expectedErrorMessage, DATABASE_NAME);
  }

  /**
   * validate the from_base64url() function fails when given invalid Base64URL formatted strings.
   */
  @Test
  public void testFromBase64UrlFunctionOnInvalidDataFormat() {
    String expectedErrorMessage =
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Failed to execute function 'from_base64url' due to an invalid input format. Problematic value:";

    // invalid character: '+' (standard Base64 character)
    tableAssertTestFail(
        "SELECT from_base64url('aGVsbG8gd29ybG+') FROM table1",
        expectedErrorMessage + " aGVsbG8gd29ybG+",
        DATABASE_NAME);

    // invalid character: '/' (standard Base64 character)
    tableAssertTestFail(
        "SELECT from_base64url('aGVsbG8gd29ybG/') FROM table1",
        expectedErrorMessage + " aGVsbG8gd29ybG/",
        DATABASE_NAME);

    // invalid character: '=' (padding character, not used in Base64URL)
    tableAssertTestFail(
        "SELECT from_base64url('YWJj=') FROM table1",
        expectedErrorMessage + " YWJj=",
        DATABASE_NAME);

    // invalid character: '$'
    tableAssertTestFail(
        "SELECT from_base64url('aGVsbG8gd29ybG$') FROM table1",
        expectedErrorMessage + " aGVsbG8gd29ybG$",
        DATABASE_NAME);

    // invalid character: '.'
    tableAssertTestFail(
        "SELECT from_base64url('a.b') FROM table1", expectedErrorMessage + " a.b", DATABASE_NAME);

    // invalid length: 5 (Base64URL encoded strings must have a length that is a multiple of 4)
    tableAssertTestFail(
        "SELECT from_base64url('YWJjY') FROM table1",
        expectedErrorMessage + " YWJjY",
        DATABASE_NAME);
  }
}
