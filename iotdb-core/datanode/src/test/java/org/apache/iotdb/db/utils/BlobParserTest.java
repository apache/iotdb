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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.exception.sql.SemanticException;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class BlobParserTest {

  @Test
  public void testHexString() {
    String validHex1 = "X'DeaF'";
    byte[] expected1 = new byte[] {(byte) 0xde, (byte) 0xaf};
    assertArrayEquals(
        "Hex conversion failed for valid input.",
        expected1,
        CommonUtils.parseBlobStringToByteArray(validHex1));
    String validHex2 = "X'0f'";
    byte[] expected2 = new byte[] {(byte) 0x0f};
    assertArrayEquals(
        "Hex conversion failed for single byte.",
        expected2,
        CommonUtils.parseBlobStringToByteArray(validHex2));
    String validHex3 = "X''";
    byte[] expected3 = new byte[] {};
    assertArrayEquals(
        "Hex conversion failed for empty hex.",
        expected3,
        CommonUtils.parseBlobStringToByteArray(validHex3));
  }

  @Test
  public void testInvalidHexString() {
    String invalidHex1 = "X'1g'";
    try {
      CommonUtils.parseBlobStringToByteArray(invalidHex1);
      fail("Expected an IllegalArgumentException for invalid hex character");
    } catch (SemanticException e) {
      // Test passes
    }
    String invalidHex2 = "X'GG'";
    try {
      CommonUtils.parseBlobStringToByteArray(invalidHex2);
      fail("Expected an IllegalArgumentException for non-hex characters");
    } catch (SemanticException e) {
      // Test passes
    }
  }
}
