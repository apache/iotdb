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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.util;

import org.apache.tsfile.common.conf.TSFileConfig;

public final class HexUtils {

  private static final byte[] HEX_CHAR_TABLE =
      "0123456789abcdef".getBytes(TSFileConfig.STRING_CHARSET);

  private HexUtils() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  /**
   * Converts a byte array to its hexadecimal representation as a new byte array. No intermediate
   * String objects are created.
   */
  public static byte[] toHex(byte[] input) {
    byte[] hexBytes = new byte[input.length * 2];
    for (int i = 0; i < input.length; i++) {
      int v = input[i] & 0xFF;
      hexBytes[i * 2] = HEX_CHAR_TABLE[v >>> 4];
      hexBytes[i * 2 + 1] = HEX_CHAR_TABLE[v & 0x0F];
    }
    return hexBytes;
  }

  /**
   * Converts a byte array representing a hexadecimal string back to its raw byte array. No
   * intermediate String objects are created.
   *
   * @param input The byte array containing hexadecimal characters.
   * @return The decoded raw byte array.
   */
  public static byte[] fromHex(byte[] input) {
    if ((input.length & 1) != 0) {
      throw new IllegalArgumentException();
    }

    byte[] rawBytes = new byte[input.length / 2];
    for (int i = 0; i < rawBytes.length; i++) {
      int high = hexCharToDigit(input[i * 2]);
      int low = hexCharToDigit(input[i * 2 + 1]);
      rawBytes[i] = (byte) ((high << 4) | low);
    }
    return rawBytes;
  }

  /**
   * Converts a single byte representing a hex character to its integer value (0-15).
   *
   * @param c The byte representing the hex character (e.g., 'a', 'F', '9').
   * @return The integer value from 0 to 15.
   */
  private static int hexCharToDigit(byte c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    throw new IllegalArgumentException();
  }
}
