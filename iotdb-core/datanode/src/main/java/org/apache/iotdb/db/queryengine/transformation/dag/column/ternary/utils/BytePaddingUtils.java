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

package org.apache.iotdb.db.queryengine.transformation.dag.column.ternary.utils;

import org.apache.iotdb.db.exception.sql.SemanticException;

public class BytePaddingUtils {

  private static String HexToString(byte[] inputBytes) {
    StringBuilder hexString = new StringBuilder("0x");
    for (byte inputByte : inputBytes) {
      hexString.append(String.format("%02x", inputByte));
    }
    return hexString.toString();
  }

  // support for the lpad and rpad function
  public static byte[] padBytes(
      byte[] originBytes,
      long targetLength,
      byte[] paddingByte,
      int paddingOffset,
      String functionName) {

    if (targetLength < 0 || targetLength > Integer.MAX_VALUE) {
      throw new SemanticException(
          String.format(
              "Failed to execute function '%s' due to the value %s corresponding to a invalid target size, the allowed range is [0, %d].",
              functionName, HexToString(originBytes), Integer.MAX_VALUE));
    }

    if (paddingByte.length == 0) {
      throw new SemanticException(
          String.format(
              "Failed to execute function '%s' due the value %s corresponding to a empty padding string.",
              functionName, HexToString(originBytes)));
    }

    int inputLength = originBytes.length;
    int resultLength = (int) targetLength;

    if (inputLength == resultLength) {
      return originBytes;
    }
    if (inputLength > resultLength) {
      byte[] resultBytes = new byte[resultLength];
      System.arraycopy(originBytes, 0, resultBytes, 0, resultLength);
      return resultBytes;
    }

    // copy the existing bytes to the result bytes
    byte[] resultBytes = new byte[resultLength];
    int fillLength = resultLength - inputLength;
    int startIndex = (paddingOffset + fillLength) % resultLength;
    System.arraycopy(originBytes, 0, resultBytes, startIndex, inputLength);

    // fill the remaining bytes with the padding bytes
    int byteIndex = paddingOffset;
    for (int i = 0; i < fillLength / paddingByte.length; i++) {
      System.arraycopy(paddingByte, 0, resultBytes, byteIndex, paddingByte.length);
      byteIndex += paddingByte.length;
    }

    // fill the last few bytes
    System.arraycopy(
        paddingByte, 0, resultBytes, byteIndex, paddingOffset + fillLength - byteIndex);

    return resultBytes;
  }
}
