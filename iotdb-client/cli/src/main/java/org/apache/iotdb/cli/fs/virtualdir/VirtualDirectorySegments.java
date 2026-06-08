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

package org.apache.iotdb.cli.fs.virtualdir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

final class VirtualDirectorySegments {

  private static final char[] HEX = "0123456789ABCDEF".toCharArray();

  private VirtualDirectorySegments() {}

  static String encode(String value) {
    StringBuilder builder = new StringBuilder();
    for (byte rawByte : value.getBytes(StandardCharsets.UTF_8)) {
      int b = rawByte & 0xFF;
      if (isUnreserved(b)) {
        builder.append((char) b);
      } else {
        builder.append('%').append(HEX[b >>> 4]).append(HEX[b & 0x0F]);
      }
    }
    return builder.toString();
  }

  static String decode(String segment) throws SQLException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (int i = 0; i < segment.length(); ) {
      char c = segment.charAt(i);
      if (c == '%') {
        if (i + 2 >= segment.length()) {
          throw new SQLException("Invalid virtual path segment: " + segment);
        }
        int high = hexValue(segment.charAt(i + 1));
        int low = hexValue(segment.charAt(i + 2));
        if (high < 0 || low < 0) {
          throw new SQLException("Invalid virtual path segment: " + segment);
        }
        bytes.write((high << 4) + low);
        i += 3;
        continue;
      }
      int codePoint = segment.codePointAt(i);
      byte[] rawBytes = new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8);
      bytes.write(rawBytes, 0, rawBytes.length);
      i += Character.charCount(codePoint);
    }
    return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
  }

  private static boolean isUnreserved(int b) {
    return (b >= 'A' && b <= 'Z')
        || (b >= 'a' && b <= 'z')
        || (b >= '0' && b <= '9')
        || b == '-'
        || b == '.'
        || b == '_'
        || b == '~';
  }

  private static int hexValue(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    return -1;
  }
}
