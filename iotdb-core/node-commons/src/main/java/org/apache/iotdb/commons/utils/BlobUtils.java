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

package org.apache.iotdb.commons.utils;

import com.google.common.base.CharMatcher;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BlobUtils {

  private BlobUtils() {}

  // the grammar could possibly include whitespace in the value it passes to us
  private static final CharMatcher WHITESPACE_MATCHER = CharMatcher.whitespace();
  private static final CharMatcher HEX_DIGIT_MATCHER =
      CharMatcher.inRange('A', 'F').or(CharMatcher.inRange('0', '9')).precomputed();

  public static byte[] parseBlobString(String value) {
    requireNonNull(value, "value is null");
    if (value.length() < 3 || !value.startsWith("X'") || !value.endsWith("'")) {
      throw new IllegalArgumentException("Binary literal must be in the form X'hexstring'");
    }
    value = value.substring(2, value.length() - 1);
    String hexString = WHITESPACE_MATCHER.removeFrom(value).toUpperCase(ENGLISH);
    if (!HEX_DIGIT_MATCHER.matchesAllOf(hexString)) {
      throw new IllegalArgumentException("Binary literal can only contain hexadecimal digits");
    }
    if (hexString.length() % 2 != 0) {
      throw new IllegalArgumentException("Binary literal must contain an even number of digits");
    }
    int len = hexString.length();
    byte[] values = new byte[len / 2];

    for (int i = 0; i < len; i += 2) {
      values[i / 2] =
          (byte)
              ((Character.digit(hexString.charAt(i), 16) << 4)
                  + Character.digit(hexString.charAt(i + 1), 16));
    }
    return values;
  }
}
