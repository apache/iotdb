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

package org.apache.iotdb.google.common.primitives;

import org.apache.iotdb.google.common.annotations.GwtCompatible;

/** A string to be parsed as a number and the radix to interpret it in. */
@GwtCompatible
@ElementTypesAreNonnullByDefault
final class ParseRequest {
  final String rawValue;
  final int radix;

  private ParseRequest(String rawValue, int radix) {
    this.rawValue = rawValue;
    this.radix = radix;
  }

  static ParseRequest fromString(String stringValue) {
    if (stringValue.length() == 0) {
      throw new NumberFormatException("empty string");
    }

    // Handle radix specifier if present
    String rawValue;
    int radix;
    char firstChar = stringValue.charAt(0);
    if (stringValue.startsWith("0x") || stringValue.startsWith("0X")) {
      rawValue = stringValue.substring(2);
      radix = 16;
    } else if (firstChar == '#') {
      rawValue = stringValue.substring(1);
      radix = 16;
    } else if (firstChar == '0' && stringValue.length() > 1) {
      rawValue = stringValue.substring(1);
      radix = 8;
    } else {
      rawValue = stringValue;
      radix = 10;
    }

    return new ParseRequest(rawValue, radix);
  }
}
