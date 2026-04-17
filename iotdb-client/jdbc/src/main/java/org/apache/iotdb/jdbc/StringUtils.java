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

package org.apache.iotdb.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;

public class StringUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(StringUtils.class);

  private static byte[] allBytes = new byte[256];

  private static char[] byteToChars = new char[256];

  private static Method toPlainStringMethod;

  static final int WILD_COMPARE_MATCH_NO_WILD = 0;

  static final int WILD_COMPARE_MATCH_WITH_WILD = 1;

  static final int WILD_COMPARE_NO_MATCH = -1;

  /**
   * Maximum absolute {@link BigDecimal#scale()} accepted by {@link
   * #consistentToString(BigDecimal)} before it falls back to scientific notation.
   *
   * <p>{@link BigDecimal#toPlainString()} expands the value to a non-scientific decimal string
   * whose length grows with {@code |scale|}. A value such as {@code 1e1000000000} would therefore
   * materialize a ~1GB {@link String}, causing {@link OutOfMemoryError} / denial of service on the
   * client. Any scale beyond this bound is rejected for expansion and the caller gets the scientific
   * form instead, which is a few dozen characters at most.
   */
  static final int MAX_PLAIN_STRING_SCALE = 10_000;

  static {
    for (int i = -128; i <= 127; i++) {
      allBytes[i - -128] = (byte) i;
    }
    String allBytesString = new String(allBytes, 0, 255);
    int allBytesStringLen = allBytesString.length();
    int j = 0;
    for (; j < 255 && j < allBytesStringLen; j++) {
      byteToChars[j] = allBytesString.charAt(j);
    }
    try {
      toPlainStringMethod = BigDecimal.class.getMethod("toPlainString");
    } catch (NoSuchMethodException nsme) {
      LOGGER.warn("To plain String method Error:", nsme);
    }
  }

  public static String consistentToString(BigDecimal decimal) {
    if (decimal == null) {
      return null;
    }
    // Guard against CVE-style DoS: BigDecimal values constructed from extreme scientific
    // notation (e.g. "1e1000000000") have a huge |scale| and would otherwise cause
    // toPlainString() to allocate a multi-GB String and OOM the JVM. Fall back to the
    // scientific form, which is always short.
    if (decimal.scale() > MAX_PLAIN_STRING_SCALE || decimal.scale() < -MAX_PLAIN_STRING_SCALE) {
      return decimal.toString();
    }
    if (toPlainStringMethod != null) {
      try {
        return (String) toPlainStringMethod.invoke(decimal, (Object[]) null);
      } catch (InvocationTargetException | IllegalAccessException e) {
        LOGGER.warn("consistent to String Error:", e);
      }
    }
    return decimal.toString();
  }

  public static final String fixDecimalExponent(String dString) {
    int ePos = dString.indexOf("E");
    if (ePos == -1) {
      ePos = dString.indexOf("e");
    }
    if (ePos != -1 && dString.length() > ePos + 1) {
      char maybeMinusChar = dString.charAt(ePos + 1);
      if (maybeMinusChar != '-' && maybeMinusChar != '+') {
        StringBuilder buf = new StringBuilder(dString.length() + 1);
        buf.append(dString.substring(0, ePos + 1));
        buf.append('+');
        buf.append(dString.substring(ePos + 1, dString.length()));
        dString = buf.toString();
      }
    }
    return dString;
  }

  private StringUtils() {}
}
