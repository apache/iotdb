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

import org.apache.commons.lang3.SystemUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class WindowsOSUtils {
  private static final String ILLEGAL_WINDOWS_CHARS = "\\/:*?\"<>|";
  private static final Set<String> ILLEGAL_WINDOWS_NAMES =
      new HashSet<>(
          Arrays.asList(
              "CON",
              "PRN",
              "AUX",
              "NUL",
              "COM\u00B9",
              "COM\u00B2",
              "COM\u00B3",
              "LPT\u00B9",
              "LPT\u00B2",
              "LPT\u00B3"));

  static {
    for (int i = 1; i < 10; ++i) {
      ILLEGAL_WINDOWS_NAMES.add("COM" + i);
      ILLEGAL_WINDOWS_NAMES.add("LPT" + i);
    }
  }

  public static final String OS_SEGMENT_ERROR =
      String.format(
          "In Windows System, the path shall not contain %s or ASCII control characters, equals one of %s with or without an extension, or ends with '.' or ' '.",
          ILLEGAL_WINDOWS_CHARS, ILLEGAL_WINDOWS_NAMES);

  public static boolean isLegalPathSegment4Windows(final String pathSegment) {
    if (!SystemUtils.IS_OS_WINDOWS) {
      return true;
    }
    if (containsIllegalWindowsChar(pathSegment)) {
      return false;
    }
    if (pathSegment.endsWith(".") || pathSegment.endsWith(" ")) {
      return false;
    }
    if (isIllegalWindowsName(pathSegment)) {
      return false;
    }
    return true;
  }

  private static boolean containsIllegalWindowsChar(final String pathSegment) {
    for (int i = 0; i < pathSegment.length(); ++i) {
      final char ch = pathSegment.charAt(i);
      if (ch < ' ' || ILLEGAL_WINDOWS_CHARS.indexOf(ch) != -1) {
        return true;
      }
    }
    return false;
  }

  private static boolean isIllegalWindowsName(final String pathSegment) {
    final int extensionStartIndex = pathSegment.indexOf('.');
    final String nameWithoutExtension =
        extensionStartIndex < 0 ? pathSegment : pathSegment.substring(0, extensionStartIndex);
    return ILLEGAL_WINDOWS_NAMES.contains(nameWithoutExtension.toUpperCase(Locale.ENGLISH));
  }
}
