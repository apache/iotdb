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

import org.apache.tsfile.external.commons.lang3.SystemUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class WindowsOSUtils {
  private static final String ILLEGAL_WINDOWS_CHARS = "\\/:*?\"<>|";
  private static final Set<String> ILLEGAL_WINDOWS_NAMES =
      new HashSet<>(Arrays.asList("CON", "PRN", "AUX", "NUL", "COM1-COM9, LPT1-LPT9"));

  static {
    for (int i = 0; i < 10; ++i) {
      ILLEGAL_WINDOWS_NAMES.add("COM" + i);
      ILLEGAL_WINDOWS_NAMES.add("LPT" + i);
    }
  }

  public static final String OS_SEGMENT_ERROR =
      String.format(
          "In Windows System, the path shall not contains %s, equals one of %s, or ends with '.' or ' '.",
          ILLEGAL_WINDOWS_CHARS, ILLEGAL_WINDOWS_NAMES);

  public static boolean isLegalPathSegment4Windows(final String pathSegment) {
    if (!SystemUtils.IS_OS_WINDOWS) {
      return true;
    }
    for (final char illegalChar : ILLEGAL_WINDOWS_CHARS.toCharArray()) {
      if (pathSegment.indexOf(illegalChar) != -1) {
        return false;
      }
    }
    if (pathSegment.endsWith(".") || pathSegment.endsWith(" ")) {
      return false;
    }
    for (final String illegalName : ILLEGAL_WINDOWS_NAMES) {
      if (pathSegment.equalsIgnoreCase(illegalName)) {
        return false;
      }
    }
    return true;
  }
}
