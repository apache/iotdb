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
 *
 */

package org.apache.iotdb.db.tools.logvisual;

public class VisualUtils {

  private VisualUtils() {
    throw new UnsupportedOperationException("Initializing a util class");
  }

  public static int[] parseIntArray(String intArrayStr) {
    if (intArrayStr != null) {
      String[] intStrs = intArrayStr.split(",");
      int[] ints = new int[intStrs.length];
      for (int i = 0; i < ints.length; i++) {
        ints[i] = Integer.parseInt(intStrs[i]);
      }
      return ints;
    }
    return null;
  }

  public static String intArrayToString(int[] ints) {
    if (ints == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder(String.valueOf(ints[0]));
    for (int i = 1; i < ints.length; i ++) {
      builder.append(",").append(ints[i]);
    }
    return builder.toString();
  }

  public static boolean strsContains(String[] strings, String target) {
    for (String str : strings) {
      if (str.equals(target)) {
        return true;
      }
    }
    return false;
  }

  public static boolean intsContains(int[] ints, int target) {
    for (int i : ints) {
      if (i == target) {
        return true;
      }
    }
    return false;
  }
}