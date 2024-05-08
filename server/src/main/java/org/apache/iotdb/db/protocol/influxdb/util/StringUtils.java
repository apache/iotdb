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
package org.apache.iotdb.db.protocol.influxdb.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringUtils {

  /**
   * if the first and last of the current string are quotation marks, they are removed
   *
   * @param str string to process
   * @return string after processing
   */
  public static String removeQuotation(String str) {
    if ((str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"')
        || str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'') {
      return str.substring(1, str.length() - 1);
    }
    return str;
  }

  /**
   * remove string list duplicate names
   *
   * @param strings the list of strings with duplicate names needs to be removed
   * @return list of de duplicated strings
   */
  public static List<String> removeDuplicate(List<String> strings) {
    Map<String, Integer> nameNums = new HashMap<>();
    List<String> result = new ArrayList<>();
    for (String tmpString : strings) {
      if (!nameNums.containsKey(tmpString)) {
        nameNums.put(tmpString, 1);
        result.add(tmpString);
      } else {
        int nums = nameNums.get(tmpString);
        result.add(tmpString + "_" + nums);
        nameNums.put(tmpString, nums + 1);
      }
    }
    return result;
  }

  /**
   * get the last node through the path in iotdb
   *
   * @param path path to process
   * @return last node
   */
  public static String getFieldByPath(String path) {
    String[] tmpList = path.split("\\.");
    return tmpList[tmpList.length - 1];
  }

  /**
   * get the devicePath through the fullPath
   *
   * @param path path to process
   * @return devicePath
   */
  public static String getDeviceByPath(String path) {
    String field = getFieldByPath(path);
    return path.substring(0, path.length() - field.length() - 1);
  }

  /**
   * determine whether the two string lists are the same
   *
   * @param list1 first list to compare
   * @param list2 second list to compare
   * @return Is it the same
   */
  public static boolean checkSameStringList(List<String> list1, List<String> list2) {
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (int i = 0; i < list1.size(); i++) {
        if (!list1.get(i).equals(list2.get(i))) {
          return false;
        }
      }
    }
    return true;
  }

  public static String generateFunctionSql(String functionName, String parameter, String path) {
    return String.format("select %s(%s) from %s.**", functionName, parameter, path);
  }
}
