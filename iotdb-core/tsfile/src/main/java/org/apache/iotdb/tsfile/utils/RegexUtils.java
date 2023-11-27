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

package org.apache.iotdb.tsfile.utils;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexUtils {

  private RegexUtils() {
    // util class
  }

  /**
   * The main idea of this part comes from
   * https://codereview.stackexchange.com/questions/36861/convert-sql-like-to-regex/36864
   */
  public static String parseLikePatternToRegex(String likePattern) {
    String unescapeValue = unescapeString(likePattern);
    String specialRegexStr = ".^$*+?{}[]|()";
    StringBuilder patternStrBuild = new StringBuilder();
    patternStrBuild.append("^");
    for (int i = 0; i < unescapeValue.length(); i++) {
      String ch = String.valueOf(unescapeValue.charAt(i));
      if (specialRegexStr.contains(ch)) {
        ch = "\\" + unescapeValue.charAt(i);
      }
      if (i == 0
          || !"\\".equals(String.valueOf(unescapeValue.charAt(i - 1)))
          || i >= 2
              && "\\\\"
                  .equals(
                      patternStrBuild.substring(
                          patternStrBuild.length() - 2, patternStrBuild.length()))) {
        String replaceStr = ch.replace("%", ".*?").replace("_", ".");
        patternStrBuild.append(replaceStr);
      } else {
        patternStrBuild.append(ch);
      }
    }
    patternStrBuild.append("$");
    return patternStrBuild.toString();
  }

  // This Method is for un-escaping strings except '\' before special string '%', '_', '\', because
  // we need to use '\' to judge whether to replace this to regexp string
  private static String unescapeString(String value) {
    StringBuilder stringBuilder = new StringBuilder();
    int curIndex = 0;
    while (curIndex < value.length()) {
      String ch = String.valueOf(value.charAt(curIndex));
      if ("\\".equals(ch) && curIndex < value.length() - 1) {
        String nextChar = String.valueOf(value.charAt(curIndex + 1));
        if ("%".equals(nextChar) || "_".equals(nextChar) || "\\".equals(nextChar)) {
          stringBuilder.append(ch);
          if ("\\".equals(nextChar)) {
            curIndex++;
          }
        }
      } else {
        stringBuilder.append(ch);
      }
      curIndex++;
    }
    return stringBuilder.toString();
  }

  public static Pattern compileRegex(String regex) {
    try {
      return Pattern.compile(regex);
    } catch (PatternSyntaxException e) {
      throw new PatternSyntaxException("Illegal regex expression: ", regex, e.getIndex());
    }
  }
}
