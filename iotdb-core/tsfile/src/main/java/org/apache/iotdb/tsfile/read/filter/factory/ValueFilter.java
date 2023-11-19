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

package org.apache.iotdb.tsfile.read.filter.factory;

import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueBetweenAnd;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueEq;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueGt;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueGtEq;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueIn;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueLt;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueLtEq;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotBetweenAnd;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotEq;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotIn;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotRegexp;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueRegexp;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class ValueFilter {

  private ValueFilter() {
    // forbidden construction
  }

  private static final String FAKE_MEASUREMENT = "";

  public static <T extends Comparable<T>> ValueGt<T> gt(T value) {
    return new ValueGt<>(FAKE_MEASUREMENT, value);
  }

  public static <T extends Comparable<T>> ValueGtEq<T> gtEq(T value) {
    return new ValueGtEq<>(FAKE_MEASUREMENT, value);
  }

  public static <T extends Comparable<T>> ValueLt<T> lt(T value) {
    return new ValueLt<>(FAKE_MEASUREMENT, value);
  }

  public static <T extends Comparable<T>> ValueLtEq<T> ltEq(T value) {
    return new ValueLtEq<>(FAKE_MEASUREMENT, value);
  }

  public static <T extends Comparable<T>> ValueEq<T> eq(T value) {
    return new ValueEq<>(FAKE_MEASUREMENT, value);
  }

  public static <T extends Comparable<T>> ValueNotEq<T> notEq(T value) {
    return new ValueNotEq<>(FAKE_MEASUREMENT, value);
  }

  public static <T extends Comparable<T>> ValueBetweenAnd<T> between(T value1, T value2) {
    return new ValueBetweenAnd<>(FAKE_MEASUREMENT, value1, value2);
  }

  public static <T extends Comparable<T>> ValueNotBetweenAnd<T> notBetween(T value1, T value2) {
    return new ValueNotBetweenAnd<>(FAKE_MEASUREMENT, value1, value2);
  }

  public static ValueRegexp like(String likePattern) {
    return regexp(FAKE_MEASUREMENT, parseLikePatternToRegex(likePattern));
  }

  public static ValueNotRegexp notLike(String likePattern) {
    return notRegexp(FAKE_MEASUREMENT, parseLikePatternToRegex(likePattern));
  }

  public static ValueRegexp regexp(String regex) {
    return new ValueRegexp(FAKE_MEASUREMENT, compileRegex(regex));
  }

  public static ValueNotRegexp notRegexp(String regex) {
    return new ValueNotRegexp(FAKE_MEASUREMENT, compileRegex(regex));
  }

  public static <T extends Comparable<T>> ValueIn<T> in(Set<T> values) {
    return new ValueIn<>(FAKE_MEASUREMENT, values);
  }

  public static <T extends Comparable<T>> ValueNotIn<T> notIn(Set<T> values) {
    return new ValueNotIn<>(FAKE_MEASUREMENT, values);
  }

  public static <T extends Comparable<T>> ValueGt<T> gt(String measurement, T value) {
    return new ValueGt<>(measurement, value);
  }

  public static <T extends Comparable<T>> ValueGtEq<T> gtEq(String measurement, T value) {
    return new ValueGtEq<>(measurement, value);
  }

  public static <T extends Comparable<T>> ValueLt<T> lt(String measurement, T value) {
    return new ValueLt<>(measurement, value);
  }

  public static <T extends Comparable<T>> ValueLtEq<T> ltEq(String measurement, T value) {
    return new ValueLtEq<>(measurement, value);
  }

  public static <T extends Comparable<T>> ValueEq<T> eq(String measurement, T value) {
    return new ValueEq<>(measurement, value);
  }

  public static <T extends Comparable<T>> ValueNotEq<T> notEq(String measurement, T value) {
    return new ValueNotEq<>(measurement, value);
  }

  public static <T extends Comparable<T>> ValueBetweenAnd<T> between(
      String measurement, T value1, T value2) {
    return new ValueBetweenAnd<>(measurement, value1, value2);
  }

  public static <T extends Comparable<T>> ValueNotBetweenAnd<T> notBetween(
      String measurement, T value1, T value2) {
    return new ValueNotBetweenAnd<>(measurement, value1, value2);
  }

  public static ValueRegexp like(String measurement, String likePattern) {
    return regexp(measurement, parseLikePatternToRegex(likePattern));
  }

  public static ValueNotRegexp notLike(String measurement, String likePattern) {
    return notRegexp(measurement, parseLikePatternToRegex(likePattern));
  }

  public static ValueRegexp regexp(String measurement, String regex) {
    return new ValueRegexp(measurement, compileRegex(regex));
  }

  public static ValueNotRegexp notRegexp(String measurement, String regex) {
    return new ValueNotRegexp(measurement, compileRegex(regex));
  }

  public static ValueRegexp regexp(String measurement, Pattern pattern) {
    return new ValueRegexp(measurement, pattern);
  }

  public static ValueNotRegexp notRegexp(String measurement, Pattern pattern) {
    return new ValueNotRegexp(measurement, pattern);
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

  /**
   * This Method is for un-escaping strings except '\' before special string '%', '_', '\', because
   * we need to use '\' to judge whether to replace this to regexp string
   */
  private static String unescapeString(String value) {
    StringBuilder stringBuilder = new StringBuilder();
    int curIndex = 0;
    for (; curIndex < value.length(); curIndex++) {
      String ch = String.valueOf(value.charAt(curIndex));
      if ("\\".equals(ch)) {
        if (curIndex < value.length() - 1) {
          String nextChar = String.valueOf(value.charAt(curIndex + 1));
          if ("%".equals(nextChar) || "_".equals(nextChar) || "\\".equals(nextChar)) {
            stringBuilder.append(ch);
          }
          if ("\\".equals(nextChar)) {
            curIndex++;
          }
        }
      } else {
        stringBuilder.append(ch);
      }
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

  public static <T extends Comparable<T>> ValueIn<T> in(String measurement, Set<T> values) {
    return new ValueIn<>(measurement, values);
  }

  public static <T extends Comparable<T>> ValueNotIn<T> notIn(String measurement, Set<T> values) {
    return new ValueNotIn<>(measurement, values);
  }
}
