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
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueIsNotNull;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueIsNull;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueLt;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueLtEq;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotBetweenAnd;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotEq;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotIn;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotRegexp;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueRegexp;
import org.apache.iotdb.tsfile.utils.RegexUtils;

import java.util.Set;
import java.util.regex.Pattern;

public class ValueFilterApi {

  private ValueFilterApi() {
    // forbidden construction
  }

  private static final int DEFAULT_MEASUREMENT_INDEX = 0;

  public static <T extends Comparable<T>> ValueGt<T> gt(T value) {
    return new ValueGt<>(DEFAULT_MEASUREMENT_INDEX, value);
  }

  public static <T extends Comparable<T>> ValueGt<T> gt(int measurementIndex, T value) {
    return new ValueGt<>(measurementIndex, value);
  }

  public static <T extends Comparable<T>> ValueGtEq<T> gtEq(T value) {
    return new ValueGtEq<>(DEFAULT_MEASUREMENT_INDEX, value);
  }

  public static <T extends Comparable<T>> ValueGtEq<T> gtEq(int measurementIndex, T value) {
    return new ValueGtEq<>(measurementIndex, value);
  }

  public static <T extends Comparable<T>> ValueLt<T> lt(T value) {
    return new ValueLt<>(DEFAULT_MEASUREMENT_INDEX, value);
  }

  public static <T extends Comparable<T>> ValueLt<T> lt(int measurementIndex, T value) {
    return new ValueLt<>(measurementIndex, value);
  }

  public static <T extends Comparable<T>> ValueLtEq<T> ltEq(T value) {
    return new ValueLtEq<>(DEFAULT_MEASUREMENT_INDEX, value);
  }

  public static <T extends Comparable<T>> ValueLtEq<T> ltEq(int measurementIndex, T value) {
    return new ValueLtEq<>(measurementIndex, value);
  }

  public static <T extends Comparable<T>> ValueEq<T> eq(T value) {
    return new ValueEq<>(DEFAULT_MEASUREMENT_INDEX, value);
  }

  public static <T extends Comparable<T>> ValueEq<T> eq(int measurementIndex, T value) {
    return new ValueEq<>(measurementIndex, value);
  }

  public static <T extends Comparable<T>> ValueNotEq<T> notEq(T value) {
    return new ValueNotEq<>(DEFAULT_MEASUREMENT_INDEX, value);
  }

  public static <T extends Comparable<T>> ValueNotEq<T> notEq(int measurementIndex, T value) {
    return new ValueNotEq<>(measurementIndex, value);
  }

  public static ValueIsNull isNull() {
    return new ValueIsNull(DEFAULT_MEASUREMENT_INDEX);
  }

  public static ValueIsNull isNull(int measurementIndex) {
    return new ValueIsNull(measurementIndex);
  }

  public static ValueIsNotNull isNotNull() {
    return new ValueIsNotNull(DEFAULT_MEASUREMENT_INDEX);
  }

  public static ValueIsNotNull isNotNull(int measurementIndex) {
    return new ValueIsNotNull(measurementIndex);
  }

  public static <T extends Comparable<T>> ValueBetweenAnd<T> between(T value1, T value2) {
    return new ValueBetweenAnd<>(DEFAULT_MEASUREMENT_INDEX, value1, value2);
  }

  public static <T extends Comparable<T>> ValueBetweenAnd<T> between(
      int measurementIndex, T value1, T value2) {
    return new ValueBetweenAnd<>(measurementIndex, value1, value2);
  }

  public static <T extends Comparable<T>> ValueNotBetweenAnd<T> notBetween(T value1, T value2) {
    return new ValueNotBetweenAnd<>(DEFAULT_MEASUREMENT_INDEX, value1, value2);
  }

  public static <T extends Comparable<T>> ValueNotBetweenAnd<T> notBetween(
      int measurementIndex, T value1, T value2) {
    return new ValueNotBetweenAnd<>(measurementIndex, value1, value2);
  }

  public static ValueRegexp like(String likePattern) {
    return regexp(DEFAULT_MEASUREMENT_INDEX, RegexUtils.parseLikePatternToRegex(likePattern));
  }

  public static ValueRegexp like(int measurementIndex, String likePattern) {
    return regexp(measurementIndex, RegexUtils.parseLikePatternToRegex(likePattern));
  }

  public static ValueNotRegexp notLike(String likePattern) {
    return notRegexp(DEFAULT_MEASUREMENT_INDEX, RegexUtils.parseLikePatternToRegex(likePattern));
  }

  public static ValueNotRegexp notLike(int measurementIndex, String likePattern) {
    return notRegexp(measurementIndex, RegexUtils.parseLikePatternToRegex(likePattern));
  }

  public static ValueRegexp regexp(String regex) {
    return new ValueRegexp(DEFAULT_MEASUREMENT_INDEX, RegexUtils.compileRegex(regex));
  }

  public static ValueRegexp regexp(int measurementIndex, String regex) {
    return new ValueRegexp(measurementIndex, RegexUtils.compileRegex(regex));
  }

  public static ValueRegexp regexp(int measurementIndex, Pattern pattern) {
    return new ValueRegexp(measurementIndex, pattern);
  }

  public static ValueNotRegexp notRegexp(String regex) {
    return new ValueNotRegexp(DEFAULT_MEASUREMENT_INDEX, RegexUtils.compileRegex(regex));
  }

  public static ValueNotRegexp notRegexp(int measurementIndex, String regex) {
    return new ValueNotRegexp(measurementIndex, RegexUtils.compileRegex(regex));
  }

  public static ValueNotRegexp notRegexp(int measurementIndex, Pattern pattern) {
    return new ValueNotRegexp(measurementIndex, pattern);
  }

  public static <T extends Comparable<T>> ValueIn<T> in(Set<T> values) {
    return new ValueIn<>(DEFAULT_MEASUREMENT_INDEX, values);
  }

  public static <T extends Comparable<T>> ValueIn<T> in(int measurementIndex, Set<T> values) {
    return new ValueIn<>(measurementIndex, values);
  }

  public static <T extends Comparable<T>> ValueNotIn<T> notIn(Set<T> values) {
    return new ValueNotIn<>(DEFAULT_MEASUREMENT_INDEX, values);
  }

  public static <T extends Comparable<T>> ValueNotIn<T> notIn(int measurementIndex, Set<T> values) {
    return new ValueNotIn<>(measurementIndex, values);
  }
}
