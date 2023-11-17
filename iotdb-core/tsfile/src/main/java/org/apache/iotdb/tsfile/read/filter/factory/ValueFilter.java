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
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueLike;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueLt;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueLtEq;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotBetweenAnd;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotEq;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotIn;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotLike;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueNotRegexp;
import org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.ValueRegexp;

import java.util.Set;

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

  public static ValueLike like(String value) {
    return new ValueLike(FAKE_MEASUREMENT, value);
  }

  public static ValueNotLike notLike(String value) {
    return new ValueNotLike(FAKE_MEASUREMENT, value);
  }

  public static ValueRegexp regexp(String value) {
    return new ValueRegexp(FAKE_MEASUREMENT, value);
  }

  public static ValueNotRegexp notRegexp(String value) {
    return new ValueNotRegexp(FAKE_MEASUREMENT, value);
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

  public static ValueLike like(String measurement, String value) {
    return new ValueLike(measurement, value);
  }

  public static ValueNotLike notLike(String measurement, String value) {
    return new ValueNotLike(measurement, value);
  }

  public static ValueRegexp regexp(String measurement, String value) {
    return new ValueRegexp(measurement, value);
  }

  public static ValueNotRegexp notRegexp(String measurement, String value) {
    return new ValueNotRegexp(measurement, value);
  }

  public static <T extends Comparable<T>> ValueIn<T> in(String measurement, Set<T> values) {
    return new ValueIn<>(measurement, values);
  }

  public static <T extends Comparable<T>> ValueNotIn<T> notIn(String measurement, Set<T> values) {
    return new ValueNotIn<>(measurement, values);
  }
}
