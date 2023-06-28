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

package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.Between;
import org.apache.iotdb.tsfile.read.filter.operator.Eq;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.iotdb.tsfile.read.filter.operator.Like;
import org.apache.iotdb.tsfile.read.filter.operator.Lt;
import org.apache.iotdb.tsfile.read.filter.operator.LtEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotEq;
import org.apache.iotdb.tsfile.read.filter.operator.Regexp;

import java.util.Set;

public class ValueFilter {

  private ValueFilter() {}

  public static <T extends Comparable<T>> ValueGt<T> gt(T value) {
    return new ValueGt<>(value);
  }

  public static <T extends Comparable<T>> ValueGtEq<T> gtEq(T value) {
    return new ValueGtEq<>(value);
  }

  public static <T extends Comparable<T>> ValueLt<T> lt(T value) {
    return new ValueLt<>(value);
  }

  public static <T extends Comparable<T>> ValueLtEq<T> ltEq(T value) {
    return new ValueLtEq<>(value);
  }

  public static <T extends Comparable<T>> ValueEq<T> eq(T value) {
    return new ValueEq<>(value);
  }

  public static <T extends Comparable<T>> ValueNotEq<T> notEq(T value) {
    return new ValueNotEq<>(value);
  }

  public static <T extends Comparable<T>> ValueBetween<T> between(T value1, T value2) {
    return new ValueBetween<>(value1, value2, false);
  }

  public static <T extends Comparable<T>> ValueBetween<T> notBetween(T value1, T value2) {
    return new ValueBetween<>(value1, value2, true);
  }

  public static <T extends Comparable<T>> ValueLike<T> like(String value) {
    return new ValueLike<>(value, false);
  }

  public static <T extends Comparable<T>> ValueLike<T> notLike(String value) {
    return new ValueLike<>(value, true);
  }

  public static <T extends Comparable<T>> ValueRegexp<T> regexp(String value) {
    return new ValueRegexp<>(value, false);
  }

  public static <T extends Comparable<T>> ValueRegexp<T> notRegexp(String value) {
    return new ValueRegexp<>(value, true);
  }

  public static <T extends Comparable<T>> ValueIn<T> in(Set<T> values) {
    return new ValueIn<>(values, false);
  }

  public static <T extends Comparable<T>> ValueIn<T> notIn(Set<T> values) {
    return new ValueIn<>(values, true);
  }

  public static class ValueGt<T extends Comparable<T>> extends Gt<T> {

    private ValueGt(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class ValueGtEq<T extends Comparable<T>> extends GtEq<T> {

    private ValueGtEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class ValueLt<T extends Comparable<T>> extends Lt<T> {

    private ValueLt(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class ValueLtEq<T extends Comparable<T>> extends LtEq<T> {

    private ValueLtEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class ValueEq<T extends Comparable<T>> extends Eq<T> {

    private ValueEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class ValueNotEq<T extends Comparable<T>> extends NotEq<T> {

    private ValueNotEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class ValueBetween<T extends Comparable<T>> extends Between<T> {

    private ValueBetween(T value1, T value2, boolean not) {
      super(value1, value2, FilterType.VALUE_FILTER, not);
    }
  }

  public static class ValueLike<T extends Comparable<T>> extends Like<T> {

    private ValueLike(String value, boolean not) {
      super(value, FilterType.VALUE_FILTER, not);
    }
  }

  public static class ValueRegexp<T extends Comparable<T>> extends Regexp<T> {
    private ValueRegexp(String value, boolean not) {
      super(value, FilterType.VALUE_FILTER, not);
    }
  }

  public static class ValueIn<T extends Comparable<T>> extends In<T> {

    private ValueIn(Set<T> values, boolean not) {
      super(values, FilterType.VALUE_FILTER, not);
    }
  }
}
