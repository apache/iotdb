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

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.Eq;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.iotdb.tsfile.read.filter.operator.Like;
import org.apache.iotdb.tsfile.read.filter.operator.Lt;
import org.apache.iotdb.tsfile.read.filter.operator.LtEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;
import org.apache.iotdb.tsfile.read.filter.operator.Regexp;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.Set;

public class ValueFilter {

  private ValueFilter() {}

  public static <T extends Comparable<T>> ValueEq<T> eq(T value) {
    return new ValueEq(value);
  }

  public static <T extends Comparable<T>> ValueGt<T> gt(T value) {
    return new ValueGt(value);
  }

  public static <T extends Comparable<T>> ValueGtEq<T> gtEq(T value) {
    return new ValueGtEq(value);
  }

  public static <T extends Comparable<T>> ValueLt<T> lt(T value) {
    return new ValueLt(value);
  }

  public static <T extends Comparable<T>> ValueLtEq<T> ltEq(T value) {
    return new ValueLtEq(value);
  }

  public static <T extends Comparable<T>> ValueIn<T> in(Set<T> values, boolean not) {
    return new ValueIn(values, not);
  }

  public static ValueNotFilter not(Filter filter) {
    return new ValueNotFilter(filter);
  }

  public static <T extends Comparable<T>> ValueNotEq<T> notEq(T value) {
    return new ValueNotEq(value);
  }

  public static <T extends Comparable<T>> ValueRegexp<T> regexp(String value) {
    return new ValueRegexp(value);
  }

  public static <T extends Comparable<T>> ValueLike<T> like(String value) {
    return new ValueLike(value);
  }

  public static class ValueIn<T extends Comparable<T>> extends In<T> {

    private ValueIn(Set<T> values, boolean not) {
      super(values, FilterType.VALUE_FILTER, not);
    }
  }

  public static class VectorValueIn<T extends Comparable<T>> extends ValueIn<T> {

    private final int index;

    private VectorValueIn(Set<T> values, boolean not, int index) {
      super(values, not);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return this.values.contains(v) != not;
    }
  }

  public static class ValueEq<T extends Comparable<T>> extends Eq<T> {

    private ValueEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class VectorValueEq<T extends Comparable<T>> extends ValueEq<T> {

    private final int index;

    private VectorValueEq(T value, int index) {
      super(value);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return this.value.equals(v);
    }
  }

  public static class ValueGt<T extends Comparable<T>> extends Gt<T> {

    private ValueGt(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class VectorValueGt<T extends Comparable<T>> extends ValueGt<T> {

    private final int index;

    private VectorValueGt(T value, int index) {
      super(value);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return this.value.compareTo((T) v) < 0;
    }
  }

  public static class ValueGtEq<T extends Comparable<T>> extends GtEq<T> {

    private ValueGtEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class VectorValueGtEq<T extends Comparable<T>> extends ValueGtEq<T> {

    private final int index;

    private VectorValueGtEq(T value, int index) {
      super(value);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return this.value.compareTo((T) v) <= 0;
    }
  }

  public static class ValueLt<T extends Comparable<T>> extends Lt<T> {

    private ValueLt(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class VectorValueLt<T extends Comparable<T>> extends ValueLt<T> {

    private final int index;

    private VectorValueLt(T value, int index) {
      super(value);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return this.value.compareTo((T) v) > 0;
    }
  }

  public static class ValueLtEq<T extends Comparable<T>> extends LtEq<T> {

    private ValueLtEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class VectorValueLtEq<T extends Comparable<T>> extends ValueLtEq<T> {

    private final int index;

    private VectorValueLtEq(T value, int index) {
      super(value);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return this.value.compareTo((T) v) >= 0;
    }
  }

  public static class ValueNotFilter extends NotFilter {

    private ValueNotFilter(Filter filter) {
      super(filter);
    }

    @Override
    public String toString() {
      return FilterType.VALUE_FILTER + super.toString();
    }
  }

  public static class ValueNotEq<T extends Comparable<T>> extends NotEq<T> {

    private ValueNotEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class VectorValueNotEq<T extends Comparable<T>> extends ValueNotEq<T> {

    private final int index;

    private VectorValueNotEq(T value, int index) {
      super(value);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return !this.value.equals(v);
    }
  }

  public static class ValueRegexp<T extends Comparable<T>> extends Regexp<T> {

    private ValueRegexp(String value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class VectorValueRegexp<T extends Comparable<T>> extends ValueRegexp<T> {

    private final int index;

    private VectorValueRegexp(String value, int index) {
      super(value);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return this.value.equals(v);
    }
  }

  public static class ValueLike<T extends Comparable<T>> extends Like<T> {

    private ValueLike(String value) {
      super(value, FilterType.VALUE_FILTER);
    }
  }

  public static class VectorValueLike<T extends Comparable<T>> extends ValueLike<T> {

    private final int index;

    private VectorValueLike(String value, int index) {
      super(value);
      this.index = index;
    }

    public boolean satisfy(long time, TsPrimitiveType[] values) {
      Object v = filterType == FilterType.TIME_FILTER ? time : values[index].getValue();
      return this.value.equals(v);
    }
  }
}
