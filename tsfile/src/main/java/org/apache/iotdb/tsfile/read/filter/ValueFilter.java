/**
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
import org.apache.iotdb.tsfile.read.filter.operator.Lt;
import org.apache.iotdb.tsfile.read.filter.operator.LtEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;

public class ValueFilter {

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

  public static ValueNotFilter not(Filter filter) {
    return new ValueNotFilter(filter);
  }

  public static <T extends Comparable<T>> ValueNotEq<T> notEq(T value) {
    return new ValueNotEq(value);
  }

  public static class ValueEq<T extends Comparable<T>> extends Eq<T> {

    private ValueEq(T value) {
      super(value, FilterType.VALUE_FILTER);
    }
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
}
