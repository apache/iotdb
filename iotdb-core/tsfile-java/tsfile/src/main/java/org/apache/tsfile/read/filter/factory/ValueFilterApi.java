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

package org.apache.tsfile.read.filter.factory;

import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.BinaryFilterOperators;
import org.apache.tsfile.read.filter.operator.BooleanFilterOperators;
import org.apache.tsfile.read.filter.operator.DoubleFilterOperators;
import org.apache.tsfile.read.filter.operator.FloatFilterOperators;
import org.apache.tsfile.read.filter.operator.IntegerFilterOperators;
import org.apache.tsfile.read.filter.operator.LongFilterOperators;
import org.apache.tsfile.read.filter.operator.StringFilterOperators;
import org.apache.tsfile.read.filter.operator.ValueIsNotNullOperator;
import org.apache.tsfile.read.filter.operator.ValueIsNullOperator;
import org.apache.tsfile.utils.Binary;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public class ValueFilterApi {

  public static final int DEFAULT_MEASUREMENT_INDEX = 0;

  private static final String CONSTANT_CANNOT_BE_NULL_MSG = " constant cannot be null";
  public static final String CANNOT_PUSH_DOWN_MSG = " operator can not be pushed down.";

  private ValueFilterApi() {
    // forbidden construction
  }

  public static Filter gt(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueGt(measurementIndex, (boolean) value);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueGt(measurementIndex, (int) value);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueGt(measurementIndex, (long) value);
      case DOUBLE:
        return new DoubleFilterOperators.ValueGt(measurementIndex, (double) value);
      case FLOAT:
        return new FloatFilterOperators.ValueGt(measurementIndex, (float) value);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueGt(measurementIndex, (Binary) value);
      case STRING:
        return new StringFilterOperators.ValueGt(measurementIndex, (Binary) value);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter gtEq(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueGtEq(measurementIndex, (boolean) value);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueGtEq(measurementIndex, (int) value);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueGtEq(measurementIndex, (long) value);
      case DOUBLE:
        return new DoubleFilterOperators.ValueGtEq(measurementIndex, (double) value);
      case FLOAT:
        return new FloatFilterOperators.ValueGtEq(measurementIndex, (float) value);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueGtEq(measurementIndex, (Binary) value);
      case STRING:
        return new StringFilterOperators.ValueGtEq(measurementIndex, (Binary) value);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter lt(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueLt(measurementIndex, (boolean) value);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueLt(measurementIndex, (int) value);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueLt(measurementIndex, (long) value);
      case DOUBLE:
        return new DoubleFilterOperators.ValueLt(measurementIndex, (double) value);
      case FLOAT:
        return new FloatFilterOperators.ValueLt(measurementIndex, (float) value);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueLt(measurementIndex, (Binary) value);
      case STRING:
        return new StringFilterOperators.ValueLt(measurementIndex, (Binary) value);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter ltEq(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueLtEq(measurementIndex, (boolean) value);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueLtEq(measurementIndex, (int) value);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueLtEq(measurementIndex, (long) value);
      case DOUBLE:
        return new DoubleFilterOperators.ValueLtEq(measurementIndex, (double) value);
      case FLOAT:
        return new FloatFilterOperators.ValueLtEq(measurementIndex, (float) value);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueLtEq(measurementIndex, (Binary) value);
      case STRING:
        return new StringFilterOperators.ValueLtEq(measurementIndex, (Binary) value);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter eq(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueEq(measurementIndex, (boolean) value);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueEq(measurementIndex, (int) value);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueEq(measurementIndex, (long) value);
      case DOUBLE:
        return new DoubleFilterOperators.ValueEq(measurementIndex, (double) value);
      case FLOAT:
        return new FloatFilterOperators.ValueEq(measurementIndex, (float) value);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueEq(measurementIndex, (Binary) value);
      case STRING:
        return new StringFilterOperators.ValueEq(measurementIndex, (Binary) value);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter notEq(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotEq(measurementIndex, (boolean) value);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueNotEq(measurementIndex, (int) value);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueNotEq(measurementIndex, (long) value);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotEq(measurementIndex, (double) value);
      case FLOAT:
        return new FloatFilterOperators.ValueNotEq(measurementIndex, (float) value);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueNotEq(measurementIndex, (Binary) value);
      case STRING:
        return new StringFilterOperators.ValueNotEq(measurementIndex, (Binary) value);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter isNull(int measurementIndex) {
    return new ValueIsNullOperator(measurementIndex);
  }

  public static Filter isNotNull(int measurementIndex) {
    return new ValueIsNotNullOperator(measurementIndex);
  }

  public static Filter between(
      int measurementIndex, Object value1, Object value2, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value1, CONSTANT_CANNOT_BE_NULL_MSG);
    Objects.requireNonNull(value2, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueBetweenAnd(
            measurementIndex, (boolean) value1, (boolean) value2);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueBetweenAnd(
            measurementIndex, (int) value1, (int) value2);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueBetweenAnd(
            measurementIndex, (long) value1, (long) value2);
      case DOUBLE:
        return new DoubleFilterOperators.ValueBetweenAnd(
            measurementIndex, (double) value1, (double) value2);
      case FLOAT:
        return new FloatFilterOperators.ValueBetweenAnd(
            measurementIndex, (float) value1, (float) value2);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueBetweenAnd(
            measurementIndex, (Binary) value1, (Binary) value2);
      case STRING:
        return new StringFilterOperators.ValueBetweenAnd(
            measurementIndex, (Binary) value1, (Binary) value2);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter notBetween(
      int measurementIndex, Object value1, Object value2, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value1, CONSTANT_CANNOT_BE_NULL_MSG);
    Objects.requireNonNull(value2, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotBetweenAnd(
            measurementIndex, (boolean) value1, (boolean) value2);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueNotBetweenAnd(
            measurementIndex, (int) value1, (int) value2);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueNotBetweenAnd(
            measurementIndex, (long) value1, (long) value2);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotBetweenAnd(
            measurementIndex, (double) value1, (double) value2);
      case FLOAT:
        return new FloatFilterOperators.ValueNotBetweenAnd(
            measurementIndex, (float) value1, (float) value2);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueNotBetweenAnd(
            measurementIndex, (Binary) value1, (Binary) value2);
      case STRING:
        return new StringFilterOperators.ValueNotBetweenAnd(
            measurementIndex, (Binary) value1, (Binary) value2);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter like(int measurementIndex, LikePattern pattern, TSDataType type) {
    Objects.requireNonNull(pattern, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueLike(measurementIndex, pattern);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueLike(measurementIndex, pattern);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueLike(measurementIndex, pattern);
      case DOUBLE:
        return new DoubleFilterOperators.ValueLike(measurementIndex, pattern);
      case FLOAT:
        return new FloatFilterOperators.ValueLike(measurementIndex, pattern);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueLike(measurementIndex, pattern);
      case STRING:
        return new StringFilterOperators.ValueLike(measurementIndex, pattern);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter notLike(int measurementIndex, LikePattern pattern, TSDataType type) {
    Objects.requireNonNull(pattern, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotLike(measurementIndex, pattern);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueNotLike(measurementIndex, pattern);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueNotLike(measurementIndex, pattern);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotLike(measurementIndex, pattern);
      case FLOAT:
        return new FloatFilterOperators.ValueNotLike(measurementIndex, pattern);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueNotLike(measurementIndex, pattern);
      case STRING:
        return new StringFilterOperators.ValueNotLike(measurementIndex, pattern);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter regexp(int measurementIndex, Pattern pattern, TSDataType type) {
    Objects.requireNonNull(pattern, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueRegexp(measurementIndex, pattern);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueRegexp(measurementIndex, pattern);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueRegexp(measurementIndex, pattern);
      case DOUBLE:
        return new DoubleFilterOperators.ValueRegexp(measurementIndex, pattern);
      case FLOAT:
        return new FloatFilterOperators.ValueRegexp(measurementIndex, pattern);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueRegexp(measurementIndex, pattern);
      case STRING:
        return new StringFilterOperators.ValueRegexp(measurementIndex, pattern);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static Filter notRegexp(int measurementIndex, Pattern pattern, TSDataType type) {
    Objects.requireNonNull(pattern, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotRegexp(measurementIndex, pattern);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueNotRegexp(measurementIndex, pattern);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueNotRegexp(measurementIndex, pattern);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotRegexp(measurementIndex, pattern);
      case FLOAT:
        return new FloatFilterOperators.ValueNotRegexp(measurementIndex, pattern);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueNotRegexp(measurementIndex, pattern);
      case STRING:
        return new StringFilterOperators.ValueNotRegexp(measurementIndex, pattern);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static <T extends Comparable<T>> Filter in(
      int measurementIndex, Set<T> values, TSDataType type) {
    // constants cannot be null
    Objects.requireNonNull(values, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueIn(measurementIndex, (Set<Boolean>) values);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueIn(measurementIndex, (Set<Integer>) values);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueIn(measurementIndex, (Set<Long>) values);
      case FLOAT:
        return new FloatFilterOperators.ValueIn(measurementIndex, (Set<Float>) values);
      case DOUBLE:
        return new DoubleFilterOperators.ValueIn(measurementIndex, (Set<Double>) values);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueIn(measurementIndex, (Set<Binary>) values);
      case STRING:
        return new StringFilterOperators.ValueIn(measurementIndex, (Set<Binary>) values);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  public static <T extends Comparable<T>> Filter notIn(
      int measurementIndex, Set<T> values, TSDataType type) {
    // constants cannot be null
    Objects.requireNonNull(values, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotIn(measurementIndex, (Set<Boolean>) values);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.ValueNotIn(measurementIndex, (Set<Integer>) values);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.ValueNotIn(measurementIndex, (Set<Long>) values);
      case FLOAT:
        return new FloatFilterOperators.ValueNotIn(measurementIndex, (Set<Float>) values);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotIn(measurementIndex, (Set<Double>) values);
      case TEXT:
      case BLOB:
        return new BinaryFilterOperators.ValueNotIn(measurementIndex, (Set<Binary>) values);
      case STRING:
        return new StringFilterOperators.ValueNotIn(measurementIndex, (Set<Binary>) values);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }
}
