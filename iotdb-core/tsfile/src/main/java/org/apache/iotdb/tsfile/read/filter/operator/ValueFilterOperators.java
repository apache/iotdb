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

package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.IDisableStatisticsValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.IValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.OperatorType;
import org.apache.iotdb.tsfile.read.filter.operator.base.ColumnCompareFilter;
import org.apache.iotdb.tsfile.read.filter.operator.base.ColumnPatternMatchFilter;
import org.apache.iotdb.tsfile.read.filter.operator.base.ColumnRangeFilter;
import org.apache.iotdb.tsfile.read.filter.operator.base.ColumnSetFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.RegexUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * These are the value column operators in a filter predicate expression tree. They are constructed
 * by using the methods in {@link org.apache.iotdb.tsfile.read.filter.factory.ValueFilter}
 */
public final class ValueFilterOperators {

  private ValueFilterOperators() {
    // forbidden construction
  }

  private static final String MEASUREMENT_CANNOT_BE_NULL_MSG = "measurement cannot be null";
  private static final String CONSTANT_CANNOT_BE_NULL_MSG = "constant cannot be null";

  // base class for ValueEq, ValueNotEq, ValueLt, ValueGt, ValueLtEq, ValueGtEq
  abstract static class ValueColumnCompareFilter<T extends Comparable<T>>
      extends ColumnCompareFilter<T> implements IValueFilter {

    protected final String measurement;

    protected ValueColumnCompareFilter(String measurement, T constant) {
      super(constant);
      this.measurement = Objects.requireNonNull(measurement, MEASUREMENT_CANNOT_BE_NULL_MSG);
    }

    public String getMeasurement() {
      return measurement;
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      ReadWriteIOUtils.write(getOperatorType().ordinal(), outputStream);
      ReadWriteIOUtils.write(measurement, outputStream);
      ReadWriteIOUtils.writeObject(constant, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ValueColumnCompareFilter<?> that = (ValueColumnCompareFilter<?>) o;
      return measurement.equals(that.measurement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(measurement);
    }
  }

  public static final class ValueEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant can be null
    // TODO: consider support IS NULL
    public ValueEq(String measurement, T constant) {
      super(measurement, constant);
    }

    public ValueEq(ByteBuffer buffer) {
      this(ReadWriteIOUtils.readString(buffer), (T) ReadWriteIOUtils.readObject(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.equals(value);
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMinValue()) >= 0
          && constant.compareTo((T) statistics.getMaxValue()) <= 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMinValue()) == 0
          && constant.compareTo((T) statistics.getMaxValue()) == 0;
    }

    @Override
    public Filter reverse() {
      return new ValueNotEq<>(measurement, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_EQ;
    }

    @Override
    public String toString() {
      return measurement + " == " + constant;
    }
  }

  public static final class ValueNotEq<T extends Comparable<T>>
      extends ValueColumnCompareFilter<T> {

    // constant can be null
    // TODO: consider support IS NOT NULL
    public ValueNotEq(String measurement, T constant) {
      super(measurement, constant);
    }

    public ValueNotEq(ByteBuffer buffer) {
      this(ReadWriteIOUtils.readString(buffer), (T) ReadWriteIOUtils.readObject(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !constant.equals(value);
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return !(constant.compareTo((T) statistics.getMinValue()) == 0
          && constant.compareTo((T) statistics.getMaxValue()) == 0);
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMinValue()) < 0
          || constant.compareTo((T) statistics.getMaxValue()) > 0;
    }

    @Override
    public Filter reverse() {
      return new ValueEq<>(measurement, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NEQ;
    }

    @Override
    public String toString() {
      return measurement + " != " + constant;
    }
  }

  public static final class ValueLt<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant cannot be null
    public ValueLt(String measurement, T constant) {
      super(measurement, Objects.requireNonNull(constant, CONSTANT_CANNOT_BE_NULL_MSG));
    }

    public ValueLt(ByteBuffer buffer) {
      this(ReadWriteIOUtils.readString(buffer), (T) ReadWriteIOUtils.readObject(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.compareTo((T) value) > 0;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMinValue()) > 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMaxValue()) > 0;
    }

    @Override
    public Filter reverse() {
      return new ValueGtEq<>(measurement, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LT;
    }

    @Override
    public String toString() {
      return measurement + " < " + constant;
    }
  }

  public static final class ValueLtEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant cannot be null
    public ValueLtEq(String measurement, T constant) {
      super(measurement, Objects.requireNonNull(constant, CONSTANT_CANNOT_BE_NULL_MSG));
    }

    public ValueLtEq(ByteBuffer buffer) {
      this(ReadWriteIOUtils.readString(buffer), (T) ReadWriteIOUtils.readObject(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.compareTo((T) value) >= 0;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMinValue()) >= 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMaxValue()) >= 0;
    }

    @Override
    public Filter reverse() {
      return new ValueGt<>(measurement, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LTEQ;
    }

    @Override
    public String toString() {
      return measurement + " <= " + constant;
    }
  }

  public static final class ValueGt<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant cannot be null
    public ValueGt(String measurement, T constant) {
      super(measurement, Objects.requireNonNull(constant, CONSTANT_CANNOT_BE_NULL_MSG));
    }

    public ValueGt(ByteBuffer buffer) {
      this(ReadWriteIOUtils.readString(buffer), (T) ReadWriteIOUtils.readObject(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.compareTo((T) value) < 0;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMaxValue()) < 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMinValue()) < 0;
    }

    @Override
    public Filter reverse() {
      return new ValueLtEq<>(measurement, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_GT;
    }

    @Override
    public String toString() {
      return measurement + " > " + constant;
    }
  }

  public static final class ValueGtEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant cannot be null
    public ValueGtEq(String measurement, T constant) {
      super(measurement, Objects.requireNonNull(constant, CONSTANT_CANNOT_BE_NULL_MSG));
    }

    public ValueGtEq(ByteBuffer buffer) {
      this(ReadWriteIOUtils.readString(buffer), (T) ReadWriteIOUtils.readObject(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.compareTo((T) value) <= 0;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMaxValue()) <= 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMinValue()) <= 0;
    }

    @Override
    public Filter reverse() {
      return new ValueLt<>(measurement, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_GTEQ;
    }

    @Override
    public String toString() {
      return measurement + " >= " + constant;
    }
  }

  // base class for ValueBetweenAnd, ValueNotBetweenAnd
  abstract static class ValueColumnRangeFilter<T extends Comparable<T>> extends ColumnRangeFilter<T>
      implements IValueFilter {

    protected final String measurement;

    protected ValueColumnRangeFilter(String measurement, T min, T max) {
      super(min, max);
      this.measurement = Objects.requireNonNull(measurement, MEASUREMENT_CANNOT_BE_NULL_MSG);
    }

    public String getMeasurement() {
      return measurement;
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      ReadWriteIOUtils.write(getOperatorType().ordinal(), outputStream);
      ReadWriteIOUtils.write(measurement, outputStream);
      ReadWriteIOUtils.writeObject(min, outputStream);
      ReadWriteIOUtils.writeObject(max, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnRangeFilter<?> that = (ValueColumnRangeFilter<?>) o;
      return measurement.equals(that.measurement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), measurement);
    }
  }

  public static final class ValueBetweenAnd<T extends Comparable<T>>
      extends ValueColumnRangeFilter<T> {

    public ValueBetweenAnd(String measurement, T min, T max) {
      super(measurement, min, max);
    }

    public ValueBetweenAnd(ByteBuffer buffer) {
      this(
          ReadWriteIOUtils.readString(buffer),
          (T) ReadWriteIOUtils.readObject(buffer),
          (T) ReadWriteIOUtils.readObject(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return min.compareTo((T) value) <= 0 && max.compareTo((T) value) >= 0;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return (((T) statistics.getMaxValue()).compareTo(min) >= 0
          && ((T) statistics.getMinValue()).compareTo(max) <= 0);
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return (((T) statistics.getMinValue()).compareTo(min) >= 0
          && ((T) statistics.getMaxValue()).compareTo(max) <= 0);
    }

    @Override
    public Filter reverse() {
      return new ValueNotBetweenAnd<>(measurement, min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_BETWEEN_AND;
    }

    @Override
    public String toString() {
      return measurement + " between " + min + " and " + max;
    }
  }

  public static final class ValueNotBetweenAnd<T extends Comparable<T>>
      extends ValueColumnRangeFilter<T> {

    public ValueNotBetweenAnd(String measurement, T min, T max) {
      super(measurement, min, max);
    }

    public ValueNotBetweenAnd(ByteBuffer buffer) {
      this(
          ReadWriteIOUtils.readString(buffer),
          (T) ReadWriteIOUtils.readObject(buffer),
          (T) ReadWriteIOUtils.readObject(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return min.compareTo((T) value) > 0 || max.compareTo((T) value) < 0;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return (((T) statistics.getMinValue()).compareTo(min) < 0
          || ((T) statistics.getMaxValue()).compareTo(max) > 0);
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return (((T) statistics.getMinValue()).compareTo(max) > 0
          || ((T) statistics.getMaxValue()).compareTo(min) < 0);
    }

    @Override
    public Filter reverse() {
      return new ValueBetweenAnd<>(measurement, min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_BETWEEN_AND;
    }

    @Override
    public String toString() {
      return measurement + " not between " + min + " and " + max;
    }
  }

  // base class for ValueIn, ValueNotIn
  abstract static class ValueColumnSetFilter<T> extends ColumnSetFilter<T>
      implements IDisableStatisticsValueFilter {

    protected final String measurement;

    protected ValueColumnSetFilter(String measurement, Set<T> candidates) {
      super(candidates);
      this.measurement = Objects.requireNonNull(measurement, MEASUREMENT_CANNOT_BE_NULL_MSG);
    }

    public String getMeasurement() {
      return measurement;
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      ReadWriteIOUtils.write(getOperatorType().ordinal(), outputStream);
      ReadWriteIOUtils.write(measurement, outputStream);
      ReadWriteIOUtils.writeObjectSet(candidates, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnSetFilter<?> that = (ValueColumnSetFilter<?>) o;
      return measurement.equals(that.measurement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), measurement);
    }
  }

  public static final class ValueIn<T> extends ValueColumnSetFilter<T> {

    public ValueIn(String measurement, Set<T> candidates) {
      super(measurement, candidates);
    }

    public ValueIn(ByteBuffer buffer) {
      this(ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readObjectSet(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return candidates.contains(value);
    }

    @Override
    public Filter reverse() {
      return new ValueNotIn<>(measurement, candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_IN;
    }

    @Override
    public String toString() {
      return measurement + " in " + candidates;
    }
  }

  public static final class ValueNotIn<T> extends ValueColumnSetFilter<T> {

    public ValueNotIn(String measurement, Set<T> candidates) {
      super(measurement, candidates);
    }

    public ValueNotIn(ByteBuffer buffer) {
      this(ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readObjectSet(buffer));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !candidates.contains(value);
    }

    @Override
    public Filter reverse() {
      return new ValueIn<>(measurement, candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_IN;
    }

    @Override
    public String toString() {
      return measurement + " not in " + candidates;
    }
  }

  // base class for ValueRegex, ValueNotRegex
  abstract static class ValueColumnPatternMatchFilter extends ColumnPatternMatchFilter
      implements IDisableStatisticsValueFilter {

    protected final String measurement;

    protected ValueColumnPatternMatchFilter(String measurement, Pattern pattern) {
      super(pattern);
      this.measurement = Objects.requireNonNull(measurement, MEASUREMENT_CANNOT_BE_NULL_MSG);
    }

    public String getMeasurement() {
      return measurement;
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      ReadWriteIOUtils.write(getOperatorType().ordinal(), outputStream);
      ReadWriteIOUtils.write(measurement, outputStream);
      ReadWriteIOUtils.write(pattern.pattern(), outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnPatternMatchFilter that = (ValueColumnPatternMatchFilter) o;
      return measurement.equals(that.measurement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), measurement);
    }
  }

  public static final class ValueRegexp extends ValueColumnPatternMatchFilter {

    public ValueRegexp(String measurement, Pattern pattern) {
      super(measurement, pattern);
    }

    public ValueRegexp(ByteBuffer buffer) {
      this(
          ReadWriteIOUtils.readString(buffer),
          RegexUtils.compileRegex(ReadWriteIOUtils.readString(buffer)));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public Filter reverse() {
      return new ValueNotRegexp(measurement, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_REGEXP;
    }

    @Override
    public String toString() {
      return measurement + " match " + pattern;
    }
  }

  public static final class ValueNotRegexp extends ValueColumnPatternMatchFilter {

    public ValueNotRegexp(String measurement, Pattern pattern) {
      super(measurement, pattern);
    }

    public ValueNotRegexp(ByteBuffer buffer) {
      this(
          ReadWriteIOUtils.readString(buffer),
          RegexUtils.compileRegex(ReadWriteIOUtils.readString(buffer)));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public Filter reverse() {
      return new ValueRegexp(measurement, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_REGEXP;
    }

    @Override
    public String toString() {
      return measurement + " not match " + pattern;
    }
  }

  private static class AccessCount {
    private int count;
    private final int accessThreshold =
        TSFileDescriptor.getInstance().getConfig().getPatternMatchingThreshold();

    public void check() throws IllegalStateException {
      if (this.count++ > accessThreshold) {
        throw new IllegalStateException("Pattern access threshold exceeded");
      }
    }
  }

  private static class MatcherInput implements CharSequence {

    private final CharSequence value;

    private final AccessCount access;

    public MatcherInput(CharSequence value, AccessCount access) {
      this.value = value;
      this.access = access;
    }

    @Override
    public char charAt(int index) {
      this.access.check();
      return this.value.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return new MatcherInput(this.value.subSequence(start, end), this.access);
    }

    @Override
    public int length() {
      return this.value.length();
    }

    @Override
    public String toString() {
      return this.value.toString();
    }
  }
}
