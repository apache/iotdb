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
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.IMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.DisableStatisticsValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.OperatorType;
import org.apache.iotdb.tsfile.read.filter.basic.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * These are the value column operators in a filter predicate expression tree. They are constructed
 * by using the methods in {@link ValueFilterApi}
 */
public final class ValueFilterOperators {

  private ValueFilterOperators() {
    // forbidden construction
  }

  private static final String CONSTANT_CANNOT_BE_NULL_MSG = "constant cannot be null";
  public static final String CANNOT_PUSH_DOWN_MSG = " operator can not be pushed down.";

  private static final String OPERATOR_TO_STRING_FORMAT = "measurements[%s] %s %s";

  // base class for ValueEq, ValueNotEq, ValueLt, ValueGt, ValueLtEq, ValueGtEq
  abstract static class ValueColumnCompareFilter<T extends Comparable<T>> extends ValueFilter {

    protected final T constant;

    protected ValueColumnCompareFilter(int measurementIndex, T constant) {
      super(measurementIndex);
      this.constant = Objects.requireNonNull(constant, CONSTANT_CANNOT_BE_NULL_MSG);
    }

    @SuppressWarnings("unchecked")
    protected ValueColumnCompareFilter(ByteBuffer buffer) {
      super(buffer);
      this.constant =
          Objects.requireNonNull(
              (T) ReadWriteIOUtils.readObject(buffer), CONSTANT_CANNOT_BE_NULL_MSG);
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
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
      if (!super.equals(o)) {
        return false;
      }
      ValueColumnCompareFilter<?> that = (ValueColumnCompareFilter<?>) o;
      return Objects.equals(constant, that.constant);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), constant);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), constant);
    }
  }

  public static final class ValueEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    public ValueEq(int measurementIndex, T constant) {
      super(measurementIndex, constant);
    }

    public ValueEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      return constant.equals(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      // drop if value < min || value > max
      return constant.compareTo((T) statistics.getMinValue()) < 0
          || constant.compareTo((T) statistics.getMaxValue()) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return constant.compareTo((T) statistics.getMinValue()) == 0
          && constant.compareTo((T) statistics.getMaxValue()) == 0;
    }

    @Override
    public Filter reverse() {
      return new ValueNotEq<>(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_EQ;
    }
  }

  public static final class ValueNotEq<T extends Comparable<T>>
      extends ValueColumnCompareFilter<T> {

    public ValueNotEq(int measurementIndex, T constant) {
      super(measurementIndex, constant);
    }

    public ValueNotEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      return !constant.equals(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      // drop if this is a column where min = max = value
      return constant.compareTo((T) statistics.getMinValue()) == 0
          && constant.compareTo((T) statistics.getMaxValue()) == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return constant.compareTo((T) statistics.getMinValue()) < 0
          || constant.compareTo((T) statistics.getMaxValue()) > 0;
    }

    @Override
    public Filter reverse() {
      return new ValueEq<>(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NEQ;
    }
  }

  public static final class ValueLt<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    public ValueLt(int measurementIndex, T constant) {
      super(measurementIndex, constant);
    }

    public ValueLt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value) {
      return constant.compareTo((T) value) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      // drop if value <= min
      return constant.compareTo((T) statistics.getMinValue()) <= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return constant.compareTo((T) statistics.getMaxValue()) > 0;
    }

    @Override
    public Filter reverse() {
      return new ValueGtEq<>(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LT;
    }
  }

  public static final class ValueLtEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    public ValueLtEq(int measurementIndex, T constant) {
      super(measurementIndex, constant);
    }

    public ValueLtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value) {
      return constant.compareTo((T) value) >= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      // drop if value < min
      return constant.compareTo((T) statistics.getMinValue()) < 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return constant.compareTo((T) statistics.getMaxValue()) >= 0;
    }

    @Override
    public Filter reverse() {
      return new ValueGt<>(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LTEQ;
    }
  }

  public static final class ValueGt<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    public ValueGt(int measurementIndex, T constant) {
      super(measurementIndex, constant);
    }

    public ValueGt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value) {
      return constant.compareTo((T) value) < 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      // drop if value >= max
      return constant.compareTo((T) statistics.getMaxValue()) >= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return constant.compareTo((T) statistics.getMinValue()) < 0;
    }

    @Override
    public Filter reverse() {
      return new ValueLtEq<>(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_GT;
    }
  }

  public static final class ValueGtEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    public ValueGtEq(int measurementIndex, T constant) {
      super(measurementIndex, constant);
    }

    public ValueGtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value) {
      return constant.compareTo((T) value) <= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      // drop if value > max
      return constant.compareTo((T) statistics.getMaxValue()) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return constant.compareTo((T) statistics.getMinValue()) <= 0;
    }

    @Override
    public Filter reverse() {
      return new ValueLt<>(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_GTEQ;
    }
  }

  // base class for ValueBetweenAnd, ValueNotBetweenAnd
  abstract static class ValueColumnRangeFilter<T extends Comparable<T>> extends ValueFilter {

    protected final T min;
    protected final T max;

    protected ValueColumnRangeFilter(int measurementIndex, T min, T max) {
      super(measurementIndex);
      this.min = Objects.requireNonNull(min, "min cannot be null");
      this.max = Objects.requireNonNull(max, "max cannot be null");
    }

    @SuppressWarnings("unchecked")
    protected ValueColumnRangeFilter(ByteBuffer buffer) {
      super(buffer);
      this.min =
          Objects.requireNonNull((T) ReadWriteIOUtils.readObject(buffer), "min cannot be null");
      this.max =
          Objects.requireNonNull((T) ReadWriteIOUtils.readObject(buffer), "max cannot be null");
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
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
      return min.equals(that.min) && max.equals(that.max);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), min, max);
    }

    @Override
    public String toString() {
      return String.format(
          "measurements[%s] %s %s AND %s",
          measurementIndex, getOperatorType().getSymbol(), min, max);
    }
  }

  public static final class ValueBetweenAnd<T extends Comparable<T>>
      extends ValueColumnRangeFilter<T> {

    public ValueBetweenAnd(int measurementIndex, T min, T max) {
      super(measurementIndex, min, max);
    }

    public ValueBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value) {
      return min.compareTo((T) value) <= 0 && max.compareTo((T) value) >= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return ((T) statistics.getMaxValue()).compareTo(min) < 0
          || ((T) statistics.getMinValue()).compareTo(max) > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return ((T) statistics.getMinValue()).compareTo(min) >= 0
          && ((T) statistics.getMaxValue()).compareTo(max) <= 0;
    }

    @Override
    public Filter reverse() {
      return new ValueNotBetweenAnd<>(measurementIndex, min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_BETWEEN_AND;
    }
  }

  public static final class ValueNotBetweenAnd<T extends Comparable<T>>
      extends ValueColumnRangeFilter<T> {

    public ValueNotBetweenAnd(int measurementIndex, T min, T max) {
      super(measurementIndex, min, max);
    }

    public ValueNotBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value) {
      return min.compareTo((T) value) > 0 || max.compareTo((T) value) < 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return ((T) statistics.getMinValue()).compareTo(min) >= 0
          && ((T) statistics.getMaxValue()).compareTo(max) <= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statisticsNotAvailable(statistics)) {
        return false;
      }

      return ((T) statistics.getMinValue()).compareTo(max) > 0
          || ((T) statistics.getMaxValue()).compareTo(min) < 0;
    }

    @Override
    public Filter reverse() {
      return new ValueBetweenAnd<>(measurementIndex, min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_BETWEEN_AND;
    }
  }

  // we have no statistics available, we cannot drop any blocks
  private static boolean statisticsNotAvailable(Statistics<?> statistics) {
    return statistics.getType() == TSDataType.TEXT
        || statistics.getType() == TSDataType.BOOLEAN
        || statistics.isEmpty();
  }

  // base class for ValueIsNull and ValueIsNotNull
  abstract static class ValueCompareNullFilter extends ValueFilter {

    protected ValueCompareNullFilter(int measurementIndex) {
      super(measurementIndex);
    }

    protected ValueCompareNullFilter(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public String toString() {
      return String.format("measurements[%s] %s", measurementIndex, getOperatorType().getSymbol());
    }
  }

  // ValueIsNull can not be pushed down
  public static final class ValueIsNull extends ValueCompareNullFilter {

    public ValueIsNull(int measurementIndex) {
      super(measurementIndex);
    }

    public ValueIsNull(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean satisfyRow(long time, Object[] values) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean canSkip(IMetadata metadata) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public boolean allSatisfy(IMetadata metadata) {
      throw new IllegalArgumentException(getOperatorType().getSymbol() + CANNOT_PUSH_DOWN_MSG);
    }

    @Override
    public Filter reverse() {
      return new ValueIsNotNull(measurementIndex);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_IS_NULL;
    }
  }

  // ValueIsNotNull are only used in ValueFilter
  public static final class ValueIsNotNull extends ValueCompareNullFilter {

    public ValueIsNotNull(int measurementIndex) {
      super(measurementIndex);
    }

    public ValueIsNotNull(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      return value != null;
    }

    @Override
    public boolean canSkip(IMetadata metadata) {
      Optional<Statistics<? extends Serializable>> statistics =
          metadata.getMeasurementStatistics(measurementIndex);

      if (!statistics.isPresent()) {
        // the measurement isn't in this block so all values are null.
        // null is always equal to null
        return true;
      }

      // we are looking for records where v notEq(null)
      // so, if this is a column of all nulls, we can drop it
      return isAllNulls(statistics.get());
    }

    @Override
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public boolean allSatisfy(IMetadata metadata) {
      Optional<Statistics<? extends Serializable>> statistics =
          metadata.getMeasurementStatistics(measurementIndex);

      if (!statistics.isPresent()) {
        // block cannot match
        return false;
      }

      return !metadata.hasNullValue(measurementIndex);
    }

    @Override
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public Filter reverse() {
      return new ValueIsNull(measurementIndex);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_IS_NOT_NULL;
    }

    private boolean isAllNulls(Statistics<? extends Serializable> statistics) {
      return statistics.getCount() == 0;
    }
  }

  // base class for ValueIn, ValueNotIn
  abstract static class ValueColumnSetFilter<T> extends DisableStatisticsValueFilter {

    protected final Set<T> candidates;

    protected ValueColumnSetFilter(int measurementIndex, Set<T> candidates) {
      super(measurementIndex);
      this.candidates = Objects.requireNonNull(candidates, "candidates cannot be null");
    }

    protected ValueColumnSetFilter(ByteBuffer buffer) {
      super(buffer);
      candidates = ReadWriteIOUtils.readObjectSet(buffer);
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
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
      return candidates.equals(that.candidates);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), candidates);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), candidates);
    }
  }

  public static final class ValueIn<T> extends ValueColumnSetFilter<T> {

    public ValueIn(int measurementIndex, Set<T> candidates) {
      super(measurementIndex, candidates);
    }

    public ValueIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      return candidates.contains(value);
    }

    @Override
    public Filter reverse() {
      return new ValueNotIn<>(measurementIndex, candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_IN;
    }
  }

  public static final class ValueNotIn<T> extends ValueColumnSetFilter<T> {

    public ValueNotIn(int measurementIndex, Set<T> candidates) {
      super(measurementIndex, candidates);
    }

    public ValueNotIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      return !candidates.contains(value);
    }

    @Override
    public Filter reverse() {
      return new ValueIn<>(measurementIndex, candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_IN;
    }
  }

  // base class for ValueRegex, ValueNotRegex
  abstract static class ValueColumnPatternMatchFilter extends DisableStatisticsValueFilter {

    protected final Pattern pattern;

    protected ValueColumnPatternMatchFilter(int measurementIndex, Pattern pattern) {
      super(measurementIndex);
      this.pattern = Objects.requireNonNull(pattern, "pattern cannot be null");
    }

    protected ValueColumnPatternMatchFilter(ByteBuffer buffer) {
      super(buffer);
      this.pattern =
          Pattern.compile(
              Objects.requireNonNull(
                  ReadWriteIOUtils.readString(buffer), "pattern cannot be null"));
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
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
      return pattern.pattern().equals(that.pattern.pattern());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), pattern.pattern());
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), pattern);
    }
  }

  public static final class ValueRegexp extends ValueColumnPatternMatchFilter {

    public ValueRegexp(int measurementIndex, Pattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueRegexp(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      return pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public Filter reverse() {
      return new ValueNotRegexp(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_REGEXP;
    }
  }

  public static final class ValueNotRegexp extends ValueColumnPatternMatchFilter {

    public ValueNotRegexp(int measurementIndex, Pattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueNotRegexp(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      return !pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public Filter reverse() {
      return new ValueRegexp(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_REGEXP;
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
