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

package org.apache.tsfile.read.filter.operator;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.read.filter.basic.TimeFilter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * These are the time column operators in a filter predicate expression tree. They are constructed
 * by using the methods in {@link TimeFilterApi}
 */
public final class TimeFilterOperators {

  private TimeFilterOperators() {
    // forbidden construction
  }

  private static final String OPERATOR_TO_STRING_FORMAT = "time %s %s";

  // base class for TimeEq, TimeNotEq, TimeLt, TimeGt, TimeLtEq, TimeGtEq
  abstract static class TimeColumnCompareFilter extends TimeFilter {

    protected final long constant;

    // constant cannot be null
    protected TimeColumnCompareFilter(long constant) {
      this.constant = constant;
    }

    protected TimeColumnCompareFilter(ByteBuffer buffer) {
      this.constant = ReadWriteIOUtils.readLong(buffer);
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(constant, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TimeColumnCompareFilter that = (TimeColumnCompareFilter) o;
      return constant == that.constant;
    }

    @Override
    public int hashCode() {
      return Objects.hash(constant);
    }

    @Override
    public String toString() {
      return String.format(OPERATOR_TO_STRING_FORMAT, getOperatorType().getSymbol(), constant);
    }
  }

  public static final class TimeEq extends TimeColumnCompareFilter {

    public TimeEq(long constant) {
      super(constant);
    }

    public TimeEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return constant == time;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return constant <= endTime && constant >= startTime;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return constant == startTime && constant == endTime;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(constant, constant));
    }

    @Override
    public Filter reverse() {
      return new TimeNotEq(constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_EQ;
    }
  }

  public static final class TimeNotEq extends TimeColumnCompareFilter {

    public TimeNotEq(long constant) {
      super(constant);
    }

    public TimeNotEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return constant != time;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return constant != startTime || constant != endTime;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return constant < startTime || constant > endTime;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      long time = constant;
      if (time == Long.MIN_VALUE) {
        return Collections.singletonList(new TimeRange(time + 1, Long.MAX_VALUE));
      } else if (time == Long.MAX_VALUE) {
        return Collections.singletonList(new TimeRange(Long.MIN_VALUE, time - 1));
      } else {
        return Arrays.asList(
            new TimeRange(Long.MIN_VALUE, time - 1), new TimeRange(time + 1, Long.MAX_VALUE));
      }
    }

    @Override
    public Filter reverse() {
      return new TimeEq(constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_NEQ;
    }
  }

  public static final class TimeLt extends TimeColumnCompareFilter {

    public TimeLt(long constant) {
      super(constant);
    }

    public TimeLt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return time < constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return startTime < constant;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return endTime < constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      long right = constant;
      if (right != Long.MIN_VALUE) {
        return Collections.singletonList(new TimeRange(Long.MIN_VALUE, right - 1));
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public Filter reverse() {
      return new TimeGtEq(constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_LT;
    }
  }

  public static final class TimeLtEq extends TimeColumnCompareFilter {

    public TimeLtEq(long constant) {
      super(constant);
    }

    public TimeLtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return time <= constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return constant >= startTime;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return endTime <= constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, constant));
    }

    @Override
    public Filter reverse() {
      return new TimeGt(constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_LTEQ;
    }
  }

  public static final class TimeGt extends TimeColumnCompareFilter {

    public TimeGt(long constant) {
      super(constant);
    }

    public TimeGt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return time > constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return constant < endTime;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return startTime > constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      long left = constant;
      if (left != Long.MAX_VALUE) {
        return Collections.singletonList(new TimeRange(left + 1, Long.MAX_VALUE));
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public Filter reverse() {
      return new TimeLtEq(constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_GT;
    }
  }

  public static final class TimeGtEq extends TimeColumnCompareFilter {

    public TimeGtEq(long constant) {
      super(constant);
    }

    public TimeGtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return time >= constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return constant <= endTime;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return startTime >= constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(constant, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new TimeLt(constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_GTEQ;
    }
  }

  // base class for TimeBetweenAnd, TimeNotBetweenAnd
  abstract static class TimeColumnRangeFilter extends TimeFilter {

    protected final long min;
    protected final long max;

    protected TimeColumnRangeFilter(long min, long max) {
      this.min = min;
      this.max = max;
    }

    protected TimeColumnRangeFilter(ByteBuffer buffer) {
      this.min = ReadWriteIOUtils.readLong(buffer);
      this.max = ReadWriteIOUtils.readLong(buffer);
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(min, outputStream);
      ReadWriteIOUtils.write(max, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TimeColumnRangeFilter that = (TimeColumnRangeFilter) o;
      return min == that.min && max == that.max;
    }

    @Override
    public int hashCode() {
      return Objects.hash(min, max);
    }

    @Override
    public String toString() {
      return String.format("time %s %s AND %s", getOperatorType().getSymbol(), min, max);
    }
  }

  public static final class TimeBetweenAnd extends TimeColumnRangeFilter {

    public TimeBetweenAnd(long min, long max) {
      super(min, max);
    }

    public TimeBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return time >= min && time <= max;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return startTime <= max && endTime >= min;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return startTime >= min && endTime <= max;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(min, max));
    }

    @Override
    public Filter reverse() {
      return new TimeNotBetweenAnd(min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_BETWEEN_AND;
    }
  }

  public static final class TimeNotBetweenAnd extends TimeColumnRangeFilter {

    public TimeNotBetweenAnd(Long min, Long max) {
      super(min, max);
    }

    public TimeNotBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return time < min || time > max;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return startTime < min || endTime > max;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return startTime > max || endTime < min;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      List<TimeRange> res = new ArrayList<>();
      if (min != Long.MIN_VALUE) {
        res.add(new TimeRange(Long.MIN_VALUE, min - 1));
      }
      if (max != Long.MAX_VALUE) {
        res.add(new TimeRange(max + 1, Long.MAX_VALUE));
      }
      return res;
    }

    @Override
    public Filter reverse() {
      return new TimeBetweenAnd(min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_NOT_BETWEEN_AND;
    }
  }

  // base class for TimeIn, TimeNotIn
  abstract static class TimeColumnSetFilter extends TimeFilter {

    protected final Set<Long> candidates;
    protected final Long candidatesMin;
    protected final Long candidatesMax;

    protected TimeColumnSetFilter(Set<Long> candidates) {
      this.candidates = Objects.requireNonNull(candidates, "candidates cannot be null");
      this.candidatesMin = Collections.min(candidates);
      this.candidatesMax = Collections.max(candidates);
    }

    protected TimeColumnSetFilter(ByteBuffer buffer) {
      this.candidates = ReadWriteIOUtils.readLongSet(buffer);
      this.candidatesMin = Collections.min(candidates);
      this.candidatesMax = Collections.max(candidates);
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      if (candidates == null) {
        throw new IllegalArgumentException("set must not be null!");
      }
      ReadWriteIOUtils.writeLongSet(candidates, outputStream);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TimeColumnSetFilter that = (TimeColumnSetFilter) o;
      return candidates.equals(that.candidates);
    }

    @Override
    public int hashCode() {
      return Objects.hash(candidates);
    }

    @Override
    public String toString() {
      return String.format(OPERATOR_TO_STRING_FORMAT, getOperatorType().getSymbol(), candidates);
    }
  }

  public static final class TimeIn extends TimeColumnSetFilter {

    public TimeIn(Set<Long> candidates) {
      super(candidates);
    }

    public TimeIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return candidates.contains(time);
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return startTime <= candidatesMax && endTime >= candidatesMin;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      // Make `allSatisfy` always return false
      return false;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      List<TimeRange> res = new ArrayList<>();
      for (long time : candidates) {
        res.add(new TimeRange(time, time));
      }
      return res;
    }

    @Override
    public Filter reverse() {
      return new TimeNotIn(candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_IN;
    }
  }

  public static final class TimeNotIn extends TimeColumnSetFilter {

    public TimeNotIn(Set<Long> candidates) {
      super(candidates);
    }

    public TimeNotIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return !candidates.contains(time);
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      // Make `canSkip` always return false
      return true;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      // Make `allSatisfy` always return false
      return false;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new TimeIn(candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.TIME_NOT_IN;
    }
  }
}
