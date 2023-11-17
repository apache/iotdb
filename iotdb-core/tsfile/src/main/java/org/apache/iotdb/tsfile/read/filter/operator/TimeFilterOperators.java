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

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.ColumnCompareFilter;
import org.apache.iotdb.tsfile.read.filter.basic.ColumnRangeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.ColumnSetFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * These are the time column operators in a filter predicate expression tree. They are constructed
 * by using the methods in {@link TimeFilter}
 */
public final class TimeFilterOperators {

  private TimeFilterOperators() {
    // forbidden construction
  }

  // base class for TimeEq, TimeNotEq, TimeLt, TimeGt, TimeLtEq, TimeGtEq
  abstract static class TimeColumnCompareFilter extends ColumnCompareFilter<Long> {

    private final String toString;

    // constant cannot be null
    protected TimeColumnCompareFilter(Long constant) {
      super(Objects.requireNonNull(constant, "constant cannot be null"));

      String name = getClass().getSimpleName().toLowerCase(Locale.ENGLISH);
      this.toString = name + "(Time, " + constant + ")";
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      return satisfyStartEndTime(statistics.getStartTime(), statistics.getEndTime());
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      return containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
    }
  }

  public static final class TimeEq extends TimeColumnCompareFilter {

    // constant can be null
    public TimeEq(Long constant) {
      super(constant);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.equals(time);
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
  }

  public static final class TimeNotEq extends TimeColumnCompareFilter {

    public TimeNotEq(Long constant) {
      super(constant);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !constant.equals(time);
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
  }

  public static final class TimeLt extends TimeColumnCompareFilter {

    public TimeLt(Long constant) {
      super(constant);
    }

    @Override
    public boolean satisfy(long time, Object value) {
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
  }

  public static final class TimeLtEq extends TimeColumnCompareFilter {

    public TimeLtEq(Long constant) {
      super(constant);
    }

    @Override
    public boolean satisfy(long time, Object value) {
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
  }

  public static final class TimeGt extends TimeColumnCompareFilter {

    public TimeGt(Long constant) {
      super(constant);
    }

    @Override
    public boolean satisfy(long time, Object value) {
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
  }

  public static final class TimeGtEq extends TimeColumnCompareFilter {

    public TimeGtEq(Long constant) {
      super(constant);
    }

    @Override
    public boolean satisfy(long time, Object value) {
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
  }

  // base class for TimeBetweenAnd, TimeNotBetweenAnd
  abstract static class TimeColumnRangeFilter extends ColumnRangeFilter<Long> {

    private final String toString;

    // constant cannot be null
    protected TimeColumnRangeFilter(Long min, Long max) {
      super(min, max);

      String name = getClass().getSimpleName().toLowerCase(Locale.ENGLISH);
      this.toString = name + "(Time, " + min + ", " + max + ")";
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      return satisfyStartEndTime(statistics.getStartTime(), statistics.getEndTime());
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      return containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
    }
  }

  public static final class TimeBetweenAnd extends TimeColumnRangeFilter {

    public TimeBetweenAnd(Long min, Long max) {
      super(min, max);
    }

    @Override
    public boolean satisfy(long time, Object value) {
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
  }

  public static final class TimeNotBetweenAnd extends TimeColumnRangeFilter {

    public TimeNotBetweenAnd(Long min, Long max) {
      super(min, max);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return time < min || time > max;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return startTime < min && endTime > max;
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
  }

  // base class for TimeIn, TimeNotIn
  abstract static class TimeColumnSetFilter extends ColumnSetFilter<Long> {

    private final String toString;

    protected TimeColumnSetFilter(Set<Long> candidates) {
      super(candidates);

      String name = getClass().getSimpleName().toLowerCase(Locale.ENGLISH);
      this.toString = name + "(Time, " + candidates + ")";
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      return satisfyStartEndTime(statistics.getStartTime(), statistics.getEndTime());
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      return containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return true;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return false;
    }
  }

  public static final class TimeIn extends TimeColumnSetFilter {

    public TimeIn(Set<Long> candidates) {
      super(candidates);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return candidates.contains(time);
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
  }

  public static final class TimeNotIn extends TimeColumnSetFilter {

    public TimeNotIn(Set<Long> candidates) {
      super(candidates);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !candidates.contains(time);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new TimeIn(candidates);
    }
  }
}
