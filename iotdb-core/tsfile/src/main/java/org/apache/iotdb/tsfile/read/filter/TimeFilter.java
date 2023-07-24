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

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.Between;
import org.apache.iotdb.tsfile.read.filter.operator.Eq;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.iotdb.tsfile.read.filter.operator.Lt;
import org.apache.iotdb.tsfile.read.filter.operator.LtEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotEq;
import org.apache.iotdb.tsfile.read.filter.operator.OrFilter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TimeFilter {

  private TimeFilter() {}

  public static TimeGt gt(long value) {
    return new TimeGt(value);
  }

  public static TimeGtEq gtEq(long value) {
    return new TimeGtEq(value);
  }

  public static TimeLt lt(long value) {
    return new TimeLt(value);
  }

  public static TimeLtEq ltEq(long value) {
    return new TimeLtEq(value);
  }

  public static TimeEq eq(long value) {
    return new TimeEq(value);
  }

  public static TimeNotEq notEq(long value) {
    return new TimeNotEq(value);
  }

  public static TimeBetween between(long value1, long value2) {
    return new TimeBetween(value1, value2, false);
  }

  public static TimeBetween notBetween(long value1, long value2) {
    return new TimeBetween(value1, value2, true);
  }

  public static TimeIn in(Set<Long> values) {
    return new TimeIn(values, false);
  }

  public static TimeIn notIn(Set<Long> values) {
    return new TimeIn(values, true);
  }

  public static class TimeGt extends Gt<Long> {

    private TimeGt(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      long left = value;
      if (left != Long.MAX_VALUE) {
        return Collections.singletonList(new TimeRange(left + 1, Long.MAX_VALUE));
      } else {
        return Collections.emptyList();
      }
    }
  }

  public static class TimeGtEq extends GtEq<Long> {

    private TimeGtEq(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(value, Long.MAX_VALUE));
    }
  }

  public static class TimeLt extends Lt<Long> {

    private TimeLt(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      long right = value;
      if (right != Long.MIN_VALUE) {
        return Collections.singletonList(new TimeRange(Long.MIN_VALUE, right - 1));
      } else {
        return Collections.emptyList();
      }
    }
  }

  public static class TimeLtEq extends LtEq<Long> {

    private TimeLtEq(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, value));
    }
  }

  public static class TimeEq extends Eq<Long> {

    private TimeEq(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(value, value));
    }
  }

  public static class TimeNotEq extends NotEq<Long> {

    private TimeNotEq(long value) {
      super(value, FilterType.TIME_FILTER);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      long time = value;
      if (time == Long.MIN_VALUE) {
        return Collections.singletonList(new TimeRange(time + 1, Long.MAX_VALUE));
      } else if (time == Long.MAX_VALUE) {
        return Collections.singletonList(new TimeRange(Long.MIN_VALUE, time - 1));
      } else {
        return Arrays.asList(
            new TimeRange(Long.MIN_VALUE, time - 1), new TimeRange(time + 1, Long.MAX_VALUE));
      }
    }
  }

  public static class TimeBetween extends Between<Long> {

    private TimeBetween(long value1, long value2, boolean not) {
      super(value1, value2, FilterType.TIME_FILTER, not);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      long left = value1;
      long right = value2;
      if (not) {
        List<TimeRange> res = new ArrayList<>();
        if (left != Long.MIN_VALUE) {
          res.add(new TimeRange(Long.MIN_VALUE, left - 1));
        }
        if (right != Long.MAX_VALUE) {
          res.add(new TimeRange(right + 1, Long.MAX_VALUE));
        }
        return res;
      } else {
        return Collections.singletonList(new TimeRange(left, right));
      }
    }
  }

  public static class TimeIn extends In<Long> {

    private TimeIn(Set<Long> values, boolean not) {
      super(values, FilterType.TIME_FILTER, not);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      if (not) {
        return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
      } else {
        return values.stream()
            .map(
                l -> {
                  long time = l;
                  return new TimeRange(time, time);
                })
            .collect(Collectors.toList());
      }
    }
  }

  /**
   * returns a default time filter by whether it's an ascending query.
   *
   * <p>If the data is read in descending order, we use the largest timestamp to set to the filter,
   * so the filter should be TimeLtEq. If the data is read in ascending order, we use the smallest
   * timestamp to set to the filter, so the filter should be TimeGtEq.
   */
  public static Filter defaultTimeFilter(boolean ascending) {
    return ascending ? TimeFilter.gtEq(Long.MIN_VALUE) : TimeFilter.ltEq(Long.MAX_VALUE);
  }

  public static class TimeGtEqAndLt implements Filter {

    private long startTime;

    private long endTime;

    public TimeGtEqAndLt() {}

    public TimeGtEqAndLt(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      return !(statistics.getEndTime() < startTime || statistics.getStartTime() >= endTime);
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      return startTime <= statistics.getStartTime() && statistics.getEndTime() < endTime;
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return startTime <= time && time < endTime;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return !(endTime < this.startTime || startTime >= this.endTime);
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return this.startTime <= startTime && endTime < this.endTime;
    }

    @Override
    public Filter copy() {
      return new TimeGtEqAndLt(startTime, endTime);
    }

    @Override
    public String toString() {
      return "TimeGtEqAndLt{" + "startTime=" + startTime + ", endTime=" + endTime + '}';
    }

    @Override
    public void serialize(DataOutputStream outputStream) {
      try {
        outputStream.write(getSerializeId().ordinal());
        outputStream.writeLong(startTime);
        outputStream.writeLong(endTime);
      } catch (IOException ignored) {
        // ignored
      }
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
      startTime = buffer.getLong();
      endTime = buffer.getLong();
    }

    @Override
    public FilterSerializeId getSerializeId() {
      return FilterSerializeId.TIME_GTEQ_AND_LT;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return startTime >= endTime
          ? Collections.emptyList()
          : Collections.singletonList(new TimeRange(startTime, endTime - 1));
    }

    @Override
    public Filter reverse() {
      return new OrFilter(new TimeLt(startTime), new TimeGtEq(endTime));
    }
  }
}
