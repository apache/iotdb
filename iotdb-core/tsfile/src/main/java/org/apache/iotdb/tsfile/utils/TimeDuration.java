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
package org.apache.iotdb.tsfile.utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class TimeDuration implements Serializable {
  // month part of time duration
  public final int monthDuration;
  // non-month part of time duration, its precision is same as current time_precision
  public final long nonMonthDuration;

  public TimeDuration(int monthDuration, long nonMonthDuration) {
    this.monthDuration = monthDuration;
    this.nonMonthDuration = nonMonthDuration;
  }

  public boolean containsMonth() {
    return monthDuration != 0;
  }

  /**
   * Convert monthDuration to current precision duration, then add currPrecisionDuration field.
   * Think month as 30 days.
   *
   * @return the total duration of this timeDuration in current precision
   */
  public long getTotalDuration(TimeUnit currPrecision) {
    return currPrecision.convert(monthDuration * 30 * 86400_000L, TimeUnit.MILLISECONDS)
        + nonMonthDuration;
  }

  /** Think month as 31 days. */
  public long getMaxTotalDuration(TimeUnit currPrecision) {
    return currPrecision.convert(monthDuration * 31 * 86400_000L, TimeUnit.MILLISECONDS)
        + nonMonthDuration;
  }

  public boolean isGreaterThan(TimeDuration right) {
    if (this.monthDuration > right.monthDuration) {
      return true;
    } else if (this.monthDuration == right.monthDuration) {
      return this.nonMonthDuration > right.nonMonthDuration;
    }
    return false;
  }

  public TimeDuration merge(TimeDuration other) {
    return new TimeDuration(
        this.monthDuration + other.monthDuration, this.nonMonthDuration + other.nonMonthDuration);
  }

  public TimeDuration multiple(long times) {
    return new TimeDuration((int) (monthDuration * times), nonMonthDuration * times);
  }

  /** Think month as 28 days. */
  public long getMinTotalDuration(TimeUnit currPrecision) {
    return currPrecision.convert(monthDuration * 28 * 86400_000L, TimeUnit.MILLISECONDS)
        + nonMonthDuration;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(monthDuration, buffer);
    ReadWriteIOUtils.write(nonMonthDuration, buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(monthDuration, stream);
    ReadWriteIOUtils.write(nonMonthDuration, stream);
  }

  public static TimeDuration deserialize(ByteBuffer buffer) {
    return new TimeDuration(ReadWriteIOUtils.readInt(buffer), ReadWriteIOUtils.readLong(buffer));
  }

  /** Get a series of time which duration contains month. */
  public static long[] getConsecutiveTimesIntervalByMonth(
      long startTime,
      TimeDuration duration,
      int length,
      TimeZone timeZone,
      TimeUnit currPrecision) {
    long[] result = new long[length];
    result[0] = startTime;
    for (int i = 1; i < length; i++) {
      result[i] = getStartTime(startTime, duration.multiple(i), currPrecision, timeZone.toZoneId());
    }
    return result;
  }

  /**
   * Add time duration contains natural months to startTime.
   *
   * <p>Attention: This method does not support accumulation. If you need to calculate the date two
   * months after the start time, just add two months directly through duration, rather than adding
   * one month first and then adding another month in a loop, it will get wrong result.
   *
   * <p>There is an example:
   *
   * <pre>
   * 1.30 + 2mo = 3.30(right)
   * 1.30 + 1mo = 2.28, 2.28 + 1mo = 3.28(wrong)
   * </pre>
   *
   * @param startTime start time
   * @param duration one duration
   * @return the time after durations elapsed
   */
  public static long calcPositiveIntervalByMonth(
      long startTime, TimeDuration duration, TimeZone timeZone, TimeUnit currPrecision) {
    return getStartTime(startTime, duration, currPrecision, timeZone.toZoneId());
  }

  private static long getStartTime(
      long startTime, TimeDuration duration, TimeUnit currPrecision, ZoneId zoneId) {
    long coarserThanMsPart = getCoarserThanMsPart(startTime, currPrecision);
    LocalDateTime localDateTime =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(coarserThanMsPart), zoneId);
    localDateTime = localDateTime.plusMonths(duration.monthDuration);
    return currPrecision.convert(
            localDateTime.atZone(zoneId).toInstant().toEpochMilli(), TimeUnit.MILLISECONDS)
        + getFinerThanMsPart(startTime, currPrecision)
        + duration.nonMonthDuration;
  }

  private static long getCoarserThanMsPart(long time, TimeUnit currPrecision) {
    return TimeUnit.MILLISECONDS.convert(time, currPrecision);
  }

  private static long getFinerThanMsPart(long time, TimeUnit currPrecision) {
    switch (currPrecision) {
      case MILLISECONDS:
        return 0;
      case MICROSECONDS:
        return time % 1000;
      case NANOSECONDS:
        return time % 1000_000;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeDuration that = (TimeDuration) o;
    return monthDuration == that.monthDuration && nonMonthDuration == that.nonMonthDuration;
  }

  @Override
  public int hashCode() {
    return Objects.hash(monthDuration, nonMonthDuration);
  }

  @Override
  public String toString() {
    return "TimeDuration{"
        + (monthDuration > 0 ? monthDuration + "mo, " : "")
        + (nonMonthDuration > 0 ? nonMonthDuration : "")
        + '}';
  }
}
