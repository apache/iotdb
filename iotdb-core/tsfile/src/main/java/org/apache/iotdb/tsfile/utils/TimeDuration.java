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
import java.util.Calendar;
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
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(timeZone);
    for (int i = 1; i < length; i++) {
      result[i] = getStartTime(result[i - 1], duration, currPrecision, calendar);
    }
    return result;
  }

  /**
   * Add several time durations contains natural months based on the startTime and avoid edge cases,
   * ie 2/28
   *
   * @param startTime start time
   * @param duration one duration
   * @param times num of duration elapsed
   * @return the time after durations elapsed
   */
  public static long calcPositiveIntervalByMonth(
      long startTime,
      TimeDuration duration,
      long times,
      TimeZone timeZone,
      TimeUnit currPrecision) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(timeZone);
    for (int i = 0; i < times; i++) {
      startTime = getStartTime(startTime, duration, currPrecision, calendar);
    }
    return startTime;
  }

  private static long getStartTime(
      long startTime, TimeDuration duration, TimeUnit currPrecision, Calendar calendar) {
    long coarserThanMsPart = getCoarserThanMsPart(startTime, currPrecision);
    calendar.setTimeInMillis(coarserThanMsPart);
    boolean isLastDayOfMonth =
        calendar.get(Calendar.DAY_OF_MONTH) == calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    calendar.add(Calendar.MONTH, duration.monthDuration);
    if (isLastDayOfMonth) {
      calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    }
    return currPrecision.convert(calendar.getTimeInMillis(), TimeUnit.MILLISECONDS)
        + getFinerThanMsPart(startTime, currPrecision)
        + duration.nonMonthDuration;
  }

  /**
   * subtract time duration contains natural months based on the startTime
   *
   * @param startTime start time
   * @param duration the duration
   * @return the time before duration
   */
  public static long calcNegativeIntervalByMonth(
      long startTime, TimeDuration duration, TimeZone timeZone, TimeUnit currPrecision) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(timeZone);
    long timeBeforeMonthElapsedInMs =
        TimeUnit.MILLISECONDS.convert(startTime - duration.nonMonthDuration, currPrecision);
    calendar.setTimeInMillis(timeBeforeMonthElapsedInMs);
    boolean isLastDayOfMonth =
        calendar.get(Calendar.DAY_OF_MONTH) == calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    calendar.add(Calendar.MONTH, -duration.monthDuration);
    if (isLastDayOfMonth) {
      calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    }
    return currPrecision.convert(calendar.getTimeInMillis(), TimeUnit.MILLISECONDS)
        + getFinerThanMsPart(startTime - duration.nonMonthDuration, currPrecision);
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
    return "TimeDuration{" +
        (monthDuration > 0 ?  monthDuration + "mo, " : "") +
        (nonMonthDuration > 0 ? nonMonthDuration : "")+
        '}';
  }
}
