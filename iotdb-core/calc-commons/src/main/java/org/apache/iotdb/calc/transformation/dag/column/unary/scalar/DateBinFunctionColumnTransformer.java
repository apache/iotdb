/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.calc.transformation.dag.column.unary.scalar;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class DateBinFunctionColumnTransformer extends UnaryColumnTransformer {

  private static final long NANOSECONDS_IN_MILLISECOND = 1_000_000;
  private static final long NANOSECONDS_IN_MICROSECOND = 1_000;
  private static final BigInteger BIG_LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger BIG_LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

  private final int monthDuration;
  private final long nonMonthDuration;
  private final long origin;
  private final ZoneId zoneId;

  public DateBinFunctionColumnTransformer(
      Type returnType,
      long monthDuration,
      long nonMonthDuration,
      ColumnTransformer childColumnTransformer,
      long origin,
      ZoneId zoneId) {
    super(returnType, childColumnTransformer);
    this.monthDuration = (int) monthDuration;
    this.nonMonthDuration = nonMonthDuration;
    this.origin = origin;
    this.zoneId = zoneId;
  }

  // Harmonized to nanosecond timestamp accuracy
  private static LocalDateTime convertToLocalDateTime(long timestamp, ZoneId zoneId) {
    Instant instant;

    switch (CommonDescriptor.getInstance().getConfig().getTimestampPrecision()) {
      case "ms":
        instant = Instant.ofEpochMilli(timestamp);
        break;
      case "us":
        instant =
            Instant.ofEpochSecond(
                TimeUnit.MICROSECONDS.toSeconds(timestamp), (timestamp % 1_000_000) * 1000);
        break;
      case "ns":
        instant =
            Instant.ofEpochSecond(
                TimeUnit.NANOSECONDS.toSeconds(timestamp), timestamp % 1_000_000_000);
        break;
      default:
        throw new IllegalArgumentException(
            CalcMessages.EXCEPTION_UNSUPPORTED_PRECISION_CDB58979
                + CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    }

    return LocalDateTime.ofInstant(instant, zoneId);
  }

  private static long convertToTimestamp(LocalDateTime dateTime, ZoneId zoneId) {
    // Converting LocalDateTime to Seconds since epoch
    long epochMilliSecond = dateTime.atZone(zoneId).toInstant().toEpochMilli();
    // Get the nanoseconds section
    long nanoAdjustment = dateTime.getNano();

    switch (CommonDescriptor.getInstance().getConfig().getTimestampPrecision()) {
      case "ms":
        return epochMilliSecond + nanoAdjustment / NANOSECONDS_IN_MILLISECOND;
      case "us":
        return TimeUnit.MILLISECONDS.toMicros(epochMilliSecond)
            + nanoAdjustment / NANOSECONDS_IN_MICROSECOND;
      case "ns":
        return TimeUnit.MILLISECONDS.toNanos(epochMilliSecond) + nanoAdjustment;
      default:
        throw new IllegalArgumentException(
            CalcMessages.EXCEPTION_UNKNOWN_PRECISION_0119BEB0
                + CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    }
  }

  private static long getNanoTimeStamp(long timestamp) {
    switch (CommonDescriptor.getInstance().getConfig().getTimestampPrecision()) {
      case "ms":
        return TimeUnit.MILLISECONDS.toNanos(timestamp);
      case "us":
        return TimeUnit.MICROSECONDS.toNanos(timestamp);
      case "ns":
        return timestamp;
      default:
        throw new IllegalArgumentException(
            CalcMessages.EXCEPTION_UNKNOWN_PRECISION_0119BEB0
                + CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    }
  }

  public static long dateBin(
      long source, long origin, int monthDuration, long nonMonthDuration, ZoneId zoneId) {
    // return source if interval is 0
    if (monthDuration == 0 && nonMonthDuration == 0) {
      return source;
    }
    if (monthDuration != 0) {
      // convert to LocalDateTime
      LocalDateTime sourceDate = convertToLocalDateTime(source, zoneId);
      LocalDateTime originDate = convertToLocalDateTime(origin, zoneId);

      // calculate the number of months between the origin and source
      long monthsDiff = ChronoUnit.MONTHS.between(originDate, sourceDate);
      // calculate the number of month cycles completed
      long completedMonthCycles = monthsDiff / monthDuration;

      LocalDateTime binStart =
          originDate
              .plusNanos(getNanoTimeStamp(nonMonthDuration) * completedMonthCycles)
              .plusMonths(completedMonthCycles * monthDuration);

      if (binStart.isAfter(sourceDate)) {
        binStart =
            binStart.minusMonths(monthDuration).minusNanos(getNanoTimeStamp(nonMonthDuration));
      }

      return convertToTimestamp(binStart, zoneId);
    }

    return saturateToLong(getNonMonthDateBinStart(source, origin, nonMonthDuration));
  }

  public long[] dateBinStartEnd(long source) {
    return dateBinStartEnd(source, false);
  }

  public long[] dateBinStartEndClosed(long source) {
    return dateBinStartEnd(source, true);
  }

  private long[] dateBinStartEnd(long source, boolean closedEnd) {
    // return source if interval is 0
    if (monthDuration == 0 && nonMonthDuration == 0) {
      return new long[] {source, source};
    }

    if (monthDuration != 0) {
      // convert to LocalDateTime
      LocalDateTime sourceDate = convertToLocalDateTime(source, zoneId);
      LocalDateTime originDate = convertToLocalDateTime(origin, zoneId);

      // calculate the number of months between the origin and source
      long monthsDiff = ChronoUnit.MONTHS.between(originDate, sourceDate);
      // calculate the number of month cycles completed
      long completedMonthCycles = monthsDiff / monthDuration;

      LocalDateTime binStart =
          originDate
              .plusNanos(getNanoTimeStamp(nonMonthDuration) * completedMonthCycles)
              .plusMonths(completedMonthCycles * monthDuration);

      if (binStart.isAfter(sourceDate)) {
        binStart =
            binStart.minusMonths(monthDuration).minusNanos(getNanoTimeStamp(nonMonthDuration));
      }

      long startTime = convertToTimestamp(binStart, zoneId);
      long endTime = convertToTimestamp(binStart.plusMonths(monthDuration), zoneId);
      return new long[] {startTime, closedEnd ? saturatingAdd(endTime, -1) : endTime};
    }

    BigInteger startTime = getNonMonthDateBinStart(source, origin, nonMonthDuration);
    BigInteger endTime = startTime.add(BigInteger.valueOf(nonMonthDuration));
    if (closedEnd) {
      endTime = endTime.subtract(BigInteger.ONE);
    }
    return new long[] {saturateToLong(startTime), saturateToLong(endTime)};
  }

  public static long nextDateBin(int monthDuration, ZoneId zoneId, long currentTime) {
    LocalDateTime currentDateTime = convertToLocalDateTime(currentTime, zoneId);
    LocalDateTime nextDateTime = currentDateTime.plusMonths(monthDuration);
    return convertToTimestamp(nextDateTime, zoneId);
  }

  public static long nextDateBin(long nonMonthDuration, long currentTime) {
    return saturatingAdd(currentTime, nonMonthDuration);
  }

  public static long saturatingAdd(long left, long right) {
    return saturateToLong(BigInteger.valueOf(left).add(BigInteger.valueOf(right)));
  }

  private static BigInteger getNonMonthDateBinStart(
      long source, long origin, long nonMonthDuration) {
    BigInteger diff = BigInteger.valueOf(source).subtract(BigInteger.valueOf(origin));
    BigInteger duration = BigInteger.valueOf(nonMonthDuration);
    BigInteger[] quotientAndRemainder = diff.divideAndRemainder(duration);
    BigInteger n = quotientAndRemainder[0];
    if (diff.signum() < 0 && quotientAndRemainder[1].signum() != 0) {
      n = n.subtract(BigInteger.ONE);
    }
    return BigInteger.valueOf(origin).add(n.multiply(duration));
  }

  private static long saturateToLong(BigInteger value) {
    if (value.compareTo(BIG_LONG_MAX) > 0) {
      return Long.MAX_VALUE;
    }
    if (value.compareTo(BIG_LONG_MIN) < 0) {
      return Long.MIN_VALUE;
    }
    return value.longValue();
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        long result = dateBin(column.getLong(i), origin, monthDuration, nonMonthDuration, zoneId);
        columnBuilder.writeLong(result);
      } else {
        // If source is null, return null
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        long result = dateBin(column.getLong(i), origin, monthDuration, nonMonthDuration, zoneId);
        columnBuilder.writeLong(result);
      } else {
        // If source is null, return null
        columnBuilder.appendNull();
      }
    }
  }
}
