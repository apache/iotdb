/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.utils.DateTimeUtils.TIMESTAMP_PRECISION;

public class DateBinFunctionColumnTransformer extends UnaryColumnTransformer {

  private static final long NANOSECONDS_IN_MILLISECOND = 1_000_000;
  private static final long NANOSECONDS_IN_MICROSECOND = 1_000;

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

    switch (TIMESTAMP_PRECISION) {
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
        throw new IllegalArgumentException("Unsupported precision: " + TIMESTAMP_PRECISION);
    }

    return LocalDateTime.ofInstant(instant, zoneId);
  }

  private static long convertToTimestamp(LocalDateTime dateTime, ZoneId zoneId) {
    // Converting LocalDateTime to Seconds since epoch
    long epochMilliSecond = dateTime.atZone(zoneId).toInstant().toEpochMilli();
    // Get the nanoseconds section
    long nanoAdjustment = dateTime.getNano();

    switch (TIMESTAMP_PRECISION) {
      case "ms":
        return epochMilliSecond + nanoAdjustment / NANOSECONDS_IN_MILLISECOND;
      case "us":
        return TimeUnit.MILLISECONDS.toMicros(epochMilliSecond)
            + nanoAdjustment / NANOSECONDS_IN_MICROSECOND;
      case "ns":
        return TimeUnit.MILLISECONDS.toNanos(epochMilliSecond) + nanoAdjustment;
      default:
        throw new IllegalArgumentException("Unknown precision: " + TIMESTAMP_PRECISION);
    }
  }

  private static long getNanoTimeStamp(long timestamp) {
    switch (TIMESTAMP_PRECISION) {
      case "ms":
        return TimeUnit.MILLISECONDS.toNanos(timestamp);
      case "us":
        return TimeUnit.MICROSECONDS.toNanos(timestamp);
      case "ns":
        return timestamp;
      default:
        throw new IllegalArgumentException("Unknown precision: " + TIMESTAMP_PRECISION);
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

    long diff = source - origin;
    long n = diff >= 0 ? diff / nonMonthDuration : (diff - nonMonthDuration + 1) / nonMonthDuration;
    return origin + (n * nonMonthDuration);
  }

  public long[] dateBinStartEnd(long source) {
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

      return new long[] {
        convertToTimestamp(binStart, zoneId),
        convertToTimestamp(binStart.plusMonths(monthDuration), zoneId)
      };
    }

    long diff = source - origin;
    long n = diff >= 0 ? diff / nonMonthDuration : (diff - nonMonthDuration + 1) / nonMonthDuration;
    return new long[] {
      origin + (n * nonMonthDuration), origin + (n * nonMonthDuration) + nonMonthDuration
    };
  }

  public static long nextDateBin(int monthDuration, ZoneId zoneId, long currentTime) {
    LocalDateTime currentDateTime = convertToLocalDateTime(currentTime, zoneId);
    LocalDateTime nextDateTime = currentDateTime.plusMonths(monthDuration);
    return convertToTimestamp(nextDateTime, zoneId);
  }

  public static long nextDateBin(long nonMonthDuration, long currentTime) {
    return currentTime + nonMonthDuration;
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
