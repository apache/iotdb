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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.concurrent.TimeUnit;

public class CommonDateTimeUtils {

  private CommonDateTimeUtils() {
    // forbidding instantiation
  }

  public static final DateTimeFormatter ISO_LOCAL_DATE_WIDTH_1_2;

  static {
    ISO_LOCAL_DATE_WIDTH_1_2 =
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
            .toFormatter();
  }

  /** such as '2011/12/03'. */
  public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_SLASH;

  static {
    ISO_LOCAL_DATE_WITH_SLASH =
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NEVER)
            .appendLiteral('/')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
            .appendLiteral('/')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
            .toFormatter();
  }

  /** such as '2011.12.03'. */
  public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_DOT;

  static {
    ISO_LOCAL_DATE_WITH_DOT =
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NEVER)
            .appendLiteral('.')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
            .appendLiteral('.')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
            .toFormatter();
  }

  /** such as '10:15:30' or '10:15:30.123'. */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_MS;

  static {
    ISO_LOCAL_TIME_WITH_MS =
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendLiteral('.')
            .appendValue(ChronoField.MILLI_OF_SECOND, 3)
            .optionalEnd()
            .toFormatter();
  }

  /** such as '10:15:30' or '10:15:30.123456'. */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_US;

  static {
    ISO_LOCAL_TIME_WITH_US =
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendLiteral('.')
            .appendValue(ChronoField.MICRO_OF_SECOND, 6)
            .optionalEnd()
            .toFormatter();
  }

  /** such as '10:15:30' or '10:15:30.123456789'. */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_NS;

  static {
    ISO_LOCAL_TIME_WITH_NS =
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendLiteral('.')
            .appendValue(ChronoField.NANO_OF_SECOND, 9)
            .optionalEnd()
            .toFormatter();
  }

  /** such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_MS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_MS =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WIDTH_1_2)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_US =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WIDTH_1_2)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456789+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_NS =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WIDTH_1_2)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_US =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456789+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_NS =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_US =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456789+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_NS =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE_US =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456789+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE_NS =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456789+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  /** such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456789+01:00'. */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  public static final DateTimeFormatter formatter =
      new DateTimeFormatterBuilder()
          /**
           * The ISO date-time formatter that formats or parses a date-time with an offset, such as
           * '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_MS)

          /** such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_US)

          /** such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456789+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_NS)

          /** such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH)

          /** such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_US)

          /** such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456789+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_NS)

          /** such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT)

          /** such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_US)

          /** such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456789+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_NS)

          /** such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE)

          /** such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE_US)

          /** such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456789+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE_NS)

          /** such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE)

          /** such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US)

          /** such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456789+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS)

          /** such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE)

          /** such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US)

          /** such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456789+01:00'. */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS)
          .toFormatter();

  public static long convertDatetimeStrToLong(String str, ZoneId zoneId) {
    return convertDatetimeStrToLong(
        str,
        toZoneOffset(zoneId),
        0,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  public static long convertDatetimeStrToLong(
      String str, ZoneId zoneId, String timestampPrecision) {
    return convertDatetimeStrToLong(str, toZoneOffset(zoneId), 0, timestampPrecision);
  }

  public static long getInstantWithPrecision(String str, String timestampPrecision) {
    try {
      ZonedDateTime zonedDateTime = ZonedDateTime.parse(str, formatter);
      Instant instant = zonedDateTime.toInstant();
      if ("us".equals(timestampPrecision)) {
        if (instant.getEpochSecond() < 0 && instant.getNano() > 0) {
          // adjustment can reduce the loss of the division
          long millis = Math.multiplyExact(instant.getEpochSecond() + 1, 1000_000L);
          long adjustment = instant.getNano() / 1000 - 1L;
          return Math.addExact(millis, adjustment);
        } else {
          long millis = Math.multiplyExact(instant.getEpochSecond(), 1000_000L);
          return Math.addExact(millis, instant.getNano() / 1000);
        }
      } else if ("ns".equals(timestampPrecision)) {
        long millis = Math.multiplyExact(instant.getEpochSecond(), 1000_000_000L);
        return Math.addExact(millis, instant.getNano());
      }
      return instant.toEpochMilli();
    } catch (DateTimeParseException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static TimeUnit timestampPrecisionStringToTimeUnit(String timestampPrecision) {
    if ("us".equals(timestampPrecision)) {
      return TimeUnit.MICROSECONDS;
    } else if ("ns".equals(timestampPrecision)) {
      return TimeUnit.NANOSECONDS;
    } else {
      return TimeUnit.MILLISECONDS;
    }
  }

  public static String convertLongToDate(long timestamp) {
    return convertLongToDate(
        timestamp, CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  public static String convertLongToDate(long timestamp, String sourcePrecision) {
    switch (sourcePrecision) {
      case "ns":
        timestamp /= 1000_000;
        break;
      case "us":
        timestamp /= 1000;
        break;
    }
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault())
        .toString();
  }

  public static ZoneOffset toZoneOffset(ZoneId zoneId) {
    return zoneId.getRules().getOffset(Instant.now());
  }

  public static ZonedDateTime convertMillsecondToZonedDateTime(long millisecond) {
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisecond), ZoneId.systemDefault());
  }

  public static long convertMilliTimeWithPrecision(long milliTime, String timePrecision) {
    long result = milliTime;
    switch (timePrecision) {
      case "ns":
        result = milliTime * 1000_000L;
        break;
      case "us":
        result = milliTime * 1000L;
        break;
      default:
        break;
    }
    return result;
  }

  /** convert date time string to millisecond, microsecond or nanosecond. */
  public static long convertDatetimeStrToLong(
      String str, ZoneOffset offset, int depth, String timestampPrecision) {
    if (depth >= 2) {
      throw new DateTimeException(
          String.format(
              "Failed to convert %s to millisecond, zone offset is %s, "
                  + "please input like 2011-12-03T10:15:30 or 2011-12-03T10:15:30+01:00",
              str, offset));
    }
    if (str.contains("Z")) {
      return convertDatetimeStrToLong(
          str.substring(0, str.indexOf('Z')) + "+00:00", offset, depth, timestampPrecision);
    } else if (str.length() == 10) {
      return convertDatetimeStrToLong(str + "T00:00:00", offset, depth, timestampPrecision);
    } else if (str.length() - str.lastIndexOf('+') != 6
        && str.length() - str.lastIndexOf('-') != 6) {
      return convertDatetimeStrToLong(str + offset, offset, depth + 1, timestampPrecision);
    } else if (str.contains("[") || str.contains("]")) {
      throw new DateTimeException(
          String.format(
              "%s with [time-region] at end is not supported now, "
                  + "please input like 2011-12-03T10:15:30 or 2011-12-03T10:15:30+01:00",
              str));
    }
    return getInstantWithPrecision(str, timestampPrecision);
  }

  public enum DurationUnit {
    y,
    mo,
    w,
    d,
    h,
    m,
    s,
    ms,
    us,
    ns
  }

  public static TimeUnit toTimeUnit(String t) {
    switch (t) {
      case "h":
        return TimeUnit.HOURS;
      case "m":
        return TimeUnit.MINUTES;
      case "s":
        return TimeUnit.SECONDS;
      case "ms":
        return TimeUnit.MILLISECONDS;
      case "u":
        return TimeUnit.MICROSECONDS;
      case "n":
        return TimeUnit.NANOSECONDS;
      default:
        throw new IllegalArgumentException("time precision must be one of: h,m,s,ms,u,n");
    }
  }

  public static final long MS_TO_MONTH = 30 * 86400_000L;
}
