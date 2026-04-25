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
package org.apache.iotdb.commons.queryengine.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;

import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.TimeDuration;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;

public class DateTimeUtils {

  private DateTimeUtils() {
    // forbidding instantiation
  }

  private static String timestampPrecision;

  public static String getTimestampPrecision() {
    return timestampPrecision;
  }

  public static long correctPrecision(long millis) {
    try {
      switch (timestampPrecision) {
        case "us":
        case "microsecond":
          return Math.multiplyExact(millis, 1_000L);
        case "ns":
        case "nanosecond":
          return Math.multiplyExact(millis, 1_000_000L);
        case "ms":
        case "millisecond":
        default:
          return millis;
      }
    } catch (ArithmeticException e) {
      throw new IoTDBRuntimeException(
          String.format(
              "Timestamp overflow, Millisecond: %s , Timestamp precision: %s",
              millis, timestampPrecision),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
    }
  }

  private static Function<Long, Long> castTimestampToMs;
  private static Function<Long, Long> extractTimestampMsPart;
  private static Function<Long, Long> extractTimestampUsPart;
  private static Function<Long, Long> extractTimestampNsPart;

  private static void updateFunction() {
    switch (timestampPrecision) {
      case "us":
      case "microsecond":
        castTimestampToMs = timestamp -> timestamp / 1000;
        extractTimestampMsPart = timestamp -> Math.floorMod(timestamp, 1000_000L) / 1000;
        extractTimestampUsPart = timestamp -> Math.floorMod(timestamp, 1000L);
        extractTimestampNsPart = timestamp -> 0L;
        break;
      case "ns":
      case "nanosecond":
        castTimestampToMs = timestamp -> timestamp / 1000000;
        extractTimestampMsPart = timestamp -> Math.floorMod(timestamp, 1000_000_000L) / 1000_000;
        extractTimestampUsPart = timestamp -> Math.floorMod(timestamp, 1000_000L) / 1000;
        extractTimestampNsPart = timestamp -> Math.floorMod(timestamp, 1000L);
        break;
      case "ms":
      case "millisecond":
      default:
        castTimestampToMs = timestamp -> timestamp;
        extractTimestampMsPart = timestamp -> Math.floorMod(timestamp, 1000L);
        extractTimestampUsPart = timestamp -> 0L;
        extractTimestampNsPart = timestamp -> 0L;
        break;
    }
  }

  public static Function<Long, Long> getExtractTimestampMsPartFunction() {
    if (extractTimestampMsPart == null) {
      throw new IllegalArgumentException("ExtractTimestampMsPart is null");
    }
    return extractTimestampMsPart;
  }

  public static Function<Long, Long> getExtractTimestampUsPartFunction() {
    if (extractTimestampUsPart == null) {
      throw new IllegalArgumentException("ExtractTimestampUsPart is null");
    }
    return extractTimestampUsPart;
  }

  public static Function<Long, Long> getExtractTimestampNsPartFunction() {
    if (extractTimestampNsPart == null) {
      throw new IllegalArgumentException("ExtractTimestampNsPart is null");
    }
    return extractTimestampNsPart;
  }

  public static void initTimestampPrecision() {
    timestampPrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    updateFunction();
  }

  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
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
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
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
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
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
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
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
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  public static long convertTimestampOrDatetimeStrToLongWithDefaultZone(String timeStr) {
    try {
      return Long.parseLong(timeStr);
    } catch (NumberFormatException e) {
      return DateTimeUtils.convertDatetimeStrToLong(timeStr, ZoneId.systemDefault());
    }
  }

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
    ZonedDateTime zonedDateTime = ZonedDateTime.parse(str, formatter);
    Instant instant = zonedDateTime.toInstant();
    if ("us".equals(timestampPrecision) || "microsecond".equals(timestampPrecision)) {
      if (instant.getEpochSecond() < 0 && instant.getNano() > 0) {
        // adjustment can reduce the loss of the division
        long millis = Math.multiplyExact(instant.getEpochSecond() + 1, 1000_000L);
        long adjustment = instant.getNano() / 1000 - 1L;
        return Math.addExact(millis, adjustment);
      } else {
        long millis = Math.multiplyExact(instant.getEpochSecond(), 1000_000L);
        return Math.addExact(millis, instant.getNano() / 1000);
      }
    } else if ("ns".equals(timestampPrecision) || "nanosecond".equals(timestampPrecision)) {
      long millis = Math.multiplyExact(instant.getEpochSecond(), 1000_000_000L);
      return Math.addExact(millis, instant.getNano());
    }
    return instant.toEpochMilli();
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

  public static TimeUnit timestampPrecisionStringToTimeUnit(String timestampPrecision) {
    if ("us".equals(timestampPrecision) || "microsecond".equals(timestampPrecision)) {
      return TimeUnit.MICROSECONDS;
    } else if ("ns".equals(timestampPrecision) || "nanosecond".equals(timestampPrecision)) {
      return TimeUnit.NANOSECONDS;
    } else {
      return TimeUnit.MILLISECONDS;
    }
  }

  public static String convertLongToDate(long timestamp) {
    return convertLongToDate(
        timestamp,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision(),
        ZoneId.systemDefault());
  }

  public static String convertLongToDate(final long timestamp, final ZoneId zoneId) {
    return convertLongToDate(
        timestamp, CommonDescriptor.getInstance().getConfig().getTimestampPrecision(), zoneId);
  }

  public static String convertLongToDate(final long timestamp, final String sourcePrecision) {
    return convertLongToDate(timestamp, sourcePrecision, ZoneId.systemDefault());
  }

  public static String convertLongToDate(
      long timestamp, final String sourcePrecision, final ZoneId zoneId) {
    switch (sourcePrecision) {
      case "ns":
      case "nanosecond":
        timestamp /= 1000_000;
        break;
      case "us":
      case "microsecond":
        timestamp /= 1000;
        break;
      default:
        break;
    }
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).toString();
  }

  public static LocalDate convertToLocalDate(long timestamp, ZoneId zoneId) {
    timestamp = castTimestampToMs.apply(timestamp);
    return Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDate();
  }

  public static ZonedDateTime convertToZonedDateTime(long timestamp, ZoneId zoneId) {
    timestamp = castTimestampToMs.apply(timestamp);
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);
  }

  public static ZoneOffset toZoneOffset(ZoneId zoneId) {
    return zoneId.getRules().getOffset(Instant.now());
  }

  public static ZonedDateTime convertMillsecondToZonedDateTime(long millisecond) {
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisecond), ZoneId.systemDefault());
  }

  public enum DurationUnit {
    y,
    year,
    mo,
    month,
    w,
    week,
    d,
    day,
    h,
    hour,
    m,
    minute,
    s,
    second,
    ms,
    millisecond,
    us,
    microsecond,
    ns,
    nanosecond
  }

  public static TimeUnit toTimeUnit(String t) {
    switch (t) {
      case "h":
      case "hour":
        return TimeUnit.HOURS;
      case "m":
      case "minute":
        return TimeUnit.MINUTES;
      case "s":
      case "second":
        return TimeUnit.SECONDS;
      case "ms":
      case "millisecond":
        return TimeUnit.MILLISECONDS;
      case "u":
      case "microsecond":
        return TimeUnit.MICROSECONDS;
      case "n":
      case "nanosecond":
        return TimeUnit.NANOSECONDS;
      default:
        throw new IllegalArgumentException("time precision must be one of: h,m,s,ms,u,n");
    }
  }

  public static final long MS_TO_MONTH = 30 * 86400_000L;

  public static long calcPositiveIntervalByMonth(
      long startTime, TimeDuration duration, ZoneId zoneId) {
    return TimeDuration.calcPositiveIntervalByMonth(
        startTime, duration, TimeZone.getTimeZone(zoneId), TimestampPrecisionUtils.currPrecision);
  }

  public static Integer parseDateExpressionToInt(String dateExpression) {
    return DateUtils.parseDateExpressionToInt(dateExpression);
  }
}
