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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;
import org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor;
import org.apache.iotdb.db.queryengine.plan.parser.SqlParseError;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
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
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class DateTimeUtils {

  private DateTimeUtils() {
    // forbidding instantiation
  }

  public static final String TIMESTAMP_PRECISION =
      CommonDescriptor.getInstance().getConfig().getTimestampPrecision();

  public static long correctPrecision(long millis) {
    switch (TIMESTAMP_PRECISION) {
      case "us":
      case "microsecond":
        return millis * 1_000L;
      case "ns":
      case "nanosecond":
        return millis * 1_000_000L;
      case "ms":
      case "millisecond":
      default:
        return millis;
    }
  }

  private static Function<Long, Long> CAST_TIMESTAMP_TO_MS;

  static {
    switch (CommonDescriptor.getInstance().getConfig().getTimestampPrecision()) {
      case "us":
      case "microsecond":
        CAST_TIMESTAMP_TO_MS = timestamp -> timestamp / 1000;
        break;
      case "ns":
      case "nanosecond":
        CAST_TIMESTAMP_TO_MS = timestamp -> timestamp / 1000000;
        break;
      case "ms":
      case "millisecond":
      default:
        CAST_TIMESTAMP_TO_MS = timestamp -> timestamp;
        break;
    }
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
          .toFormatter();

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
    try {
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
    } catch (DateTimeParseException e) {
      throw new RuntimeException(e.getMessage());
    }
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

  /**
   * Convert duration string to time value. CurrentTime is used to calculate the days of natural
   * month. If it's set as -1, which means a context free situation, then '1mo' will be thought as
   * 30 days.
   *
   * @param duration represent duration string like: 12d8m9ns, 1y1mo, etc.
   * @return time in milliseconds, microseconds, or nanoseconds depending on the profile
   */
  public static long convertDurationStrToLong(String duration) {
    return convertDurationStrToLong(-1, duration, false);
  }

  public static long convertDurationStrToLong(String duration, boolean convertYearToMonth) {
    return convertDurationStrToLong(-1, duration, convertYearToMonth);
  }

  public static long convertDurationStrToLong(
      String duration, String timestampPrecision, boolean convertYearToMonth) {
    return convertDurationStrToLong(-1, duration, timestampPrecision, convertYearToMonth);
  }

  public static long convertDurationStrToLong(
      long currentTime, String duration, boolean convertYearToMonth) {
    return convertDurationStrToLong(
        currentTime,
        duration,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision(),
        convertYearToMonth);
  }

  /**
   * convert duration string to time value.
   *
   * @param duration represent duration string like: 12d8m9ns, 1y1mo, etc.
   * @param convertYearToMonth if we need convert year to month. eg: 1y -> 12mo
   * @return time in milliseconds, microseconds, or nanoseconds depending on the profile
   */
  public static long convertDurationStrToLong(
      long currentTime, String duration, String timestampPrecision, boolean convertYearToMonth) {
    long total = 0;
    long temp = 0;
    for (int i = 0; i < duration.length(); i++) {
      char ch = duration.charAt(i);
      if (Character.isDigit(ch)) {
        temp *= 10;
        temp += (ch - '0');
      } else {
        String unit = String.valueOf(duration.charAt(i));
        // This is to identify units with two letters.
        if (i + 1 < duration.length() && !Character.isDigit(duration.charAt(i + 1))) {
          i++;
          unit += duration.charAt(i);
        }
        unit = unit.toLowerCase();
        if (convertYearToMonth && unit.equals("y")) {
          temp *= 12;
          unit = "mo";
        }
        total +=
            DateTimeUtils.convertDurationStrToLong(
                currentTime == -1 ? -1 : currentTime + total, temp, unit, timestampPrecision);
        temp = 0;
      }
    }
    return total;
  }

  @TestOnly
  public static long convertDurationStrToLongForTest(
      long value, String unit, String timestampPrecision) {
    return convertDurationStrToLong(-1, value, unit, timestampPrecision);
  }

  /** convert duration string to millisecond, microsecond or nanosecond. */
  public static long convertDurationStrToLong(
      long currentTime, long value, String unit, String timestampPrecision) {
    DurationUnit durationUnit = DurationUnit.valueOf(unit);
    long res = value;
    switch (durationUnit) {
      case y:
      case year:
        res *= 365 * 86_400_000L;
        break;
      case mo:
      case month:
        if (currentTime == -1) {
          res *= 30 * 86_400_000L;
        } else {
          Calendar calendar = Calendar.getInstance();
          calendar.setTimeZone(SessionManager.getInstance().getSessionTimeZone());
          calendar.setTimeInMillis(currentTime);
          calendar.add(Calendar.MONTH, (int) (value));
          res = calendar.getTimeInMillis() - currentTime;
        }
        break;
      case w:
      case week:
        res *= 7 * 86_400_000L;
        break;
      case d:
      case day:
        res *= 86_400_000L;
        break;
      case h:
      case hour:
        res *= 3_600_000L;
        break;
      case m:
      case minute:
        res *= 60_000L;
        break;
      case s:
      case second:
        res *= 1_000L;
        break;
      default:
        break;
    }

    if ("us".equals(timestampPrecision) || "microsecond".equals(timestampPrecision)) {
      if (unit.equals(DurationUnit.ns.toString())
          || unit.equals(DurationUnit.nanosecond.toString())) {
        return value / 1000;
      } else if (unit.equals(DurationUnit.us.toString())
          || unit.equals(DurationUnit.microsecond.toString())) {
        return value;
      } else {
        return res * 1000;
      }
    } else if ("ns".equals(timestampPrecision) || "nanosecond".equals(timestampPrecision)) {
      if (unit.equals(DurationUnit.ns.toString())
          || unit.equals(DurationUnit.nanosecond.toString())) {
        return value;
      } else if (unit.equals(DurationUnit.us.toString())
          || unit.equals(DurationUnit.microsecond.toString())) {
        return value * 1000;
      } else {
        return res * 1000_000;
      }
    } else {
      if (unit.equals(DurationUnit.ns.toString())
          || unit.equals(DurationUnit.nanosecond.toString())) {
        return value / 1000_000;
      } else if (unit.equals(DurationUnit.us.toString())
          || unit.equals(DurationUnit.microsecond.toString())) {
        return value / 1000;
      } else {
        return res;
      }
    }
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
    timestamp = CAST_TIMESTAMP_TO_MS.apply(timestamp);
    return Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDate();
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

  public static long calcPositiveIntervalByMonth(long startTime, TimeDuration duration) {
    return TimeDuration.calcPositiveIntervalByMonth(
        startTime,
        duration,
        SessionManager.getInstance().getSessionTimeZone(),
        TimestampPrecisionUtils.currPrecision);
  }

  /**
   * Storage the duration into two parts: month part and non-month part, the non-month part's
   * precision is depended on current time precision. e.g. ms precision: '1y1mo1ms' -> monthDuration
   * = 13, nonMonthDuration = 1, ns precision: '1y1mo1ms' -> monthDuration = 13, nonMonthDuration =
   * 1000_000.
   *
   * @param duration the input duration string
   * @return the TimeDuration instance contains month part and non-month part
   */
  public static TimeDuration constructTimeDuration(String duration) {
    duration = duration.toLowerCase();
    String currTimePrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    long temp = 0;
    long monthDuration = 0;
    long nonMonthDuration = 0;
    int i = 0;
    for (; i < duration.length(); i++) {
      char ch = duration.charAt(i);
      if (Character.isDigit(ch)) {
        temp *= 10;
        temp += (ch - '0');
      } else {
        StringBuilder unit = new StringBuilder(String.valueOf(duration.charAt(i)));
        i++;
        // This is to identify units.
        while (i < duration.length() && !Character.isDigit(duration.charAt(i))) {
          unit.append(duration.charAt(i));
          i++;
        }
        i--;
        if ("y".contentEquals(unit) || "year".contentEquals(unit)) {
          monthDuration += temp * 12;
          temp = 0;
          continue;
        }
        if ("mo".contentEquals(unit) || "month".contentEquals(unit)) {
          monthDuration += temp;
          temp = 0;
          continue;
        }
        nonMonthDuration +=
            DateTimeUtils.convertDurationStrToLong(-1, temp, unit.toString(), currTimePrecision);
        temp = 0;
      }
    }
    return new TimeDuration((int) monthDuration, nonMonthDuration);
  }

  public static Long parseDateTimeExpressionToLong(String dateExpression, ZoneId zoneId) {
    ASTVisitor astVisitor = new ASTVisitor();
    astVisitor.setZoneId(zoneId);

    CharStream charStream1 = CharStreams.fromString(dateExpression);

    SqlLexer lexer1 = new SqlLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SqlParseError.INSTANCE);

    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);

    IoTDBSqlParser parser1 = new IoTDBSqlParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(SqlParseError.INSTANCE);
    return astVisitor.parseDateExpression(parser1.dateExpression(), TIMESTAMP_PRECISION);
  }

  public static Integer parseDateExpressionToInt(String dateExpression) {
    return DateUtils.parseDateExpressionToInt(dateExpression);
  }
}
