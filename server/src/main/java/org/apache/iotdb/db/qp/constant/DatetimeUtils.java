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
package org.apache.iotdb.db.qp.constant;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;

public class DatetimeUtils {

  private DatetimeUtils() {
    // forbidding instantiation
  }

  public static final DateTimeFormatter ISO_LOCAL_DATE_WIDTH_1_2;

  static {
    ISO_LOCAL_DATE_WIDTH_1_2 = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD).appendLiteral('-')
        .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('-')
        .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER).toFormatter();
  }

  /**
   * such as '2011/12/03'.
   */
  public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_SLASH;

  static {
    ISO_LOCAL_DATE_WITH_SLASH = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD).appendLiteral('/')
        .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('/')
        .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER).toFormatter();
  }

  /**
   * such as '2011.12.03'.
   */
  public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_DOT;

  static {
    ISO_LOCAL_DATE_WITH_DOT = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD).appendLiteral('.')
        .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('.')
        .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER).toFormatter();
  }

  /**
   * such as '10:15:30' or '10:15:30.123'.
   */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_MS;

  static {
    ISO_LOCAL_TIME_WITH_MS = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendLiteral('.')
        .appendValue(ChronoField.MILLI_OF_SECOND, 3).optionalEnd().toFormatter();
  }

  /**
   * such as '10:15:30' or '10:15:30.123456'.
   */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_US;

  static {
    ISO_LOCAL_TIME_WITH_US = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendLiteral('.')
        .appendValue(ChronoField.MICRO_OF_SECOND, 6).optionalEnd().toFormatter();
  }

  /**
   * such as '10:15:30' or '10:15:30.123456789'.
   */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_NS;

  static {
    ISO_LOCAL_TIME_WITH_NS = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendLiteral('.')
        .appendValue(ChronoField.NANO_OF_SECOND, 9).optionalEnd().toFormatter();
  }

  /**
   * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_MS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_MS = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WIDTH_1_2).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_MS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WIDTH_1_2).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_US)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WIDTH_1_2).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_NS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_MS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_US)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_NS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_MS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_US)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_NS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_MS)
        .appendOffsetId().toFormatter();
  }

  /**
   * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_US)
        .appendOffsetId().toFormatter();
  }

  /**
   * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_NS)
        .appendOffsetId().toFormatter();
  }

  /**
   * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_MS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_US)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_NS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_MS)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_US)
        .appendOffsetId()
        .toFormatter();
  }

  /**
   * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_NS)
        .appendOffsetId()
        .toFormatter();
  }

  public static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
      /**
       * The ISO date-time formatter that formats or parses a date-time with an offset, such as
       * '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_MS)

      /**
       * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_US)

      /**
       * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456789+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_NS)

      /**
       * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH)

      /**
       * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_US)

      /**
       * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456789+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_NS)

      /**
       * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT)

      /**
       * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_US)

      /**
       * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456789+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_NS)

      /**
       * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE)

      /**
       * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE_US)

      /**
       * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456789+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE_NS)

      /**
       * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE)

      /**
       * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US)

      /**
       * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456789+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS)

      /**
       * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE)

      /**
       * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US)

      /**
       * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456789+01:00'.
       */
      .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS).toFormatter();

  public static long convertDatetimeStrToLong(String str, ZoneId zoneId)
      throws LogicalOperatorException {
    return convertDatetimeStrToLong(str, toZoneOffset(zoneId), 0);
  }

  public static long getInstantWithPrecision(String str, String timestampPrecision)
      throws LogicalOperatorException {
    try {
      ZonedDateTime zonedDateTime = ZonedDateTime.parse(str, formatter);
      Instant instant = zonedDateTime.toInstant();
      if (timestampPrecision.equals("us")) {
        if (instant.getEpochSecond() < 0 && instant.getNano() > 0) {
          // adjustment can reduce the loss of the division
          long millis = Math.multiplyExact(instant.getEpochSecond() + 1, 1000_000);
          long adjustment = instant.getNano() / 1000 - 1L;
          return Math.addExact(millis, adjustment);
        } else {
          long millis = Math.multiplyExact(instant.getEpochSecond(), 1000_000);
          return Math
              .addExact(millis, instant.getNano() / 1000);
        }
      } else if (timestampPrecision.equals("ns")) {
        long millis = Math.multiplyExact(instant.getEpochSecond(), 1000_000_000L);
        return Math
            .addExact(millis, instant.getNano());
      }
      return instant.toEpochMilli();
    } catch (DateTimeParseException e) {
      throw new LogicalOperatorException(e.getMessage());
    }
  }

  /**
   * convert date time string to millisecond, microsecond or nanosecond.
   */
  public static long convertDatetimeStrToLong(String str, ZoneOffset offset, int depth)
      throws LogicalOperatorException {

    String timestampPrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();

    if (depth >= 2) {
      throw new DateTimeException(
          String.format("Failed to convert %s to millisecond, zone offset is %s, "
              + "please input like 2011-12-03T10:15:30 or 2011-12-03T10:15:30+01:00", str, offset));
    }
    if (str.contains("Z")) {
      return convertDatetimeStrToLong(str.substring(0, str.indexOf('Z')) + "+00:00", offset,
          depth);
    } else if (str.length() == 10) {
      return convertDatetimeStrToLong(str + "T00:00:00", offset, depth);
    } else if (str.length() - str.lastIndexOf('+') != 6
        && str.length() - str.lastIndexOf('-') != 6) {
      return convertDatetimeStrToLong(str + offset, offset, depth + 1);
    } else if (str.contains("[") || str.contains("]")) {
      throw new DateTimeException(
          String.format("%s with [time-region] at end is not supported now, "
              + "please input like 2011-12-03T10:15:30 or 2011-12-03T10:15:30+01:00", str));
    }
    return getInstantWithPrecision(str, timestampPrecision);
  }

  /**
   * convert duration string to millisecond, microsecond or nanosecond.
   */
  public static long convertDurationStrToLong(long value, String unit, String timestampPrecision) {
    DurationUnit durationUnit = DurationUnit.valueOf(unit);
    long res = value;
    switch (durationUnit) {
      case y:
        res *= 365 * 86400_000L;
        break;
      case mo:
        res *= 30 * 86400_000L;
        break;
      case w:
        res *= 7 * 86400_000L;
        break;
      case d:
        res *= 86400_000L;
        break;
      case h:
        res *= 3600_000L;
        break;
      case m:
        res *= 60_000L;
        break;
      case s:
        res *= 1_000L;
        break;
      default:
        break;
    }

    if (timestampPrecision.equals("us")) {
      if (unit.equals(DurationUnit.ns.toString())) {
        return value / 1000;
      } else if (unit.equals(DurationUnit.us.toString())) {
        return value;
      } else {
        return res * 1000;
      }
    } else if (timestampPrecision.equals("ns")) {
      if (unit.equals(DurationUnit.ns.toString())) {
        return value;
      } else if (unit.equals(DurationUnit.us.toString())) {
        return value * 1000;
      } else {
        return res * 1000_000;
      }
    } else {
      if (unit.equals(DurationUnit.ns.toString())) {
        return value / 1000_000;
      } else if (unit.equals(DurationUnit.us.toString())) {
        return value / 1000;
      } else {
        return res;
      }
    }

  }

  public static ZoneOffset toZoneOffset(ZoneId zoneId) {
    return zoneId.getRules().getOffset(Instant.now());
  }

  public static ZonedDateTime convertMillsecondToZonedDateTime(long millisecond) {
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisecond), ZoneId.systemDefault());
  }

  public enum DurationUnit {
    y, mo, w, d, h, m, s, ms, us, ns
  }
}
