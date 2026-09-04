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
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;
import org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor;
import org.apache.iotdb.db.queryengine.plan.parser.SqlParseError;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.tsfile.utils.TimeDuration;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataNodeDateTimeUtils {
  private static final Pattern CQ_DURATION_COMPONENT =
      // Match multi-character units before their one-character prefixes (for example, ms before
      // m), otherwise 1ms would be tokenized as 1m followed by an invalid trailing s.
      Pattern.compile("(\\d+)(y|mo|w|d|h|ms|us|ns|m|s)", Pattern.CASE_INSENSITIVE);

  /**
   * Parses the CQ duration grammar while retaining calendar months. Full aliases are deliberate not
   * accepted here; CQ uses the same mo/y abbreviations as Tree SQL.
   */
  public static TimeDuration constructTimeDurationForCQ(String duration) {
    if (duration == null || duration.isEmpty()) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_CQ_DURATION_CANNOT_BE_EMPTY_C7269AB2);
    }
    Matcher matcher = CQ_DURATION_COMPONENT.matcher(duration);
    long months = 0;
    long fixed = 0;
    int end = 0;
    String precision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    while (matcher.find()) {
      if (matcher.start() != end) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_INVALID_CQ_DURATION_ARG_F4917D5C, duration));
      }
      long value;
      try {
        value = Long.parseLong(matcher.group(1));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_CQ_DURATION_COMPONENT_OVERFLOWS_ARG_ED5B0962,
                duration),
            e);
      }
      String unit = matcher.group(2).toLowerCase(Locale.ROOT);
      if (unit.equals("y")) {
        months = Math.addExact(months, Math.multiplyExact(value, 12));
      } else if (unit.equals("mo")) {
        months = Math.addExact(months, value);
      } else {
        fixed = Math.addExact(fixed, convertDurationStrToLong(-1, value, unit, precision));
      }
      end = matcher.end();
    }
    if (end != duration.length()) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_INVALID_CQ_DURATION_ARG_F4917D5C, duration));
    }
    if (months > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_CQ_DURATION_MONTH_COMPONENT_OVERFLOWS_ARG_EBF2A2B6,
              duration));
    }
    return new TimeDuration((int) months, fixed);
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
    return astVisitor.parseDateExpression(
        parser1.dateExpression(), DateTimeUtils.getTimestampPrecision());
  }

  /** convert duration string to millisecond, microsecond or nanosecond. */
  public static long convertDurationStrToLong(
      long currentTime, long value, String unit, String timestampPrecision) {
    DateTimeUtils.DurationUnit durationUnit = DateTimeUtils.DurationUnit.valueOf(unit);
    long res = value;
    switch (durationUnit) {
      case y:
      case year:
        res = Math.multiplyExact(res, 365 * 86_400_000L);
        break;
      case mo:
      case month:
        if (currentTime == -1) {
          res = Math.multiplyExact(res, 30 * 86_400_000L);
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
        res = Math.multiplyExact(res, 7 * 86_400_000L);
        break;
      case d:
      case day:
        res = Math.multiplyExact(res, 86_400_000L);
        break;
      case h:
      case hour:
        res = Math.multiplyExact(res, 3_600_000L);
        break;
      case m:
      case minute:
        res = Math.multiplyExact(res, 60_000L);
        break;
      case s:
      case second:
        res = Math.multiplyExact(res, 1_000L);
        break;
      default:
        break;
    }

    if ("us".equals(timestampPrecision) || "microsecond".equals(timestampPrecision)) {
      if (unit.equals(DateTimeUtils.DurationUnit.ns.toString())
          || unit.equals(DateTimeUtils.DurationUnit.nanosecond.toString())) {
        return value / 1000;
      } else if (unit.equals(DateTimeUtils.DurationUnit.us.toString())
          || unit.equals(DateTimeUtils.DurationUnit.microsecond.toString())) {
        return value;
      } else {
        return Math.multiplyExact(res, 1000);
      }
    } else if ("ns".equals(timestampPrecision) || "nanosecond".equals(timestampPrecision)) {
      if (unit.equals(DateTimeUtils.DurationUnit.ns.toString())
          || unit.equals(DateTimeUtils.DurationUnit.nanosecond.toString())) {
        return value;
      } else if (unit.equals(DateTimeUtils.DurationUnit.us.toString())
          || unit.equals(DateTimeUtils.DurationUnit.microsecond.toString())) {
        return Math.multiplyExact(value, 1000);
      } else {
        return Math.multiplyExact(res, 1000_000);
      }
    } else {
      if (unit.equals(DateTimeUtils.DurationUnit.ns.toString())
          || unit.equals(DateTimeUtils.DurationUnit.nanosecond.toString())) {
        return value / 1000_000;
      } else if (unit.equals(DateTimeUtils.DurationUnit.us.toString())
          || unit.equals(DateTimeUtils.DurationUnit.microsecond.toString())) {
        return value / 1000;
      } else {
        return res;
      }
    }
  }

  @TestOnly
  public static long convertDurationStrToLongForTest(
      long value, String unit, String timestampPrecision) {
    return convertDurationStrToLong(-1, value, unit, timestampPrecision);
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
        temp = Math.addExact(Math.multiplyExact(temp, 10), ch - '0');
      } else {
        String unit = String.valueOf(duration.charAt(i));
        // This is to identify units with two letters.
        if (i + 1 < duration.length() && !Character.isDigit(duration.charAt(i + 1))) {
          i++;
          unit += duration.charAt(i);
        }
        unit = unit.toLowerCase();
        if (convertYearToMonth && unit.equals("y")) {
          temp = Math.multiplyExact(temp, 12);
          unit = "mo";
        }
        total =
            Math.addExact(
                total,
                convertDurationStrToLong(
                    currentTime == -1 ? -1 : Math.addExact(currentTime, total),
                    temp,
                    unit,
                    timestampPrecision));
        temp = 0;
      }
    }
    return total;
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
        nonMonthDuration += convertDurationStrToLong(-1, temp, unit.toString(), currTimePrecision);
        temp = 0;
      }
    }
    return new TimeDuration((int) monthDuration, nonMonthDuration);
  }
}
