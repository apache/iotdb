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

package org.apache.iotdb.postgresql.sql;

import org.apache.iotdb.postgresql.types.TimestampPrecision;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date and time rules: {@code date_trunc}, {@code date_part}/{@code EXTRACT} and interval
 * arithmetic, each only where the table-model form gives PostgreSQL's result in the session time
 * zone. The engine's {@code date_bin} is calendar-aware for months (in the session time zone) and
 * fixed-length for everything else; its casts between TIMESTAMP and DATE use the session time zone.
 *
 * <p>Templates contain {@code %s} where the argument goes, already cast to TIMESTAMP.
 */
final class DateTimeRules {

  /** Transitions after this instant are checked (data before 1970 is not expected). */
  private static final Instant SINCE = Instant.parse("1970-01-01T00:00:00Z");

  private static final Instant UNTIL = Instant.parse("2100-01-01T00:00:00Z");

  private static final long MICROS_PER_SECOND = 1_000_000L;

  private DateTimeRules() {}

  /** The template for {@code date_trunc(unit, %s)}. */
  static String truncate(String rawUnit, ZoneId zone, TimestampPrecision precision) {
    String unit = rawUnit.toLowerCase(Locale.ROOT);
    switch (unit) {
      case "microseconds":
        return fixedBin(1, zone, precision, 1, unit);
      case "milliseconds":
        return fixedBin(1_000, zone, precision, 1, unit);
      case "second":
        return fixedBin(MICROS_PER_SECOND, zone, precision, 1, unit);
      case "minute":
        return fixedBin(60 * MICROS_PER_SECOND, zone, precision, 60, unit);
      case "hour":
        return fixedBin(3600 * MICROS_PER_SECOND, zone, precision, 3600, unit);
      case "day":
        // the engine's casts between TIMESTAMP and DATE use the session time zone: local midnight
        return "CAST(CAST(%s AS DATE) AS TIMESTAMP)";
      case "week":
        if (!withoutTransitions(zone)) {
          throw new UnsupportedConstructException(
              "date_trunc('week') in a time zone with daylight saving time (" + zone + ")");
        }
        // weeks start on Monday; 1970-01-05 was one
        return "date_bin(1w, %s, " + localMidnight(LocalDate.of(1970, 1, 5), zone, precision) + ")";
      case "month":
        return monthBin(1, zone, precision);
      case "quarter":
        return monthBin(3, zone, precision);
      case "year":
        return monthBin(12, zone, precision);
      case "decade":
        return monthBin(120, zone, precision);
      default:
        throw new UnsupportedConstructException("date_trunc('" + rawUnit + "')");
    }
  }

  private static String fixedBin(
      long micros, ZoneId zone, TimestampPrecision precision, int offsetMultiple, String unit) {
    if (offsetMultiple > 1 && !offsetsAreMultiplesOf(zone, offsetMultiple)) {
      throw new UnsupportedConstructException(
          "date_trunc('" + unit + "') in the time zone " + zone + " (its UTC offset is not whole)");
    }
    return "date_bin(" + duration(micros, precision, "date_trunc('" + unit + "')") + ", %s, 0)";
  }

  private static String monthBin(int months, ZoneId zone, TimestampPrecision precision) {
    // month bins are calendar-aware; the origin is local 1970-01-01 00:00
    return "date_bin("
        + months
        + "mo, %s, "
        + localMidnight(LocalDate.of(1970, 1, 1), zone, precision)
        + ")";
  }

  /** The table-model duration literal of a whole number of microseconds. */
  private static String duration(long micros, TimestampPrecision precision, String what) {
    long units = precision.fromEpochMicros(micros);
    if (precision.toEpochMicros(units) != micros) {
      throw new UnsupportedConstructException(what + " below the timestamp precision");
    }
    if (micros % (3600 * MICROS_PER_SECOND) == 0) {
      return micros / (3600 * MICROS_PER_SECOND) + "h";
    }
    if (micros % (60 * MICROS_PER_SECOND) == 0) {
      return micros / (60 * MICROS_PER_SECOND) + "m";
    }
    if (micros % MICROS_PER_SECOND == 0) {
      return micros / MICROS_PER_SECOND + "s";
    }
    if (micros % 1000 == 0) {
      return micros / 1000 + "ms";
    }
    return micros + "us";
  }

  private static long localMidnight(LocalDate date, ZoneId zone, TimestampPrecision precision) {
    Instant instant = date.atStartOfDay(zone).toInstant();
    return precision.fromEpochMicros(
        instant.getEpochSecond() * MICROS_PER_SECOND + instant.getNano() / 1000);
  }

  /** True if every UTC offset of {@code zone} since 1970 is a multiple of {@code seconds}. */
  static boolean offsetsAreMultiplesOf(ZoneId zone, int seconds) {
    ZoneRules rules = zone.getRules();
    if (rules.getOffset(SINCE).getTotalSeconds() % seconds != 0) {
      return false;
    }
    ZoneOffsetTransition transition = rules.nextTransition(SINCE);
    while (transition != null && transition.getInstant().isBefore(UNTIL)) {
      if (transition.getOffsetAfter().getTotalSeconds() % seconds != 0) {
        return false;
      }
      transition = rules.nextTransition(transition.getInstant());
    }
    return true;
  }

  /** True if {@code zone} has had one UTC offset since 1970 (no daylight saving time). */
  static boolean withoutTransitions(ZoneId zone) {
    ZoneRules rules = zone.getRules();
    if (rules.isFixedOffset()) {
      return true;
    }
    ZoneOffsetTransition transition = rules.nextTransition(SINCE);
    return transition == null || !transition.getInstant().isBefore(UNTIL);
  }

  // ---------------------------------------------------------------------------- literals

  private static final Pattern TIMESTAMP =
      Pattern.compile(
          "(\\d{4}-\\d{1,2}-\\d{1,2})(?:[ T](\\d{1,2}):(\\d{2})(?::(\\d{2})(?:\\.(\\d{1,9}))?)?)?"
              + "\\s*(Z|[+-]\\d{1,2}(?::?\\d{2})?)?");

  /**
   * A timestamp literal in timestamp units, as PostgreSQL reads it: {@code timestamp} literals
   * ignore an offset and are local times in the session time zone; {@code timestamptz} literals use
   * their offset, or the session time zone without one.
   */
  static long timestampLiteral(
      String text, boolean withTimeZone, ZoneId zone, TimestampPrecision precision) {
    Matcher m = TIMESTAMP.matcher(text.trim());
    if (!m.matches()) {
      throw new UnsupportedConstructException("the timestamp literal '" + text + "'");
    }
    java.time.LocalDateTime local;
    try {
      local =
          java.time.LocalDate.parse(normalizeDate(m.group(1)))
              .atTime(
                  m.group(2) == null ? 0 : Integer.parseInt(m.group(2)),
                  m.group(3) == null ? 0 : Integer.parseInt(m.group(3)),
                  m.group(4) == null ? 0 : Integer.parseInt(m.group(4)),
                  m.group(5) == null
                      ? 0
                      : Integer.parseInt((m.group(5) + "000000000").substring(0, 9)));
    } catch (java.time.DateTimeException e) {
      throw new UnsupportedConstructException("the timestamp literal '" + text + "'");
    }
    Instant instant;
    if (withTimeZone && m.group(6) != null) {
      instant = local.toInstant(offset(m.group(6)));
    } else {
      instant = local.atZone(zone).toInstant();
    }
    long micros = instant.getEpochSecond() * MICROS_PER_SECOND + instant.getNano() / 1000;
    long units = precision.fromEpochMicros(micros);
    if (precision.toEpochMicros(units) != micros) {
      throw new UnsupportedConstructException(
          "the timestamp literal '" + text + "' (below the timestamp precision)");
    }
    return units;
  }

  /** A DATE literal as ISO text ({@code 2026-3-1} → {@code 2026-03-01}). */
  static String dateLiteral(String text) {
    try {
      return java.time.LocalDate.parse(normalizeDate(text.trim())).toString();
    } catch (java.time.DateTimeException e) {
      throw new UnsupportedConstructException("the date literal '" + text + "'");
    }
  }

  private static String normalizeDate(String date) {
    String[] parts = date.split("-");
    if (parts.length != 3) {
      return date;
    }
    return parts[0]
        + "-"
        + (parts[1].length() == 1 ? "0" : "")
        + parts[1]
        + "-"
        + (parts[2].length() == 1 ? "0" : "")
        + parts[2];
  }

  private static java.time.ZoneOffset offset(String text) {
    if (text.equals("Z")) {
      return java.time.ZoneOffset.UTC;
    }
    String digits = text.substring(1).replace(":", "");
    int hours =
        Integer.parseInt(digits.length() <= 2 ? digits : digits.substring(0, digits.length() - 2));
    int minutes =
        digits.length() <= 2 ? 0 : Integer.parseInt(digits.substring(digits.length() - 2));
    int sign = text.startsWith("-") ? -1 : 1;
    return java.time.ZoneOffset.ofHoursMinutes(sign * hours, sign * minutes);
  }

  // ---------------------------------------------------------------------------- EXTRACT

  /** The expression for {@code EXTRACT(field FROM %s)} / {@code date_part('field', %s)}. */
  static String extract(String rawField, ZoneId zone, TimestampPrecision precision) {
    String field = rawField.toLowerCase(Locale.ROOT);
    switch (field) {
      case "year":
      case "quarter":
      case "month":
      case "day":
      case "doy":
      case "hour":
      case "minute":
        return "EXTRACT(" + field.toUpperCase(Locale.ROOT) + " FROM %s)";
      case "isodow":
        // the engine counts Monday = 1 ... Sunday = 7, as isodow
        return "EXTRACT(DOW FROM %s)";
      case "dow":
        // PostgreSQL: Sunday = 0 ... Saturday = 6
        return "(EXTRACT(DOW FROM %s) % 7)";
      case "second":
        {
          if (!offsetsAreMultiplesOf(zone, 60)) {
            throw new UnsupportedConstructException(
                "the date part 'second' in the time zone "
                    + zone
                    + " (its UTC offset is not whole)");
          }
          // with the fraction, as PostgreSQL (microsecond resolution)
          long units = precision.fromEpochMicros(MICROS_PER_SECOND);
          long minute = 60 * units;
          return "(CAST(((CAST(%s AS INT64) % "
              + minute
              + ") + "
              + minute
              + ") % "
              + minute
              + " AS DOUBLE) / "
              + units
              + ")";
        }
      case "epoch":
        return "(CAST(CAST(%s AS INT64) AS DOUBLE) / "
            + precision.fromEpochMicros(MICROS_PER_SECOND)
            + ")";
      default:
        throw new UnsupportedConstructException("the date part '" + rawField + "'");
    }
  }

  // ---------------------------------------------------------------------------- intervals

  private static final Pattern PART =
      Pattern.compile("([+-]?+\\d++(?:\\.\\d++)?+)\\s*+([a-z]++)", Pattern.CASE_INSENSITIVE);
  private static final Pattern CLOCK =
      Pattern.compile("([+-]?+)(\\d++):(\\d{2})(?::(\\d{2}(?:\\.\\d++)?+))?+");

  /**
   * The length of an interval literal in timestamp units, for arithmetic with timestamps. Only
   * units of fixed length: days and weeks only in time zones without daylight saving time (in
   * PostgreSQL, adding a day keeps the local time across a DST change); months and years never.
   */
  static long intervalUnits(String text, ZoneId zone, TimestampPrecision precision) {
    String rest = text.trim().toLowerCase(Locale.ROOT);
    double micros = 0;
    boolean any = false;
    Matcher clock = CLOCK.matcher(rest);
    if (clock.find() && clock.end() == rest.length()) {
      double sign = clock.group(1).equals("-") ? -1 : 1;
      double seconds =
          Long.parseLong(clock.group(2)) * 3600
              + Long.parseLong(clock.group(3)) * 60
              + (clock.group(4) == null ? 0 : Double.parseDouble(clock.group(4)));
      micros += sign * seconds * MICROS_PER_SECOND;
      rest = rest.substring(0, clock.start()).trim();
      any = true;
    }
    Matcher m = PART.matcher(rest);
    int position = 0;
    while (m.find()) {
      if (!rest.substring(position, m.start()).trim().isEmpty()) {
        throw new UnsupportedConstructException("the interval '" + text + "'");
      }
      position = m.end();
      double amount = Double.parseDouble(m.group(1));
      micros += amount * unitMicros(m.group(2), zone, text);
      any = true;
    }
    if (!any || !rest.substring(position).trim().isEmpty()) {
      throw new UnsupportedConstructException("the interval '" + text + "'");
    }
    long whole = Math.round(micros);
    if (Math.abs(micros - whole) > 1e-6) {
      throw new UnsupportedConstructException("the interval '" + text + "' (below a microsecond)");
    }
    long units = precision.fromEpochMicros(whole);
    if (precision.toEpochMicros(units) != whole) {
      throw new UnsupportedConstructException(
          "the interval '" + text + "' (below the timestamp precision)");
    }
    return units;
  }

  private static double unitMicros(String unit, ZoneId zone, String text) {
    switch (unit) {
      case "us":
      case "usec":
      case "usecs":
      case "microsecond":
      case "microseconds":
        return 1;
      case "ms":
      case "msec":
      case "msecs":
      case "millisecond":
      case "milliseconds":
        return 1_000;
      case "s":
      case "sec":
      case "secs":
      case "second":
      case "seconds":
        return MICROS_PER_SECOND;
      case "m":
      case "min":
      case "mins":
      case "minute":
      case "minutes":
        return 60.0 * MICROS_PER_SECOND;
      case "h":
      case "hr":
      case "hrs":
      case "hour":
      case "hours":
        return 3600.0 * MICROS_PER_SECOND;
      case "d":
      case "day":
      case "days":
      case "w":
      case "week":
      case "weeks":
        if (!withoutTransitions(zone)) {
          throw new UnsupportedConstructException(
              "interval days in a time zone with daylight saving time (" + zone + ")");
        }
        return (unit.startsWith("w") ? 7 : 1) * 86_400.0 * MICROS_PER_SECOND;
      default:
        throw new UnsupportedConstructException(
            "the interval unit '"
                + unit
                + "' in '"
                + text
                + "' (months and years have no fixed length)");
    }
  }
}
