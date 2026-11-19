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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.cli.fs.command;

import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.FsWindowMessages;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** An explicit, origin-anchored grid; no data-dependent or benchmark-dependent inference. */
public final class WindowGrid {
  public final List<Bounds> bounds = new ArrayList<>();
  public final ZoneId zone;
  public final String unit;
  public final Long origin;
  public final long width, step;

  private WindowGrid(ZoneId zone, String unit, Long origin, long width, long step) {
    this.zone = zone;
    this.unit = unit;
    this.origin = origin;
    this.width = width;
    this.step = step;
  }

  public static IllegalArgumentException invalid() {
    return new IllegalArgumentException(
        FsWindowMessages
            .EXCEPTION_INVALID_WINDOW_GRID_REQUIRE_INCLUSIVE_START_END_ORIGIN_EPOCH_MS_OR_ISO_LOCAL_OFFSET_DATETIME_POSITIVE_WIDTH_STEP_UNIT_MS_DAY_MONTH_AND_TIMEZONE_LOCAL_BOUNDARIES_MUST_BE_UNAMBIGUOUS_AND_EXISTENT_AT_MOST_256_STARTS_WITHIN_THE_REQUESTED_RANGE_OVERFLOW_IS_REJECTED_BEBD1447);
  }

  public static WindowGrid from(FilesystemCommand c) {
    try {
      ReadOptions read = c.getReadOptions();
      ZoneId zone = ZoneId.of(c.optionValue("--timezone", "UTC"));
      boolean generated = c.hasOption("--origin");
      if (!generated) {
        for (String flag : Arrays.asList("--width", "--step", "--unit"))
          if (c.hasOption(flag)) throw invalid();
        WindowGrid g = new WindowGrid(zone, "explicit", null, 0, 0);
        if (read.getWindows().isEmpty()) {
          if (read.getStart() != null && read.getEnd() != null && read.getStart() > read.getEnd())
            throw invalid();
          g.bounds.add(new Bounds(read.getStart(), read.getEnd(), null, null));
        } else
          for (ReadOptions.Window w : read.getWindows())
            g.bounds.add(new Bounds(w.getStart(), w.getEnd(), null, null));
        if (c.getType() == FilesystemCommand.Type.WINDOWS) throw invalid();
        return g;
      }
      if (read.getStart() == null
          || read.getEnd() == null
          || read.getStart() > read.getEnd()
          || !read.getWindows().isEmpty()) throw invalid();
      long origin = parseOrigin(c.optionValue("--origin", ""), zone);
      long width = Long.parseLong(c.optionValue("--width", "")),
          step = Long.parseLong(c.optionValue("--step", Long.toString(width)));
      String unit = c.optionValue("--unit", "ms");
      if (width <= 0 || step <= 0 || !Arrays.asList("ms", "day", "month").contains(unit))
        throw invalid();
      WindowGrid g = new WindowGrid(zone, unit, origin, width, step);
      long lo = read.getStart(), hi = read.getEnd();
      BigInteger k;
      if (unit.equals("ms")) {
        BigInteger d = BigInteger.valueOf(lo).subtract(BigInteger.valueOf(origin));
        BigInteger[] qr = d.divideAndRemainder(BigInteger.valueOf(step));
        k = qr[0].add(qr[1].signum() > 0 ? BigInteger.ONE : BigInteger.ZERO);
      } else {
        LocalDateTime o = Instant.ofEpochMilli(origin).atZone(zone).toLocalDateTime(),
            l = Instant.ofEpochMilli(lo).atZone(zone).toLocalDateTime();
        long distance =
            unit.equals("day")
                ? ChronoUnit.DAYS.between(o.toLocalDate(), l.toLocalDate())
                : ChronoUnit.MONTHS.between(YearMonth.from(o), YearMonth.from(l));
        k = BigInteger.valueOf(Math.floorDiv(distance, step));
        while (g.at(k).compareTo(BigInteger.valueOf(lo)) < 0) k = k.add(BigInteger.ONE);
        while (g.at(k.subtract(BigInteger.ONE)).compareTo(BigInteger.valueOf(lo)) >= 0)
          k = k.subtract(BigInteger.ONE);
      }
      for (; g.at(k).compareTo(BigInteger.valueOf(hi)) <= 0; k = k.add(BigInteger.ONE)) {
        if (g.bounds.size() == 256) throw invalid();
        long start = g.at(k).longValueExact();
        BigInteger end =
            unit.equals("ms")
                ? BigInteger.valueOf(start).add(BigInteger.valueOf(width))
                : BigInteger.valueOf(calendarEnd(start, width, unit, zone));
        if (end.compareTo(BigInteger.valueOf(start)) <= 0) throw invalid();
        long nominalEnd = end.longValueExact();
        long effective = end.subtract(BigInteger.ONE).min(BigInteger.valueOf(hi)).longValueExact();
        g.bounds.add(new Bounds(start, effective, start, nominalEnd));
      }
      return g;
    } catch (ArithmeticException | DateTimeException e) {
      throw invalid();
    } catch (NumberFormatException e) {
      throw invalid();
    }
  }

  private BigInteger at(BigInteger index) {
    BigInteger amount = index.multiply(BigInteger.valueOf(step));
    if (unit.equals("ms")) return BigInteger.valueOf(origin).add(amount);
    LocalDateTime local = Instant.ofEpochMilli(origin).atZone(zone).toLocalDateTime();
    long n = amount.longValueExact();
    return BigInteger.valueOf(
        resolve(unit.equals("day") ? local.plusDays(n) : local.plusMonths(n), zone));
  }

  private static long calendarEnd(long start, long width, String unit, ZoneId zone) {
    LocalDateTime l = Instant.ofEpochMilli(start).atZone(zone).toLocalDateTime();
    return resolve(unit.equals("day") ? l.plusDays(width) : l.plusMonths(width), zone);
  }

  private static long resolve(LocalDateTime local, ZoneId zone) {
    if (local.getNano() % 1000000 != 0) throw invalid();
    List<ZoneOffset> offsets = zone.getRules().getValidOffsets(local);
    if (offsets.size() != 1) throw invalid();
    return local.toInstant(offsets.get(0)).toEpochMilli();
  }

  private static long parseOrigin(String s, ZoneId zone) {
    if (s.matches("-?[0-9]+")) return Long.parseLong(s);
    try {
      OffsetDateTime parsed = OffsetDateTime.parse(s);
      if (parsed.getNano() % 1000000 != 0) throw invalid();
      return parsed.toInstant().toEpochMilli();
    } catch (java.time.format.DateTimeParseException e) {
      return resolve(LocalDateTime.parse(s), zone);
    }
  }

  public String label(Long ms) {
    return ms == null ? null : Instant.ofEpochMilli(ms).atZone(zone).toString();
  }

  public Map<String, String> descriptor(int i) {
    Bounds b = bounds.get(i);
    Map<String, String> m = new LinkedHashMap<>();
    m.put("window_index", Integer.toString(i));
    m.put("window_start", str(b.start));
    m.put("window_end", str(b.end));
    m.put("nominal_start", str(b.nominalStart));
    m.put("nominal_end_exclusive", str(b.nominalEnd));
    m.put("start_local", label(b.start));
    m.put("end_local", label(b.end));
    m.put("timezone", zone.getId());
    m.put("window_unit", unit);
    m.put("origin_ms", str(origin));
    m.put("width", origin == null ? null : Long.toString(width));
    m.put("step", origin == null ? null : Long.toString(step));
    m.put("boundary", "inclusive");
    m.put("grid_selection", origin == null ? "explicit" : "starts-within-range;right-clipped");
    return m;
  }

  public List<SqlRow> rows() {
    List<SqlRow> r = new ArrayList<>();
    for (int i = 0; i < bounds.size(); i++) r.add(new SqlRow(descriptor(i)));
    return r;
  }

  private static String str(Long n) {
    return n == null ? null : n.toString();
  }

  public String ranges() {
    List<String> r = new ArrayList<>();
    for (Bounds b : bounds) r.add(b.start + ":" + b.end);
    return String.join(",", r);
  }

  public static final class Bounds {
    public final Long start, end, nominalStart, nominalEnd;

    Bounds(Long start, Long end, Long nominalStart, Long nominalEnd) {
      this.start = start;
      this.end = end;
      this.nominalStart = nominalStart;
      this.nominalEnd = nominalEnd;
    }

    public boolean contains(long time) {
      return (start == null || time >= start) && (end == null || time <= end);
    }
  }
}
