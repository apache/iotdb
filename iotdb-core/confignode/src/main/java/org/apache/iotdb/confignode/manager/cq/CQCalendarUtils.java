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
package org.apache.iotdb.confignode.manager.cq;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.i18n.ManagerMessages;

import org.apache.tsfile.utils.TimeDuration;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/** Pure calendar arithmetic used by CQ scheduling and recovery. */
public final class CQCalendarUtils {
  private CQCalendarUtils() {}

  public static long apply(long base, TimeDuration duration, long multiplier, ZoneId zone) {
    long months = Math.multiplyExact((long) duration.monthDuration, multiplier);
    long fixed = Math.multiplyExact(duration.nonMonthDuration, multiplier);
    return applyVector(base, months, fixed, zone);
  }

  public static long applyVector(long base, long months, long fixed, ZoneId zone) {
    Instant instant = toInstant(base);
    if (months != 0) {
      ZonedDateTime local = instant.atZone(zone);
      instant = local.toLocalDateTime().plusMonths(months).atZone(zone).toInstant();
    }
    if (fixed != 0) {
      instant = instant.plusNanos(toNanos(fixed));
    }
    return fromInstant(instant);
  }

  public static long occurrence(long boundary, TimeDuration every, long index, ZoneId zone) {
    return apply(boundary, every, index, zone);
  }

  public static long localEpochBoundary(ZoneId zone) {
    return fromInstant(java.time.LocalDate.of(1970, 1, 1).atStartOfDay(zone).toInstant());
  }

  public static long firstOccurrenceIndex(
      long boundary, TimeDuration every, long now, ZoneId zone) {
    if (every.monthDuration <= 0 && every.nonMonthDuration <= 0) {
      throw new IllegalArgumentException(
          ManagerMessages.EXCEPTION_CQ_EVERY_DURATION_MUST_BE_POSITIVE_69C29D26);
    }
    if (now <= boundary) {
      return 0;
    }
    long high = 1;
    while (occurrence(boundary, every, high, zone) < now) {
      high = Math.multiplyExact(high, 2);
    }
    long low = high / 2;
    while (low < high) {
      long mid = low + (high - low) / 2;
      if (occurrence(boundary, every, mid, zone) < now) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  public static long nextAfter(long boundary, TimeDuration every, long previous, ZoneId zone) {
    long index = firstOccurrenceIndex(boundary, every, previous, zone);
    if (occurrence(boundary, every, index, zone) <= previous) {
      index = Math.addExact(index, 1);
    }
    return occurrence(boundary, every, index, zone);
  }

  private static Instant toInstant(long value) {
    String precision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    if ("us".equals(precision)) {
      return Instant.ofEpochSecond(
          Math.floorDiv(value, 1_000_000), Math.floorMod(value, 1_000_000) * 1_000L);
    }
    if ("ns".equals(precision)) {
      return Instant.ofEpochSecond(
          Math.floorDiv(value, 1_000_000_000), Math.floorMod(value, 1_000_000_000));
    }
    return Instant.ofEpochMilli(value);
  }

  private static long fromInstant(Instant instant) {
    String precision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    try {
      if ("us".equals(precision)) {
        return Math.addExact(
            Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
      }
      if ("ns".equals(precision)) {
        return Math.addExact(
            Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
      }
      return Math.addExact(
          Math.multiplyExact(instant.getEpochSecond(), 1_000L), instant.getNano() / 1_000_000L);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          ManagerMessages.EXCEPTION_CQ_TIMESTAMP_OVERFLOWS_CONFIGURED_PRECISION_F5FB230C, e);
    }
  }

  private static long toNanos(long fixed) {
    String precision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    if ("us".equals(precision)) {
      return Math.multiplyExact(fixed, 1_000L);
    }
    if ("ns".equals(precision)) {
      return fixed;
    }
    return Math.multiplyExact(fixed, 1_000_000L);
  }
}
