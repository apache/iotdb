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

import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.confignode.rpc.thrift.TCQDuration;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;

import org.apache.tsfile.utils.TimeDuration;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class CQCalendarUtilsTest {

  private static final ZoneId UTC = ZoneId.of("UTC");

  @Test
  public void testMonthEndSequenceIsAnchoredToOriginalBoundary() {
    long boundary = epochTimestamp(2024, 1, 31, 0, 0, UTC);
    TimeDuration every = new TimeDuration(1, 0);

    assertEquals(
        epochTimestamp(2024, 2, 29, 0, 0, UTC),
        CQCalendarUtils.occurrence(boundary, every, 1, UTC));
    assertEquals(
        epochTimestamp(2024, 3, 31, 0, 0, UTC),
        CQCalendarUtils.occurrence(boundary, every, 2, UTC));
    assertEquals(
        epochTimestamp(2024, 4, 30, 0, 0, UTC),
        CQCalendarUtils.occurrence(boundary, every, 3, UTC));
  }

  @Test
  public void testLeapDayYearSequenceReachesNextLeapYear() {
    long boundary = epochTimestamp(2020, 2, 29, 0, 0, UTC);
    TimeDuration everyYear = new TimeDuration(12, 0);

    assertEquals(
        epochTimestamp(2021, 2, 28, 0, 0, UTC),
        CQCalendarUtils.occurrence(boundary, everyYear, 1, UTC));
    assertEquals(
        epochTimestamp(2024, 2, 29, 0, 0, UTC),
        CQCalendarUtils.occurrence(boundary, everyYear, 4, UTC));
  }

  @Test
  public void testOmittedBoundaryUsesLocalEpochInPersistedZone() {
    ZoneId shanghai = ZoneId.of("Asia/Shanghai");
    assertEquals(
        epochTimestamp(1970, 1, 1, 0, 0, shanghai), CQCalendarUtils.localEpochBoundary(shanghai));
  }

  @Test
  public void testRangeEndpointsAreDerivedFromBoundaryVector() {
    long boundary = epochTimestamp(2024, 1, 31, 0, 0, UTC);
    TimeDuration every = new TimeDuration(1, 0);
    long start = CQCalendarUtils.applyVector(boundary, 1, 0, UTC);
    long end = CQCalendarUtils.applyVector(boundary, 2, 0, UTC);

    assertEquals(epochTimestamp(2024, 2, 29, 0, 0, UTC), start);
    assertEquals(epochTimestamp(2024, 3, 31, 0, 0, UTC), end);
    assertEquals(end, CQCalendarUtils.occurrence(boundary, every, 2, UTC));
  }

  @Test
  public void testFixedCadenceRangeUsesCurrentCalendarMonth() {
    long day = TimestampPrecisionUtils.currPrecision.convert(1, TimeUnit.DAYS);
    long boundary = epochTimestamp(2024, 1, 1, 0, 0, UTC);
    long execution = epochTimestamp(2024, 4, 1, 0, 0, UTC);
    TimeDuration start = new TimeDuration(1, day);
    TimeDuration end = new TimeDuration(1, 0);
    CQScheduleTask task =
        calendarTask(boundary, execution, new TimeDuration(0, day), start, end, UTC);

    // EVERY 1d RANGE 1mo1d, 1mo passes the component-wise duration validation. Its two
    // offsets must use March's calendar boundary, rather than January's 31-day length.
    assertEquals(
        epochTimestamp(2024, 2, 29, 0, 0, UTC), task.calculateCalendarRangeEndpoint(start, 91));
    assertEquals(
        epochTimestamp(2024, 3, 1, 0, 0, UTC), task.calculateCalendarRangeEndpoint(end, 91));

    // The calculation must remain anchored to the original boundary even when the fixed cadence
    // has not reached a month boundary. Subtracting RANGE from the occurrence would yield Jan 29.
    long marchFirst = epochTimestamp(2024, 3, 1, 0, 0, UTC);
    task = calendarTask(boundary, marchFirst, new TimeDuration(0, day), start, end, UTC);
    assertEquals(
        epochTimestamp(2024, 1, 29, 0, 0, UTC),
        task.calculateCalendarRangeEndpoint(start, 60));
  }

  @Test
  public void testFixedCadenceRangePreservesMonthClampingAndDst() {
    long day = TimestampPrecisionUtils.currPrecision.convert(1, TimeUnit.DAYS);
    TimeDuration every = new TimeDuration(0, day);
    TimeDuration start = new TimeDuration(1, day);
    TimeDuration end = new TimeDuration(0, 0);
    long boundary = epochTimestamp(2024, 1, 1, 0, 0, UTC);
    long execution = epochTimestamp(2024, 3, 31, 0, 0, UTC);
    CQScheduleTask task = calendarTask(boundary, execution, every, start, end, UTC);
    assertEquals(
        epochTimestamp(2024, 2, 28, 0, 0, UTC), task.calculateCalendarRangeEndpoint(start, 90));
    assertEquals(execution, task.calculateCalendarRangeEndpoint(end, 90));

    ZoneId newYork = ZoneId.of("America/New_York");
    boundary = epochTimestamp(2024, 1, 1, 11, 0, newYork);
    execution = epochTimestamp(2024, 4, 10, 12, 0, newYork);
    task = calendarTask(boundary, execution, every, start, end, newYork);
    // Subtracting one calendar month reaches March 10 at noon; subtracting another 24 hours
    // crosses the spring DST transition and reaches March 9 at 11:00.
    assertEquals(
        epochTimestamp(2024, 3, 9, 11, 0, newYork),
        task.calculateCalendarRangeEndpoint(start, 100));
  }

  @Test
  public void testCalendarCadenceRangeRetainsOriginalMonthEndAnchor() {
    long boundary = epochTimestamp(2024, 1, 31, 0, 0, UTC);
    long execution = epochTimestamp(2024, 4, 30, 0, 0, UTC);
    TimeDuration month = new TimeDuration(1, 0);
    TimeDuration zero = new TimeDuration(0, 0);
    CQScheduleTask task = calendarTask(boundary, execution, month, month, zero, UTC);

    assertEquals(
        epochTimestamp(2024, 3, 31, 0, 0, UTC), task.calculateCalendarRangeEndpoint(month, 3));
    assertEquals(execution, task.calculateCalendarRangeEndpoint(zero, 3));
  }

  private static CQScheduleTask calendarTask(
      long boundary,
      long execution,
      TimeDuration every,
      TimeDuration start,
      TimeDuration end,
      ZoneId zone) {
    TCreateCQReq req =
        new TCreateCQReq(
            "rangeCq",
            0,
            boundary,
            0,
            0,
            TimeoutPolicy.BLOCKED.getType(),
            "select s1 into root.backup.d1.s1 from root.sg.d1",
            "create cq rangeCq",
            zone.getId(),
            "root");
    req.setDurationEncodingVersion((short) 1);
    req.setEveryDuration(new TCQDuration(every.monthDuration, every.nonMonthDuration));
    req.setStartOffsetDuration(new TCQDuration(start.monthDuration, start.nonMonthDuration));
    req.setEndOffsetDuration(new TCQDuration(end.monthDuration, end.nonMonthDuration));
    req.setBoundaryExplicit(true);
    return new CQScheduleTask(req, execution, "token", null, null);
  }

  @Test
  public void testDstUsesZoneRulesForCalendarAndElapsedParts() {
    ZoneId newYork = ZoneId.of("America/New_York");
    long monthBoundary = epochTimestamp(2024, 2, 10, 2, 30, newYork);
    long monthOccurrence =
        CQCalendarUtils.occurrence(monthBoundary, new TimeDuration(1, 0), 1, newYork);
    assertEquals(epochTimestamp(2024, 3, 10, 3, 30, newYork), monthOccurrence);

    long elapsedBoundary = epochTimestamp(2024, 3, 9, 12, 0, newYork);
    long oneDay = TimestampPrecisionUtils.currPrecision.convert(1, TimeUnit.DAYS);
    long elapsedOccurrence =
        CQCalendarUtils.occurrence(elapsedBoundary, new TimeDuration(0, oneDay), 1, newYork);
    assertEquals(epochTimestamp(2024, 3, 10, 13, 0, newYork), elapsedOccurrence);
  }

  @Test
  public void testBlockedKeepsTheNextOccurrenceWhileDiscardSkipsMissedOccurrences() {
    long boundary = epochTimestamp(2024, 1, 1, 0, 0, UTC);
    TimeDuration every = new TimeDuration(1, 0);
    long callbackTime = epochTimestamp(2024, 1, 4, 12, 0, UTC);
    long executionTime = CQCalendarUtils.occurrence(boundary, every, 1, UTC);

    assertEquals(
        2,
        CQScheduleTask.calculateNextOccurrenceIndex(
            TimeoutPolicy.BLOCKED, 1, callbackTime, executionTime, 0, 1, boundary, every, UTC));
    assertEquals(
        2,
        CQScheduleTask.calculateNextOccurrenceIndex(
            TimeoutPolicy.DISCARD, 1, callbackTime, executionTime, 0, 1, boundary, every, UTC));
  }

  @Test
  public void testDiscardNeverMovesBeforeTheCurrentOccurrence() {
    long boundary = epochTimestamp(2024, 1, 1, 0, 0, UTC);
    TimeDuration every = new TimeDuration(1, 0);
    long currentOccurrence = CQCalendarUtils.occurrence(boundary, every, 2, UTC);

    assertEquals(
        3,
        CQScheduleTask.calculateNextOccurrenceIndex(
            TimeoutPolicy.DISCARD,
            2,
            currentOccurrence,
            currentOccurrence,
            0,
            2,
            boundary,
            every,
            UTC));
  }

  private static long epochTimestamp(
      int year, int month, int day, int hour, int minute, ZoneId zone) {
    Instant instant = ZonedDateTime.of(year, month, day, hour, minute, 0, 0, zone).toInstant();
    return Math.addExact(
        TimestampPrecisionUtils.currPrecision.convert(instant.getEpochSecond(), TimeUnit.SECONDS),
        TimestampPrecisionUtils.currPrecision.convert(instant.getNano(), TimeUnit.NANOSECONDS));
  }

  private static long epochTimestamp(int year, int month, int day, int hour, ZoneId zone) {
    return epochTimestamp(year, month, day, hour, 0, zone);
  }
}
