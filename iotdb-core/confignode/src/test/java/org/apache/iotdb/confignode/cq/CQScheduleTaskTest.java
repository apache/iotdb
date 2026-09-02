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
package org.apache.iotdb.confignode.cq;

import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.confignode.manager.cq.CQCalendarUtils;
import org.apache.iotdb.confignode.manager.cq.CQScheduleTask;

import org.apache.tsfile.utils.TimeDuration;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class CQScheduleTaskTest {

  @Test
  public void testGetFirstExecutionTime1() {
    long now = 100L;
    long boundaryTime = 0L;
    long everyInterval = 30L;
    assertEquals(120L, CQScheduleTask.getFirstExecutionTime(boundaryTime, everyInterval, now));
  }

  @Test
  public void testGetFirstExecutionTime2() {
    long now = 100L;
    long boundaryTime = 110L;
    long everyInterval = 30L;
    assertEquals(110L, CQScheduleTask.getFirstExecutionTime(boundaryTime, everyInterval, now));
  }

  @Test
  public void testFixedDurationCqDoesNotRequireCanonicalZoneId() {
    new CQScheduleTask(
        "testCq",
        1000,
        0,
        1000,
        TimeoutPolicy.BLOCKED,
        "select s1 into root.backup.d1.s1 from root.sg.d1",
        "token",
        "Asia",
        "root",
        null,
        null,
        1000);
  }

  @Test
  public void testCalendarOccurrencesRecomputeFromOriginalBoundary() {
    ZoneId zone = ZoneId.of("UTC");
    long boundary = ZonedDateTime.of(2024, 1, 31, 0, 0, 0, 0, zone).toInstant().toEpochMilli();
    TimeDuration month = new TimeDuration(1, 0);
    assertEquals(
        ZonedDateTime.of(2024, 2, 29, 0, 0, 0, 0, zone).toInstant().toEpochMilli(),
        CQCalendarUtils.occurrence(boundary, month, 1, zone));
    assertEquals(
        ZonedDateTime.of(2024, 3, 31, 0, 0, 0, 0, zone).toInstant().toEpochMilli(),
        CQCalendarUtils.occurrence(boundary, month, 2, zone));
  }

  @Test
  public void testDiscardLowerBoundNeverMovesBeforeCurrentOccurrence() {
    ZoneId zone = ZoneId.of("UTC");
    long boundary = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, zone).toInstant().toEpochMilli();
    TimeDuration month = new TimeDuration(1, 0);
    long current = CQCalendarUtils.occurrence(boundary, month, 2, zone);
    long lowerBound = CQCalendarUtils.firstOccurrenceIndex(boundary, month, current, zone);
    assertEquals(2, lowerBound);
    assertEquals(3, Math.max(2 + 1, lowerBound));
  }
}
