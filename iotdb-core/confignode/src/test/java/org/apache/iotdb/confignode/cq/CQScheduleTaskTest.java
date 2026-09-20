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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.cq.CQCalendarUtils;
import org.apache.iotdb.confignode.manager.cq.CQManager;
import org.apache.iotdb.confignode.manager.cq.CQScheduleTask;
import org.apache.iotdb.confignode.rpc.thrift.TCQDuration;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.TimeDuration;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
  public void testFixedDurationVersionedCqUsesLegacySchedulerPath() {
    TCreateCQReq req =
        new TCreateCQReq(
            "fixedVersionedCq",
            1000,
            0,
            1000,
            0,
            TimeoutPolicy.BLOCKED.getType(),
            "select 1",
            "create cq fixedVersionedCq",
            "Asia",
            "root");
    req.setDurationEncodingVersion((short) 1);
    req.setEveryDuration(new TCQDuration(0, 1000));
    req.setStartOffsetDuration(new TCQDuration(0, 1000));
    req.setEndOffsetDuration(new TCQDuration(0, 0));
    req.setBoundaryExplicit(true);

    // A versioned fixed-duration CQ must not enter the calendar path (which requires a ZoneId).
    new CQScheduleTask(req, 1000, "token", null, null);
  }

  @Test
  public void testCalendarConstructorKeepsProcedureSelectedFirstOccurrence() throws Exception {
    TCreateCQReq req =
        new TCreateCQReq(
            "calendarCq",
            0,
            0,
            0,
            0,
            TimeoutPolicy.BLOCKED.getType(),
            "select 1",
            "create cq calendarCq",
            "UTC",
            "root");
    req.setDurationEncodingVersion((short) 1);
    req.setEveryDuration(new TCQDuration(1, 0));
    req.setStartOffsetDuration(new TCQDuration(1, 0));
    req.setEndOffsetDuration(new TCQDuration(0, 0));
    req.setBoundaryExplicit(false);
    long firstOccurrence =
        ZonedDateTime.of(2030, 2, 1, 0, 0, 0, 0, ZoneId.of("UTC")).toInstant().toEpochMilli();

    CQScheduleTask task = new CQScheduleTask(req, firstOccurrence, "token", null, null);
    Field executionTime = CQScheduleTask.class.getDeclaredField("executionTime");
    executionTime.setAccessible(true);
    assertEquals(firstOccurrence, executionTime.getLong(task));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCalendarConstructorRejectsMismatchedFirstOccurrence() {
    TCreateCQReq req =
        new TCreateCQReq(
            "calendarCq",
            0,
            0,
            0,
            0,
            TimeoutPolicy.BLOCKED.getType(),
            "select 1",
            "create cq calendarCq",
            "UTC",
            "root");
    req.setDurationEncodingVersion((short) 1);
    req.setEveryDuration(new TCQDuration(1, 0));
    req.setStartOffsetDuration(new TCQDuration(1, 0));
    req.setEndOffsetDuration(new TCQDuration(0, 0));
    req.setBoundaryExplicit(false);
    new CQScheduleTask(req, 1L, "token", null, null);
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

  @Test
  public void staleLastExecUpdateOnLegacyCqReconciles() throws Exception {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    CQManager cqManager = Mockito.mock(CQManager.class);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(configManager.getCQManager()).thenReturn(cqManager);
    Mockito.when(consensusManager.isLeader()).thenReturn(true);
    Mockito.when(consensusManager.write(Mockito.any()))
        .thenReturn(new TSStatus(TSStatusCode.CQ_UPDATE_LAST_EXEC_TIME_ERROR.getStatusCode()));

    ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(executor.isShutdown()).thenReturn(false);
    ScheduledFuture<?> future = Mockito.mock(ScheduledFuture.class);
    Mockito.when(
            executor.schedule(
                Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class)))
        .thenReturn((ScheduledFuture) future);

    CQScheduleTask task =
        new CQScheduleTask(
            "legacyCq",
            1000,
            0,
            1000,
            TimeoutPolicy.BLOCKED,
            "select 1",
            "token",
            "Asia",
            "root",
            executor,
            configManager,
            10_000);

    Field occurrenceIndex = CQScheduleTask.class.getDeclaredField("occurrenceIndex");
    occurrenceIndex.setAccessible(true);
    assertEquals(-1L, occurrenceIndex.getLong(task));

    Class<?> callbackClass = null;
    for (Class<?> nested : CQScheduleTask.class.getDeclaredClasses()) {
      if (nested.getSimpleName().equals("AsyncExecuteCQCallback")) {
        callbackClass = nested;
        break;
      }
    }
    assertNotNull(callbackClass);
    Constructor<?> constructor =
        callbackClass.getDeclaredConstructor(
            CQScheduleTask.class, long.class, long.class, long.class);
    constructor.setAccessible(true);
    Object callback = constructor.newInstance(task, 0L, 1000L, 0L);
    Method onComplete = callbackClass.getMethod("onComplete", TSStatus.class);
    onComplete.setAccessible(true);
    onComplete.invoke(callback, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    Mockito.verify(consensusManager, Mockito.times(1)).write(Mockito.any());
    Mockito.verify(cqManager).reconcileCQ("legacyCq", "token");
    Mockito.verify(executor, Mockito.never())
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
  }
}
