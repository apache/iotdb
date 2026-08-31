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

package org.apache.iotdb.confignode.manager.cq;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.consensus.request.write.cq.UpdateCQLastExecTimePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TExecuteCQ;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.tsfile.utils.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CQScheduleTask implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CQScheduleTask.class);

  private static final long DEFAULT_RETRY_WAIT_TIME_IN_MS = 20L * 1_000;

  // ms is 1
  // us is 1_000
  // ns is 1_000_000
  private static final long FACTOR;

  static {
    String timestampPrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    if ("us".equals(timestampPrecision)) {
      FACTOR = 1_000;
    } else if ("ns".equals(timestampPrecision)) {
      FACTOR = 1_000_000;
    } else {
      FACTOR = 1;
    }
  }

  private final String cqId;
  private final long everyInterval;
  private final long startTimeOffset;
  private final long endTimeOffset;
  private final TimeoutPolicy timeoutPolicy;
  private final String queryBody;
  private final String md5;

  private final String zoneId;

  private final String username;

  private final ScheduledExecutorService executor;

  private final ConfigManager configManager;

  private long retryWaitTimeInMS;

  private long executionTime;
  private TimeDuration everyDuration;
  private TimeDuration startDuration;
  private TimeDuration endDuration;
  private long boundaryTime;
  private boolean calendarAware;
  private ZoneId scheduleZone;

  public CQScheduleTask(
      TCreateCQReq req,
      long firstExecutionTime,
      String md5,
      ScheduledExecutorService executor,
      ConfigManager configManager) {
    this(
        req.cqId,
        req.everyInterval,
        req.startTimeOffset,
        req.endTimeOffset,
        TimeoutPolicy.deserialize(req.timeoutPolicy),
        req.queryBody,
        md5,
        req.zoneId,
        req.username,
        executor,
        configManager,
        firstExecutionTime);
    this.everyDuration =
        req.isSetEveryDuration()
            ? new TimeDuration(
                Math.toIntExact(req.getEveryDuration().getMonthPart()),
                req.getEveryDuration().getNonMonthDuration())
            : new TimeDuration(0, req.everyInterval);
    this.startDuration =
        req.isSetStartOffsetDuration()
            ? new TimeDuration(
                Math.toIntExact(req.getStartOffsetDuration().getMonthPart()),
                req.getStartOffsetDuration().getNonMonthDuration())
            : new TimeDuration(0, req.startTimeOffset);
    this.endDuration =
        req.isSetEndOffsetDuration()
            ? new TimeDuration(
                Math.toIntExact(req.getEndOffsetDuration().getMonthPart()),
                req.getEndOffsetDuration().getNonMonthDuration())
            : new TimeDuration(0, req.endTimeOffset);
    this.calendarAware =
        everyDuration.monthDuration != 0
            || startDuration.monthDuration != 0
            || endDuration.monthDuration != 0;
    this.boundaryTime = req.boundaryTime;
    this.scheduleZone = ZoneId.of(req.zoneId);
    if (calendarAware) {
      this.retryWaitTimeInMS =
          Math.min(
              DEFAULT_RETRY_WAIT_TIME_IN_MS, Math.max(1L, everyDuration.nonMonthDuration / FACTOR));
    }
    if (calendarAware && req.isSetBoundaryExplicit() && !req.isBoundaryExplicit()) {
      this.boundaryTime = CQCalendarUtils.localEpochBoundary(scheduleZone);
      this.executionTime =
          CQCalendarUtils.occurrence(
              boundaryTime,
              everyDuration,
              CQCalendarUtils.firstOccurrenceIndex(
                  boundaryTime, everyDuration, System.currentTimeMillis() * FACTOR, scheduleZone),
              scheduleZone);
    }
  }

  public CQScheduleTask(
      CQInfo.CQEntry entry, ScheduledExecutorService executor, ConfigManager configManager) {
    this(
        entry.getCqId(),
        entry.getEveryInterval(),
        entry.getStartTimeOffset(),
        entry.getEndTimeOffset(),
        entry.getTimeoutPolicy(),
        entry.getQueryBody(),
        entry.getMd5(),
        entry.getZoneId(),
        entry.getUsername(),
        executor,
        configManager,
        entry.getLastExecutionTime() + entry.getEveryInterval());
    this.everyDuration = entry.getEveryDuration();
    this.startDuration = entry.getStartTimeOffsetDuration();
    this.endDuration = entry.getEndTimeOffsetDuration();
    this.calendarAware =
        everyDuration.monthDuration != 0
            || startDuration.monthDuration != 0
            || endDuration.monthDuration != 0;
    this.boundaryTime = entry.getBoundaryTime();
    this.scheduleZone = ZoneId.of(entry.getZoneId());
    if (calendarAware) {
      if (!entry.isBoundaryExplicit()) {
        this.boundaryTime = CQCalendarUtils.localEpochBoundary(scheduleZone);
      }
      long index =
          CQCalendarUtils.firstOccurrenceIndex(
              boundaryTime, everyDuration, entry.getLastExecutionTime(), scheduleZone);
      if (CQCalendarUtils.occurrence(boundaryTime, everyDuration, index, scheduleZone)
          <= entry.getLastExecutionTime()) {
        index = Math.addExact(index, 1);
      }
      this.executionTime =
          CQCalendarUtils.occurrence(boundaryTime, everyDuration, index, scheduleZone);
    }
  }

  @SuppressWarnings("squid:S107")
  public CQScheduleTask(
      String cqId,
      long everyInterval,
      long startTimeOffset,
      long endTimeOffset,
      TimeoutPolicy timeoutPolicy,
      String queryBody,
      String md5,
      String zoneId,
      String username,
      ScheduledExecutorService executor,
      ConfigManager configManager,
      long executionTime) {
    this.cqId = cqId;
    this.everyInterval = everyInterval;
    this.startTimeOffset = startTimeOffset;
    this.endTimeOffset = endTimeOffset;
    this.timeoutPolicy = timeoutPolicy;
    this.queryBody = queryBody;
    this.md5 = md5;
    this.zoneId = zoneId;
    this.username = username;
    this.executor = executor;
    this.configManager = configManager;
    this.retryWaitTimeInMS = Math.min(DEFAULT_RETRY_WAIT_TIME_IN_MS, everyInterval / FACTOR);
    this.executionTime = executionTime;
    this.everyDuration = new TimeDuration(0, everyInterval);
    this.startDuration = new TimeDuration(0, startTimeOffset);
    this.endDuration = new TimeDuration(0, endTimeOffset);
    this.boundaryTime = 0;
    this.scheduleZone = ZoneId.of(zoneId);
  }

  public static long getFirstExecutionTime(long boundaryTime, long everyInterval) {
    long now = System.currentTimeMillis() * FACTOR;
    return getFirstExecutionTime(boundaryTime, everyInterval, now);
  }

  public static long getFirstExecutionTime(long boundaryTime, long everyInterval, long now) {
    if (now <= boundaryTime) {
      return boundaryTime;
    } else {
      return (((now - boundaryTime - 1) / everyInterval) + 1) * everyInterval + boundaryTime;
    }
  }

  public static long getFirstExecutionTime(
      long boundaryTime, TimeDuration everyDuration, long now, ZoneId zoneId) {
    long index = CQCalendarUtils.firstOccurrenceIndex(boundaryTime, everyDuration, now, zoneId);
    return CQCalendarUtils.occurrence(boundaryTime, everyDuration, index, zoneId);
  }

  @Override
  public void run() {
    long occurrenceIndex = 0;
    long startTime = executionTime - startTimeOffset;
    long endTime = executionTime - endTimeOffset;
    if (calendarAware) {
      occurrenceIndex =
          CQCalendarUtils.firstOccurrenceIndex(
              boundaryTime, everyDuration, executionTime, scheduleZone);
      if (CQCalendarUtils.occurrence(boundaryTime, everyDuration, occurrenceIndex, scheduleZone)
          != executionTime) {
        occurrenceIndex = Math.max(0, occurrenceIndex - 1);
      }
      startTime =
          CQCalendarUtils.applyVector(
              boundaryTime,
              Math.subtractExact(
                  Math.multiplyExact((long) everyDuration.monthDuration, occurrenceIndex),
                  startDuration.monthDuration),
              Math.subtractExact(
                  Math.multiplyExact(everyDuration.nonMonthDuration, occurrenceIndex),
                  startDuration.nonMonthDuration),
              scheduleZone);
      endTime =
          CQCalendarUtils.applyVector(
              boundaryTime,
              Math.subtractExact(
                  Math.multiplyExact((long) everyDuration.monthDuration, occurrenceIndex),
                  endDuration.monthDuration),
              Math.subtractExact(
                  Math.multiplyExact(everyDuration.nonMonthDuration, occurrenceIndex),
                  endDuration.nonMonthDuration),
              scheduleZone);
    }

    Optional<TDataNodeLocation> targetDataNode =
        configManager.getNodeManager().getLowestLoadDataNode();
    // no usable DataNode to execute CQ
    if (!targetDataNode.isPresent()) {
      LOGGER.warn("There is no RUNNING DataNode to execute CQ {}", cqId);
      if (needSubmit()) {
        submitSelf(retryWaitTimeInMS, TimeUnit.MILLISECONDS);
      }
    } else {
      LOGGER.info(
          "[StartExecuteCQ] execute CQ {} on DataNode[{}], time range is [{}, {}), current time is {}",
          cqId,
          targetDataNode.get().dataNodeId,
          startTime,
          endTime,
          System.currentTimeMillis() * FACTOR);
      TExecuteCQ executeCQReq =
          new TExecuteCQ(
              queryBody,
              startTime,
              endTime,
              toTimeoutMillis(
                  calendarAware
                      ? CQCalendarUtils.occurrence(
                              boundaryTime, everyDuration, occurrenceIndex + 1, scheduleZone)
                          - executionTime
                      : everyInterval),
              zoneId,
              cqId,
              username);
      try {
        AsyncDataNodeInternalServiceClient client =
            CnToDnInternalServiceAsyncRequestManager.getInstance()
                .getAsyncClient(targetDataNode.get());
        client.executeCQ(executeCQReq, new AsyncExecuteCQCallback(startTime, endTime));
      } catch (Exception t) {
        LOGGER.warn("Execute CQ {} failed", cqId, t);
        if (needSubmit()) {
          submitSelf(retryWaitTimeInMS, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  private static long toTimeoutMillis(long deltaTicks) {
    if (deltaTicks <= 0) {
      return 1;
    }
    try {
      return Math.addExact(deltaTicks, FACTOR - 1) / FACTOR;
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  public void submitSelf() {
    submitSelf(
        Math.max(0, executionTime / FACTOR - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
  }

  private void submitSelf(long delay, TimeUnit unit) {
    executor.schedule(this, delay, unit);
  }

  private boolean needSubmit() {
    // current node is still leader and thread pool is not shut down.
    return configManager.getConsensusManager().isLeader() && !executor.isShutdown();
  }

  private class AsyncExecuteCQCallback implements AsyncMethodCallback<TSStatus> {

    private final long startTime;
    private final long endTime;

    public AsyncExecuteCQCallback(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    private void updateExecutionTime() {
      if (timeoutPolicy == TimeoutPolicy.BLOCKED) {
        if (calendarAware) {
          long index =
              CQCalendarUtils.firstOccurrenceIndex(
                  boundaryTime, everyDuration, executionTime, scheduleZone);
          executionTime =
              CQCalendarUtils.occurrence(boundaryTime, everyDuration, index + 1, scheduleZone);
        } else {
          executionTime = executionTime + everyInterval;
        }
      } else if (timeoutPolicy == TimeoutPolicy.DISCARD) {
        long now = System.currentTimeMillis() * FACTOR;
        if (calendarAware) {
          long index =
              CQCalendarUtils.firstOccurrenceIndex(boundaryTime, everyDuration, now, scheduleZone);
          executionTime =
              CQCalendarUtils.occurrence(boundaryTime, everyDuration, index, scheduleZone);
        } else {
          executionTime =
              executionTime + ((now - executionTime - 1) / everyInterval + 1) * everyInterval;
        }
      } else {
        throw new IllegalArgumentException("Unknown TimeoutPolicy: " + timeoutPolicy);
      }
    }

    @Override
    public void onComplete(TSStatus response) {
      if (response.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {

        LOGGER.info(
            "[EndExecuteCQ] {}, time range is [{}, {}), current time is {}",
            cqId,
            startTime,
            endTime,
            System.currentTimeMillis() * FACTOR);
        TSStatus result;
        try {
          result =
              configManager
                  .getConsensusManager()
                  .write(new UpdateCQLastExecTimePlan(cqId, executionTime, md5));
        } catch (ConsensusException e) {
          result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
          result.setMessage(e.getMessage());
        }

        // while leadership changed, the update last exec time operation for CQTasks in new leader
        // may still update failed because stale CQTask in old leader may update it in advance
        if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn(
              "Failed to update the last execution time {} of CQ {}, because {}",
              executionTime,
              cqId,
              result.getMessage());
          // no such cq, we don't need to submit it again
          if (result.getCode() == TSStatusCode.NO_SUCH_CQ.getStatusCode()) {
            LOGGER.info("Stop submitting CQ {} because {}", cqId, result.getMessage());
            return;
          }
          // The persisted progress did not advance. Keep the same occurrence and retry; in
          // particular, stale callbacks must never create a competing scheduling chain.
          if (needSubmit()) {
            submitSelf(retryWaitTimeInMS, TimeUnit.MILLISECONDS);
          }
          return;
        }

        if (needSubmit()) {
          updateExecutionTime();
          submitSelf();
        } else {
          LOGGER.info(
              "Stop submitting CQ {} because current node is not leader or current scheduled thread pool is shut down.",
              cqId);
        }

      } else {
        LOGGER.warn("Execute CQ {} failed, TSStatus is {}", cqId, response);
        if (needSubmit()) {
          submitSelf(retryWaitTimeInMS, TimeUnit.MILLISECONDS);
        }
      }
    }

    @Override
    public void onError(Exception exception) {
      LOGGER.warn("Execute CQ {} failed", cqId, exception);
      if (needSubmit()) {
        submitSelf(retryWaitTimeInMS, TimeUnit.MILLISECONDS);
      }
    }
  }
}
