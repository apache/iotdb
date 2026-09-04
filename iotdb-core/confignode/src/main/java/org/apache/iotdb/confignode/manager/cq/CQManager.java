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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.cq.CQState;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.cq.ShowCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.response.cq.ShowCQResp;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCQDuration;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CQManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(CQManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private final ConfigManager configManager;

  private final ReadWriteLock lock;

  // Key: CQ id. Value: the local task and the metadata token it owns.
  private final ConcurrentMap<String, LocallyScheduledCQ> locallyScheduledCQs;

  private ScheduledExecutorService executor;

  public CQManager(ConfigManager configManager) {
    this.configManager = configManager;
    this.lock = new ReentrantReadWriteLock();
    this.locallyScheduledCQs = new ConcurrentHashMap<>();
    this.executor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(
            CONF.getCqSubmitThread(), ThreadName.CQ_SCHEDULER.getName());
  }

  public TSStatus createCQ(TCreateCQReq req) {
    TSStatus validation = validateDurationEncoding(req);
    if (validation != null) {
      return validation;
    }
    lock.readLock().lock();
    try {
      ScheduledExecutorService currentExecutor = executor;
      return configManager.getProcedureManager().createCQ(req, currentExecutor);
    } finally {
      lock.readLock().unlock();
    }
  }

  private TSStatus validateDurationEncoding(TCreateCQReq req) {
    if (!req.isSetDurationEncodingVersion()) {
      // New CQ creation must use the versioned representation. Legacy requests are still
      // supported when loading old procedures/plans/snapshots, but accepting them here would let
      // an old DataNode flatten a calendar duration and bypass the mixed-version capability gate.
      return new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode())
          .setMessage(ManagerMessages.MESSAGE_CQ_DURATION_ENCODING_MARKER_REQUIRED_9035980A);
    }
    if (req.getDurationEncodingVersion() != 1
        || !req.isSetEveryDuration()
        || !req.isSetStartOffsetDuration()
        || !req.isSetEndOffsetDuration()
        || !req.isSetBoundaryExplicit()) {
      return new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode())
          .setMessage(
              ManagerMessages
                  .MESSAGE_INVALID_CQ_DURATION_ENCODING_VERSION_1_REQUIRES_ALL_STRUCTURED_FIELDS_FEAD7F92);
    }
    if (req.getEveryDuration().getMonthPart() < 0
        || req.getStartOffsetDuration().getMonthPart() < 0
        || req.getEndOffsetDuration().getMonthPart() < 0
        || req.getEveryDuration().getNonMonthDuration() < 0
        || req.getStartOffsetDuration().getNonMonthDuration() < 0
        || req.getEndOffsetDuration().getNonMonthDuration() < 0) {
      return new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode())
          .setMessage(ManagerMessages.MESSAGE_CQ_DURATIONS_MUST_BE_NON_NEGATIVE_BE23CE04);
    }
    if (req.getEveryDuration().getMonthPart() > Integer.MAX_VALUE
        || req.getStartOffsetDuration().getMonthPart() > Integer.MAX_VALUE
        || req.getEndOffsetDuration().getMonthPart() > Integer.MAX_VALUE) {
      return new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode())
          .setMessage(ManagerMessages.MESSAGE_CQ_DURATIONS_MUST_BE_NON_NEGATIVE_BE23CE04);
    }
    boolean hasCalendarDuration =
        req.getEveryDuration().getMonthPart() != 0
            || req.getStartOffsetDuration().getMonthPart() != 0
            || req.getEndOffsetDuration().getMonthPart() != 0;
    // If any structured component contains a calendar month, all legacy fields must carry the
    // invalid zero sentinel. Otherwise they must exactly mirror the fixed structured values.
    boolean legacyFieldsMatch =
        hasCalendarDuration
            ? req.everyInterval == 0 && req.startTimeOffset == 0 && req.endTimeOffset == 0
            : req.everyInterval == req.getEveryDuration().getNonMonthDuration()
                && req.startTimeOffset == req.getStartOffsetDuration().getNonMonthDuration()
                && req.endTimeOffset == req.getEndOffsetDuration().getNonMonthDuration();
    if (!legacyFieldsMatch) {
      return new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode())
          .setMessage(
              ManagerMessages
                  .MESSAGE_CQ_LEGACY_DURATION_FIELDS_CONFLICT_WITH_STRUCTURED_DURATION_FIELDS_4D6C6D67);
    }
    TSStatus semanticValidation = validateDurationSemantics(req);
    if (semanticValidation != null) {
      return semanticValidation;
    }
    if (hasCalendarDuration && !allClusterNodesSupportDurationEncodingV1()) {
      return new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode())
          .setMessage(
              ManagerMessages.MESSAGE_CQ_CALENDAR_DURATION_REQUIRES_ALL_NODES_SUPPORT_49534072);
    }
    return null;
  }

  private TSStatus validateDurationSemantics(TCreateCQReq req) {
    TCQDuration every = req.getEveryDuration();
    TCQDuration start = req.getStartOffsetDuration();
    TCQDuration end = req.getEndOffsetDuration();

    if (!isPositive(every)) {
      return semanticError(ManagerMessages.EXCEPTION_CQ_EVERY_DURATION_MUST_BE_POSITIVE_69C29D26);
    }
    if (!isPositive(start)) {
      return semanticError(ManagerMessages.MESSAGE_CQ_START_OFFSET_MUST_BE_POSITIVE_B837C4F5);
    }
    if (end.getMonthPart() < 0 || end.getNonMonthDuration() < 0) {
      return semanticError(ManagerMessages.MESSAGE_CQ_END_OFFSET_MUST_BE_NON_NEGATIVE_64171164);
    }
    if (!dominates(start, end, true)) {
      return semanticError(
          ManagerMessages.MESSAGE_CQ_START_OFFSET_MUST_BE_GREATER_THAN_END_OFFSET_5924C189);
    }
    if (!dominates(start, every, false)) {
      return semanticError(
          ManagerMessages
              .MESSAGE_CQ_START_OFFSET_MUST_BE_GREATER_THAN_OR_EQUAL_TO_EVERY_DURATION_89628D43);
    }
    return null;
  }

  private static TSStatus semanticError(String message) {
    return new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode()).setMessage(message);
  }

  private static boolean isPositive(TCQDuration duration) {
    return duration.getMonthPart() > 0 || duration.getNonMonthDuration() > 0;
  }

  private static boolean dominates(TCQDuration left, TCQDuration right, boolean strict) {
    boolean result =
        left.getMonthPart() >= right.getMonthPart()
            && left.getNonMonthDuration() >= right.getNonMonthDuration();
    return result
        && (!strict
            || left.getMonthPart() != right.getMonthPart()
            || left.getNonMonthDuration() != right.getNonMonthDuration());
  }

  /** Returns true when any persisted CQ requires the structured calendar-duration reader. */
  public boolean hasCalendarDurationCQ() {
    try {
      DataSet response = configManager.getConsensusManager().read(new ShowCQPlan());
      if (!(response instanceof ShowCQResp)) {
        // Do not allow a node with unknown metadata to join while the reader barrier is active.
        return true;
      }
      if (((ShowCQResp) response).getCqList() == null) {
        // A malformed response is just as unsafe as an unavailable response for this barrier.
        return true;
      }
      return ((ShowCQResp) response)
          .getCqList().stream().anyMatch(CQInfo.CQEntry::hasCalendarDuration);
    } catch (ConsensusException e) {
      // A failed metadata read must fail closed: an old reader must never be admitted blindly.
      LOGGER.warn(ManagerMessages.UNEXPECTED_ERROR_HAPPENED_WHILE_FETCHING_CQ_LIST, e);
      return true;
    }
  }

  private boolean allClusterNodesSupportDurationEncodingV1() {
    java.util.Map<Integer, TNodeVersionInfo> versionInfo =
        configManager.getNodeManager().getNodeVersionInfo();
    if (versionInfo == null || versionInfo.isEmpty()) {
      return false;
    }
    boolean hasRegisteredNode = false;
    // Check every registered node explicitly. A missing heartbeat/version entry must not allow a
    // calendar CQ to be created during a rolling upgrade.
    List<org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation> configNodes =
        configManager.getNodeManager().getRegisteredConfigNodes();
    List<org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration> dataNodes =
        configManager.getNodeManager().getRegisteredDataNodes();
    if (configNodes == null || dataNodes == null) {
      return false;
    }
    for (org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation node : configNodes) {
      hasRegisteredNode = true;
      if (!supportsDurationEncodingV1(versionInfo.get(node.getConfigNodeId()))) {
        return false;
      }
    }
    for (org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration node : dataNodes) {
      hasRegisteredNode = true;
      if (!supportsDurationEncodingV1(versionInfo.get(node.getLocation().getDataNodeId()))) {
        return false;
      }
    }
    return hasRegisteredNode;
  }

  private boolean supportsDurationEncodingV1(TNodeVersionInfo info) {
    if (info == null
        || !info.isSetSupportedCQDurationEncodingVersions()
        || !info.getSupportedCQDurationEncodingVersions().contains((short) 1)) {
      return false;
    }
    return true;
  }

  public TSStatus dropCQ(TDropCQReq req) {
    lock.readLock().lock();
    try {
      TSStatus status = configManager.getConsensusManager().write(new DropCQPlan(req.cqId));
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        cancelLocallyScheduledCQ(req.cqId);
      }
      return status;
    } catch (ConsensusException e) {
      LOGGER.warn(ManagerMessages.UNEXPECTED_ERROR_HAPPENED_WHILE_DROPPING_CQ, req.cqId, e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    } finally {
      lock.readLock().unlock();
    }
  }

  public TShowCQResp showCQ() {
    try {
      DataSet response = configManager.getConsensusManager().read(new ShowCQPlan());
      return ((ShowCQResp) response).convertToRpcShowCQResp();
    } catch (ConsensusException e) {
      LOGGER.warn(ManagerMessages.UNEXPECTED_ERROR_HAPPENED_WHILE_SHOWING_CQ, e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new TShowCQResp(res, Collections.emptyList());
    }
  }

  public ScheduledExecutorService getExecutor() {
    ScheduledExecutorService res;
    lock.readLock().lock();
    try {
      res = executor;
    } finally {
      lock.readLock().unlock();
    }
    return res;
  }

  public void startCQScheduler() {
    lock.writeLock().lock();
    try {
      // 1. shutdown previous cq schedule thread pool
      try {
        cancelAllLocallyScheduledCQs();
        if (executor != null) {
          executor.shutdown();
        }
      } catch (Exception t) {
        // just print the error log because we should make sure we can start a new cq schedule pool
        // successfully in the next steps
        LOGGER.error(
            ManagerMessages.ERROR_HAPPENED_WHILE_SHUTTING_DOWN_PREVIOUS_CQ_SCHEDULE_THREAD_POOL, t);
      }

      // 2. start a new schedule thread pool
      executor =
          IoTDBThreadPoolFactory.newScheduledThreadPool(
              CONF.getCqSubmitThread(), ThreadName.CQ_SCHEDULER.getName());

      // 3. get all CQs
      List<CQInfo.CQEntry> allCQs = null;
      // keep fetching until we get all CQEntries if this node is still leader
      while (needFetch(allCQs)) {
        try {
          DataSet response = configManager.getConsensusManager().read(new ShowCQPlan());
          allCQs = ((ShowCQResp) response).getCqList();
        } catch (ConsensusException e) {
          // consensus layer related errors
          LOGGER.warn(ManagerMessages.UNEXPECTED_ERROR_HAPPENED_WHILE_FETCHING_CQ_LIST, e);
          try {
            Thread.sleep(500);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      }

      // 4. recover the scheduling of active CQs
      if (allCQs != null) {
        for (CQInfo.CQEntry entry : allCQs) {
          if (entry.getState() == CQState.ACTIVE) {
            CQScheduleTask cqScheduleTask = new CQScheduleTask(entry, executor, configManager);
            if (!markCQLocallyScheduled(entry.getCqId(), entry.getCqToken(), cqScheduleTask)) {
              continue;
            }
            try {
              cqScheduleTask.submitSelf();
            } catch (RuntimeException e) {
              unmarkCQLocallyScheduled(entry.getCqId(), entry.getCqToken());
              throw e;
            }
          }
        }
      }

    } finally {
      lock.writeLock().unlock();
    }
  }

  private boolean needFetch(List<CQInfo.CQEntry> allCQs) {
    return allCQs == null && configManager.getConsensusManager().isLeader();
  }

  public void stopCQScheduler() {
    ScheduledExecutorService previous;
    lock.writeLock().lock();
    try {
      previous = executor;
      executor = null;
      cancelAllLocallyScheduledCQs();
    } finally {
      lock.writeLock().unlock();
    }
    if (previous != null) {
      previous.shutdown();
    }
  }

  public boolean markCQLocallyScheduled(String cqId, String cqToken, CQScheduleTask task) {
    AtomicBoolean shouldSchedule = new AtomicBoolean(false);
    LocallyScheduledCQ schedule = new LocallyScheduledCQ(cqToken, task);
    lock.readLock().lock();
    try {
      locallyScheduledCQs.compute(
          cqId,
          (ignored, previousSchedule) -> {
            if (previousSchedule != null && previousSchedule.hasToken(cqToken)) {
              return previousSchedule;
            }
            if (previousSchedule != null) {
              previousSchedule.cancel();
            }
            shouldSchedule.set(true);
            return schedule;
          });
      if (!shouldSchedule.get()) {
        task.cancel();
      }
      return shouldSchedule.get();
    } finally {
      lock.readLock().unlock();
    }
  }

  public void unmarkCQLocallyScheduled(String cqId, String cqToken) {
    lock.readLock().lock();
    try {
      locallyScheduledCQs.computeIfPresent(
          cqId,
          (ignored, schedule) -> {
            if (schedule.hasToken(cqToken)) {
              schedule.cancel();
              return null;
            }
            return schedule;
          });
    } finally {
      lock.readLock().unlock();
    }
  }

  private void cancelLocallyScheduledCQ(String cqId) {
    LocallyScheduledCQ schedule = locallyScheduledCQs.remove(cqId);
    if (schedule != null) {
      schedule.cancel();
    }
  }

  private void cancelAllLocallyScheduledCQs() {
    locallyScheduledCQs.values().forEach(LocallyScheduledCQ::cancel);
    locallyScheduledCQs.clear();
  }

  private static class LocallyScheduledCQ {

    private final String cqToken;
    private final CQScheduleTask task;

    private LocallyScheduledCQ(String cqToken, CQScheduleTask task) {
      this.cqToken = cqToken;
      this.task = task;
    }

    private boolean hasToken(String cqToken) {
      return this.cqToken.equals(cqToken);
    }

    private void cancel() {
      task.cancel();
    }
  }
}
