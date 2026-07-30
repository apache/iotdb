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
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
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
    lock.readLock().lock();
    try {
      ScheduledExecutorService currentExecutor = executor;
      return configManager.getProcedureManager().createCQ(req, currentExecutor);
    } finally {
      lock.readLock().unlock();
    }
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
