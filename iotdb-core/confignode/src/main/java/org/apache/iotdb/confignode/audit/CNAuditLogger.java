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

package org.apache.iotdb.confignode.audit;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.audit.DataNodeWriteAuditLogHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.mpp.rpc.thrift.TAuditLogReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class CNAuditLogger extends AbstractAuditLogger {
  private static final Logger logger = LoggerFactory.getLogger(CNAuditLogger.class);
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int RETRY_QUEUE_MAX_SIZE = 1024;
  private static final long RETRY_INTERVAL_IN_MS = 1000L;
  private static final long RETRY_REQUEST_TIMEOUT_IN_MS = 10_000L;
  private static final String AUDIT_DATABASE = "root.__audit";
  private static final String RETRY_THREAD_NAME = "ConfigNode-AuditLog-Retry";

  protected final IManager configManager;
  private final BlockingDeque<RetryAuditLogTask> retryQueue =
      new LinkedBlockingDeque<>(RETRY_QUEUE_MAX_SIZE);
  private final ExecutorService retryExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(RETRY_THREAD_NAME);
  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private final AtomicLong lastLogTimestamp = new AtomicLong(Long.MIN_VALUE);

  public CNAuditLogger(ConfigManager configManager) {
    this.configManager = configManager;
    retryExecutor.submit(this::retryAuditLog);
  }

  @Override
  public void log(IAuditEntity auditLogFields, Supplier<String> log) {
    try {
      if (!CommonDescriptor.getInstance().getConfig().isEnableAuditLog()) {
        return;
      }
      if (noNeedInsertAuditLog(auditLogFields)) {
        return;
      }
      TAuditLogReq req =
          new TAuditLogReq(
                  nullToString(auditLogFields.getUsername()),
                  auditLogFields.getUserId(),
                  nullToString(auditLogFields.getCliHostname()),
                  auditLogFields.getAuditEventType().toString(),
                  auditLogFields.getAuditLogOperation().toString(),
                  nullToString(auditLogFields.getPrivilegeTypeString()),
                  auditLogFields.getResult(),
                  nullToString(auditLogFields.getDatabase()),
                  nullToString(auditLogFields.getSqlString()),
                  nullToString(log.get()),
                  CONF.getConfigNodeId())
              .setLogTimestamp(nextLogTimestamp());
      RetryAuditLogTask task = new RetryAuditLogTask(req);
      TDataNodeLocation regionLeader = getAuditRegionLeader(task);
      if (regionLeader == null) {
        enqueueRetryLast(task);
        return;
      }
      writeAuditLog(task, regionLeader);
    } catch (Exception e) {
      logger.warn("Failed to write ConfigNode audit log because", e);
    }
  }

  public void close() {
    isStopped.set(true);
    retryExecutor.shutdownNow();
  }

  private TConsensusGroupId getAuditRegionId() {
    // find database "__audit"'s data_region
    List<TRegionReplicaSet> auditReplicaSets =
        configManager
            .getPartitionManager()
            .getAllReplicaSets(AUDIT_DATABASE, TConsensusGroupType.DataRegion);
    if (auditReplicaSets.isEmpty()) {
      logger.warn("Database {} does not exist.", AUDIT_DATABASE);
      return null;
    }
    return auditReplicaSets.get(0).getRegionId();
  }

  private TDataNodeLocation getAuditRegionLeader(TConsensusGroupId regionId) {
    // use ConfigManager.getLoadManager().getLoadCache().getRegionLeaderMap() to get regionLeaderId
    TDataNodeLocation regionLeader = configManager.getRegionLeaderLocation(regionId);
    if (regionLeader == null || regionLeader.getInternalEndPoint() == null) {
      logger.warn("Audit region leader for {} is not ready yet.", regionId);
      return null;
    }
    return regionLeader;
  }

  private TDataNodeLocation getAuditRegionLeader(RetryAuditLogTask task) {
    if (task.regionId == null) {
      task.regionId = getAuditRegionId();
    }
    if (task.regionId == null) {
      return null;
    }
    return getAuditRegionLeader(task.regionId);
  }

  private void writeAuditLog(RetryAuditLogTask task, TDataNodeLocation regionLeader) {
    // refer the implementation of HeartbeatService.pingRegisteredDataNode(). By appending a new
    // writeAuditLog() interface in AsyncDataNodeHeartbeatClientPool, the main thread is not
    // required to wait until the write audit log request to be complete.
    boolean dispatched =
        AsyncDataNodeHeartbeatClientPool.getInstance()
            .writeAuditLog(
                regionLeader.getInternalEndPoint(),
                task.req,
                new DataNodeWriteAuditLogHandler(
                    regionLeader.getDataNodeId(), () -> enqueueRetryLast(task)));
    if (!dispatched) {
      enqueueRetryLast(task);
    }
  }

  private boolean retryWriteAuditLog(RetryAuditLogTask task) throws InterruptedException {
    TDataNodeLocation regionLeader = getAuditRegionLeader(task);
    if (regionLeader == null) {
      return false;
    }
    CountDownLatch countDownLatch = new CountDownLatch(1);
    AtomicBoolean isSucceed = new AtomicBoolean(false);
    boolean dispatched =
        AsyncDataNodeHeartbeatClientPool.getInstance()
            .writeAuditLog(
                regionLeader.getInternalEndPoint(),
                task.req,
                new DataNodeWriteAuditLogHandler(
                    regionLeader.getDataNodeId(),
                    countDownLatch::countDown,
                    () -> {
                      isSucceed.set(true);
                      countDownLatch.countDown();
                    }));
    if (!dispatched) {
      return false;
    }
    if (!countDownLatch.await(RETRY_REQUEST_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)) {
      logger.warn(
          "Timed out retrying ConfigNode audit log to DataNode {} after {} ms",
          regionLeader.getDataNodeId(),
          RETRY_REQUEST_TIMEOUT_IN_MS);
      return false;
    }
    return isSucceed.get();
  }

  private void enqueueRetryLast(RetryAuditLogTask task) {
    if (isStopped.get()) {
      return;
    }
    while (!retryQueue.offerLast(task)) {
      RetryAuditLogTask discardedTask = retryQueue.pollFirst();
      if (discardedTask != null) {
        logger.warn(
            "ConfigNode audit log retry queue is full, discard the oldest failed audit log. limit: {}",
            RETRY_QUEUE_MAX_SIZE);
      }
    }
  }

  private void enqueueRetryFirst(RetryAuditLogTask task) {
    if (isStopped.get()) {
      return;
    }
    while (!retryQueue.offerFirst(task)) {
      RetryAuditLogTask discardedTask = retryQueue.pollLast();
      if (discardedTask != null) {
        logger.warn(
            "ConfigNode audit log retry queue is full, discard the newest failed audit log. limit: {}",
            RETRY_QUEUE_MAX_SIZE);
      }
    }
  }

  private void retryAuditLog() {
    while (!Thread.currentThread().isInterrupted()) {
      RetryAuditLogTask task;
      try {
        task = retryQueue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      try {
        if (!retryWriteAuditLog(task)) {
          enqueueRetryFirst(task);
          waitBeforeNextRetry();
        }
      } catch (InterruptedException e) {
        enqueueRetryFirst(task);
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.warn("Failed to retry ConfigNode audit log because", e);
        enqueueRetryFirst(task);
        try {
          waitBeforeNextRetry();
        } catch (InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private void waitBeforeNextRetry() throws InterruptedException {
    TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL_IN_MS);
  }

  private long nextLogTimestamp() {
    while (true) {
      long last = lastLogTimestamp.get();
      long current = CommonDateTimeUtils.currentTime();
      long next = Math.max(current, last + 1);
      if (lastLogTimestamp.compareAndSet(last, next)) {
        return next;
      }
    }
  }

  private static String nullToString(String value) {
    return value == null ? "null" : value;
  }

  private static class RetryAuditLogTask {
    private TConsensusGroupId regionId;
    private final TAuditLogReq req;

    private RetryAuditLogTask(TAuditLogReq req) {
      this.req = req;
    }
  }
}
