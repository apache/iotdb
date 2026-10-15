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

package org.apache.iotdb.consensus.iot.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserDataTransferAuditEvent;
import org.apache.iotdb.commons.audit.UserDataTransferAuditHandler;
import org.apache.iotdb.commons.audit.UserDataTransferProtectionMethod;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.consensus.i18n.IoTConsensusMessages;
import org.apache.iotdb.consensus.iot.logdispatcher.Batch;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher.LogDispatcherThread;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcherThreadMetrics;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesRes;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.tsfile.external.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DispatchLogHandler implements AsyncMethodCallback<TSyncLogEntriesRes> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DispatchLogHandler.class);

  private final LogDispatcherThread thread;
  private final Batch batch;
  private final long createTime;
  private final LogDispatcherThreadMetrics logDispatcherThreadMetrics;
  private int retryCount;
  private long retryInterval;

  public DispatchLogHandler(
      LogDispatcherThread thread,
      LogDispatcherThreadMetrics logDispatcherThreadMetrics,
      Batch batch) {
    this.thread = thread;
    this.logDispatcherThreadMetrics = logDispatcherThreadMetrics;
    this.batch = batch;
    this.createTime = System.nanoTime();
    this.retryInterval = thread.getConfig().getReplication().getBasicRetryWaitTimeMs();
  }

  @Override
  public void onComplete(TSyncLogEntriesRes response) {
    recordTransferAttempt(response);
    if (response.getStatuses().stream()
        .anyMatch(status -> RetryUtils.needRetryForWrite(status.getCode()))) {
      List<String> retryStatusMessages =
          response.getStatuses().stream()
              .filter(status -> RetryUtils.needRetryForWrite(status.getCode()))
              .map(TSStatus::getMessage)
              .collect(Collectors.toList());

      String messages = String.join(", ", retryStatusMessages);
      if (++retryCount == 1) {
        LOGGER.warn(
            IoTConsensusMessages.CANNOT_SEND_TO_PEER,
            batch,
            thread.getPeer(),
            retryCount,
            messages);
      } else {
        LOGGER.debug(
            IoTConsensusMessages.CANNOT_SEND_TO_PEER,
            batch,
            thread.getPeer(),
            retryCount,
            messages);
      }
      sleepCorrespondingTimeAndRetryAsynchronous();
    } else {
      if (LOGGER.isDebugEnabled()) {
        boolean containsError =
            response.getStatuses().stream()
                .anyMatch(
                    status -> status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode());
        if (containsError) {
          LOGGER.debug(
              IoTConsensusMessages.SEND_COMPLETE_BUT_CONTAINS_ERROR,
              batch,
              thread.getPeer(),
              response.getStatuses());
        }
      }
      completeBatch(batch);
    }
    if (response.isSetReceiverMemSize()) {
      LogDispatcher.getReceiverMemSizeSum().addAndGet(response.getReceiverMemSize());
      LogDispatcher.getSenderMemSizeSum().addAndGet(batch.getMemorySize());
    }
    logDispatcherThreadMetrics.recordSyncLogTimePerRequest(System.nanoTime() - createTime);
  }

  @Override
  public void onError(Exception exception) {
    recordTransferAttempt(false, null, exception);
    ++retryCount;
    Throwable rootCause = ExceptionUtils.getRootCause(exception);
    final Throwable actualCause = rootCause == null ? exception : rootCause;
    if (retryCount == 1) {
      LOGGER.warn(
          IoTConsensusMessages.CANNOT_SEND_TO_PEER_ON_ERROR,
          batch,
          thread.getPeer(),
          retryCount,
          actualCause.toString());
    } else {
      LOGGER.debug(
          IoTConsensusMessages.CANNOT_SEND_TO_PEER_ON_ERROR,
          batch,
          thread.getPeer(),
          retryCount,
          actualCause.toString());
    }
    // skip TApplicationException caused by follower
    if (actualCause instanceof TApplicationException) {
      completeBatch(batch);
      LOGGER.warn(IoTConsensusMessages.SKIP_RETRY_TAPPLICATION_EXCEPTION, batch);
      logDispatcherThreadMetrics.recordSyncLogTimePerRequest(System.nanoTime() - createTime);
      return;
    }
    sleepCorrespondingTimeAndRetryAsynchronous();
  }

  private void sleepCorrespondingTimeAndRetryAsynchronous() {
    if (retryInterval != thread.getConfig().getReplication().getMaxRetryWaitTimeMs()) {
      retryInterval =
          Math.min(retryInterval * 2, thread.getConfig().getReplication().getMaxRetryWaitTimeMs());
    }

    thread
        .getImpl()
        .getBackgroundTaskService()
        .schedule(
            () -> {
              if (thread.isStopped()) {
                LOGGER.debug(
                    IoTConsensusMessages.LOG_DISPATCHER_STOPPED_NO_RETRY,
                    thread.getPeer(),
                    batch,
                    retryCount);
              } else {
                thread.sendBatchAsync(batch, this);
              }
            },
            retryInterval,
            TimeUnit.MILLISECONDS);
  }

  private void completeBatch(Batch batch) {
    thread.getSyncStatus().removeBatch(batch);
    // update safely deleted search index after last flushed sync index may be updated by
    // removeBatch
    thread.updateSafelyDeletedSearchIndex();
  }

  private void recordTransferAttempt(boolean success, String errorCode, Throwable error) {
    try {
      final UserDataTransferAuditHandler auditHandler =
          thread.getImpl().getUserDataTransferAuditHandler();
      if (!batch.containsUserData() || !auditHandler.isEnabled()) {
        return;
      }
      recordTransferAttempt(
          auditHandler,
          batch,
          thread.getImpl().getThisNode().getEndpoint(),
          thread.getPeer().getEndpoint(),
          UserDataTransferProtectionMethod.fromTlsEnabled(
              thread.getConfig().getRpc().isEnableSSL()),
          success,
          errorCode,
          error);
    } catch (RuntimeException auditFailure) {
      warnAuditFailure(auditFailure);
    }
  }

  private void recordTransferAttempt(TSyncLogEntriesRes response) {
    try {
      final UserDataTransferAuditHandler auditHandler =
          thread.getImpl().getUserDataTransferAuditHandler();
      if (!batch.containsUserData() || !auditHandler.isEnabled()) {
        return;
      }
      recordTransferAttempt(
          auditHandler,
          batch,
          thread.getImpl().getThisNode().getEndpoint(),
          thread.getPeer().getEndpoint(),
          UserDataTransferProtectionMethod.fromTlsEnabled(
              thread.getConfig().getRpc().isEnableSSL()),
          response);
    } catch (RuntimeException auditFailure) {
      warnAuditFailure(auditFailure);
    }
  }

  static void recordTransferAttempt(
      UserDataTransferAuditHandler auditHandler,
      Batch batch,
      TEndPoint source,
      TEndPoint target,
      UserDataTransferProtectionMethod protectionMethod,
      TSyncLogEntriesRes response) {
    try {
      if (!batch.containsUserData() || !auditHandler.isEnabled()) {
        return;
      }
      // One batch RPC is one physical transfer attempt, so keep one representative error value in
      // the minimum audit record instead of concatenating an unbounded number of response details.
      final TSStatus firstFailedStatus =
          response.getStatuses().stream()
              .filter(status -> status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
              .findFirst()
              .orElse(null);
      recordTransferAttempt(
          auditHandler,
          batch,
          source,
          target,
          protectionMethod,
          firstFailedStatus == null,
          firstFailedStatus == null ? null : String.valueOf(firstFailedStatus.getCode()),
          null);
    } catch (RuntimeException auditFailure) {
      warnAuditFailure(auditFailure);
    }
  }

  static void recordTransferAttempt(
      UserDataTransferAuditHandler auditHandler,
      Batch batch,
      TEndPoint source,
      TEndPoint target,
      UserDataTransferProtectionMethod protectionMethod,
      boolean success,
      String errorCode,
      Throwable error) {
    try {
      if (!batch.containsUserData() || !auditHandler.isEnabled()) {
        return;
      }
      auditHandler.onAttempt(
          new UserDataTransferAuditEvent(
              source,
              source,
              target,
              protectionMethod,
              success,
              errorCode != null ? errorCode : error == null ? null : error.getClass().getName()));
    } catch (RuntimeException auditFailure) {
      warnAuditFailure(auditFailure);
    }
  }

  private static void warnAuditFailure(RuntimeException auditFailure) {
    LOGGER.warn(
        IoTConsensusMessages
            .LOG_FAILED_TO_RECORD_A_USER_DATA_TRANSFER_AUDIT_EVENT_CONSENSUS_REPLICATION_WILL_CONTINUE_F215E222,
        auditFailure);
  }
}
