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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.iot.logdispatcher.Batch;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher.LogDispatcherThread;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcherThreadMetrics;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesRes;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DispatchLogHandler implements AsyncMethodCallback<TSyncLogEntriesRes> {

  private final Logger logger = LoggerFactory.getLogger(DispatchLogHandler.class);

  private final LogDispatcherThread thread;
  private final Batch batch;
  private final long createTime;
  private final LogDispatcherThreadMetrics logDispatcherThreadMetrics;
  private int retryCount;

  public DispatchLogHandler(
      LogDispatcherThread thread,
      LogDispatcherThreadMetrics logDispatcherThreadMetrics,
      Batch batch) {
    this.thread = thread;
    this.logDispatcherThreadMetrics = logDispatcherThreadMetrics;
    this.batch = batch;
    this.createTime = System.nanoTime();
  }

  @Override
  public void onComplete(TSyncLogEntriesRes response) {
    if (response.getStatuses().stream().anyMatch(status -> needRetry(status.getCode()))) {
      List<String> retryStatusMessages =
          response.getStatuses().stream()
              .filter(status -> needRetry(status.getCode()))
              .map(TSStatus::getMessage)
              .collect(Collectors.toList());

      String messages = String.join(", ", retryStatusMessages);
      logger.warn(
          "Can not send {} to peer {} for {} times because {}",
          batch,
          thread.getPeer(),
          ++retryCount,
          messages);
      sleepCorrespondingTimeAndRetryAsynchronous();
    } else {
      if (logger.isDebugEnabled()) {
        boolean containsError =
            response.getStatuses().stream()
                .anyMatch(
                    status -> status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode());
        if (containsError) {
          logger.debug(
              "Send {} to peer {} complete but contains unsuccessful status: {}",
              batch,
              thread.getPeer(),
              response.getStatuses());
        }
      }
      completeBatch(batch);
    }
    logDispatcherThreadMetrics.recordSyncLogTimePerRequest(System.nanoTime() - createTime);
  }

  public static boolean needRetry(int statusCode) {
    return statusCode == TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
        || statusCode == TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()
        || statusCode == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode();
  }

  @Override
  public void onError(Exception exception) {
    ++retryCount;
    Throwable rootCause = ExceptionUtils.getRootCause(exception);
    logger.warn(
        "Can not send {} to peer for {} times {} because {}",
        batch,
        thread.getPeer(),
        retryCount,
        rootCause.toString());
    // skip TApplicationException caused by follower
    if (rootCause instanceof TApplicationException) {
      completeBatch(batch);
      logger.warn("Skip retrying this Batch {} because of TApplicationException.", batch);
      logDispatcherThreadMetrics.recordSyncLogTimePerRequest(System.nanoTime() - createTime);
      return;
    }
    sleepCorrespondingTimeAndRetryAsynchronous();
  }

  private void sleepCorrespondingTimeAndRetryAsynchronous() {
    long sleepTime =
        Math.min(
            (long)
                (thread.getConfig().getReplication().getBasicRetryWaitTimeMs()
                    * Math.pow(2, retryCount)),
            thread.getConfig().getReplication().getMaxRetryWaitTimeMs());
    thread
        .getImpl()
        .getBackgroundTaskService()
        .schedule(
            () -> {
              if (thread.isStopped()) {
                logger.debug(
                    "LogDispatcherThread {} has been stopped, "
                        + "we will not retrying this Batch {} after {} times",
                    thread.getPeer(),
                    batch,
                    retryCount);
              } else {
                thread.sendBatchAsync(batch, this);
              }
            },
            sleepTime,
            TimeUnit.MILLISECONDS);
  }

  private void completeBatch(Batch batch) {
    thread.getSyncStatus().removeBatch(batch);
    // update safely deleted search index after last flushed sync index may be updated by
    // removeBatch
    thread.updateSafelyDeletedSearchIndex();
  }
}
