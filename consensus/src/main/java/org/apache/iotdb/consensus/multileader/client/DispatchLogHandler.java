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

package org.apache.iotdb.consensus.multileader.client;

import org.apache.iotdb.consensus.multileader.logdispatcher.LogDispatcher.LogDispatcherThread;
import org.apache.iotdb.consensus.multileader.logdispatcher.PendingBatch;
import org.apache.iotdb.consensus.multileader.thrift.TSyncLogRes;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class DispatchLogHandler implements AsyncMethodCallback<TSyncLogRes> {

  private final Logger logger = LoggerFactory.getLogger(DispatchLogHandler.class);

  private final LogDispatcherThread thread;
  private final PendingBatch batch;
  private int retryCount;

  public DispatchLogHandler(LogDispatcherThread thread, PendingBatch batch) {
    this.thread = thread;
    this.batch = batch;
  }

  @Override
  public void onComplete(TSyncLogRes response) {
    if (response.getStatus().size() == 1 && needRetry(response.getStatus().get(0).getCode())) {
      logger.warn(
          "Can not send {} to peer {} for {} times because {}",
          batch,
          thread.getPeer(),
          ++retryCount,
          response.getStatus().get(0).getMessage());
      sleepCorrespondingTimeAndRetryAsynchronous();
    } else {
      thread.getSyncStatus().removeBatch(batch);
      // update safely deleted search index after current sync index is updated by removeBatch
      thread.updateSafelyDeletedSearchIndex();
    }
  }

  private boolean needRetry(int statusCode) {
    return statusCode == TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
        || statusCode == TSStatusCode.READ_ONLY_SYSTEM_ERROR.getStatusCode()
        || statusCode == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode();
  }

  @Override
  public void onError(Exception exception) {
    logger.warn(
        "Can not send {} to peer for {} times {} because {}",
        batch,
        thread.getPeer(),
        ++retryCount,
        exception);
    sleepCorrespondingTimeAndRetryAsynchronous();
  }

  private void sleepCorrespondingTimeAndRetryAsynchronous() {
    // TODO handle forever retry
    CompletableFuture.runAsync(
        () -> {
          try {
            long defaultSleepTime =
                (long)
                    (thread.getConfig().getReplication().getBasicRetryWaitTimeMs()
                        * Math.pow(2, retryCount));
            Thread.sleep(
                Math.min(
                    defaultSleepTime, thread.getConfig().getReplication().getMaxRetryWaitTimeMs()));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Unexpected interruption during retry pending batch");
          }
          if (thread.isStopped()) {
            logger.debug(
                "LogDispatcherThread {} has been stopped, we will ignore the retry PendingBatch {} after {} times",
                thread.getPeer(),
                batch,
                retryCount);
          } else {
            thread.sendBatchAsync(batch, this);
          }
        });
  }
}
