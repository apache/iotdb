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

package org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.consensus.metric.PipeConsensusSinkMetrics;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.PipeConsensusAsyncSink;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConsensusDeleteEventHandler
    implements AsyncMethodCallback<TPipeConsensusTransferResp> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusDeleteEventHandler.class);

  private final PipeDeleteDataNodeEvent event;

  private final TPipeConsensusTransferReq req;

  private final PipeConsensusAsyncSink connector;

  private final PipeConsensusSinkMetrics metric;

  private final long createTime;

  public PipeConsensusDeleteEventHandler(
      PipeDeleteDataNodeEvent event,
      TPipeConsensusTransferReq req,
      PipeConsensusAsyncSink connector,
      PipeConsensusSinkMetrics metric) {
    this.event = event;
    this.req = req;
    this.connector = connector;
    this.metric = metric;
    this.createTime = System.nanoTime();
  }

  public void transfer(AsyncPipeConsensusServiceClient client) throws TException {
    client.pipeConsensusTransfer(req, this);
  }

  @Override
  public void onComplete(TPipeConsensusTransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TPipeConsensusTransferResp is null"));
      return;
    }

    final TSStatus status = response.getStatus();
    try {
      // Only handle the failed statuses to avoid string format performance overhead
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        connector.statusHandler().handle(status, status.getMessage(), event.toString(), true);
      }
      event.decreaseReferenceCount(PipeConsensusDeleteEventHandler.class.getName(), true);

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "DeleteNodeTransfer: no.{} event successfully processed!",
            event.getReplicateIndexForIoTV2());
      }
      // if code flow reach here, meaning the file will not be resent and will be ignored.
      // events that don't need to be retried will be removed from the buffer
      connector.removeEventFromBuffer(event);

      long duration = System.nanoTime() - createTime;
      metric.recordConnectorWalTransferTimer(duration);
    } catch (Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(Exception e) {
    LOGGER.warn(
        "Failed to transfer PipeDeleteNodeEvent {} (committer key={}, replicate index={}).",
        event.coreReportMessage(),
        event.getCommitterKey(),
        event.getReplicateIndexForIoTV2(),
        e);

    if (RetryUtils.needRetryWithIncreasingInterval(e)) {
      // just in case for overflow
      if (event.getRetryInterval() << 2 <= 0) {
        event.setRetryInterval(1000L * 20);
      } else {
        event.setRetryInterval(Math.min(1000L * 20, event.getRetryInterval() << 2));
      }
    }
    // IoTV2 ensures that only use PipeInsertionEvent, which is definitely EnrichedEvent.
    connector.addFailureEventToRetryQueue(event);
    metric.recordRetryCounter();
  }
}
