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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletInsertionEventHandler;
import org.apache.iotdb.db.pipe.consensus.metric.PipeConsensusConnectorMetrics;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PipeConsensusTabletInsertionEventHandler<E extends TPipeConsensusTransferResp>
    implements AsyncMethodCallback<E> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusTabletInsertionEventHandler.class);

  protected final TabletInsertionEvent event;

  protected final TPipeConsensusTransferReq req;

  protected final PipeConsensusAsyncConnector connector;

  protected final PipeConsensusConnectorMetrics metric;

  private final long createTime;

  protected PipeConsensusTabletInsertionEventHandler(
      TabletInsertionEvent event,
      TPipeConsensusTransferReq req,
      PipeConsensusAsyncConnector connector,
      PipeConsensusConnectorMetrics metric) {
    this.event = event;
    this.req = req;
    this.connector = connector;
    this.metric = metric;
    this.createTime = System.nanoTime();
  }

  public void transfer(AsyncPipeConsensusServiceClient client) throws TException {
    doTransfer(client, req);
  }

  protected abstract void doTransfer(
      AsyncPipeConsensusServiceClient client, TPipeConsensusTransferReq req) throws TException;

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
        connector.statusHandler().handle(status, status.getMessage(), event.toString());
      }
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event)
            .decreaseReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName(), true);
      }

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "Debug only: no.{} event successfully processed!",
            ((EnrichedEvent) event).getCommitId());
        connector.removeEventFromBuffer((EnrichedEvent) event);
      }

      long duration = System.nanoTime() - createTime;
      metric.recordConnectorWalTransferTimer(duration);
    } catch (Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(Exception exception) {
    LOGGER.warn(
        "Failed to transfer TabletInsertionEvent {} (committer key={}, commit id={}).",
        event instanceof EnrichedEvent
            ? ((EnrichedEvent) event).coreReportMessage()
            : event.toString(),
        event instanceof EnrichedEvent ? ((EnrichedEvent) event).getCommitterKey() : null,
        event instanceof EnrichedEvent ? ((EnrichedEvent) event).getCommitId() : null,
        exception);

    connector.addFailureEventToRetryQueue(event);
    metric.recordRetryCounter();
  }
}
