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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncIoTConsensusV2ServiceClient;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferResp;
import org.apache.iotdb.db.pipe.consensus.metric.IoTConsensusV2SinkMetrics;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.IoTConsensusV2AsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler.PipeTransferTabletInsertionEventHandler;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IoTConsensusV2TabletInsertionEventHandler<
        E extends TIoTConsensusV2TransferResp>
    implements AsyncMethodCallback<E> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTConsensusV2TabletInsertionEventHandler.class);

  protected final TabletInsertionEvent event;

  protected final TIoTConsensusV2TransferReq req;

  protected final IoTConsensusV2AsyncSink connector;

  protected final IoTConsensusV2SinkMetrics metric;

  private final long createTime;

  protected IoTConsensusV2TabletInsertionEventHandler(
      TabletInsertionEvent event,
      TIoTConsensusV2TransferReq req,
      IoTConsensusV2AsyncSink connector,
      IoTConsensusV2SinkMetrics metric) {
    this.event = event;
    this.req = req;
    this.connector = connector;
    this.metric = metric;
    this.createTime = System.nanoTime();
  }

  public void transfer(AsyncIoTConsensusV2ServiceClient client) throws TException {
    doTransfer(client, req);
  }

  protected abstract void doTransfer(
      AsyncIoTConsensusV2ServiceClient client, TIoTConsensusV2TransferReq req) throws TException;

  @Override
  public void onComplete(TIoTConsensusV2TransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TIoTConsensusV2TransferResp is null"));
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
            "InsertNodeTransfer: no.{} event successfully processed!",
            ((EnrichedEvent) event).getReplicateIndexForIoTV2());
      }

      // if code flow reach here, meaning the file will not be resent and will be ignored.
      // events that don't need to be retried will be removed from the buffer
      connector.removeEventFromBuffer((EnrichedEvent) event);

      long duration = System.nanoTime() - createTime;
      metric.recordConnectorWalTransferTimer(duration);
    } catch (Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(Exception exception) {
    EnrichedEvent event = (EnrichedEvent) this.event;
    LOGGER.warn(
        "Failed to transfer TabletInsertionEvent {} (committer key={}, replicate index={}).",
        event.coreReportMessage(),
        event.getCommitterKey(),
        event.getReplicateIndexForIoTV2(),
        exception);

    if (RetryUtils.needRetryWithIncreasingInterval(exception)) {
      // just in case for overflow
      if (event.getRetryInterval() << 2 <= 0) {
        event.setRetryInterval(1000L * 20);
      } else {
        event.setRetryInterval(Math.min(1000L * 20, event.getRetryInterval() << 2));
      }
    }
    // IoTV2 ensures that only use PipeInsertionEvent, which is definitely EnrichedEvent.
    connector.addFailureEventToRetryQueue((EnrichedEvent) event);
    metric.recordRetryCounter();
  }
}
