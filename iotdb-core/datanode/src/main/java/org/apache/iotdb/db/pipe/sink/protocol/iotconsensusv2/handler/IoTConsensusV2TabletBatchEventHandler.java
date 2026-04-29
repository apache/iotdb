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
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2BatchTransferReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2BatchTransferResp;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferResp;
import org.apache.iotdb.db.pipe.consensus.metric.IoTConsensusV2SinkMetrics;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.IoTConsensusV2AsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.builder.IoTConsensusV2AsyncBatchReqBuilder;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class IoTConsensusV2TabletBatchEventHandler
    implements AsyncMethodCallback<TIoTConsensusV2BatchTransferResp> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTConsensusV2TabletBatchEventHandler.class);
  private final List<Long> requestCommitIds;
  private final List<EnrichedEvent> events;
  private final TIoTConsensusV2BatchTransferReq req;
  private final IoTConsensusV2AsyncSink connector;
  private final IoTConsensusV2SinkMetrics iotConsensusV2SinkMetrics;

  public IoTConsensusV2TabletBatchEventHandler(
      final IoTConsensusV2AsyncBatchReqBuilder batchBuilder,
      final IoTConsensusV2AsyncSink connector,
      final IoTConsensusV2SinkMetrics iotConsensusV2SinkMetrics)
      throws IOException {
    // Deep copy to keep Ids' and events' reference
    requestCommitIds = batchBuilder.deepCopyRequestCommitIds();
    events = batchBuilder.deepCopyEvents();
    req = batchBuilder.toTIoTConsensusV2BatchTransferReq();

    this.iotConsensusV2SinkMetrics = iotConsensusV2SinkMetrics;
    this.connector = connector;
  }

  public void transfer(final AsyncIoTConsensusV2ServiceClient client) throws TException {
    client.iotConsensusV2BatchTransfer(req, this);
  }

  @Override
  public void onComplete(final TIoTConsensusV2BatchTransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TIoTConsensusV2BatchTransferResp is null"));
      return;
    }

    try {
      final List<TSStatus> status =
          response.getBatchResps().stream()
              .map(TIoTConsensusV2TransferResp::getStatus)
              .collect(Collectors.toList());

      if (status.stream()
          .anyMatch(
              tsStatus -> tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())) {
        status.stream()
            .filter(tsStatus -> tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
            .forEach(
                tsStatus -> {
                  iotConsensusV2SinkMetrics.recordRetryCounter();
                  connector
                      .statusHandler()
                      .handle(tsStatus, tsStatus.getMessage(), events.toString());
                });
        // if any events failed, we will resend it all.
        connector.addFailureEventsToRetryQueue(events);
      }
      // if all events success, remove them from transferBuffer
      else {
        events.forEach(event -> connector.removeEventFromBuffer((EnrichedEvent) event));
      }

      for (final Event event : events) {
        if (event instanceof EnrichedEvent) {
          ((EnrichedEvent) event)
              .decreaseReferenceCount(IoTConsensusV2TabletBatchEventHandler.class.getName(), true);
        }
      }
    } catch (final Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(final Exception exception) {
    LOGGER.warn(
        "IoTConsensusV2: Failed to transfer TabletInsertionEvent batch. Total failed events: {}, related pipe names: {}",
        events.size(),
        events.stream()
            .map(
                event ->
                    event instanceof EnrichedEvent
                        ? ((EnrichedEvent) event).getPipeName()
                        : "UNKNOWN")
            .collect(Collectors.toSet()),
        exception);

    connector.addFailureEventsToRetryQueue(events);
  }
}
