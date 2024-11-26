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
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferResp;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.builder.PipeConsensusAsyncBatchReqBuilder;
import org.apache.iotdb.db.pipe.consensus.metric.PipeConsensusConnectorMetrics;
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

public class PipeConsensusTabletBatchEventHandler
    implements AsyncMethodCallback<TPipeConsensusBatchTransferResp> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusTabletBatchEventHandler.class);
  private final List<Long> requestCommitIds;
  private final List<Event> events;
  private final TPipeConsensusBatchTransferReq req;
  private final PipeConsensusAsyncConnector connector;
  private final PipeConsensusConnectorMetrics pipeConsensusConnectorMetrics;

  public PipeConsensusTabletBatchEventHandler(
      final PipeConsensusAsyncBatchReqBuilder batchBuilder,
      final PipeConsensusAsyncConnector connector,
      final PipeConsensusConnectorMetrics pipeConsensusConnectorMetrics)
      throws IOException {
    // Deep copy to keep Ids' and events' reference
    requestCommitIds = batchBuilder.deepCopyRequestCommitIds();
    events = batchBuilder.deepCopyEvents();
    req = batchBuilder.toTPipeConsensusBatchTransferReq();

    this.pipeConsensusConnectorMetrics = pipeConsensusConnectorMetrics;
    this.connector = connector;
  }

  public void transfer(final AsyncPipeConsensusServiceClient client) throws TException {
    client.pipeConsensusBatchTransfer(req, this);
  }

  @Override
  public void onComplete(final TPipeConsensusBatchTransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TPipeConsensusBatchTransferResp is null"));
      return;
    }

    try {
      final List<TSStatus> status =
          response.getBatchResps().stream()
              .map(TPipeConsensusTransferResp::getStatus)
              .collect(Collectors.toList());

      if (status.stream()
          .anyMatch(
              tsStatus -> tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())) {
        status.stream()
            .filter(tsStatus -> tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
            .forEach(
                tsStatus -> {
                  pipeConsensusConnectorMetrics.recordRetryCounter();
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
              .decreaseReferenceCount(PipeConsensusTabletBatchEventHandler.class.getName(), true);
        }
      }
    } catch (final Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(final Exception exception) {
    LOGGER.warn(
        "PipeConsensus: Failed to transfer TabletInsertionEvent batch. Total failed events: {}, related pipe names: {}",
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
