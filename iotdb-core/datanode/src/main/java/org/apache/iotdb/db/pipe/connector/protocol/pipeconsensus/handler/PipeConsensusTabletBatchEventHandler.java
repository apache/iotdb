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
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.consensus.pipe.client.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.builder.PipeConsensusAsyncBatchReqBuilder;
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
    implements AsyncMethodCallback<TPipeConsensusTransferResp> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusTabletBatchEventHandler.class);
  private final List<Long> requestCommitIds;
  private final List<Event> events;
  private final TPipeConsensusTransferReq req;
  private final PipeConsensusAsyncConnector connector;

  public PipeConsensusTabletBatchEventHandler(
      final PipeConsensusAsyncBatchReqBuilder batchBuilder,
      final PipeConsensusAsyncConnector connector)
      throws IOException {
    // Deep copy to keep Ids' and events' reference
    requestCommitIds = batchBuilder.deepCopyRequestCommitIds();
    events = batchBuilder.deepCopyEvents();
    req = batchBuilder.toTPipeConsensusTransferReq();

    this.connector = connector;
  }

  public void transfer(final AsyncPipeConsensusServiceClient client) throws TException {
    client.pipeConsensusTransfer(req, this);
  }

  @Override
  public void onComplete(final TPipeConsensusTransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TPipeConsensusResp is null"));
      return;
    }

    try {
      final TSStatus status = response.getStatus();
      // Only handle the failed statuses to avoid string format performance overhead
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        connector
            .statusHandler()
            .handle(status, response.getStatus().getMessage(), events.toString());
      }
      for (final Event event : events) {
        if (event instanceof EnrichedEvent) {
          ((EnrichedEvent) event)
              .decreaseReferenceCount(PipeConsensusTabletBatchEventHandler.class.getName(), true);
        }

        connector.removeEventFromBuffer(event);
      }
    } catch (final Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(final Exception exception) {
    LOGGER.warn(
        "Failed to transfer TabletInsertionEvent batch {} (request commit ids={}).",
        events.stream()
            .map(
                event ->
                    event instanceof EnrichedEvent
                        ? ((EnrichedEvent) event).coreReportMessage()
                        : event.toString())
            .collect(Collectors.toList()),
        requestCommitIds,
        exception);

    connector.addFailureEventsToRetryQueue(events);
  }
}
