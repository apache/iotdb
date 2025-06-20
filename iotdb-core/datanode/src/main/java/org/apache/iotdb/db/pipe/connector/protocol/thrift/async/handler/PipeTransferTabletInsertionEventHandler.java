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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PipeTransferTabletInsertionEventHandler extends PipeTransferTrackableHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger("PipeLog");

  protected final TabletInsertionEvent event;
  protected final TPipeTransferReq req;

  protected PipeTransferTabletInsertionEventHandler(
      final TabletInsertionEvent event,
      final TPipeTransferReq req,
      final IoTDBDataRegionAsyncConnector connector) {
    super(connector);

    this.event = event;
    this.req = req;
  }

  public void transfer(final AsyncPipeDataTransferServiceClient client) throws TException {
    if (event instanceof EnrichedEvent) {
      connector.rateLimitIfNeeded(
          ((EnrichedEvent) event).getPipeName(),
          ((EnrichedEvent) event).getCreationTime(),
          client.getEndPoint(),
          req.getBody().length);
    }

    tryTransfer(client, req);
  }

  @Override
  protected boolean onCompleteInternal(final TPipeTransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TPipeTransferResp is null"));
      return false;
    }

    final TSStatus status = response.getStatus();
    try {
      // Only handle the failed statuses to avoid string format performance overhead
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        connector
            .statusHandler()
            .handle(response.getStatus(), response.getStatus().getMessage(), event.toString());
      }
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event)
            .decreaseReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName(), true);
      }
      if (status.isSetRedirectNode()) {
        updateLeaderCache(status);
      }
    } catch (final Exception e) {
      onError(e);
      return false;
    }

    return true;
  }

  @Override
  protected void onErrorInternal(final Exception exception) {
    try {
      LOGGER.warn(
          "Failed to transfer TabletInsertionEvent {} (committer key={}, commit id={}).",
          event instanceof EnrichedEvent
              ? ((EnrichedEvent) event).coreReportMessage()
              : event.toString(),
          event instanceof EnrichedEvent ? ((EnrichedEvent) event).getCommitterKey() : null,
          event instanceof EnrichedEvent ? ((EnrichedEvent) event).getCommitId() : null,
          exception);
    } finally {
      connector.addFailureEventToRetryQueue(event);
    }
  }

  @Override
  public void clearEventsReferenceCount() {
    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event)
          .clearReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName());
    }
  }

  protected abstract void updateLeaderCache(final TSStatus status);
}
