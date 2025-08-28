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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PipeTransferTabletInsertionEventHandler extends PipeTransferTrackableHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTransferTabletInsertionEventHandler.class);

  protected final PipeInsertionEvent event;
  protected final TPipeTransferReq req;

  protected PipeTransferTabletInsertionEventHandler(
      final PipeInsertionEvent event,
      final TPipeTransferReq req,
      final IoTDBDataRegionAsyncSink connector) {
    super(connector);

    this.event = event;
    this.req = req;
  }

  public void transfer(final AsyncPipeDataTransferServiceClient client) throws TException {
    connector.rateLimitIfNeeded(
        event.getPipeName(), event.getCreationTime(), client.getEndPoint(), req.getBody().length);

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
      event.decreaseReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName(), true);
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
      PipeLogger.log(
          LOGGER::warn,
          exception,
          "Failed to transfer TabletInsertionEvent %s (committer key=%s, commit id=%s).",
          event.coreReportMessage(),
          event.getCommitterKey(),
          event.getCommitId());
    } finally {
      connector.addFailureEventToRetryQueue(event);
    }
  }

  @Override
  public void clearEventsReferenceCount() {
    event.clearReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName());
  }

  protected abstract void updateLeaderCache(final TSStatus status);
}
