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
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBThriftAsyncConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PipeTransferTabletInsertionEventHandler<E extends TPipeTransferResp>
    implements AsyncMethodCallback<E> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTransferTabletInsertionEventHandler.class);

  protected final TabletInsertionEvent event;
  protected final TPipeTransferReq req;

  protected final IoTDBThriftAsyncConnector connector;

  protected PipeTransferTabletInsertionEventHandler(
      TabletInsertionEvent event, TPipeTransferReq req, IoTDBThriftAsyncConnector connector) {
    this.event = event;
    this.req = req;
    this.connector = connector;

    if (this.event instanceof EnrichedEvent) {
      ((EnrichedEvent) this.event)
          .increaseReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName());
    }
  }

  public void transfer(AsyncPipeDataTransferServiceClient client) throws TException {
    doTransfer(client, req);
  }

  protected abstract void doTransfer(
      AsyncPipeDataTransferServiceClient client, TPipeTransferReq req) throws TException;

  @Override
  public void onComplete(TPipeTransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TPipeTransferResp is null"));
      return;
    }

    final TSStatus status = response.getStatus();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event)
            .decreaseReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName(), true);
      }
      if (status.isSetRedirectNode()) {
        updateLeaderCache(status);
      }
    } else {
      onError(new PipeException(status.getMessage()));
    }
  }

  protected abstract void updateLeaderCache(TSStatus status);

  @Override
  public void onError(Exception exception) {
    LOGGER.warn(
        "Failed to transfer TabletInsertionEvent {} (committer key={}, commit id={}).",
        event,
        event instanceof EnrichedEvent ? ((EnrichedEvent) event).getCommitterKey() : null,
        event instanceof EnrichedEvent ? ((EnrichedEvent) event).getCommitId() : null,
        exception);

    connector.addFailureEventToRetryQueue(event);
  }
}
