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

  private final TabletInsertionEvent event;
  private final TPipeTransferReq req;

  private final IoTDBThriftAsyncConnector connector;

  protected PipeTransferTabletInsertionEventHandler(
      TabletInsertionEvent event, TPipeTransferReq req, IoTDBThriftAsyncConnector connector) {
    this.event = event;
    this.req = req;
    this.connector = connector;
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

    if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      connector.commit(event instanceof EnrichedEvent ? (EnrichedEvent) event : null);
    } else {
      onError(new PipeException(response.getStatus().getMessage()));
    }
  }

  @Override
  public void onError(Exception exception) {
    LOGGER.warn(
        "Failed to transfer TabletInsertionEvent {} (requestCommitId={}).",
        event,
        event instanceof EnrichedEvent ? ((EnrichedEvent) event).getCommitId() : null,
        exception);

    connector.addFailureEventToRetryQueue(event);
  }
}
