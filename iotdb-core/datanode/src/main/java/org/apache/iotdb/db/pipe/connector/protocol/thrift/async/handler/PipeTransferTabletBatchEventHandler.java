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
import org.apache.iotdb.db.pipe.connector.payload.evolvable.builder.IoTDBThriftAsyncPipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBThriftAsyncConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PipeTransferTabletBatchEventHandler implements AsyncMethodCallback<TPipeTransferResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTransferTabletBatchEventHandler.class);

  private final List<Long> requestCommitIds;
  private final List<Event> events;
  private final TPipeTransferReq req;

  private final IoTDBThriftAsyncConnector connector;

  public PipeTransferTabletBatchEventHandler(
      IoTDBThriftAsyncPipeTransferBatchReqBuilder batchBuilder, IoTDBThriftAsyncConnector connector)
      throws IOException {
    // Deep copy to keep Ids' and events' reference
    requestCommitIds = batchBuilder.deepcopyRequestCommitIds();
    events = batchBuilder.deepcopyEvents();
    req = PipeTransferTabletBatchReq.toTPipeTransferReq(batchBuilder.getTPipeTransferReqs());

    this.connector = connector;
  }

  public void transfer(AsyncPipeDataTransferServiceClient client) throws TException {
    client.pipeTransfer(req, this);
  }

  @Override
  public void onComplete(TPipeTransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TPipeTransferResp is null"));
      return;
    }

    if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      for (int i = 0; i < events.size(); ++i) {
        connector.commit(
            requestCommitIds.get(i),
            events.get(i) instanceof EnrichedEvent ? (EnrichedEvent) events.get(i) : null);
      }
    } else {
      onError(new PipeException(response.getStatus().getMessage()));
    }
  }

  @Override
  public void onError(Exception exception) {
    LOGGER.warn(
        "Failed to transfer TabletInsertionEvent {} (requestCommitId={}).",
        events,
        requestCommitIds,
        exception);

    for (int i = 0; i < events.size(); ++i) {
      connector.addFailureEventToRetryQueue(requestCommitIds.get(i), events.get(i));
    }
  }
}
