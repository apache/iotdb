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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;

import java.util.concurrent.atomic.AtomicBoolean;

/** Transfers every Object value immediately as a direct raw-tablet request. */
public class PipeTransferTabletObjectHandler extends PipeTransferTabletInsertionEventHandler {

  private final PipeTabletObjectRequestIterator requestIterator;
  private final long handlerStartTimeInNanos = System.nanoTime();
  private final AtomicBoolean isHandlerMetricRecorded = new AtomicBoolean(false);
  private TPipeTransferReq currentRequest;
  private long requestPreparationStartTimeInNanos;
  private long requestSendTimeInNanos;
  private long callbackCount;

  public PipeTransferTabletObjectHandler(
      final PipeInsertionEvent event, final IoTDBDataRegionAsyncSink sink) {
    this(event, sink, new PipeTabletObjectRequestIterator(event));
  }

  private PipeTransferTabletObjectHandler(
      final PipeInsertionEvent event,
      final IoTDBDataRegionAsyncSink sink,
      final PipeTabletObjectRequestIterator requestIterator) {
    super(event, null, sink);
    this.requestIterator = requestIterator;
    currentRequest = nextCompressedRequest();
  }

  @Override
  public void transfer(final AsyncPipeDataTransferServiceClient client) throws TException {
    client.setShouldReturnSelf(false);
    try {
      sendCurrentRequest(client);
    } catch (final TException e) {
      returnClientToPool(client);
      throw e;
    }
  }

  private void sendCurrentRequest(final AsyncPipeDataTransferServiceClient client)
      throws TException {
    sink.rateLimitIfNeeded(
        event.getPipeName(),
        event.getCreationTime(),
        client.getEndPoint(),
        currentRequest.getBody().length);
    sink.recordObjectTabletRequestPrepareTime(
        false, System.nanoTime() - requestPreparationStartTimeInNanos);
    requestSendTimeInNanos = System.nanoTime();
    tryTransfer(client, currentRequest);
  }

  @Override
  protected void doTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException {
    transferWithOptionalRequestSlicing(client, req);
  }

  @Override
  protected boolean onCompleteInternal(final TPipeTransferResp response) {
    callbackCount++;
    sink.recordObjectTabletRequestTime(false, System.nanoTime() - requestSendTimeInNanos);
    if (response == null) {
      onError(new PipeException(DataNodePipeMessages.TPIPETRANSFERRESP_IS_NULL));
      return false;
    }
    try {
      final TSStatus status = response.getStatus();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        sink.statusHandler().handle(status, status.getMessage(), event.toString());
      }
      if (requestIterator.hasNext()) {
        currentRequest = nextCompressedRequest();
        sendCurrentRequest(client);
        return false;
      }
      requestIterator.close();
      final boolean completed = super.onCompleteInternal(response);
      if (completed) {
        returnClientToPool(client);
        recordHandlerMetric();
      }
      return completed;
    } catch (final Exception e) {
      onError(e);
      return false;
    }
  }

  @Override
  public void onError(final Exception exception) {
    try {
      requestIterator.close();
      super.onError(exception);
    } finally {
      returnClientToPool(client);
      recordHandlerMetric();
    }
  }

  @Override
  protected void updateLeaderCache(final TSStatus status) {
    // Object requests can span multiple devices. A retry uses the normal load balancer.
  }

  private TPipeTransferReq nextCompressedRequest() {
    if (!requestIterator.hasNext()) {
      throw new IllegalStateException();
    }
    try {
      requestPreparationStartTimeInNanos = System.nanoTime();
      return sink.compressIfNeeded(requestIterator.next());
    } catch (final Exception e) {
      requestIterator.close();
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void clearEventsReferenceCount() {
    try {
      requestIterator.close();
      super.clearEventsReferenceCount();
    } finally {
      returnClientToPool(client);
      recordHandlerMetric();
    }
  }

  private void recordHandlerMetric() {
    if (isHandlerMetricRecorded.compareAndSet(false, true)) {
      sink.recordObjectTabletHandler(
          false, System.nanoTime() - handlerStartTimeInNanos, callbackCount);
    }
  }
}
