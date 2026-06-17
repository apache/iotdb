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

import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferSliceReqBuilder;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class PipeTransferTrackableHandler
    implements AsyncMethodCallback<TPipeTransferResp>, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferTsFileHandler.class);

  protected final IoTDBDataRegionAsyncSink sink;
  protected volatile AsyncPipeDataTransferServiceClient client;

  public PipeTransferTrackableHandler(final IoTDBDataRegionAsyncSink sink) {
    this.sink = sink;
  }

  @Override
  public void onComplete(final TPipeTransferResp response) {
    if (Objects.nonNull(client) && Objects.nonNull(response)) {
      sink.recordReceiverStatus(client.getEndPoint(), response.getStatus());
    }

    if (sink.isClosed()) {
      clearEventsReferenceCount();
      sink.eliminateHandler(this, true);
      return;
    }

    if (onCompleteInternal(response)) {
      // eliminate handler only when all transmissions corresponding to the handler have been
      // completed
      // NOTE: We should not clear the reference count of events, as this would cause the
      // `org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic.IoTDBPipeDataSinkIT#testSinkTsFileFormat3` test to fail.
      sink.eliminateHandler(this, false);
    }
  }

  @Override
  public void onError(final Exception exception) {
    if (client != null) {
      ThriftClient.resolveException(exception, client);
      client.setPrintLogWhenEncounterException(false);
    }

    if (sink.isClosed()) {
      clearEventsReferenceCount();
      sink.eliminateHandler(this, true);
      return;
    }

    onErrorInternal(exception);
    sink.eliminateHandler(this, false);
  }

  /**
   * Attempts to transfer data using the provided client and request.
   *
   * @param client the client used for data transfer
   * @param req the request containing transfer details
   * @return {@code true} if the transfer was initiated successfully, {@code false} if the connector
   *     is closed
   * @throws TException if an error occurs during the transfer
   */
  protected boolean tryTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException {
    if (Objects.isNull(this.client)) {
      this.client = client;
    }
    // track handler before checking if connector is closed
    sink.trackHandler(this);
    if (returnFalseIfSinkIsClosed(client)) {
      return false;
    }
    sink.waitIfReceiverTemporarilyUnavailable(client.getEndPoint());
    if (returnFalseIfSinkIsClosed(client)) {
      return false;
    }
    doTransfer(client, req);
    return true;
  }

  private boolean returnFalseIfSinkIsClosed(final AsyncPipeDataTransferServiceClient client) {
    if (!sink.isClosed()) {
      return false;
    }

    clearEventsReferenceCount();
    sink.eliminateHandler(this, true);
    client.setShouldReturnSelf(true);
    client.returnSelf(
        (e) -> {
          if (e instanceof IllegalStateException) {
            PipeLogger.log(
                ignored ->
                    LOGGER.info(DataNodePipeMessages.ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO),
                DataNodePipeMessages.ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO);
            return true;
          }
          return false;
        });
    this.client = null;
    return true;
  }

  /**
   * @return {@code true} if all transmissions corresponding to the handler have been completed,
   *     {@code false} otherwise
   */
  protected abstract boolean onCompleteInternal(final TPipeTransferResp response);

  protected abstract void onErrorInternal(final Exception exception);

  protected abstract void doTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException;

  protected final void transferWithOptionalRequestSlicing(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException {
    final int bodySizeLimit = PipeTransferSliceReqBuilder.getBodySizeLimit();
    if (!PipeTransferSliceReqBuilder.shouldSlice(req, bodySizeLimit)) {
      client.pipeTransfer(req, this);
      return;
    }

    PipeLogger.log(
        LOGGER::warn,
        "The body size of the request is too large. The request will be sliced. Origin req: %s-%s. Request body size: %s, threshold: %s",
        req.getVersion(),
        req.getType(),
        req.body.limit(),
        bodySizeLimit);

    final int sliceCount = PipeTransferSliceReqBuilder.getSliceCount(req, bodySizeLimit);
    final boolean shouldReturnSelf = client.shouldReturnSelf();
    try {
      transferSlicedRequest(
          client,
          req,
          shouldReturnSelf,
          PipeTransferSliceReqBuilder.nextSliceOrderId(),
          0,
          sliceCount,
          bodySizeLimit);
    } catch (final Exception e) {
      fallbackToWholeRequest(client, req, shouldReturnSelf, e);
    }
  }

  public abstract void clearEventsReferenceCount();

  private void transferSlicedRequest(
      final AsyncPipeDataTransferServiceClient client,
      final TPipeTransferReq originalReq,
      final boolean shouldReturnSelf,
      final int sliceOrderId,
      final int sliceIndex,
      final int sliceCount,
      final int bodySizeLimit)
      throws Exception {
    client.setShouldReturnSelf(shouldReturnSelf && sliceIndex == sliceCount - 1);
    client.pipeTransfer(
        PipeTransferSliceReqBuilder.buildSliceReq(
            originalReq, sliceOrderId, sliceIndex, sliceCount, bodySizeLimit),
        new AsyncMethodCallback<TPipeTransferResp>() {
          @Override
          public void onComplete(final TPipeTransferResp response) {
            if (sink.isClosed() || sliceIndex == sliceCount - 1) {
              PipeTransferTrackableHandler.this.onComplete(response);
              return;
            }

            if (Objects.nonNull(response)) {
              sink.recordReceiverStatus(client.getEndPoint(), response.getStatus());
            }

            if (response == null) {
              fallbackToWholeRequest(
                  client,
                  originalReq,
                  shouldReturnSelf,
                  new PipeException(
                      DataNodePipeMessages.TPIPE_TRANSFER_RESP_IS_NULL_WHEN_TRANSFERRING_SLICE));
              return;
            }

            if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              fallbackToWholeRequest(
                  client,
                  originalReq,
                  shouldReturnSelf,
                  new PipeConnectionException(
                      String.format(
                          "Failed to transfer slice. Origin req: %s-%s, slice index: %d, slice count: %d. Reason: %s",
                          originalReq.getVersion(),
                          originalReq.getType(),
                          sliceIndex,
                          sliceCount,
                          response.getStatus())));
              return;
            }

            try {
              transferSlicedRequest(
                  client,
                  originalReq,
                  shouldReturnSelf,
                  sliceOrderId,
                  sliceIndex + 1,
                  sliceCount,
                  bodySizeLimit);
            } catch (final Exception e) {
              fallbackToWholeRequest(client, originalReq, shouldReturnSelf, e);
            }
          }

          @Override
          public void onError(final Exception exception) {
            if (sink.isClosed() || sliceIndex == sliceCount - 1) {
              PipeTransferTrackableHandler.this.onError(exception);
              return;
            }
            fallbackToWholeRequest(client, originalReq, shouldReturnSelf, exception);
          }
        });
  }

  private void fallbackToWholeRequest(
      final AsyncPipeDataTransferServiceClient client,
      final TPipeTransferReq originalReq,
      final boolean shouldReturnSelf,
      final Exception exception) {
    PipeLogger.log(
        LOGGER::warn,
        exception,
        "Failed to transfer slice. Origin req: %s-%s. Retry the whole transfer.",
        originalReq.getVersion(),
        originalReq.getType());

    try {
      client.setShouldReturnSelf(shouldReturnSelf);
      sink.waitIfReceiverTemporarilyUnavailable(client.getEndPoint());
      if (returnFalseIfSinkIsClosed(client)) {
        return;
      }
      client.pipeTransfer(originalReq, this);
    } catch (final Exception e) {
      PipeTransferTrackableHandler.this.onError(e);
    }
  }

  public void closeClient() {
    if (Objects.isNull(client)) {
      return;
    }
    try {
      client.close();
      client.invalidateAll();
    } catch (final Exception e) {
      LOGGER.warn(
          DataNodePipeMessages.FAILED_TO_CLOSE_OR_INVALIDATE_CLIENT_WHEN,
          client,
          e.getMessage(),
          e);
    }
  }

  @Override
  public void close() {
    // Do nothing
  }
}
