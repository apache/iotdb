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
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkNonReportTimeConfigurableException;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferSliceReqBuilder;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
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

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class PipeTransferTrackableHandler
    implements AsyncMethodCallback<TPipeTransferResp>, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferTrackableHandler.class);

  protected final IoTDBDataRegionAsyncSink sink;
  protected volatile AsyncPipeDataTransferServiceClient client;
  private final AtomicBoolean terminal = new AtomicBoolean(false);
  private final AtomicReference<PipeMemoryBlock> requestMemoryBlock = new AtomicReference<>();
  private final Set<PipeMemoryBlock> sliceMemoryBlocks = ConcurrentHashMap.newKeySet();

  public PipeTransferTrackableHandler(final IoTDBDataRegionAsyncSink sink) {
    this.sink = sink;
  }

  protected final boolean markTerminal() {
    return terminal.compareAndSet(false, true);
  }

  protected final void setRequestMemoryBlock(final PipeMemoryBlock memoryBlock) {
    requestMemoryBlock.set(memoryBlock);
  }

  protected final void releaseRequestMemoryBlock() {
    final PipeMemoryBlock memoryBlock = requestMemoryBlock.getAndSet(null);
    if (memoryBlock != null) {
      memoryBlock.close();
    }
  }

  protected final TPipeTransferReq compressAndAccountRequest(
      final TPipeTransferReq req, final boolean allowWaitingForMemory) throws IOException {
    final PipeMemoryBlock memoryBlock =
        allowWaitingForMemory
            ? PipeAsyncThriftRequestMemoryBlock.allocateForRequest(req, this)
            : PipeAsyncThriftRequestMemoryBlock.tryAllocateForRequest(req, this);
    if (memoryBlock == null) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          DataNodePipeMessages.TEMPORARILY_OUT_OF_MEMORY_IN_PIPE_EVENT_PROCESSING);
    }
    return compressAndAccountRequest(req, allowWaitingForMemory, memoryBlock);
  }

  protected final TPipeTransferReq compressAndAccountRequest(
      final TPipeTransferReq req,
      final boolean allowWaitingForMemory,
      final PipeMemoryBlock memoryBlock)
      throws IOException {
    if (memoryBlock == null) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          DataNodePipeMessages.TEMPORARILY_OUT_OF_MEMORY_IN_PIPE_EVENT_PROCESSING);
    }
    requestMemoryBlock.set(memoryBlock);

    try {
      final TPipeTransferReq compressedReq =
          sink.compressIfNeeded(req, memoryBlock, allowWaitingForMemory);
      if (compressedReq != req) {
        req.body = null;
      }
      final boolean requestMemoryResized =
          allowWaitingForMemory
              ? resizeRequestMemoryBlock(memoryBlock, compressedReq)
              : PipeAsyncThriftRequestMemoryBlock.tryResizeForRequest(memoryBlock, compressedReq);
      if (!requestMemoryResized) {
        throw new PipeRuntimeOutOfMemoryCriticalException(
            DataNodePipeMessages.TEMPORARILY_OUT_OF_MEMORY_IN_PIPE_EVENT_PROCESSING);
      }
      return compressedReq;
    } catch (final IOException | RuntimeException | Error e) {
      requestMemoryBlock.compareAndSet(memoryBlock, null);
      memoryBlock.close();
      throw e;
    }
  }

  private static boolean resizeRequestMemoryBlock(
      final PipeMemoryBlock memoryBlock, final TPipeTransferReq req) {
    PipeAsyncThriftRequestMemoryBlock.resizeForRequest(memoryBlock, req);
    return true;
  }

  private synchronized void accountRequestMemory(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req) {
    try {
      PipeMemoryBlock memoryBlock = requestMemoryBlock.get();
      if (memoryBlock == null) {
        memoryBlock = PipeAsyncThriftRequestMemoryBlock.tryAllocateForRequest(req, this);
        if (memoryBlock == null) {
          throw new PipeRuntimeOutOfMemoryCriticalException(
              DataNodePipeMessages.TEMPORARILY_OUT_OF_MEMORY_IN_PIPE_EVENT_PROCESSING);
        }
        requestMemoryBlock.set(memoryBlock);
      } else if (!PipeAsyncThriftRequestMemoryBlock.tryResizeForRequest(memoryBlock, req)) {
        throw new PipeRuntimeOutOfMemoryCriticalException(
            DataNodePipeMessages.TEMPORARILY_OUT_OF_MEMORY_IN_PIPE_EVENT_PROCESSING);
      }
    } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
      returnClientToPool(client);
      throw e;
    }
  }

  @Override
  public synchronized void onComplete(final TPipeTransferResp response) {
    if (terminal.get()) {
      return;
    }

    final PipeMemoryBlock completedRequestMemoryBlock = requestMemoryBlock.getAndSet(null);

    try {
      if (Objects.nonNull(client) && Objects.nonNull(response)) {
        sink.recordReceiverStatus(client.getEndPoint(), response.getStatus());
      }

      if (sink.isClosed()) {
        if (!terminal.compareAndSet(false, true)) {
          return;
        }
        clearEventsReferenceCount();
        sink.eliminateHandler(this, true);
        return;
      }

      final boolean completed;
      try {
        completed = onCompleteInternal(response);
      } catch (final Exception e) {
        onError(e);
        return;
      }
      if (completed) {
        if (!terminal.compareAndSet(false, true)) {
          return;
        }
        // Do not clear the reference count here: IoTDBPipeDataSinkIT#testSinkTsFileFormat3 relies
        // on the handler keeping it until the events are dropped.
        sink.eliminateHandler(this, false);
      }
    } catch (final Error error) {
      cleanUpAfterSynchronousError(error);
      throw error;
    } finally {
      if (completedRequestMemoryBlock != null) {
        completedRequestMemoryBlock.close();
      }
    }
  }

  @Override
  public synchronized void onError(final Exception exception) {
    if (!terminal.compareAndSet(false, true)) {
      return;
    }

    final PipeMemoryBlock failedRequestMemoryBlock = requestMemoryBlock.getAndSet(null);

    try {
      if (client != null) {
        try {
          ThriftClient.resolveException(exception, client);
        } catch (final Exception resolveException) {
          exception.addSuppressed(resolveException);
          LOGGER.warn(
              DataNodePipeMessages.LOG_FAILED_TO_RESOLVE_TRANSFER_EXCEPTION_A4F5397A,
              resolveException);
        } finally {
          client.setPrintLogWhenEncounterException(false);
        }
      }

      if (sink.isClosed()) {
        clearEventsReferenceCount();
        sink.eliminateHandler(this, true);
        return;
      }

      try {
        onErrorInternal(exception);
      } finally {
        sink.eliminateHandler(this, false);
      }
    } finally {
      if (failedRequestMemoryBlock != null) {
        failedRequestMemoryBlock.close();
      }
    }
  }

  /**
   * Attempts to transfer data using the provided client and request.
   *
   * @param client the client used for data transfer
   * @param req the request containing transfer details
   * @return {@code true} if the transfer was initiated successfully, {@code false} if the connector
   *     is closed or the receiver probe is delayed
   * @throws TException if an error occurs during the transfer
   */
  protected synchronized boolean tryTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException {
    if (terminal.get()) {
      return false;
    }
    if (Objects.isNull(this.client)) {
      this.client = client;
    }
    // track handler before checking if connector is closed
    sink.trackHandler(this);
    if (handleSinkClosed(client)) {
      return false;
    }
    try {
      sink.waitIfReceiverRetryIsBackedOff(client.getEndPoint());
    } catch (final PipeRuntimeSinkNonReportTimeConfigurableException e) {
      returnClientToPool(client);
      onError(e);
      return false;
    }
    if (handleSinkClosed(client)) {
      return false;
    }
    accountRequestMemory(client, req);
    try {
      doTransfer(client, req);
    } catch (final TException | RuntimeException e) {
      try {
        onError(e);
      } finally {
        returnClientToPool(client);
      }
      throw e;
    }
    return true;
  }

  private synchronized boolean handleSinkClosed(final AsyncPipeDataTransferServiceClient client) {
    if (!sink.isClosed()) {
      return false;
    }

    if (!terminal.compareAndSet(false, true)) {
      return true;
    }
    clearEventsReferenceCount();
    releaseRequestMemoryBlock();
    sliceMemoryBlocks.forEach(this::releaseSliceMemoryBlock);
    sink.eliminateHandler(this, true);
    if (client != null) {
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
    }
    this.client = null;
    return true;
  }

  protected final void returnClientToPool(final AsyncPipeDataTransferServiceClient client) {
    synchronized (this) {
      if (this.client != client) {
        return;
      }
      this.client = null;
    }
    client.setShouldReturnSelf(true);
    client.returnSelf(
        e -> {
          if (e instanceof IllegalStateException) {
            PipeLogger.log(
                ignored ->
                    LOGGER.info(DataNodePipeMessages.ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO),
                DataNodePipeMessages.ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO);
            return true;
          }
          return false;
        });
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

  public synchronized void onSynchronousError() {
    if (!markTerminal()) {
      return;
    }

    try {
      onSynchronousErrorInternal();
    } finally {
      try {
        releaseRequestMemoryBlock();
      } finally {
        try {
          sliceMemoryBlocks.forEach(this::releaseSliceMemoryBlock);
        } finally {
          sink.eliminateHandler(this, true);
        }
      }
    }
  }

  private void cleanUpAfterSynchronousError(final Error error) {
    try {
      onSynchronousError();
    } catch (final RuntimeException | Error cleanupError) {
      if (cleanupError != error) {
        error.addSuppressed(cleanupError);
      }
    }
  }

  protected void onSynchronousErrorInternal() {
    clearEventsReferenceCount();
  }

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
        DataNodePipeMessages.TRANSFER_REQUEST_BODY_TOO_LARGE_WILL_BE_SLICED,
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

  private synchronized void transferSlicedRequest(
      final AsyncPipeDataTransferServiceClient client,
      final TPipeTransferReq originalReq,
      final boolean shouldReturnSelf,
      final int sliceOrderId,
      final int sliceIndex,
      final int sliceCount,
      final int bodySizeLimit)
      throws Exception {
    if (terminal.get()) {
      return;
    }

    final int sliceStartIndex = sliceIndex * bodySizeLimit;
    final int sliceEndIndex = Math.min((sliceIndex + 1) * bodySizeLimit, originalReq.body.limit());
    final int sliceBodyLength = sliceEndIndex - sliceStartIndex;
    final PipeMemoryBlock sliceMemoryBlock =
        PipeAsyncThriftRequestMemoryBlock.tryAllocateForSlice(sliceBodyLength, this);
    if (sliceMemoryBlock == null) {
      fallbackToWholeRequest(
          client,
          originalReq,
          shouldReturnSelf,
          new PipeRuntimeOutOfMemoryCriticalException(
              DataNodePipeMessages.TEMPORARILY_OUT_OF_MEMORY_IN_PIPE_EVENT_PROCESSING));
      return;
    }
    boolean sliceMemoryBlockTracked = false;
    boolean callbackRegistered = false;
    try {
      sliceMemoryBlocks.add(sliceMemoryBlock);
      sliceMemoryBlockTracked = true;
      final TPipeTransferReq sliceReq =
          PipeTransferSliceReqBuilder.buildSliceReq(
              originalReq, sliceOrderId, sliceIndex, sliceCount, bodySizeLimit);
      client.setShouldReturnSelf(shouldReturnSelf && sliceIndex == sliceCount - 1);
      client.pipeTransfer(
          sliceReq,
          new AsyncMethodCallback<TPipeTransferResp>() {
            @Override
            public void onComplete(final TPipeTransferResp response) {
              try {
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
                          DataNodePipeMessages
                              .TPIPE_TRANSFER_RESP_IS_NULL_WHEN_TRANSFERRING_SLICE));
                  return;
                }

                if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  fallbackToWholeRequest(
                      client,
                      originalReq,
                      shouldReturnSelf,
                      new PipeConnectionException(
                          String.format(
                              DataNodePipeMessages
                                  .PIPE_EXCEPTION_FAILED_TO_TRANSFER_SLICE_ORIGIN_REQ_S_S_SLICE_INDEX_D_SLICE_44E1CF32,
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
              } catch (final Error error) {
                cleanUpAfterSynchronousError(error);
                throw error;
              } finally {
                releaseSliceMemoryBlock(sliceMemoryBlock);
              }
            }

            @Override
            public void onError(final Exception exception) {
              try {
                if (sink.isClosed() || sliceIndex == sliceCount - 1) {
                  PipeTransferTrackableHandler.this.onError(exception);
                  return;
                }
                fallbackToWholeRequest(client, originalReq, shouldReturnSelf, exception);
              } catch (final Error error) {
                cleanUpAfterSynchronousError(error);
                throw error;
              } finally {
                releaseSliceMemoryBlock(sliceMemoryBlock);
              }
            }
          });
      callbackRegistered = true;
    } finally {
      if (!callbackRegistered) {
        if (sliceMemoryBlockTracked) {
          releaseSliceMemoryBlock(sliceMemoryBlock);
        } else {
          sliceMemoryBlock.close();
        }
      }
    }
  }

  private void releaseSliceMemoryBlock(final PipeMemoryBlock memoryBlock) {
    if (sliceMemoryBlocks.remove(memoryBlock)) {
      memoryBlock.close();
    }
  }

  private void fallbackToWholeRequest(
      final AsyncPipeDataTransferServiceClient client,
      final TPipeTransferReq originalReq,
      final boolean shouldReturnSelf,
      final Exception exception) {
    PipeLogger.log(
        LOGGER::warn,
        exception,
        DataNodePipeMessages.FAILED_TO_TRANSFER_SLICE_RETRY_WHOLE_TRANSFER,
        originalReq.getVersion(),
        originalReq.getType());

    try {
      client.setShouldReturnSelf(shouldReturnSelf);
      sink.waitIfReceiverRetryIsBackedOff(client.getEndPoint());
      transferWholeRequest(client, originalReq);
    } catch (final PipeRuntimeSinkNonReportTimeConfigurableException e) {
      try {
        PipeTransferTrackableHandler.this.onError(e);
      } finally {
        returnClientToPool(client);
      }
    } catch (final Exception e) {
      try {
        PipeTransferTrackableHandler.this.onError(e);
      } finally {
        returnClientToPool(client);
      }
    }
  }

  private synchronized void transferWholeRequest(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq request)
      throws TException {
    if (terminal.get() || handleSinkClosed(client)) {
      return;
    }
    accountRequestMemory(client, request);
    client.pipeTransfer(request, this);
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
  public synchronized void close() {
    terminal.set(true);
    try {
      releaseRequestMemoryBlock();
    } finally {
      sliceMemoryBlocks.forEach(this::releaseSliceMemoryBlock);
    }
  }
}
