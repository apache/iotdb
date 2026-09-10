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
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletObjectEventBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletObjectBatchResp;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Transfers one emitted Object batch that may contain both InsertNode and RawTablet events. */
public class PipeTransferTabletObjectBatchHandler extends PipeTransferTrackableHandler {

  // The batch builder increases every event's reference count with this holder before emitting it.
  // Keep the same holder here so the terminal paths release that hold.
  private static final String REFERENCE_HOLDER = PipeTransferBatchReqBuilder.class.getName();
  private final List<EnrichedEvent> events;
  private final PipeTabletObjectEventBatch.EmittedBatch batch;
  private final PipeTabletObjectBatchRequestIterator requestIterator;
  private final long sendBufferSizeInBytes;
  private final long handlerStartTimeInNanos = System.nanoTime();
  private final AtomicBoolean isHandlerMetricRecorded = new AtomicBoolean(false);
  private TPipeTransferReq currentRequest;
  private Map<Pair<String, Long>, Long> currentRequestPipeIdentity2Bytes;
  private PipeMemoryBlock sendBufferMemoryBlock;
  private long requestPreparationStartTimeInNanos;
  private long requestSendTimeInNanos;
  private long callbackCount;

  public PipeTransferTabletObjectBatchHandler(
      final PipeTabletObjectEventBatch.EmittedBatch batch, final IoTDBDataRegionAsyncSink sink) {
    super(sink);
    this.batch = batch;
    events = batch.getEvents();
    requestIterator = new PipeTabletObjectBatchRequestIterator(batch);
    sendBufferSizeInBytes = batch.getMaxRequestSizeInBytes();
    currentRequest = nextCompressedRequest();
  }

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
    allocateSendBufferMemoryBlock();
    final long totalRequestEventBytes =
        currentRequestPipeIdentity2Bytes.values().stream().mapToLong(Long::longValue).sum();
    if (totalRequestEventBytes > 0) {
      final double compressionRatio =
          (double) currentRequest.getBody().length / totalRequestEventBytes;
      for (final Map.Entry<Pair<String, Long>, Long> entry :
          currentRequestPipeIdentity2Bytes.entrySet()) {
        sink.rateLimitIfNeeded(
            entry.getKey().getLeft(),
            entry.getKey().getRight(),
            client.getEndPoint(),
            (long) (entry.getValue() * compressionRatio));
      }
    }
    sink.recordObjectTabletRequestPrepareTime(
        true, System.nanoTime() - requestPreparationStartTimeInNanos);
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
    sink.recordObjectTabletRequestTime(true, System.nanoTime() - requestSendTimeInNanos);
    if (response == null) {
      onError(new PipeException(DataNodePipeMessages.TPIPETRANSFERRESP_IS_NULL));
      return false;
    }
    try {
      final TSStatus status = response.getStatus();
      if (status.getCode()
          == TSStatusCode.PIPE_TRANSFER_TABLET_OBJECT_BATCH_SEQUENCE_RESET.getStatusCode()) {
        requestIterator.resetToSequence(
            PipeTransferTabletObjectBatchResp.fromTPipeTransferResp(response)
                .getExpectedSequenceId());
        currentRequest = nextCompressedRequest();
        sendCurrentRequest(client);
        return false;
      }
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        sink.statusHandler().handle(status, status.getMessage(), events.toString());
      }
      if (requestIterator.hasNext()) {
        currentRequest = nextCompressedRequest();
        sendCurrentRequest(client);
        return false;
      }
      events.forEach(event -> event.decreaseReferenceCount(REFERENCE_HOLDER, true));
      batch.onSuccess();
      releaseSendBufferMemoryBlock();
      requestIterator.close();
      returnClientToPool(client);
      recordHandlerMetric();
      return true;
    } catch (final Exception e) {
      onError(e);
      return false;
    }
  }

  @Override
  protected void onErrorInternal(final Exception exception) {
    releaseSendBufferMemoryBlock();
    sink.addFailureEventsToRetryQueue(events, exception, this);
    batch.onFailure();
    requestIterator.close();
    returnClientToPool(client);
    recordHandlerMetric();
  }

  @Override
  public void clearEventsReferenceCount() {
    releaseSendBufferMemoryBlock();
    events.forEach(event -> event.clearReferenceCount(REFERENCE_HOLDER));
    batch.onFailure();
    requestIterator.close();
    returnClientToPool(client);
    recordHandlerMetric();
  }

  private void allocateSendBufferMemoryBlock() {
    final long requiredSize = Math.max(sendBufferSizeInBytes, currentRequest.getBody().length);
    if (sendBufferMemoryBlock == null) {
      sendBufferMemoryBlock = PipeDataNodeResourceManager.memory().forceAllocate(requiredSize);
    } else {
      PipeDataNodeResourceManager.memory().forceResize(sendBufferMemoryBlock, requiredSize);
    }
  }

  private void releaseSendBufferMemoryBlock() {
    if (sendBufferMemoryBlock != null) {
      sendBufferMemoryBlock.close();
      sendBufferMemoryBlock = null;
    }
  }

  private TPipeTransferReq nextCompressedRequest() {
    try {
      requestPreparationStartTimeInNanos = System.nanoTime();
      final TPipeTransferReq uncompressedRequest = requestIterator.next();
      currentRequestPipeIdentity2Bytes = requestIterator.getCurrentRequestPipeIdentity2Bytes();
      return sink.compressIfNeeded(uncompressedRequest);
    } catch (final Exception e) {
      requestIterator.close();
      throw new IllegalStateException(e);
    }
  }

  private void recordHandlerMetric() {
    if (isHandlerMetricRecorded.compareAndSet(false, true)) {
      sink.recordObjectTabletHandler(
          true, System.nanoTime() - handlerStartTimeInNanos, callbackCount);
    }
  }
}
