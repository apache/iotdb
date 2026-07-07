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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.util.PipeDataLossDebugUtil;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventPlainBatch;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.pipe.sink.util.cacher.LeaderCacheUtils;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class PipeTransferTabletBatchEventHandler extends PipeTransferTrackableHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTransferTabletBatchEventHandler.class);

  private static final AtomicLong DEBUG_BATCH_ID_GENERATOR = new AtomicLong(0);

  private final long debugBatchId;
  private final List<EnrichedEvent> events;
  private final Map<Pair<String, Long>, Long> pipeName2BytesAccumulated;

  private final TPipeTransferReq req;
  private final String uncompressedReqDebugInfo;
  private final String compressedReqDebugInfo;
  private final double reqCompressionRatio;

  public PipeTransferTabletBatchEventHandler(
      final PipeTabletEventPlainBatch batch, final IoTDBDataRegionAsyncSink connector)
      throws IOException {
    super(connector);

    debugBatchId = DEBUG_BATCH_ID_GENERATOR.incrementAndGet();

    // Deep copy to keep events' reference
    events = batch.deepCopyEvents();
    pipeName2BytesAccumulated = batch.deepCopyPipeName2BytesAccumulated();

    final TPipeTransferReq uncompressedReq = batch.toTPipeTransferReq();
    uncompressedReqDebugInfo = PipeDataLossDebugUtil.formatReq(uncompressedReq);
    req = connector.compressIfNeeded(uncompressedReq);
    compressedReqDebugInfo = PipeDataLossDebugUtil.formatReq(req);
    reqCompressionRatio = (double) req.getBody().length / uncompressedReq.getBody().length;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "{} sender built tablet batch request, debugBatchId={}, uncompressedReq={}, transferredReq={}, compressionRatio={}, events={}, bytesByPipe={}",
          PipeDataLossDebugUtil.PREFIX,
          debugBatchId,
          uncompressedReqDebugInfo,
          compressedReqDebugInfo,
          reqCompressionRatio,
          PipeDataLossDebugUtil.formatEvents(events),
          pipeName2BytesAccumulated);
    }
  }

  public void transfer(final AsyncPipeDataTransferServiceClient client) throws TException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "{} sender transferring tablet batch, debugBatchId={}, endpoint={}, transferredReq={}, events={}, bytesByPipe={}",
          PipeDataLossDebugUtil.PREFIX,
          debugBatchId,
          client.getEndPoint(),
          compressedReqDebugInfo,
          PipeDataLossDebugUtil.formatEvents(events),
          pipeName2BytesAccumulated);
    }

    for (final Map.Entry<Pair<String, Long>, Long> entry : pipeName2BytesAccumulated.entrySet()) {
      sink.rateLimitIfNeeded(
          entry.getKey().getLeft(),
          entry.getKey().getRight(),
          client.getEndPoint(),
          (long) (entry.getValue() * reqCompressionRatio));
    }

    tryTransfer(client, req);
  }

  @Override
  protected boolean onCompleteInternal(final TPipeTransferResp response) {
    // Just in case
    if (response == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{} sender tablet batch callback got null response, debugBatchId={}, endpoint={}, events={}",
            PipeDataLossDebugUtil.PREFIX,
            debugBatchId,
            client == null ? null : client.getEndPoint(),
            PipeDataLossDebugUtil.formatEvents(events));
      }
      onError(new PipeException(DataNodePipeMessages.TPIPETRANSFERRESP_IS_NULL));
      return false;
    }

    try {
      final TSStatus status = response.getStatus();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{} sender tablet batch callback complete, debugBatchId={}, endpoint={}, status={}, transferredReq={}, events={}",
            PipeDataLossDebugUtil.PREFIX,
            debugBatchId,
            client == null ? null : client.getEndPoint(),
            PipeDataLossDebugUtil.formatStatus(status),
            compressedReqDebugInfo,
            PipeDataLossDebugUtil.formatEvents(events));
      }
      // Only handle the failed statuses to avoid string format performance overhead
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        sink.statusHandler().handle(status, response.getStatus().getMessage(), events.toString());
      }
      for (final Pair<String, TEndPoint> redirectPair :
          LeaderCacheUtils.parseRecommendedRedirections(status)) {
        sink.updateLeaderCache(redirectPair.getLeft(), redirectPair.getRight());
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{} sender tablet batch decreasing event references after callback, debugBatchId={}, shouldReport=true, events={}",
            PipeDataLossDebugUtil.PREFIX,
            debugBatchId,
            PipeDataLossDebugUtil.formatEvents(events));
      }
      events.forEach(
          event ->
              event.decreaseReferenceCount(
                  PipeTransferTabletBatchEventHandler.class.getName(), true));
    } catch (final Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{} sender tablet batch callback handling failed, debugBatchId={}, endpoint={}, exception={}, events={}",
            PipeDataLossDebugUtil.PREFIX,
            debugBatchId,
            client == null ? null : client.getEndPoint(),
            PipeDataLossDebugUtil.formatException(e),
            PipeDataLossDebugUtil.formatEvents(events));
      }
      onError(e);
      return false;
    }

    return true;
  }

  @Override
  protected void onErrorInternal(final Exception exception) {
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{} sender tablet batch transfer failed, debugBatchId={}, endpoint={}, exception={}, events={}",
            PipeDataLossDebugUtil.PREFIX,
            debugBatchId,
            client == null ? null : client.getEndPoint(),
            PipeDataLossDebugUtil.formatException(exception),
            PipeDataLossDebugUtil.formatEvents(events));
      }
      PipeLogger.log(
          LOGGER::warn,
          exception,
          "Failed to transfer TabletInsertionEvent batch. Total failed events: %s, related pipe names: %s",
          events.size(),
          events.stream().map(EnrichedEvent::getPipeName).collect(Collectors.toSet()));
    } finally {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{} sender tablet batch adding events to retry queue, debugBatchId={}, exception={}, events={}",
            PipeDataLossDebugUtil.PREFIX,
            debugBatchId,
            PipeDataLossDebugUtil.formatException(exception),
            PipeDataLossDebugUtil.formatEvents(events));
      }
      sink.addFailureEventsToRetryQueue(events, exception);
    }
  }

  @Override
  protected void doTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException {
    transferWithOptionalRequestSlicing(client, req);
  }

  @Override
  public void clearEventsReferenceCount() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "{} sender tablet batch clearing event references, debugBatchId={}, events={}",
          PipeDataLossDebugUtil.PREFIX,
          debugBatchId,
          PipeDataLossDebugUtil.formatEvents(events));
    }
    events.forEach(
        event -> event.clearReferenceCount(PipeTransferTabletBatchEventHandler.class.getName()));
  }

  @Override
  protected String getDebugHandlerInfo() {
    return "debugBatchId="
        + debugBatchId
        + ", transferredReq={"
        + compressedReqDebugInfo
        + "}, events={"
        + PipeDataLossDebugUtil.formatEvents(events)
        + "}";
  }
}
