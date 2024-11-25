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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventPlainBatch;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.db.pipe.connector.util.LeaderCacheUtils;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipeTransferTabletBatchEventHandler implements AsyncMethodCallback<TPipeTransferResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTransferTabletBatchEventHandler.class);

  private final List<EnrichedEvent> events;
  private final Map<Pair<String, Long>, Long> pipeName2BytesAccumulated;

  private final TPipeTransferReq req;
  private final double reqCompressionRatio;

  private final IoTDBDataRegionAsyncConnector connector;

  public PipeTransferTabletBatchEventHandler(
      final PipeTabletEventPlainBatch batch, final IoTDBDataRegionAsyncConnector connector)
      throws IOException {
    // Deep copy to keep events' reference
    events = batch.deepCopyEvents();
    pipeName2BytesAccumulated = batch.deepCopyPipeName2BytesAccumulated();

    final TPipeTransferReq uncompressedReq = batch.toTPipeTransferReq();
    req =
        connector.isRpcCompressionEnabled()
            ? PipeTransferCompressedReq.toTPipeTransferReq(
                uncompressedReq, connector.getCompressors())
            : uncompressedReq;
    reqCompressionRatio = (double) req.getBody().length / uncompressedReq.getBody().length;

    this.connector = connector;
  }

  public void transfer(final AsyncPipeDataTransferServiceClient client) throws TException {
    for (final Map.Entry<Pair<String, Long>, Long> entry : pipeName2BytesAccumulated.entrySet()) {
      connector.rateLimitIfNeeded(
          entry.getKey().getLeft(),
          entry.getKey().getRight(),
          client.getEndPoint(),
          (long) (entry.getValue() * reqCompressionRatio));
    }

    client.pipeTransfer(req, this);
  }

  @Override
  public void onComplete(final TPipeTransferResp response) {
    // Just in case
    if (response == null) {
      onError(new PipeException("TPipeTransferResp is null"));
      return;
    }

    try {
      final TSStatus status = response.getStatus();
      // Only handle the failed statuses to avoid string format performance overhead
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        connector
            .statusHandler()
            .handle(status, response.getStatus().getMessage(), events.toString());
      }
      for (final Pair<String, TEndPoint> redirectPair :
          LeaderCacheUtils.parseRecommendedRedirections(status)) {
        connector.updateLeaderCache(redirectPair.getLeft(), redirectPair.getRight());
      }

      events.forEach(
          event ->
              event.decreaseReferenceCount(
                  PipeTransferTabletBatchEventHandler.class.getName(), true));
    } catch (final Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(final Exception exception) {
    try {
      LOGGER.warn(
          "Failed to transfer TabletInsertionEvent batch. Total failed events: {}, related pipe names: {}",
          events.size(),
          events.stream().map(EnrichedEvent::getPipeName).collect(Collectors.toSet()),
          exception);
    } finally {
      connector.addFailureEventsToRetryQueue(events);
    }
  }
}
