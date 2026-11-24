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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.common;

import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;

public class PipeTransferSliceReqHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferSliceReqHandler.class);
  private static final int MAX_RETIRED_UDP_ORDER_IDS = 64;

  private int orderId = -1;

  private short originReqType = -1;
  private int originBodySize = -1;

  private int sliceCount = -1;
  private final List<byte[]> sliceBodies = new ArrayList<>();
  private final Map<Integer, byte[]> outOfOrderSliceBodies = new HashMap<>();
  private boolean initialized;
  private boolean outOfOrderMode;
  private int receivedSliceCount;
  private long receivedBodySize = 0;
  private long lastUdpSliceTimeNanos;
  private final Map<Long, PipeTransferSliceReqHandler> pendingUdpRequests =
      new LinkedHashMap<>(16, 0.75f, true);
  private final Map<Long, SliceReceiveResult> retiredUdpResults = new LinkedHashMap<>();

  public boolean receiveSlice(final PipeTransferSliceReq req) {
    return receiveSlice(req, false, () -> {}) == SliceReceiveResult.ACCEPTED;
  }

  public SliceReceiveResult receiveSliceWithReordering(final PipeTransferSliceReq req) {
    return receiveSliceWithReordering(req, () -> {}, orderId -> {});
  }

  public SliceReceiveResult receiveSliceWithReordering(
      final PipeTransferSliceReq req,
      final Runnable reserveSliceMemory,
      final LongConsumer onEviction) {
    final long udpOrderId = req.getUdpOrderId();
    if (!isValidSliceReq(req, true)) {
      discardUdpRequest(udpOrderId);
      return SliceReceiveResult.INVALID;
    }
    final SliceReceiveResult retiredResult = retiredUdpResults.get(udpOrderId);
    if (retiredResult != null) {
      return retiredResult;
    }

    PipeTransferSliceReqHandler requestHandler = pendingUdpRequests.get(udpOrderId);
    final boolean isNewRequest = requestHandler == null;
    if (isNewRequest) {
      requestHandler = new PipeTransferSliceReqHandler();
    }

    requestHandler.lastUdpSliceTimeNanos = System.nanoTime();
    final SliceReceiveResult result =
        requestHandler.receiveSlice(
            req,
            true,
            () -> {
              if (isNewRequest
                  && pendingUdpRequests.size()
                      >= PipeConfig.getInstance()
                          .getPipeAirGapReceiverUdpMaxPendingRequestCount()) {
                final Iterator<Map.Entry<Long, PipeTransferSliceReqHandler>> iterator =
                    pendingUdpRequests.entrySet().iterator();
                evictUdpRequest(iterator, iterator.next(), onEviction);
              }
              reserveSliceMemory.run();
            });
    if (result == SliceReceiveResult.INVALID) {
      discardUdpRequest(udpOrderId);
      return result;
    }
    if (isNewRequest) {
      pendingUdpRequests.put(udpOrderId, requestHandler);
      return SliceReceiveResult.NEW_ORDER;
    }
    return result;
  }

  private SliceReceiveResult receiveSlice(
      final PipeTransferSliceReq req,
      final boolean allowOutOfOrder,
      final Runnable reserveSliceMemory) {
    if (!isValidSliceReq(req, allowOutOfOrder)) {
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    }

    if (!initialized) {
      initialize(req, allowOutOfOrder);
    } else if (orderId != req.getOrderId()) {
      LOGGER.warn(PipeMessages.ORDER_ID_MISMATCH, orderId, req.getOrderId());
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    } else if (outOfOrderMode != allowOutOfOrder) {
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    }

    if (originReqType != req.getOriginReqType()) {
      LOGGER.warn(
          PipeMessages.LOG_ORIGIN_REQUEST_TYPE_MISMATCH_EXPECTED_ARG_ACTUAL_ARG_D96D10AE,
          originReqType,
          req.getOriginReqType());
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    }
    if (originBodySize != req.getOriginBodySize()) {
      LOGGER.warn(
          PipeMessages.LOG_ORIGIN_BODY_SIZE_MISMATCH_EXPECTED_ARG_ACTUAL_ARG_5D410B75,
          originBodySize,
          req.getOriginBodySize());
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    }
    if (sliceCount != req.getSliceCount()) {
      LOGGER.warn(PipeMessages.SLICE_COUNT_MISMATCH, sliceCount, req.getSliceCount());
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    }

    if (allowOutOfOrder) {
      final byte[] existingSliceBody = outOfOrderSliceBodies.get(req.getSliceIndex());
      if (existingSliceBody != null) {
        if (Arrays.equals(existingSliceBody, req.getSliceBody())) {
          return SliceReceiveResult.DUPLICATE;
        }
        clearCurrentRequest();
        return SliceReceiveResult.INVALID;
      }
    } else if (sliceBodies.size() != req.getSliceIndex()) {
      LOGGER.warn(
          PipeMessages.LOG_INVALID_SLICE_INDEX_EXPECTED_ARG_ACTUAL_ARG_2AC41628,
          sliceBodies.size(),
          req.getSliceIndex());
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    }

    final long newBodySize = receivedBodySize + req.getSliceBody().length;
    if (newBodySize > originBodySize) {
      LOGGER.warn(
          PipeMessages.LOG_RECEIVED_SLICE_BODY_SIZE_ARG_EXCEEDS_ORIGIN_BODY_SIZE_ARG_12E71952,
          newBodySize,
          originBodySize);
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    }

    if (receivedSliceCount + 1 == sliceCount && newBodySize != originBodySize) {
      LOGGER.warn(
          PipeMessages
              .LOG_RECEIVED_SLICE_BODY_SIZE_ARG_IS_NOT_EQUAL_TO_ORIGIN_BODY_SIZE_ARG_EBE1BB1E,
          newBodySize,
          originBodySize);
      clearCurrentRequest();
      return SliceReceiveResult.INVALID;
    }

    if (allowOutOfOrder) {
      reserveSliceMemory.run();
      outOfOrderSliceBodies.put(req.getSliceIndex(), req.getSliceBody());
    } else {
      sliceBodies.add(req.getSliceBody());
    }
    receivedSliceCount++;
    receivedBodySize = newBodySize;
    return SliceReceiveResult.ACCEPTED;
  }

  private void initialize(final PipeTransferSliceReq req, final boolean allowOutOfOrder) {
    orderId = req.getOrderId();
    originReqType = req.getOriginReqType();
    originBodySize = req.getOriginBodySize();
    sliceCount = req.getSliceCount();
    initialized = true;
    outOfOrderMode = allowOutOfOrder;
    receivedSliceCount = 0;
    receivedBodySize = 0;
  }

  private boolean isValidSliceReq(final PipeTransferSliceReq req, final boolean allowOutOfOrder) {
    return req.getOriginBodySize() >= 0
        && req.getSliceCount() > 0
        && (!allowOutOfOrder || req.getSliceCount() <= Math.max(1, req.getOriginBodySize()))
        && req.getSliceIndex() >= 0
        && req.getSliceIndex() < req.getSliceCount()
        && req.getSliceBody() != null
        && req.getSliceBody().length <= req.getOriginBodySize();
  }

  public boolean isComplete() {
    return initialized && receivedSliceCount == sliceCount && receivedBodySize == originBodySize;
  }

  public boolean isComplete(final long udpOrderId) {
    final PipeTransferSliceReqHandler requestHandler = pendingUdpRequests.get(udpOrderId);
    return requestHandler != null && requestHandler.isComplete();
  }

  public Optional<TPipeTransferReq> makeReqIfComplete() {
    if (!isComplete()) {
      return Optional.empty();
    }

    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = originReqType;

    final ByteBuffer body = ByteBuffer.allocate(originBodySize);
    if (outOfOrderMode) {
      for (int sliceIndex = 0; sliceIndex < sliceCount; sliceIndex++) {
        final byte[] sliceBody = outOfOrderSliceBodies.get(sliceIndex);
        if (sliceBody == null) {
          return Optional.empty();
        }
        body.put(sliceBody);
      }
    } else {
      sliceBodies.forEach(body::put);
    }
    body.flip();
    req.body = body;

    return Optional.of(req);
  }

  public Optional<TPipeTransferReq> makeReqIfComplete(final long udpOrderId) {
    final PipeTransferSliceReqHandler requestHandler = pendingUdpRequests.get(udpOrderId);
    return requestHandler == null ? Optional.empty() : requestHandler.makeReqIfComplete();
  }

  public void clear() {
    clearCurrentRequest();
    pendingUdpRequests.values().forEach(PipeTransferSliceReqHandler::clearCurrentRequest);
    pendingUdpRequests.clear();
    retiredUdpResults.clear();
  }

  public void completeUdpRequest(final long udpOrderId, final boolean succeeded) {
    discardUdpRequest(udpOrderId);
    rememberRetiredUdpResult(
        udpOrderId, succeeded ? SliceReceiveResult.STALE : SliceReceiveResult.DROPPED);
  }

  private void discardUdpRequest(final long udpOrderId) {
    final PipeTransferSliceReqHandler requestHandler = pendingUdpRequests.remove(udpOrderId);
    if (requestHandler != null) {
      requestHandler.clearCurrentRequest();
    }
  }

  public void expireUdpRequests(
      final long nowNanos, final long idleTimeoutNanos, final LongConsumer onEviction) {
    final Iterator<Map.Entry<Long, PipeTransferSliceReqHandler>> iterator =
        pendingUdpRequests.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<Long, PipeTransferSliceReqHandler> entry = iterator.next();
      if (nowNanos - entry.getValue().lastUdpSliceTimeNanos >= idleTimeoutNanos) {
        evictUdpRequest(iterator, entry, onEviction);
      }
    }
  }

  private void evictUdpRequest(
      final Iterator<Map.Entry<Long, PipeTransferSliceReqHandler>> iterator,
      final Map.Entry<Long, PipeTransferSliceReqHandler> entry,
      final LongConsumer onEviction) {
    final long udpOrderId = entry.getKey();
    entry.getValue().clearCurrentRequest();
    iterator.remove();
    rememberRetiredUdpResult(udpOrderId, SliceReceiveResult.DROPPED);
    onEviction.accept(udpOrderId);
  }

  private void rememberRetiredUdpResult(final long udpOrderId, final SliceReceiveResult result) {
    retiredUdpResults.put(udpOrderId, result);
    if (retiredUdpResults.size() > MAX_RETIRED_UDP_ORDER_IDS) {
      final Iterator<Long> iterator = retiredUdpResults.keySet().iterator();
      iterator.next();
      iterator.remove();
    }
  }

  public void clearCurrentRequest() {
    orderId = -1;
    originReqType = -1;
    originBodySize = -1;
    sliceCount = -1;
    initialized = false;
    outOfOrderMode = false;
    sliceBodies.clear();
    outOfOrderSliceBodies.clear();
    receivedSliceCount = 0;
    receivedBodySize = 0;
  }

  public enum SliceReceiveResult {
    ACCEPTED,
    DUPLICATE,
    NEW_ORDER,
    STALE,
    DROPPED,
    INVALID
  }
}
