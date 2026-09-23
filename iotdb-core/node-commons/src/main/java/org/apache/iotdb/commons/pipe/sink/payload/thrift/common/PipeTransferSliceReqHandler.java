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
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PipeTransferSliceReqHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferSliceReqHandler.class);

  private int orderId = -1;

  private short originReqType = -1;
  private int originBodySize = -1;

  private int sliceCount = -1;
  private final List<byte[]> sliceBodies = new ArrayList<>();
  private long receivedBodySize = 0;

  public boolean receiveSlice(final PipeTransferSliceReq req) {
    if (!isValidSliceReq(req)) {
      clear();
      return false;
    }

    if (orderId == -1
        || originReqType == -1
        || originBodySize == -1
        || sliceCount == -1
        || sliceBodies.isEmpty()) {
      if (orderId == -1
          && originReqType == -1
          && originBodySize == -1
          && sliceCount == -1
          && sliceBodies.isEmpty()) {
        orderId = req.getOrderId();
        originReqType = req.getOriginReqType();
        originBodySize = req.getOriginBodySize();
        sliceCount = req.getSliceCount();
        receivedBodySize = 0;
      } else {
        LOGGER.warn(
            PipeMessages.INVALID_STATE_SLICE,
            orderId,
            originReqType,
            originBodySize,
            sliceCount,
            sliceBodies.size());
        clear();
        return false;
      }
    }

    if (orderId != req.getOrderId()) {
      LOGGER.warn(PipeMessages.ORDER_ID_MISMATCH, orderId, req.getOrderId());
      clear();
      return false;
    }
    if (originReqType != req.getOriginReqType()) {
      LOGGER.warn(
          PipeMessages.LOG_ORIGIN_REQUEST_TYPE_MISMATCH_EXPECTED_ARG_ACTUAL_ARG_D96D10AE,
          originReqType,
          req.getOriginReqType());
      clear();
      return false;
    }
    if (originBodySize != req.getOriginBodySize()) {
      LOGGER.warn(
          PipeMessages.LOG_ORIGIN_BODY_SIZE_MISMATCH_EXPECTED_ARG_ACTUAL_ARG_5D410B75,
          originBodySize,
          req.getOriginBodySize());
      clear();
      return false;
    }
    if (sliceCount != req.getSliceCount()) {
      LOGGER.warn(PipeMessages.SLICE_COUNT_MISMATCH, sliceCount, req.getSliceCount());
      clear();
      return false;
    }
    if (sliceBodies.size() != req.getSliceIndex()) {
      LOGGER.warn(
          PipeMessages.LOG_INVALID_SLICE_INDEX_EXPECTED_ARG_ACTUAL_ARG_2AC41628,
          sliceBodies.size(),
          req.getSliceIndex());
      clear();
      return false;
    }

    if (receivedBodySize + req.getSliceBody().length > originBodySize) {
      LOGGER.warn(
          "Received slice body size {} exceeds origin body size {}",
          receivedBodySize + req.getSliceBody().length,
          originBodySize);
      clear();
      return false;
    }

    sliceBodies.add(req.getSliceBody());
    receivedBodySize += req.getSliceBody().length;

    if (sliceBodies.size() == sliceCount && receivedBodySize != originBodySize) {
      LOGGER.warn(
          "Received slice body size {} is not equal to origin body size {}",
          receivedBodySize,
          originBodySize);
      clear();
      return false;
    }

    return true;
  }

  private boolean isValidSliceReq(final PipeTransferSliceReq req) {
    return req.getOriginBodySize() >= 0
        && req.getSliceCount() > 0
        && req.getSliceIndex() >= 0
        && req.getSliceIndex() < req.getSliceCount()
        && req.getSliceBody() != null
        && req.getSliceBody().length <= req.getOriginBodySize();
  }

  public Optional<TPipeTransferReq> makeReqIfComplete() {
    if (sliceBodies.size() != sliceCount) {
      return Optional.empty();
    }

    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = originReqType;

    final ByteBuffer body = ByteBuffer.allocate(originBodySize);
    sliceBodies.forEach(body::put);
    body.flip();
    req.body = body;

    return Optional.of(req);
  }

  public void clear() {
    orderId = -1;
    originReqType = -1;
    originBodySize = -1;
    sliceCount = -1;
    sliceBodies.clear();
    receivedBodySize = 0;
  }
}
