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

package org.apache.iotdb.commons.pipe.connector.payload.thrift.common;

import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferSliceReq;
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

  public boolean receiveSlice(final PipeTransferSliceReq req) {
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
      } else {
        LOGGER.warn(
            "Invalid state: orderId={}, originReqType={}, originBodySize={}, sliceCount={}, sliceBodies.size={}",
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
      LOGGER.warn("Order ID mismatch: expected {}, actual {}", orderId, req.getOrderId());
      clear();
      return false;
    }
    if (originReqType != req.getOriginReqType()) {
      LOGGER.warn(
          "Origin request type mismatch: expected {}, actual {}",
          originReqType,
          req.getOriginReqType());
      clear();
      return false;
    }
    if (originBodySize != req.getOriginBodySize()) {
      LOGGER.warn(
          "Origin body size mismatch: expected {}, actual {}",
          originBodySize,
          req.getOriginBodySize());
      clear();
      return false;
    }
    if (sliceCount != req.getSliceCount()) {
      LOGGER.warn("Slice count mismatch: expected {}, actual {}", sliceCount, req.getSliceCount());
      clear();
      return false;
    }
    if (sliceBodies.size() != req.getSliceIndex()) {
      LOGGER.warn(
          "Invalid slice index: expected {}, actual {}", sliceBodies.size(), req.getSliceIndex());
      clear();
      return false;
    }

    sliceBodies.add(req.getSliceBody());
    return true;
  }

  public Optional<TPipeTransferReq> makeReqIfComplete() {
    if (sliceBodies.size() != sliceCount) {
      return Optional.empty();
    }

    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
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
  }
}
