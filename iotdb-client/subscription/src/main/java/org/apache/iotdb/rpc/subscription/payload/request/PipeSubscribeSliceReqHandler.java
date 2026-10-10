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

package org.apache.iotdb.rpc.subscription.payload.request;

import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;

import org.apache.tsfile.utils.PublicBAOS;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

public class PipeSubscribeSliceReqHandler {

  private static final int INITIAL_BUFFER_SIZE = 8 * 1024;

  private final int maxOriginBodySize;

  private int orderId;
  private byte originReqVersion = -1;
  private short originReqType = -1;
  private int originBodySize = -1;
  private int sliceCount = -1;
  private int nextSliceIndex;
  private int receivedBodySize;
  private PublicBAOS assembledBody;

  public PipeSubscribeSliceReqHandler(final int maxOriginBodySize) {
    if (maxOriginBodySize <= 0) {
      throw new IllegalArgumentException();
    }
    this.maxOriginBodySize = maxOriginBodySize;
  }

  public synchronized boolean receiveSlice(final PipeSubscribeSliceReq req) {
    if (!isValidSlice(req)) {
      clear();
      return false;
    }

    if (assembledBody == null) {
      if (req.getSliceIndex() != 0) {
        return false;
      }
      orderId = req.getOrderId();
      originReqVersion = req.getOriginReqVersion();
      originReqType = req.getOriginReqType();
      originBodySize = req.getOriginBodySize();
      sliceCount = req.getSliceCount();
      assembledBody = new PublicBAOS(Math.min(INITIAL_BUFFER_SIZE, originBodySize));
    }

    if (orderId != req.getOrderId()
        || originReqVersion != req.getOriginReqVersion()
        || originReqType != req.getOriginReqType()
        || originBodySize != req.getOriginBodySize()
        || sliceCount != req.getSliceCount()
        || nextSliceIndex != req.getSliceIndex()) {
      clear();
      return false;
    }

    final byte[] sliceBody = req.getSliceBody();
    if (receivedBodySize > originBodySize - sliceBody.length) {
      clear();
      return false;
    }
    assembledBody.write(sliceBody, 0, sliceBody.length);
    receivedBodySize += sliceBody.length;
    nextSliceIndex++;

    if (nextSliceIndex < sliceCount && receivedBodySize >= originBodySize) {
      clear();
      return false;
    }
    if (nextSliceIndex == sliceCount && receivedBodySize != originBodySize) {
      clear();
      return false;
    }
    return true;
  }

  private boolean isValidSlice(final PipeSubscribeSliceReq req) {
    return Objects.nonNull(req)
        && req.getVersion() == PipeSubscribeRequestVersion.VERSION_1.getVersion()
        && req.getType() == PipeSubscribeRequestType.SLICE.getType()
        && req.getOriginReqVersion() == PipeSubscribeRequestVersion.VERSION_1.getVersion()
        && PipeSubscribeSliceReqBuilder.isSliceableRequestType(req.getOriginReqType())
        && req.getOriginBodySize() > 0
        && req.getOriginBodySize() <= maxOriginBodySize
        && req.getSliceCount() > 1
        && req.getSliceIndex() >= 0
        && req.getSliceIndex() < req.getSliceCount()
        && Objects.nonNull(req.getSliceBody())
        && req.getSliceBody().length > 0
        && req.getSliceBody().length <= req.getOriginBodySize();
  }

  public synchronized Optional<TPipeSubscribeReq> makeReqIfComplete() {
    if (assembledBody == null
        || nextSliceIndex != sliceCount
        || receivedBodySize != originBodySize) {
      return Optional.empty();
    }

    final TPipeSubscribeReq req = new TPipeSubscribeReq();
    req.version = originReqVersion;
    req.type = originReqType;
    req.body = ByteBuffer.wrap(assembledBody.getBuf(), 0, assembledBody.size());
    clear();
    return Optional.of(req);
  }

  public synchronized void clear() {
    orderId = 0;
    originReqVersion = -1;
    originReqType = -1;
    originBodySize = -1;
    sliceCount = -1;
    nextSliceIndex = 0;
    receivedBodySize = 0;
    assembledBody = null;
  }
}
