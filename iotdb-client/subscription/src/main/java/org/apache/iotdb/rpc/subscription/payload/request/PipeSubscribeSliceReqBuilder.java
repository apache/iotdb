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

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public final class PipeSubscribeSliceReqBuilder {

  private static final AtomicInteger ORDER_ID_GENERATOR = new AtomicInteger(0);

  private PipeSubscribeSliceReqBuilder() {}

  public static boolean shouldSlice(final TPipeSubscribeReq req, final int bodySizeLimit) {
    return bodySizeLimit > 0
        && Objects.nonNull(req)
        && req.getVersion() == PipeSubscribeRequestVersion.VERSION_1.getVersion()
        && req.isSetBody()
        && req.body.remaining() > bodySizeLimit
        && isSliceableRequestType(req.getType());
  }

  public static boolean isSliceableRequestType(final short type) {
    return type != PipeSubscribeRequestType.HANDSHAKE.getType()
        && type != PipeSubscribeRequestType.CLOSE.getType()
        && type != PipeSubscribeRequestType.SLICE.getType()
        && PipeSubscribeRequestType.isValidatedRequestType(type);
  }

  public static int nextOrderId() {
    return ORDER_ID_GENERATOR.getAndIncrement();
  }

  public static int getSliceCount(final TPipeSubscribeReq req, final int bodySizeLimit) {
    if (bodySizeLimit <= 0 || Objects.isNull(req) || !req.isSetBody()) {
      throw new IllegalArgumentException();
    }
    final int bodySize = req.body.remaining();
    return bodySize == 0 ? 1 : (bodySize - 1) / bodySizeLimit + 1;
  }

  public static PipeSubscribeSliceReq buildSliceReq(
      final TPipeSubscribeReq originReq,
      final int orderId,
      final int sliceIndex,
      final int sliceCount,
      final int bodySizeLimit)
      throws IOException {
    if (bodySizeLimit <= 0
        || sliceCount <= 1
        || sliceIndex < 0
        || sliceIndex >= sliceCount
        || !shouldSlice(originReq, bodySizeLimit)
        || getSliceCount(originReq, bodySizeLimit) != sliceCount) {
      throw new IllegalArgumentException();
    }
    return PipeSubscribeSliceReq.toTPipeSubscribeReq(
        originReq, orderId, sliceIndex, sliceCount, bodySizeLimit);
  }
}
