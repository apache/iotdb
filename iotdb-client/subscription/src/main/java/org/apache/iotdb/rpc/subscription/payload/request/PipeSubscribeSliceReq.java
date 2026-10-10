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

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class PipeSubscribeSliceReq extends TPipeSubscribeReq {

  private transient int orderId;
  private transient byte originReqVersion;
  private transient short originReqType;
  private transient int originBodySize;
  private transient int sliceIndex;
  private transient int sliceCount;
  private transient byte[] sliceBody;

  public int getOrderId() {
    return orderId;
  }

  public byte getOriginReqVersion() {
    return originReqVersion;
  }

  public short getOriginReqType() {
    return originReqType;
  }

  public int getOriginBodySize() {
    return originBodySize;
  }

  public int getSliceIndex() {
    return sliceIndex;
  }

  public int getSliceCount() {
    return sliceCount;
  }

  public byte[] getSliceBody() {
    return sliceBody;
  }

  public static PipeSubscribeSliceReq toTPipeSubscribeReq(
      final TPipeSubscribeReq originReq,
      final int orderId,
      final int sliceIndex,
      final int sliceCount,
      final int bodySizeLimit)
      throws IOException {
    final ByteBuffer originBody = originReq.body.duplicate();
    final int originPosition = originBody.position();
    final int originBodySize = originBody.remaining();
    final int startOffset = sliceIndex * bodySizeLimit;
    final int endOffset = startOffset + Math.min(bodySizeLimit, originBodySize - startOffset);

    final PipeSubscribeSliceReq sliceReq = new PipeSubscribeSliceReq();
    sliceReq.orderId = orderId;
    sliceReq.originReqVersion = originReq.getVersion();
    sliceReq.originReqType = originReq.getType();
    sliceReq.originBodySize = originBodySize;
    sliceReq.sliceIndex = sliceIndex;
    sliceReq.sliceCount = sliceCount;
    sliceReq.sliceBody = new byte[endOffset - startOffset];
    originBody.position(originPosition + startOffset);
    originBody.get(sliceReq.sliceBody);

    sliceReq.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    sliceReq.type = PipeSubscribeRequestType.SLICE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(sliceReq.orderId, outputStream);
      ReadWriteIOUtils.write(sliceReq.originReqVersion, outputStream);
      ReadWriteIOUtils.write(sliceReq.originReqType, outputStream);
      ReadWriteIOUtils.write(sliceReq.originBodySize, outputStream);
      ReadWriteIOUtils.write(sliceReq.sliceIndex, outputStream);
      ReadWriteIOUtils.write(sliceReq.sliceCount, outputStream);
      ReadWriteIOUtils.write(new Binary(sliceReq.sliceBody), outputStream);
      sliceReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
    return sliceReq;
  }

  public static PipeSubscribeSliceReq fromTPipeSubscribeReq(final TPipeSubscribeReq thriftReq) {
    final PipeSubscribeSliceReq sliceReq = new PipeSubscribeSliceReq();
    final ByteBuffer body = thriftReq.body.duplicate();
    sliceReq.orderId = ReadWriteIOUtils.readInt(body);
    sliceReq.originReqVersion = ReadWriteIOUtils.readByte(body);
    sliceReq.originReqType = ReadWriteIOUtils.readShort(body);
    sliceReq.originBodySize = ReadWriteIOUtils.readInt(body);
    sliceReq.sliceIndex = ReadWriteIOUtils.readInt(body);
    sliceReq.sliceCount = ReadWriteIOUtils.readInt(body);
    final int sliceBodySize = ReadWriteIOUtils.readInt(body);
    if (sliceBodySize < 0 || sliceBodySize != body.remaining()) {
      throw new IllegalArgumentException();
    }
    sliceReq.sliceBody = new byte[sliceBodySize];
    body.get(sliceReq.sliceBody);
    sliceReq.version = thriftReq.version;
    sliceReq.type = thriftReq.type;
    sliceReq.body = thriftReq.body;
    return sliceReq;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeSubscribeSliceReq that = (PipeSubscribeSliceReq) obj;
    return orderId == that.orderId
        && originReqVersion == that.originReqVersion
        && originReqType == that.originReqType
        && originBodySize == that.originBodySize
        && sliceIndex == that.sliceIndex
        && sliceCount == that.sliceCount
        && Arrays.equals(sliceBody, that.sliceBody)
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        orderId,
        originReqVersion,
        originReqType,
        originBodySize,
        sliceIndex,
        sliceCount,
        Arrays.hashCode(sliceBody),
        version,
        type,
        body);
  }
}
