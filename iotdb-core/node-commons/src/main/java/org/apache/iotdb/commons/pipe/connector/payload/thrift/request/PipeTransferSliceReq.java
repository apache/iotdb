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

package org.apache.iotdb.commons.pipe.connector.payload.thrift.request;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class PipeTransferSliceReq extends TPipeTransferReq {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferSliceReq.class);

  private transient int originBodySize;

  private transient int sliceSize;
  private transient byte[] sliceBody;

  private transient int sliceIndex;
  private transient int sliceCount;

  public int getOriginBodySize() {
    return originBodySize;
  }

  public int getSliceSize() {
    return sliceSize;
  }

  public byte[] getSliceBody() {
    return sliceBody;
  }

  public int getSliceIndex() {
    return sliceIndex;
  }

  public int getSliceCount() {
    return sliceCount;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferSliceReq toTPipeTransferReq(
      final int sliceIndex,
      final int sliceCount,
      final ByteBuffer duplicatedOriginBody,
      final int startIndexInBody,
      final int endIndexInBody)
      throws IOException {
    final PipeTransferSliceReq sliceReq = new PipeTransferSliceReq();

    sliceReq.originBodySize = duplicatedOriginBody.limit();

    sliceReq.sliceSize = endIndexInBody - startIndexInBody;
    sliceReq.sliceBody = new byte[sliceReq.sliceSize];
    duplicatedOriginBody.position(startIndexInBody);
    duplicatedOriginBody.get(sliceReq.sliceBody);

    sliceReq.sliceIndex = sliceIndex;
    sliceReq.sliceCount = sliceCount;

    sliceReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    sliceReq.type = PipeRequestType.TRANSFER_SLICE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(sliceReq.originBodySize, outputStream);

      ReadWriteIOUtils.write(sliceReq.sliceSize, outputStream);
      ReadWriteIOUtils.write(new Binary(sliceReq.sliceBody), outputStream);

      ReadWriteIOUtils.write(sliceReq.sliceIndex, outputStream);
      ReadWriteIOUtils.write(sliceReq.sliceCount, outputStream);

      sliceReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return sliceReq;
  }

  public static PipeTransferSliceReq fromTPipeTransferReq(final TPipeTransferReq transferReq) {
    final PipeTransferSliceReq sliceReq = new PipeTransferSliceReq();

    sliceReq.originBodySize = ReadWriteIOUtils.readInt(transferReq.body);

    sliceReq.sliceSize = ReadWriteIOUtils.readInt(transferReq.body);
    sliceReq.sliceBody = ReadWriteIOUtils.readBinary(transferReq.body).getValues();

    sliceReq.sliceIndex = ReadWriteIOUtils.readInt(transferReq.body);
    sliceReq.sliceCount = ReadWriteIOUtils.readInt(transferReq.body);

    sliceReq.version = transferReq.version;
    sliceReq.type = transferReq.type;
    sliceReq.body = transferReq.body;

    return sliceReq;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeTransferSliceReq that = (PipeTransferSliceReq) obj;
    return Objects.equals(originBodySize, that.originBodySize)
        && Objects.equals(sliceSize, that.sliceSize)
        && Arrays.equals(sliceBody, that.sliceBody)
        && Objects.equals(sliceIndex, that.sliceIndex)
        && Objects.equals(sliceCount, that.sliceCount)
        && Objects.equals(version, that.version)
        && Objects.equals(type, that.type)
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        originBodySize,
        sliceSize,
        Arrays.hashCode(sliceBody),
        sliceIndex,
        sliceCount,
        version,
        type,
        body);
  }
}
