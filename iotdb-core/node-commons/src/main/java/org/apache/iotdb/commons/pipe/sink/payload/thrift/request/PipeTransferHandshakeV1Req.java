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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.request;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class PipeTransferHandshakeV1Req extends TPipeTransferReq {

  private transient String timestampPrecision;

  public final String getTimestampPrecision() {
    return timestampPrecision;
  }

  protected abstract PipeRequestType getPlanType();

  /////////////////////////////// Thrift ///////////////////////////////

  public final PipeTransferHandshakeV1Req convertToTPipeTransferReq(String timestampPrecision)
      throws IOException {
    this.timestampPrecision = timestampPrecision;

    this.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    this.type = getPlanType().getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      this.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return this;
  }

  protected final PipeTransferHandshakeV1Req translateFromTPipeTransferReq(
      TPipeTransferReq transferReq) {
    timestampPrecision = ReadWriteIOUtils.readString(transferReq.body);

    version = transferReq.version;
    type = transferReq.type;

    return this;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  protected final byte[] convertToTransferHandshakeBytes(String timestampPrecision)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(getPlanType().getType(), outputStream);
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      return byteArrayOutputStream.toByteArray();
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeTransferHandshakeV1Req that = (PipeTransferHandshakeV1Req) obj;
    return timestampPrecision.equals(that.timestampPrecision)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestampPrecision, version, type, body);
  }
}
