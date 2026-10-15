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

public class PipeTransferPipeReceiverRuntimeInfoCleanupReq extends TPipeTransferReq {

  private String pipeName;
  private long pipeCreationTime;

  private PipeTransferPipeReceiverRuntimeInfoCleanupReq() {
    // Empty constructor
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getPipeCreationTime() {
    return pipeCreationTime;
  }

  public static PipeTransferPipeReceiverRuntimeInfoCleanupReq toTPipeTransferReq(
      final String pipeName, final long pipeCreationTime) throws IOException {
    final PipeTransferPipeReceiverRuntimeInfoCleanupReq req =
        new PipeTransferPipeReceiverRuntimeInfoCleanupReq();
    req.pipeName = pipeName;
    req.pipeCreationTime = pipeCreationTime;
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_PIPE_RECEIVER_RUNTIME_INFO_CLEANUP.getType();

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(pipeName, outputStream);
      ReadWriteIOUtils.write(pipeCreationTime, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return req;
  }

  public static PipeTransferPipeReceiverRuntimeInfoCleanupReq fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    final PipeTransferPipeReceiverRuntimeInfoCleanupReq req =
        new PipeTransferPipeReceiverRuntimeInfoCleanupReq();
    req.version = transferReq.version;
    req.type = transferReq.type;
    req.body = transferReq.body;

    final ByteBuffer bodyBuffer = transferReq.body.duplicate();
    req.pipeName = ReadWriteIOUtils.readString(bodyBuffer);
    req.pipeCreationTime = ReadWriteIOUtils.readLong(bodyBuffer);
    return req;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeTransferPipeReceiverRuntimeInfoCleanupReq that =
        (PipeTransferPipeReceiverRuntimeInfoCleanupReq) obj;
    return pipeCreationTime == that.pipeCreationTime
        && version == that.version
        && type == that.type
        && Objects.equals(pipeName, that.pipeName)
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName, pipeCreationTime, version, type, body);
  }
}
