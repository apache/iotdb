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

package org.apache.iotdb.rpc.subscription.payload.response;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

public class PipeSubscribePollTsFilePieceResp extends TPipeSubscribeResp {

  private transient String fileName;
  private transient long startWritingOffset;
  private transient byte[] filePiece;

  public final String getFileName() {
    return fileName;
  }

  public final long getStartWritingOffset() {
    return startWritingOffset;
  }

  public final byte[] getFilePiece() {
    return filePiece;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeSubscribePollTsFilePieceResp toTPipeSubscribeResp(
      TSStatus status, String fileName, long startWritingOffset, byte[] filePiece) {
    final PipeSubscribePollTsFilePieceResp resp = new PipeSubscribePollTsFilePieceResp();

    resp.fileName = fileName;
    resp.startWritingOffset = startWritingOffset;
    resp.filePiece = filePiece;

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(startWritingOffset, outputStream);
      ReadWriteIOUtils.write(new Binary(filePiece), outputStream);
      resp.body =
          Collections.singletonList(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    } catch (final IOException e) {
      resp.status =
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_POLL_TS_FILE_ERROR, e.getMessage());
    }

    return resp;
  }

  public static PipeSubscribePollTsFilePieceResp fromTPipeSubscribeResp(
      TPipeSubscribeResp pollTsFilePieceResp) {
    final PipeSubscribePollTsFilePieceResp resp = new PipeSubscribePollTsFilePieceResp();

    if (Objects.nonNull(pollTsFilePieceResp.body)) {
      for (final ByteBuffer byteBuffer : pollTsFilePieceResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          resp.fileName = ReadWriteIOUtils.readString(byteBuffer);
          resp.startWritingOffset = ReadWriteIOUtils.readLong(byteBuffer);
          resp.filePiece = ReadWriteIOUtils.readBinary(byteBuffer).getValues();
        }
      }
    }

    resp.status = pollTsFilePieceResp.status;
    resp.version = pollTsFilePieceResp.version;
    resp.type = pollTsFilePieceResp.type;
    resp.body = pollTsFilePieceResp.body;

    return resp;
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
    final PipeSubscribePollTsFilePieceResp that = (PipeSubscribePollTsFilePieceResp) obj;
    return Objects.equals(this.fileName, that.fileName)
        && this.startWritingOffset == that.startWritingOffset
        && Arrays.equals(this.filePiece, that.filePiece)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fileName, startWritingOffset, Arrays.hashCode(filePiece), status, version, type, body);
  }
}
