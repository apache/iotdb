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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.response;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeTransferFilePieceResp extends TPipeTransferResp {

  public static final long ERROR_END_OFFSET = -1;

  private long endWritingOffset;

  private PipeTransferFilePieceResp() {
    // Empty constructor
  }

  public long getEndWritingOffset() {
    return endWritingOffset;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferFilePieceResp toTPipeTransferResp(
      TSStatus status, long endWritingOffset) throws IOException {
    final PipeTransferFilePieceResp filePieceResp = new PipeTransferFilePieceResp();

    filePieceResp.status = status;

    filePieceResp.endWritingOffset = endWritingOffset;
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(endWritingOffset, outputStream);
      filePieceResp.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return filePieceResp;
  }

  public static PipeTransferFilePieceResp toTPipeTransferResp(TSStatus status) {
    final PipeTransferFilePieceResp filePieceResp = new PipeTransferFilePieceResp();

    filePieceResp.status = status;

    return filePieceResp;
  }

  public static PipeTransferFilePieceResp fromTPipeTransferResp(TPipeTransferResp transferResp) {
    final PipeTransferFilePieceResp filePieceResp = new PipeTransferFilePieceResp();

    filePieceResp.status = transferResp.status;

    if (transferResp.isSetBody()) {
      filePieceResp.endWritingOffset = ReadWriteIOUtils.readLong(transferResp.body);
      filePieceResp.body = transferResp.body;
    }

    return filePieceResp;
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
    PipeTransferFilePieceResp that = (PipeTransferFilePieceResp) obj;
    return endWritingOffset == that.endWritingOffset
        && status.equals(that.status)
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(endWritingOffset, status, body);
  }
}
