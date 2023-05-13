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

package org.apache.iotdb.db.pipe.core.receiver.request;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeTransferFilePieceReq extends TPipeTransferReq {
  private final String fileName;
  private final long startOffset;

  public PipeTransferFilePieceReq(
      String pipeVersion, ByteBuffer body, String fileName, long startOffset) {
    this.pipeVersion = pipeVersion;
    this.body = body;
    this.fileName = fileName;
    this.startOffset = startOffset;
  }

  public String getFileName() {
    return fileName;
  }

  public long getStartOffset() {
    return startOffset;
  }

  @Override
  public short getType() {
    return PipeTransferReqType.FILE_PIECE.getNodeType();
  }

  public TPipeTransferReq toTPipeTransferReq() throws IOException {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      this.type = getType();
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(startOffset, outputStream);
      this.transferInfo =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      return this;
    }
  }

  public static PipeTransferFilePieceReq fromTPipeTransferReq(TPipeTransferReq req) {
    String fileName = ReadWriteIOUtils.readString(req.transferInfo);
    long offset = ReadWriteIOUtils.read(req.transferInfo);
    return new PipeTransferFilePieceReq(req.pipeVersion, req.body, fileName, offset);
  }
}
