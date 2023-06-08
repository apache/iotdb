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

package org.apache.iotdb.db.pipe.connector.v1.request;

import org.apache.iotdb.db.pipe.connector.IoTDBThriftConnectorVersion;
import org.apache.iotdb.db.pipe.connector.v1.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeTransferFilePieceReq extends TPipeTransferReq {

  private String fileName;
  private long startWritingOffset;
  private byte[] filePiece;

  private PipeTransferFilePieceReq() {}

  public String getFileName() {
    return fileName;
  }

  public long getStartWritingOffset() {
    return startWritingOffset;
  }

  public byte[] getFilePiece() {
    return filePiece;
  }

  public static PipeTransferFilePieceReq toTPipeTransferReq(
      String fileName, long startWritingOffset, byte[] filePiece) throws IOException {
    final PipeTransferFilePieceReq filePieceReq = new PipeTransferFilePieceReq();

    filePieceReq.fileName = fileName;
    filePieceReq.startWritingOffset = startWritingOffset;
    filePieceReq.filePiece = filePiece;

    filePieceReq.version = IoTDBThriftConnectorVersion.VERSION_1.getVersion();
    filePieceReq.type = PipeRequestType.TRANSFER_FILE_PIECE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(startWritingOffset, outputStream);
      ReadWriteIOUtils.write(new Binary(filePiece), outputStream);
      filePieceReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return filePieceReq;
  }

  public static PipeTransferFilePieceReq fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferFilePieceReq filePieceReq = new PipeTransferFilePieceReq();

    filePieceReq.fileName = ReadWriteIOUtils.readString(transferReq.body);
    filePieceReq.startWritingOffset = ReadWriteIOUtils.readLong(transferReq.body);
    filePieceReq.filePiece = ReadWriteIOUtils.readBinary(transferReq.body).getValues();

    filePieceReq.version = transferReq.version;
    filePieceReq.type = transferReq.type;
    filePieceReq.body = transferReq.body;

    return filePieceReq;
  }
}
