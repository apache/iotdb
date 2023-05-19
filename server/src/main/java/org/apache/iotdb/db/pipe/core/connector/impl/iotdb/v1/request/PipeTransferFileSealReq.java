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

package org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1.request;

import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.IoTDBThriftConnectorVersion;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeTransferFileSealReq extends TPipeTransferReq {

  private String fileName;
  private long fileLength;

  private PipeTransferFileSealReq() {}

  public String getFileName() {
    return fileName;
  }

  public long getFileLength() {
    return fileLength;
  }

  public static PipeTransferFileSealReq toTPipeTransferReq(String fileName, long fileLength)
      throws IOException {
    final PipeTransferFileSealReq fileSealReq = new PipeTransferFileSealReq();

    fileSealReq.fileName = fileName;
    fileSealReq.fileLength = fileLength;

    fileSealReq.version = IoTDBThriftConnectorVersion.VERSION_ONE.getVersion();
    fileSealReq.type = PipeRequestType.TRANSFER_FILE_SEAL.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(fileLength, outputStream);
      fileSealReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return fileSealReq;
  }

  public static PipeTransferFileSealReq fromTPipeTransferReq(TPipeTransferReq req) {
    final PipeTransferFileSealReq fileSealReq = new PipeTransferFileSealReq();

    fileSealReq.fileName = ReadWriteIOUtils.readString(req.body);
    fileSealReq.fileLength = ReadWriteIOUtils.readLong(req.body);

    fileSealReq.version = req.version;
    fileSealReq.type = req.type;
    fileSealReq.body = req.body;

    return fileSealReq;
  }
}
