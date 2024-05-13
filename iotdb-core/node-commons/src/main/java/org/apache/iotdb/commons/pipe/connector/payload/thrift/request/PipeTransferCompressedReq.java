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

import org.apache.iotdb.commons.pipe.connector.compress.PipeCompressor;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipeTransferCompressedReq extends TPipeTransferReq {

  public static TPipeTransferReq toTPipeTransferReq(
      TPipeTransferReq originalReq, List<PipeCompressor> compressors) throws IOException {
    if (compressors.isEmpty()) {
      return originalReq;
    }

    final PipeTransferCompressedReq compressedReq = new PipeTransferCompressedReq();
    compressedReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    compressedReq.type = PipeRequestType.TRANSFER_COMPRESSED.getType();

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write((byte) compressors.size(), outputStream);
      for (PipeCompressor compressor : compressors) {
        ReadWriteIOUtils.write(compressor.serialize(), outputStream);
      }

      ReadWriteIOUtils.write(originalReq.version, outputStream);
      ReadWriteIOUtils.write(originalReq.type, outputStream);

      byte[] body = originalReq.getBody();
      for (PipeCompressor compressor : compressors) {
        body = compressor.compress(body);
      }
      outputStream.write(body);

      compressedReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
    return compressedReq;
  }

  public static TPipeTransferReq decompressFromTPipeTransferReq(TPipeTransferReq transferReq)
      throws IOException {
    ByteBuffer compressedBuffer = transferReq.body;

    List<PipeCompressor> compressors = new ArrayList<>();
    int compressorsSize = ReadWriteIOUtils.readByte(compressedBuffer);
    for (int i = 0; i < compressorsSize; ++i) {
      compressors.add(PipeCompressor.getCompressor(ReadWriteIOUtils.readByte(compressedBuffer)));
    }

    final TPipeTransferReq decompressedReq = new TPipeTransferReq();
    decompressedReq.version = ReadWriteIOUtils.readByte(compressedBuffer);
    decompressedReq.type = ReadWriteIOUtils.readShort(compressedBuffer);

    byte[] body = new byte[compressedBuffer.remaining()];
    compressedBuffer.get(body);

    for (int i = compressors.size() - 1; i >= 0; --i) {
      body = compressors.get(i).decompress(body);
    }
    decompressedReq.body = ByteBuffer.wrap(body);

    return decompressedReq;
  }
}
