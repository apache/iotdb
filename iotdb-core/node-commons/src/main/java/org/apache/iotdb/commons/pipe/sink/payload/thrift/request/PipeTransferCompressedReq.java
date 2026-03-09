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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressor;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressorFactory;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PipeTransferCompressedReq extends TPipeTransferReq {

  /** Generate a compressed req with provided compressors. */
  public static TPipeTransferReq toTPipeTransferReq(
      final TPipeTransferReq originalReq, final List<PipeCompressor> compressors)
      throws IOException {
    // The generated PipeTransferCompressedReq consists of:
    // version
    // type: TRANSFER_COMPRESSED
    // body:
    //   (byte) count of compressors (n)
    //   (n*3 bytes) for each compressor:
    //     (byte) compressor type
    //     (int) length of uncompressed bytes
    //   compressed req:
    //     (byte) version
    //     (2 bytes) type
    //     (bytes) body
    final PipeTransferCompressedReq compressedReq = new PipeTransferCompressedReq();
    compressedReq.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    compressedReq.type = PipeRequestType.TRANSFER_COMPRESSED.getType();

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      byte[] body =
          BytesUtils.concatByteArrayList(
              Arrays.asList(
                  new byte[] {originalReq.version},
                  BytesUtils.shortToBytes(originalReq.type),
                  originalReq.getBody()));

      ReadWriteIOUtils.write((byte) compressors.size(), outputStream);
      for (final PipeCompressor compressor : compressors) {
        ReadWriteIOUtils.write(compressor.serialize(), outputStream);
        ReadWriteIOUtils.write(body.length, outputStream);
        body = compressor.compress(body);
      }
      outputStream.write(body);

      compressedReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
    return compressedReq;
  }

  /** Get the original req from a compressed req. */
  public static TPipeTransferReq fromTPipeTransferReq(final TPipeTransferReq transferReq)
      throws IOException {
    final ByteBuffer compressedBuffer = transferReq.body;

    final List<PipeCompressor> compressors = new ArrayList<>();
    final List<Integer> uncompressedLengths = new ArrayList<>();
    final int compressorsSize = ReadWriteIOUtils.readByte(compressedBuffer);
    for (int i = 0; i < compressorsSize; ++i) {
      compressors.add(
          PipeCompressorFactory.getCompressor(ReadWriteIOUtils.readByte(compressedBuffer)));
      uncompressedLengths.add(ReadWriteIOUtils.readInt(compressedBuffer));
      checkDecompressedLength(uncompressedLengths.get(i));
    }

    byte[] body = new byte[compressedBuffer.remaining()];
    compressedBuffer.get(body);

    for (int i = compressors.size() - 1; i >= 0; --i) {
      body = compressors.get(i).decompress(body, uncompressedLengths.get(i));
    }

    final ByteBuffer decompressedBuffer = ByteBuffer.wrap(body);

    final TPipeTransferReq decompressedReq = new TPipeTransferReq();
    decompressedReq.version = ReadWriteIOUtils.readByte(decompressedBuffer);
    decompressedReq.type = ReadWriteIOUtils.readShort(decompressedBuffer);
    decompressedReq.body = decompressedBuffer.slice();

    return decompressedReq;
  }

  /** This method is used to prevent decompression bomb attacks. */
  private static void checkDecompressedLength(final int decompressedLength)
      throws IllegalArgumentException {
    final int maxDecompressedLength =
        PipeConfig.getInstance().getPipeReceiverReqDecompressedMaxLengthInBytes();
    if (decompressedLength < 0 || decompressedLength > maxDecompressedLength) {
      throw new IllegalArgumentException(
          String.format(
              "Decompressed length should be between 0 and %d, but got %d.",
              maxDecompressedLength, decompressedLength));
    }
  }

  /**
   * For air-gap connectors. Generate the bytes of a compressed req from the bytes of original req.
   */
  public static byte[] toTPipeTransferReqBytes(
      final byte[] rawReqInBytes, final List<PipeCompressor> compressors) throws IOException {
    // The generated bytes consists of:
    // (byte) version
    // (2 bytes) type: TRANSFER_COMPRESSED
    // (byte) count of compressors (n)
    // (n*3 bytes) for each compressor:
    //   (byte) compressor type
    //   (int) length of uncompressed bytes
    // compressed req:
    //   (byte) version
    //   (2 bytes) type
    //   (bytes) body
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      byte[] body = rawReqInBytes;

      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_COMPRESSED.getType(), outputStream);
      ReadWriteIOUtils.write((byte) compressors.size(), outputStream);
      for (final PipeCompressor compressor : compressors) {
        ReadWriteIOUtils.write(compressor.serialize(), outputStream);
        ReadWriteIOUtils.write(body.length, outputStream);
        body = compressor.compress(body);
      }
      outputStream.write(body);

      return byteArrayOutputStream.toByteArray();
    }
  }

  private PipeTransferCompressedReq() {
    // Empty constructor
  }
}
