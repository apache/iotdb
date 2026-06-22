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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressor;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressorFactory;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.commons.pipe.sink.payload.thrift.PipeTransferReqTestUtils.assertVersionAndType;
import static org.apache.iotdb.commons.pipe.sink.payload.thrift.PipeTransferReqTestUtils.readTransferReqFrom;
import static org.apache.iotdb.commons.pipe.sink.payload.thrift.PipeTransferReqTestUtils.toTransferReqBytes;

public class PipeTransferCompressedReqTest {

  @Test
  public void testPipeTransferCompressedReq() throws IOException {
    final TPipeTransferReq originalReq = createReq(new byte[] {1, 2, 3, 4});

    final TPipeTransferReq compressedReq =
        PipeTransferCompressedReq.toTPipeTransferReq(
            originalReq,
            Collections.singletonList(
                PipeCompressorFactory.getCompressor(
                    PipeCompressor.PipeCompressionType.GZIP.getIndex())));

    assertVersionAndType(
        compressedReq, IoTDBSinkRequestVersion.VERSION_1, PipeRequestType.TRANSFER_COMPRESSED);
    assertRoundTrip(originalReq, compressedReq);
  }

  @Test
  public void testPipeTransferCompressedReqWithMultipleCompressors() throws IOException {
    final TPipeTransferReq originalReq = createReq(new byte[] {1, 2, 3, 4});

    final TPipeTransferReq compressedReq =
        PipeTransferCompressedReq.toTPipeTransferReq(
            originalReq,
            Arrays.asList(
                PipeCompressorFactory.getCompressor(
                    PipeCompressor.PipeCompressionType.GZIP.getIndex()),
                PipeCompressorFactory.getCompressor(
                    PipeCompressor.PipeCompressionType.LZ4.getIndex())));

    assertRoundTrip(originalReq, compressedReq);
  }

  @Test
  public void testPipeTransferCompressedReqWithNoCompressor() throws IOException {
    final TPipeTransferReq originalReq = createReq(new byte[] {1, 2, 3, 4});

    final TPipeTransferReq compressedReq =
        PipeTransferCompressedReq.toTPipeTransferReq(originalReq, Collections.emptyList());

    assertVersionAndType(
        compressedReq, IoTDBSinkRequestVersion.VERSION_1, PipeRequestType.TRANSFER_COMPRESSED);
    assertRoundTrip(originalReq, compressedReq);
  }

  @Test
  public void testPipeTransferCompressedReqBytes() throws IOException {
    final TPipeTransferReq originalReq = createReq(new byte[] {1, 2, 3, 4});
    final TPipeTransferReq compressedReq =
        readTransferReqFrom(
            PipeTransferCompressedReq.toTPipeTransferReqBytes(
                toTransferReqBytes(originalReq),
                Collections.singletonList(
                    PipeCompressorFactory.getCompressor(
                        PipeCompressor.PipeCompressionType.GZIP.getIndex()))));

    assertVersionAndType(
        compressedReq, IoTDBSinkRequestVersion.VERSION_1, PipeRequestType.TRANSFER_COMPRESSED);
    assertRoundTrip(originalReq, compressedReq);
  }

  @Test
  public void testPipeTransferCompressedReqFromLegacyV13Body() throws IOException {
    final TPipeTransferReq originalReq = createReq(new byte[] {1, 2, 3, 4});

    final TPipeTransferReq compressedReq = new TPipeTransferReq();
    compressedReq.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    compressedReq.type = PipeRequestType.TRANSFER_COMPRESSED.getType();
    compressedReq.body =
        serializeLegacyCompressedBody(
            originalReq,
            Collections.singletonList(
                PipeCompressorFactory.getCompressor(
                    PipeCompressor.PipeCompressionType.GZIP.getIndex())));

    assertRoundTrip(originalReq, compressedReq);
  }

  @Test
  public void testPipeTransferCompressedReqRejectsInvalidUncompressedLength() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            PipeTransferCompressedReq.fromTPipeTransferReq(
                createCompressedReqWithUncompressedLength(-1)));

    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalMaxLength = commonConfig.getPipeReceiverReqDecompressedMaxLengthInBytes();
    try {
      commonConfig.setPipeReceiverReqDecompressedMaxLengthInBytes(8);
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              PipeTransferCompressedReq.fromTPipeTransferReq(
                  createCompressedReqWithUncompressedLength(9)));
    } finally {
      commonConfig.setPipeReceiverReqDecompressedMaxLengthInBytes(originalMaxLength);
    }
  }

  private static void assertRoundTrip(
      final TPipeTransferReq originalReq, final TPipeTransferReq compressedReq) throws IOException {
    final TPipeTransferReq decompressedReq =
        PipeTransferCompressedReq.fromTPipeTransferReq(compressedReq);

    Assert.assertEquals(originalReq.version, decompressedReq.version);
    Assert.assertEquals(originalReq.type, decompressedReq.type);
    Assert.assertArrayEquals(originalReq.getBody(), decompressedReq.getBody());
  }

  private static TPipeTransferReq createReq(final byte[] body) {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_BINARY.getType();
    req.body = ByteBuffer.wrap(body);
    return req;
  }

  private static TPipeTransferReq createCompressedReqWithUncompressedLength(
      final int uncompressedLength) throws IOException {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_COMPRESSED.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(PipeCompressor.PipeCompressionType.GZIP.getIndex(), outputStream);
      ReadWriteIOUtils.write(uncompressedLength, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
    return req;
  }

  private static ByteBuffer serializeLegacyCompressedBody(
      final TPipeTransferReq originalReq, final List<PipeCompressor> compressors)
      throws IOException {
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

      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }
}
