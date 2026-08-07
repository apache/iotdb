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

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.commons.pipe.sink.payload.thrift.PipeTransferReqTestUtils.assertAirGapReqBytes;
import static org.apache.iotdb.commons.pipe.sink.payload.thrift.PipeTransferReqTestUtils.assertVersionAndType;
import static org.apache.iotdb.commons.pipe.sink.payload.thrift.PipeTransferReqTestUtils.copyOf;

public class PipeTransferFilePieceReqTest {

  private static final String FILE_NAME = "1-0-0-0.tsfile";
  private static final long START_WRITING_OFFSET = 12L;
  private static final byte[] FILE_PIECE = new byte[] {1, 2, 3, 4};

  @Test
  public void testFilePieceReqRoundTripKeepsOffsetAndBody() throws IOException {
    assertFilePieceReqRoundTrip(FILE_NAME, START_WRITING_OFFSET, FILE_PIECE);
  }

  @Test
  public void testEmptyFilePieceReqRoundTripKeepsOffsetAndBody() throws IOException {
    assertFilePieceReqRoundTrip(FILE_NAME, 0L, new byte[0]);
  }

  @Test
  public void testFilePieceAirGapBytesKeepSameBodyFormat() throws IOException {
    assertAirGapReqBytes(
        new DummyFilePieceReq()
            .convertToTPipeTransferBytes(FILE_NAME, START_WRITING_OFFSET, FILE_PIECE),
        IoTDBSinkRequestVersion.VERSION_1,
        PipeRequestType.TRANSFER_TS_FILE_PIECE,
        body -> assertFilePieceBody(body, FILE_NAME, START_WRITING_OFFSET, FILE_PIECE));
  }

  private static void assertFilePieceReqRoundTrip(
      final String expectedFileName,
      final long expectedStartWritingOffset,
      final byte[] expectedFilePiece)
      throws IOException {
    final DummyFilePieceReq req =
        DummyFilePieceReq.toTPipeTransferReq(
            expectedFileName, expectedStartWritingOffset, expectedFilePiece);

    assertVersionAndType(
        req, IoTDBSinkRequestVersion.VERSION_1, PipeRequestType.TRANSFER_TS_FILE_PIECE);
    Assert.assertEquals(expectedFileName, req.getFileName());
    Assert.assertEquals(expectedStartWritingOffset, req.getStartWritingOffset());
    Assert.assertArrayEquals(expectedFilePiece, req.getFilePiece());
    assertFilePieceBody(
        req.body.duplicate(), expectedFileName, expectedStartWritingOffset, expectedFilePiece);

    final DummyFilePieceReq deserializedReq =
        (DummyFilePieceReq) new DummyFilePieceReq().translateFromTPipeTransferReq(copyOf(req));

    Assert.assertEquals(req.version, deserializedReq.version);
    Assert.assertEquals(req.type, deserializedReq.type);
    Assert.assertEquals(expectedFileName, deserializedReq.getFileName());
    Assert.assertEquals(expectedStartWritingOffset, deserializedReq.getStartWritingOffset());
    Assert.assertArrayEquals(expectedFilePiece, deserializedReq.getFilePiece());
  }

  private static void assertFilePieceBody(
      final ByteBuffer body,
      final String expectedFileName,
      final long expectedStartWritingOffset,
      final byte[] expectedFilePiece) {
    Assert.assertEquals(expectedFileName, ReadWriteIOUtils.readString(body));
    Assert.assertEquals(expectedStartWritingOffset, ReadWriteIOUtils.readLong(body));
    Assert.assertArrayEquals(expectedFilePiece, ReadWriteIOUtils.readBinary(body).getValues());
    Assert.assertFalse(body.hasRemaining());
  }

  private static class DummyFilePieceReq extends PipeTransferFilePieceReq {

    private static DummyFilePieceReq toTPipeTransferReq(
        final String fileName, final long startWritingOffset, final byte[] filePiece)
        throws IOException {
      return (DummyFilePieceReq)
          new DummyFilePieceReq()
              .convertToTPipeTransferReq(fileName, startWritingOffset, filePiece);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.TRANSFER_TS_FILE_PIECE;
    }
  }
}
