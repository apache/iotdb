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

public class PipeTransferFileSealReqV1Test {

  private static final String FILE_NAME = "1-0-0-0.tsfile";
  private static final long FILE_LENGTH = 1024L;

  @Test
  public void testFileSealReqV1RoundTripKeepsFileNameAndLength() throws IOException {
    assertFileSealReqRoundTrip(FILE_NAME, FILE_LENGTH);
  }

  @Test
  public void testEmptyFileSealReqV1RoundTripKeepsFileNameAndLength() throws IOException {
    assertFileSealReqRoundTrip(FILE_NAME, 0L);
  }

  @Test
  public void testFileSealAirGapBytesKeepSameBodyFormat() throws IOException {
    assertAirGapReqBytes(
        new DummyFileSealReqV1().convertToTPipeTransferSnapshotSealBytes(FILE_NAME, FILE_LENGTH),
        IoTDBSinkRequestVersion.VERSION_1,
        PipeRequestType.TRANSFER_TS_FILE_SEAL,
        body -> assertFileSealBody(body, FILE_NAME, FILE_LENGTH));
  }

  private static void assertFileSealReqRoundTrip(
      final String expectedFileName, final long expectedFileLength) throws IOException {
    final DummyFileSealReqV1 req =
        DummyFileSealReqV1.toTPipeTransferReq(expectedFileName, expectedFileLength);

    assertVersionAndType(
        req, IoTDBSinkRequestVersion.VERSION_1, PipeRequestType.TRANSFER_TS_FILE_SEAL);
    Assert.assertEquals(expectedFileName, req.getFileName());
    Assert.assertEquals(expectedFileLength, req.getFileLength());
    assertFileSealBody(req.body.duplicate(), expectedFileName, expectedFileLength);

    final DummyFileSealReqV1 deserializedReq =
        (DummyFileSealReqV1) new DummyFileSealReqV1().translateFromTPipeTransferReq(copyOf(req));

    Assert.assertEquals(req.version, deserializedReq.version);
    Assert.assertEquals(req.type, deserializedReq.type);
    Assert.assertEquals(expectedFileName, deserializedReq.getFileName());
    Assert.assertEquals(expectedFileLength, deserializedReq.getFileLength());
  }

  private static void assertFileSealBody(
      final ByteBuffer body, final String expectedFileName, final long expectedFileLength) {
    Assert.assertEquals(expectedFileName, ReadWriteIOUtils.readString(body));
    Assert.assertEquals(expectedFileLength, ReadWriteIOUtils.readLong(body));
    Assert.assertFalse(body.hasRemaining());
  }

  private static class DummyFileSealReqV1 extends PipeTransferFileSealReqV1 {

    private static DummyFileSealReqV1 toTPipeTransferReq(
        final String fileName, final long fileLength) throws IOException {
      return (DummyFileSealReqV1)
          new DummyFileSealReqV1().convertToTPipeTransferReq(fileName, fileLength);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.TRANSFER_TS_FILE_SEAL;
    }
  }
}
