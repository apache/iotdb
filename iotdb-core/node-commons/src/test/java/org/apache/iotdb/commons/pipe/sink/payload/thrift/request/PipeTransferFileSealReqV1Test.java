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

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeTransferFileSealReqV1Test {

  private static final String FILE_NAME = "1-0-0-0.tsfile";
  private static final long FILE_LENGTH = 1024L;

  @Test
  public void testFileSealReqV1RoundTripKeepsFileNameAndLength() throws IOException {
    final DummyFileSealReqV1 req = DummyFileSealReqV1.toTPipeTransferReq(FILE_NAME, FILE_LENGTH);

    Assert.assertEquals(IoTDBSinkRequestVersion.VERSION_1.getVersion(), req.version);
    Assert.assertEquals(PipeRequestType.TRANSFER_TS_FILE_SEAL.getType(), req.type);
    Assert.assertEquals(FILE_NAME, req.getFileName());
    Assert.assertEquals(FILE_LENGTH, req.getFileLength());
    assertFileSealBody(req.body.duplicate());

    final DummyFileSealReqV1 deserializedReq =
        (DummyFileSealReqV1) new DummyFileSealReqV1().translateFromTPipeTransferReq(copyOf(req));

    Assert.assertEquals(req.version, deserializedReq.version);
    Assert.assertEquals(req.type, deserializedReq.type);
    Assert.assertEquals(FILE_NAME, deserializedReq.getFileName());
    Assert.assertEquals(FILE_LENGTH, deserializedReq.getFileLength());
  }

  @Test
  public void testFileSealAirGapBytesKeepSameBodyFormat() throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.wrap(
            new DummyFileSealReqV1()
                .convertToTPipeTransferSnapshotSealBytes(FILE_NAME, FILE_LENGTH));

    Assert.assertEquals(
        IoTDBSinkRequestVersion.VERSION_1.getVersion(), ReadWriteIOUtils.readByte(buffer));
    Assert.assertEquals(
        PipeRequestType.TRANSFER_TS_FILE_SEAL.getType(), ReadWriteIOUtils.readShort(buffer));
    assertFileSealBody(buffer);
  }

  private static void assertFileSealBody(final ByteBuffer body) {
    Assert.assertEquals(FILE_NAME, ReadWriteIOUtils.readString(body));
    Assert.assertEquals(FILE_LENGTH, ReadWriteIOUtils.readLong(body));
    Assert.assertFalse(body.hasRemaining());
  }

  private static TPipeTransferReq copyOf(final TPipeTransferReq req) {
    final TPipeTransferReq copy = new TPipeTransferReq();
    copy.version = req.version;
    copy.type = req.type;
    copy.body = req.body.duplicate();
    return copy;
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
