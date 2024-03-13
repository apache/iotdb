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

package org.apache.iotdb.confignode.manager.pipe;

import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotSealReq;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class PipeConfigNodeThriftRequestTest {

  private static final String TIME_PRECISION = "ms";

  @Test
  public void testPipeTransferConfigHandshakeReq() throws IOException {
    PipeTransferConfigNodeHandshakeV1Req req =
        PipeTransferConfigNodeHandshakeV1Req.toTPipeTransferReq(TIME_PRECISION);
    PipeTransferConfigNodeHandshakeV1Req deserializeReq =
        PipeTransferConfigNodeHandshakeV1Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferConfigPlanReq() {
    PipeTransferConfigPlanReq req =
        PipeTransferConfigPlanReq.toTPipeTransferReq(new ActiveCQPlan("cqId", "md5"));
    PipeTransferConfigPlanReq deserializeReq = PipeTransferConfigPlanReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
  }

  @Test
  public void testPipeTransferConfigSnapshotPieceReq() throws IOException {
    byte[] body = "testPipeTransferConfigSnapshotPieceReq".getBytes();
    String fileName = "1.temp";

    PipeTransferConfigSnapshotPieceReq req =
        PipeTransferConfigSnapshotPieceReq.toTPipeTransferReq(fileName, 0, body);
    PipeTransferConfigSnapshotPieceReq deserializeReq =
        PipeTransferConfigSnapshotPieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferConfigSnapshotSealReq() throws IOException {
    String fileName = "1.temp";

    PipeTransferConfigSnapshotSealReq req =
        PipeTransferConfigSnapshotSealReq.toTPipeTransferReq(fileName, 100);
    PipeTransferConfigSnapshotSealReq deserializeReq =
        PipeTransferConfigSnapshotSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getFileLength(), deserializeReq.getFileLength());
  }
}
