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

package org.apache.iotdb.confignode.manager.pipe.sink;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PipeConfigNodeThriftRequestTest {

  private static final String TIME_PRECISION = "ms";

  @Test
  public void testPipeTransferConfigHandshakeReq() throws IOException {
    PipeTransferConfigNodeHandshakeV1Req req =
        PipeTransferConfigNodeHandshakeV1Req.toTPipeTransferReq(TIME_PRECISION);
    final int originalBodyPosition = req.body.position();
    PipeTransferConfigNodeHandshakeV1Req deserializeReq =
        PipeTransferConfigNodeHandshakeV1Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(originalBodyPosition, req.body.position());

    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferConfigHandshakeV2Req() throws IOException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION, TIME_PRECISION);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, "cluster-a");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, "root");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PIPE_NAME, "pipe-a");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PIPE_CREATION_TIME, "1");

    final PipeTransferConfigNodeHandshakeV2Req req =
        PipeTransferConfigNodeHandshakeV2Req.toTPipeTransferReq(params);
    final int originalBodyPosition = req.body.position();
    final PipeTransferConfigNodeHandshakeV2Req deserializeReq =
        PipeTransferConfigNodeHandshakeV2Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(originalBodyPosition, req.body.position());
    Assert.assertEquals(params, deserializeReq.getParams());
  }

  @Test
  public void testPipeTransferConfigPlanReq() {
    PipeTransferConfigPlanReq req =
        PipeTransferConfigPlanReq.toTPipeTransferReq(new ActiveCQPlan("cqId", "md5"));
    PipeTransferConfigPlanReq deserializeReq = PipeTransferConfigPlanReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
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

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferConfigSnapshotSealReq() throws IOException {
    String snapshotName = "cluster_schema.bin";
    String templateInfoName = "template_info.bin";
    CNSnapshotFileType fileType = CNSnapshotFileType.SCHEMA;
    // CreateDatabase
    String typeString = "200";

    PipeTransferConfigSnapshotSealReq req =
        PipeTransferConfigSnapshotSealReq.toTPipeTransferReq(
            "root.**",
            "db",
            "table",
            true,
            true,
            snapshotName,
            100,
            templateInfoName,
            10,
            fileType,
            typeString,
            "");
    PipeTransferConfigSnapshotSealReq deserializeReq =
        PipeTransferConfigSnapshotSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getFileNames(), deserializeReq.getFileNames());
    Assert.assertEquals(req.getFileLengths(), deserializeReq.getFileLengths());
    Assert.assertEquals(req.getParameters(), deserializeReq.getParameters());
  }
}
