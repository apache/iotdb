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

package org.apache.iotdb.db.pipe.receive;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.pipe.receive.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.receive.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.receive.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.receive.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.receive.request.PipeValidateHandshakeReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeThriftRequestTest {
  private static final String IoTDB_VERSION = "1.2.0";
  private static final String PIPE_VERSION = "v1.0";
  private static final String TIME_PRECISION = "ms";

  @Test
  public void testPipeValidateHandshakeReq() throws IOException {
    PipeValidateHandshakeReq req =
        new PipeValidateHandshakeReq(PIPE_VERSION, IoTDB_VERSION, TIME_PRECISION);
    PipeValidateHandshakeReq deserializeReq =
        PipeValidateHandshakeReq.fromTPipeHandshakeReq(req.toTPipeHandshakeReq());
    Assert.assertEquals(req.getPipeVersion(), deserializeReq.getPipeVersion());
    Assert.assertEquals(req.getIoTDBVersion(), deserializeReq.getIoTDBVersion());
    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferInsertNodeReq() {
    PipeTransferInsertNodeReq req =
        new PipeTransferInsertNodeReq(
            PIPE_VERSION,
            new InsertRowNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "sg", "d"}),
                false,
                new String[] {"s"},
                new TSDataType[] {TSDataType.INT32},
                1,
                new Object[] {1},
                false));
    PipeTransferInsertNodeReq deserializeReq =
        PipeTransferInsertNodeReq.fromTPipeTransferReq(req.toTPipeTransferReq());
    Assert.assertEquals(req.getPipeVersion(), deserializeReq.getPipeVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(req.getInsertNode(), deserializeReq.getInsertNode());
    Assert.assertEquals(req.getTransferInfo(), deserializeReq.getTransferInfo());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
  }

  @Test
  public void testPipeTransferFilePieceReq() throws IOException {
    byte[] body = "testPipeTransferFilePieceReq".getBytes();
    String fileName = "1.tsfile";
    PipeTransferFilePieceReq req =
        new PipeTransferFilePieceReq(PIPE_VERSION, ByteBuffer.wrap(body), fileName, 0);
    PipeTransferFilePieceReq deserializeReq =
        PipeTransferFilePieceReq.fromTPipeTransferReq(req.toTPipeTransferReq());
    Assert.assertEquals(req.getPipeVersion(), deserializeReq.getPipeVersion());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartOffset(), deserializeReq.getStartOffset());
  }

  @Test
  public void testPipeTransferFileSealReq() throws IOException {
    String fileName = "1.tsfile";
    PipeTransferFileSealReq req = new PipeTransferFileSealReq(PIPE_VERSION, fileName, 100);
    PipeTransferFileSealReq deserializeReq =
        PipeTransferFileSealReq.fromTPipeTransferReq(req.toTPipeTransferReq());
    Assert.assertEquals(req.getPipeVersion(), deserializeReq.getPipeVersion());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getFileLength(), deserializeReq.getFileLength());
  }

  @Test
  public void testPIpeTransferFilePieceResp() {
    PipeTransferFilePieceResp resp = new PipeTransferFilePieceResp(RpcUtils.SUCCESS_STATUS, 100);
    PipeTransferFilePieceResp deserializeResp =
        PipeTransferFilePieceResp.fromTPipeTransferResp(resp.toTPipeTransferResp());
    Assert.assertEquals(resp.getStatus(), deserializeResp.getStatus());
    Assert.assertEquals(resp.getEndOffset(), deserializeResp.getEndOffset());
  }
}
