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

package org.apache.iotdb.db.pipe.connector;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.pipe.connector.v1.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.v1.request.*;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PipeThriftRequestTest {

  private static final String TIME_PRECISION = "ms";

  @Test
  public void testPipeValidateHandshakeReq() throws IOException {
    PipeTransferHandshakeReq req = PipeTransferHandshakeReq.toTPipeTransferReq(TIME_PRECISION);
    PipeTransferHandshakeReq deserializeReq = PipeTransferHandshakeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferInsertNodeReq() {
    PipeTransferInsertNodeReq req =
        PipeTransferInsertNodeReq.toTPipeTransferReq(
            new InsertRowNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "sg", "d"}),
                false,
                new String[] {"s"},
                new TSDataType[] {TSDataType.INT32},
                1,
                new Object[] {1},
                false));
    PipeTransferInsertNodeReq deserializeReq = PipeTransferInsertNodeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getInsertNode(), deserializeReq.getInsertNode());

    Statement statement = req.constructStatement();
    List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath(new String[] {"root", "sg", "d", "s"}));
    Assert.assertEquals(statement.getPaths(), paths);
  }

  @Test
  public void testPipeTransferTabletReq() {
    try {
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.FLOAT));
      schemaList.add(new MeasurementSchema("s4", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));
      schemaList.add(new MeasurementSchema("s6", TSDataType.TEXT));
      Tablet t = new Tablet("root.sg.d", schemaList, 1024);
      t.rowSize = 2;
      t.addTimestamp(0, 2000);
      t.addTimestamp(1, 1000);
      t.addValue("s1", 0, 2);
      t.addValue("s6", 0, "2");
      t.addValue("s1", 1, 1);
      t.addValue("s6", 1, "1");
      PipeTransferTabletReq req = PipeTransferTabletReq.toTPipeTransferReq(t, false);
      PipeTransferTabletReq deserializeReq = PipeTransferTabletReq.fromTPipeTransferReq(req);

      Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
      Assert.assertEquals(req.getType(), deserializeReq.getType());
      Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

      Statement statement =
          req.constructStatement(); // will call PipeTransferTabletReq.sortTablet() here
      List<PartialPath> paths = new ArrayList<>();
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s1"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s2"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s3"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s4"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s5"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s6"}));
      Assert.assertEquals(statement.getPaths(), paths);
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPipeTransferFilePieceReq() throws IOException {
    byte[] body = "testPipeTransferFilePieceReq".getBytes();
    String fileName = "1.tsfile";

    PipeTransferFilePieceReq req = PipeTransferFilePieceReq.toTPipeTransferReq(fileName, 0, body);
    PipeTransferFilePieceReq deserializeReq = PipeTransferFilePieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferFileSealReq() throws IOException {
    String fileName = "1.tsfile";

    PipeTransferFileSealReq req = PipeTransferFileSealReq.toTPipeTransferReq(fileName, 100);
    PipeTransferFileSealReq deserializeReq = PipeTransferFileSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getFileLength(), deserializeReq.getFileLength());
  }

  @Test
  public void testPIpeTransferFilePieceResp() throws IOException {
    PipeTransferFilePieceResp resp =
        PipeTransferFilePieceResp.toTPipeTransferResp(RpcUtils.SUCCESS_STATUS, 100);
    PipeTransferFilePieceResp deserializeResp =
        PipeTransferFilePieceResp.fromTPipeTransferResp(resp);

    Assert.assertEquals(resp.getStatus(), deserializeResp.getStatus());
    Assert.assertEquals(resp.getEndWritingOffset(), deserializeResp.getEndWritingOffset());
  }
}
