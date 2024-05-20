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
import org.apache.iotdb.commons.pipe.connector.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PipeDataNodeThriftRequestTest {

  private static final String TIME_PRECISION = "ms";

  @Test
  public void testPipeTransferDataNodeHandshakeReq() throws IOException {
    PipeTransferDataNodeHandshakeV1Req req =
        PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(TIME_PRECISION);
    PipeTransferDataNodeHandshakeV1Req deserializeReq =
        PipeTransferDataNodeHandshakeV1Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferInsertNodeReq() {
    PipeTransferTabletInsertNodeReq req =
        PipeTransferTabletInsertNodeReq.toTPipeTransferReq(
            new InsertRowNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "sg", "d"}),
                false,
                new String[] {"s"},
                new TSDataType[] {TSDataType.INT32},
                1,
                new Object[] {1},
                false));
    PipeTransferTabletInsertNodeReq deserializeReq =
        PipeTransferTabletInsertNodeReq.fromTPipeTransferReq(req);

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
  public void testPipeTransferTabletBinaryReq() {
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    PipeTransferTabletBinaryReq req =
        PipeTransferTabletBinaryReq.toTPipeTransferReq(ByteBuffer.wrap(new byte[] {'a', 'b'}));
    PipeTransferTabletBinaryReq deserializeReq =
        PipeTransferTabletBinaryReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
  }

  @Test
  public void testPipeTransferSchemaPlanReq() {
    PipeTransferPlanNodeReq req =
        PipeTransferPlanNodeReq.toTPipeTransferReq(
            new CreateAlignedTimeSeriesNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "sg", "d"}),
                Collections.singletonList("s"),
                Collections.singletonList(TSDataType.INT32),
                Collections.singletonList(TSEncoding.PLAIN),
                Collections.singletonList(CompressionType.UNCOMPRESSED),
                null,
                null,
                null));

    PipeTransferPlanNodeReq deserializeReq = PipeTransferPlanNodeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getPlanNode(), deserializeReq.getPlanNode());
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
      PipeTransferTabletRawReq req = PipeTransferTabletRawReq.toTPipeTransferReq(t, false);
      PipeTransferTabletRawReq deserializeReq = PipeTransferTabletRawReq.fromTPipeTransferReq(req);

      Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
      Assert.assertEquals(req.getType(), deserializeReq.getType());
      Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

      Statement statement =
          req.constructStatement(); // will call PipeTransferTabletRawReq.sortTablet() here
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
  public void testPipeTransferTabletBatchReq() throws IOException {
    final List<ByteBuffer> binaryBuffers = new ArrayList<>();
    final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
    final List<ByteBuffer> tabletBuffers = new ArrayList<>();

    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"root", "sg", "d"}),
            false,
            new String[] {"s"},
            new TSDataType[] {TSDataType.INT32},
            1,
            new Object[] {1},
            false);

    // InsertNode buffer
    insertNodeBuffers.add(node.serializeToByteBuffer());

    // Binary buffer
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    binaryBuffers.add(ByteBuffer.wrap(new byte[] {'a', 'b'}));

    // Raw buffer
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

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      t.serialize(outputStream);
      ReadWriteIOUtils.write(false, outputStream);
      tabletBuffers.add(
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    }

    PipeTransferTabletBatchReq req =
        PipeTransferTabletBatchReq.toTPipeTransferReq(
            binaryBuffers, insertNodeBuffers, tabletBuffers);

    PipeTransferTabletBatchReq deserializedReq =
        PipeTransferTabletBatchReq.fromTPipeTransferReq(req);

    Assert.assertArrayEquals(
        new byte[] {'a', 'b'}, deserializedReq.getBinaryReqs().get(0).getBody());
    Assert.assertEquals(node, deserializedReq.getInsertNodeReqs().get(0).getInsertNode());
    Assert.assertEquals(t, deserializedReq.getTabletReqs().get(0).getTablet());
    Assert.assertFalse(deserializedReq.getTabletReqs().get(0).getIsAligned());
  }

  @Test
  public void testPipeTransferFilePieceReq() throws IOException {
    byte[] body = "testPipeTransferFilePieceReq".getBytes();
    String fileName = "1.tsfile";

    PipeTransferTsFilePieceReq req =
        PipeTransferTsFilePieceReq.toTPipeTransferReq(fileName, 0, body);
    PipeTransferTsFilePieceReq deserializeReq =
        PipeTransferTsFilePieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferFilePieceWithModReq() throws IOException {
    byte[] body = "testPipeTransferFilePieceWithModReq".getBytes();
    String fileName = "1.tsfile.mod";

    PipeTransferTsFilePieceWithModReq req =
        PipeTransferTsFilePieceWithModReq.toTPipeTransferReq(fileName, 0, body);
    PipeTransferTsFilePieceWithModReq deserializeReq =
        PipeTransferTsFilePieceWithModReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferSchemaSnapshotPieceReq() throws IOException {
    byte[] body = "testPipeTransferSchemaSnapshotPieceReq".getBytes();
    String fileName = "1.temp";

    PipeTransferSchemaSnapshotPieceReq req =
        PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq(fileName, 0, body);
    PipeTransferSchemaSnapshotPieceReq deserializeReq =
        PipeTransferSchemaSnapshotPieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferTsFileSealReq() throws IOException {
    String fileName = "1.tsfile";

    PipeTransferTsFileSealReq req = PipeTransferTsFileSealReq.toTPipeTransferReq(fileName, 100);
    PipeTransferTsFileSealReq deserializeReq = PipeTransferTsFileSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getFileLength(), deserializeReq.getFileLength());
  }

  @Test
  public void testPipeTransferSchemaSnapshotSealReq() throws IOException {
    String mTreeSnapshotName = "mtree.snapshot";
    String tLogName = "tlog.txt";
    String databaseName = "root.db";
    // CREATE_TIME_SERIES
    String typeString = "19";

    PipeTransferSchemaSnapshotSealReq req =
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferReq(
            mTreeSnapshotName, 100, tLogName, 10, databaseName, typeString);
    PipeTransferSchemaSnapshotSealReq deserializeReq =
        PipeTransferSchemaSnapshotSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileNames(), deserializeReq.getFileNames());
    Assert.assertEquals(req.getFileLengths(), deserializeReq.getFileLengths());
    Assert.assertEquals(req.getParameters(), deserializeReq.getParameters());
  }

  @Test
  public void testPipeTransferFilePieceResp() throws IOException {
    PipeTransferFilePieceResp resp =
        PipeTransferFilePieceResp.toTPipeTransferResp(RpcUtils.SUCCESS_STATUS, 100);
    PipeTransferFilePieceResp deserializeResp =
        PipeTransferFilePieceResp.fromTPipeTransferResp(resp);

    Assert.assertEquals(resp.getStatus(), deserializeResp.getStatus());
    Assert.assertEquals(resp.getEndWritingOffset(), deserializeResp.getEndWritingOffset());
  }
}
