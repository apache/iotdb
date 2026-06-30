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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.CombineRequest;
import org.apache.iotdb.db.pipe.processor.twostage.state.CountState;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipeDataNodeThriftRequestTest {

  private static final String TIME_PRECISION = "ms";

  @Test
  public void testCombineRequest() throws Exception {
    final CombineRequest req =
        CombineRequest.toTPipeTransferReq("pipe", 1L, 2, "combine", new CountState(123L));
    final CombineRequest deserializeReq = CombineRequest.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals("pipe", deserializeReq.getPipeName());
    Assert.assertEquals(1L, deserializeReq.getCreationTime());
    Assert.assertEquals(2, deserializeReq.getRegionId());
    Assert.assertEquals("combine", deserializeReq.getCombineId());
    Assert.assertTrue(deserializeReq.getState() instanceof CountState);
    Assert.assertEquals(123L, ((CountState) deserializeReq.getState()).getCount());
  }

  @Test
  public void testCombineRequestWithUnexpectedStateClassName() throws Exception {
    final CombineRequest req =
        CombineRequest.toTPipeTransferReq("pipe", 1L, 2, "combine", new CountState(123L));

    final ByteBuffer bodyBuffer = req.body.duplicate();
    final String pipeName = ReadWriteIOUtils.readString(bodyBuffer);
    final long creationTime = ReadWriteIOUtils.readLong(bodyBuffer);
    final int regionId = ReadWriteIOUtils.readInt(bodyBuffer);
    final String combineId = ReadWriteIOUtils.readString(bodyBuffer);
    ReadWriteIOUtils.readString(bodyBuffer);
    final long count = ReadWriteIOUtils.readLong(bodyBuffer);

    final ByteBuffer tamperedBody;
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(pipeName, outputStream);
      ReadWriteIOUtils.write(creationTime, outputStream);
      ReadWriteIOUtils.write(regionId, outputStream);
      ReadWriteIOUtils.write(combineId, outputStream);
      ReadWriteIOUtils.write("java.lang.String", outputStream);
      ReadWriteIOUtils.write(count, outputStream);
      tamperedBody =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    final TPipeTransferReq tamperedReq = new TPipeTransferReq();
    tamperedReq.version = req.version;
    tamperedReq.type = req.type;
    tamperedReq.body = tamperedBody;

    try {
      CombineRequest.fromTPipeTransferReq(tamperedReq);
      Assert.fail("Expected IllegalArgumentException");
    } catch (final IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Unexpected state class"));
    }
  }

  @Test
  public void testPipeTransferDataNodeHandshakeReq() throws IOException {
    final PipeTransferDataNodeHandshakeV1Req req =
        PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(TIME_PRECISION);
    final int originalBodyPosition = req.body.position();
    final PipeTransferDataNodeHandshakeV1Req deserializeReq =
        PipeTransferDataNodeHandshakeV1Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(originalBodyPosition, req.body.position());

    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferDataNodeHandshakeReqFromLegacyV13Body() throws IOException {
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.HANDSHAKE_DATANODE_V1, serializeLegacyHandshakeV1Body(TIME_PRECISION));

    final PipeTransferDataNodeHandshakeV1Req deserializeReq =
        PipeTransferDataNodeHandshakeV1Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(TIME_PRECISION, deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferDataNodeHandshakeV2Req() throws IOException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION, TIME_PRECISION);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, "cluster-a");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, "root");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, "root");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PIPE_NAME, "pipe-a");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PIPE_CREATION_TIME, "1");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, "async");
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE, Boolean.TRUE.toString());

    final PipeTransferDataNodeHandshakeV2Req req =
        PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params);
    final int originalBodyPosition = req.body.position();
    final PipeTransferDataNodeHandshakeV2Req deserializeReq =
        PipeTransferDataNodeHandshakeV2Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(originalBodyPosition, req.body.position());
    Assert.assertEquals(params, deserializeReq.getParams());
  }

  @Test
  public void testPipeTransferDataNodeHandshakeV2ReqFromLegacyV13Body() throws IOException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION, TIME_PRECISION);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, "cluster");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, "root");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, "root");

    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.HANDSHAKE_DATANODE_V2, serializeLegacyHandshakeV2Body(params));

    final PipeTransferDataNodeHandshakeV2Req deserializeReq =
        PipeTransferDataNodeHandshakeV2Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(params, deserializeReq.getParams());
  }

  @Test
  public void testPipeTransferInsertNodeReq() {
    final PipeTransferTabletInsertNodeReq req =
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
    final PipeTransferTabletInsertNodeReq deserializeReq =
        PipeTransferTabletInsertNodeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getInsertNode(), deserializeReq.getInsertNode());

    final Statement statement = req.constructStatement();
    final List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath(new String[] {"root", "sg", "d", "s"}));
    Assert.assertEquals(statement.getPaths(), paths);
  }

  @Test
  public void testPipeTransferInsertNodeReqFromLegacyV13Body() {
    final InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"root", "sg", "d"}),
            false,
            new String[] {"s"},
            new TSDataType[] {TSDataType.INT32},
            1,
            new Object[] {1},
            false);
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_TABLET_INSERT_NODE, node.serializeToByteBuffer());

    final PipeTransferTabletInsertNodeReq deserializeReq =
        PipeTransferTabletInsertNodeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(node, deserializeReq.getInsertNode());

    final List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath(new String[] {"root", "sg", "d", "s"}));
    Assert.assertEquals(paths, deserializeReq.constructStatement().getPaths());
  }

  @Test
  public void testPipeTransferInsertNodeReqV2() {
    final PipeTransferTabletInsertNodeReqV2 req =
        PipeTransferTabletInsertNodeReqV2.toTPipeTransferReq(
            new InsertRowNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "sg", "d"}),
                false,
                new String[] {"s"},
                new TSDataType[] {TSDataType.INT32},
                1,
                new Object[] {1},
                false),
            "test");
    final PipeTransferTabletInsertNodeReqV2 deserializeReq =
        PipeTransferTabletInsertNodeReqV2.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getInsertNode(), deserializeReq.getInsertNode());
    Assert.assertEquals(req.getDataBaseName(), deserializeReq.getDataBaseName());

    final InsertBaseStatement statement = req.constructStatement();
    final List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath(new String[] {"root", "sg", "d", "s"}));

    Assert.assertEquals(statement.getPaths(), paths);
    Assert.assertTrue(statement.isWriteToTable());
    Assert.assertTrue(statement.getDatabaseName().isPresent());
    Assert.assertEquals(statement.getDatabaseName().get(), "test");
  }

  @Test
  public void testPipeTransferInsertNodeReqV2WithTreeModelDatabase() {
    final PipeTransferTabletInsertNodeReqV2 req =
        PipeTransferTabletInsertNodeReqV2.toTPipeTransferReq(
            new InsertRowNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "test", "d"}),
                false,
                new String[] {"s"},
                new TSDataType[] {TSDataType.INT32},
                1,
                new Object[] {1},
                false),
            "root.test");
    final PipeTransferTabletInsertNodeReqV2 deserializeReq =
        PipeTransferTabletInsertNodeReqV2.fromTPipeTransferReq(req);

    final InsertBaseStatement statement = deserializeReq.constructStatement();
    final List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath(new String[] {"root", "test", "d", "s"}));

    Assert.assertEquals(statement.getPaths(), paths);
    Assert.assertFalse(statement.isWriteToTable());
    Assert.assertTrue(statement.getDatabaseName().isPresent());
    Assert.assertEquals("root.test", statement.getDatabaseName().get());
  }

  @Test
  public void testPipeTransferTabletBinaryReq() {
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    final PipeTransferTabletBinaryReq req =
        PipeTransferTabletBinaryReq.toTPipeTransferReq(ByteBuffer.wrap(new byte[] {'a', 'b'}));
    final PipeTransferTabletBinaryReq deserializeReq =
        PipeTransferTabletBinaryReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(
        new byte[] {'a', 'b'}, byteBufferToByteArray(deserializeReq.getByteBuffer()));
  }

  @Test
  public void testPipeTransferTabletBinaryReqFromLegacyV13Body() {
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_TABLET_BINARY, ByteBuffer.wrap(new byte[] {'a', 'b'}));
    final PipeTransferTabletBinaryReq deserializeReq =
        PipeTransferTabletBinaryReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(
        new byte[] {'a', 'b'}, byteBufferToByteArray(deserializeReq.getByteBuffer()));
  }

  @Test
  public void testPipeTransferTabletBinaryReqV2() throws IOException {
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    final PipeTransferTabletBinaryReqV2 req =
        PipeTransferTabletBinaryReqV2.toTPipeTransferReq(
            ByteBuffer.wrap(new byte[] {'a', 'b'}), "test");
    final PipeTransferTabletBinaryReqV2 deserializeReq =
        PipeTransferTabletBinaryReqV2.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
    Assert.assertEquals(req.getDataBaseName(), deserializeReq.getDataBaseName());
  }

  @Test
  public void testPipeTransferPlanNodeReq() {
    final PipeTransferPlanNodeReq req =
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

    final PipeTransferPlanNodeReq deserializeReq =
        PipeTransferPlanNodeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getPlanNode(), deserializeReq.getPlanNode());
  }

  @Test
  public void testPipeTransferPlanNodeReqBytesWithPartialDirectByteBuffer() throws IOException {
    final CreateAlignedTimeSeriesNode node =
        new CreateAlignedTimeSeriesNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"root", "sg", "d"}),
            Collections.singletonList("s"),
            Collections.singletonList(TSDataType.INT32),
            Collections.singletonList(TSEncoding.PLAIN),
            Collections.singletonList(CompressionType.UNCOMPRESSED),
            null,
            null,
            null);
    final byte[] serializedPlanNodeBytes = byteBufferToByteArray(node.serializeToByteBuffer());
    final ByteBuffer serializedPlanNode =
        ByteBuffer.allocateDirect(serializedPlanNodeBytes.length + 2);
    serializedPlanNode.put((byte) 0);
    serializedPlanNode.put(serializedPlanNodeBytes);
    serializedPlanNode.put((byte) 1);
    serializedPlanNode.flip();
    serializedPlanNode.position(1);
    serializedPlanNode.limit(1 + serializedPlanNodeBytes.length);

    final byte[] transferBytes = PipeTransferPlanNodeReq.toTPipeTransferBytes(serializedPlanNode);

    Assert.assertEquals(1, serializedPlanNode.position());
    Assert.assertEquals(1 + serializedPlanNodeBytes.length, serializedPlanNode.limit());
    final ByteBuffer transferBuffer = ByteBuffer.wrap(transferBytes);
    Assert.assertEquals(
        IoTDBSinkRequestVersion.VERSION_1.getVersion(), ReadWriteIOUtils.readByte(transferBuffer));
    Assert.assertEquals(
        PipeRequestType.TRANSFER_PLAN_NODE.getType(), ReadWriteIOUtils.readShort(transferBuffer));
    Assert.assertEquals(node, PlanNodeType.deserialize(transferBuffer));
    Assert.assertEquals(0, ReadWriteIOUtils.readInt(transferBuffer));
    Assert.assertFalse(transferBuffer.hasRemaining());
  }

  @Test
  public void testPipeTransferPlanNodeReqFromLegacyV13SchemaPlanBody() {
    final CreateAlignedTimeSeriesNode node =
        new CreateAlignedTimeSeriesNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"root", "sg", "d"}),
            Collections.singletonList("s"),
            Collections.singletonList(TSDataType.INT32),
            Collections.singletonList(TSEncoding.PLAIN),
            Collections.singletonList(CompressionType.UNCOMPRESSED),
            null,
            null,
            null);
    final TPipeTransferReq req =
        legacyTransferReq(PipeRequestType.TRANSFER_PLAN_NODE, node.serializeToByteBuffer());

    final PipeTransferPlanNodeReq deserializeReq =
        PipeTransferPlanNodeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(node, deserializeReq.getPlanNode());
  }

  @Test
  public void testPipeTransferTabletReq() {
    try {
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.FLOAT));
      schemaList.add(new MeasurementSchema("s4", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));
      schemaList.add(new MeasurementSchema("s6", TSDataType.TEXT));
      schemaList.add(new MeasurementSchema("s7", TSDataType.TIMESTAMP));
      schemaList.add(new MeasurementSchema("s8", TSDataType.DATE));
      schemaList.add(new MeasurementSchema("s9", TSDataType.BLOB));
      schemaList.add(new MeasurementSchema("s10", TSDataType.STRING));
      final Tablet t = new Tablet("root.sg.d", schemaList, 1024);
      t.addTimestamp(0, 2000);
      t.addTimestamp(1, 1000);
      t.addValue("s1", 0, 2);
      t.addValue("s6", 0, "2");
      t.addValue("s8", 0, LocalDate.of(2024, 2, 2));
      t.addValue("s9", 0, new Binary("2", TSFileConfig.STRING_CHARSET));
      t.addValue("s10", 0, "2");
      t.addValue("s1", 1, 1);
      t.addValue("s6", 1, "1");
      t.addValue("s8", 1, LocalDate.of(2024, 1, 1));
      t.addValue("s9", 1, new Binary("1", TSFileConfig.STRING_CHARSET));
      t.addValue("s10", 1, "1");
      final PipeTransferTabletRawReq req = PipeTransferTabletRawReq.toTPipeTransferReq(t, false);
      final PipeTransferTabletRawReq deserializeReq =
          PipeTransferTabletRawReq.fromTPipeTransferReq(req);

      Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
      Assert.assertEquals(req.getType(), deserializeReq.getType());

      final Statement statement =
          req.constructStatement(); // will call PipeTransferTabletRawReq.sortTablet() here
      List<PartialPath> paths = new ArrayList<>();
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s1"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s2"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s3"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s4"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s5"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s6"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s7"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s8"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s9"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s10"}));
      Assert.assertEquals(statement.getPaths(), paths);
    } catch (final IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPipeTransferTabletReqV2() {
    try {
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.FLOAT));
      schemaList.add(new MeasurementSchema("s4", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));
      schemaList.add(new MeasurementSchema("s6", TSDataType.TEXT));
      schemaList.add(new MeasurementSchema("s7", TSDataType.TIMESTAMP));
      schemaList.add(new MeasurementSchema("s8", TSDataType.DATE));
      schemaList.add(new MeasurementSchema("s9", TSDataType.BLOB));
      schemaList.add(new MeasurementSchema("s10", TSDataType.STRING));
      final Tablet t = new Tablet("t", schemaList, 1024);
      t.addTimestamp(0, 2000);
      t.addTimestamp(1, 1000);
      t.addValue("s1", 0, 2);
      t.addValue("s6", 0, "2");
      t.addValue("s8", 0, LocalDate.of(2024, 2, 2));
      t.addValue("s9", 0, new Binary("2", TSFileConfig.STRING_CHARSET));
      t.addValue("s10", 0, "2");
      t.addValue("s1", 1, 1);
      t.addValue("s6", 1, "1");
      t.addValue("s8", 1, LocalDate.of(2024, 1, 1));
      t.addValue("s9", 1, new Binary("1", TSFileConfig.STRING_CHARSET));
      t.addValue("s10", 1, "1");
      final PipeTransferTabletRawReqV2 req =
          PipeTransferTabletRawReqV2.toTPipeTransferReq(t, false, "test");
      final PipeTransferTabletRawReqV2 deserializeReq =
          PipeTransferTabletRawReqV2.fromTPipeTransferReq(req);

      Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
      Assert.assertEquals(req.getType(), deserializeReq.getType());
      Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

      final InsertBaseStatement statement =
          req.constructStatement(); // will call PipeTransferTabletRawReq.sortTablet() here
      List<PartialPath> paths = new ArrayList<>();
      paths.add(new PartialPath(new String[] {"t", "s1"}));
      paths.add(new PartialPath(new String[] {"t", "s2"}));
      paths.add(new PartialPath(new String[] {"t", "s3"}));
      paths.add(new PartialPath(new String[] {"t", "s4"}));
      paths.add(new PartialPath(new String[] {"t", "s5"}));
      paths.add(new PartialPath(new String[] {"t", "s6"}));
      paths.add(new PartialPath(new String[] {"t", "s7"}));
      paths.add(new PartialPath(new String[] {"t", "s8"}));
      paths.add(new PartialPath(new String[] {"t", "s9"}));
      paths.add(new PartialPath(new String[] {"t", "s10"}));
      Assert.assertEquals(statement.getPaths(), paths);

      Assert.assertTrue(statement.isWriteToTable());
      Assert.assertTrue(statement.getDatabaseName().isPresent());
      Assert.assertEquals(statement.getDatabaseName().get(), "test");
      Assert.assertEquals(t.getColumnTypes(), deserializeReq.getTablet().getColumnTypes());
    } catch (final IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPipeTransferTabletReqV2WithTreeModelDatabase() {
    try {
      final List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));
      schemaList.add(new MeasurementSchema("s2", TSDataType.TEXT));
      final Tablet tablet = new Tablet("root.test.d", schemaList, 8);
      tablet.addTimestamp(0, 2);
      tablet.addTimestamp(1, 1);
      tablet.addValue("s1", 0, 2);
      tablet.addValue("s2", 0, "2");
      tablet.addValue("s1", 1, 1);
      tablet.addValue("s2", 1, "1");

      final PipeTransferTabletRawReqV2 req =
          PipeTransferTabletRawReqV2.toTPipeTransferReq(tablet, false, "root.test");
      final PipeTransferTabletRawReqV2 deserializeReq =
          PipeTransferTabletRawReqV2.fromTPipeTransferReq(req);

      final InsertBaseStatement statement = deserializeReq.constructStatement();
      final List<PartialPath> paths = new ArrayList<>();
      paths.add(new PartialPath(new String[] {"root", "test", "d", "s1"}));
      paths.add(new PartialPath(new String[] {"root", "test", "d", "s2"}));

      Assert.assertEquals(paths, statement.getPaths());
      Assert.assertFalse(statement.isWriteToTable());
      Assert.assertTrue(statement.getDatabaseName().isPresent());
      Assert.assertEquals("root.test", statement.getDatabaseName().get());
    } catch (final IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPipeTransferTabletBatchReq() throws IOException {
    final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
    final List<ByteBuffer> tabletBuffers = new ArrayList<>();

    final InsertRowNode node =
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

    // Raw buffer
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s4", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));
    schemaList.add(new MeasurementSchema("s6", TSDataType.TEXT));
    schemaList.add(new MeasurementSchema("s7", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s8", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s9", TSDataType.BLOB));
    schemaList.add(new MeasurementSchema("s10", TSDataType.STRING));

    final Tablet t = new Tablet("root.sg.d", schemaList, 1024);
    t.addTimestamp(0, 2000);
    t.addTimestamp(1, 1000);
    t.addValue("s1", 0, 2);
    t.addValue("s6", 0, "2");
    t.addValue("s8", 0, LocalDate.of(2024, 2, 2));
    t.addValue("s9", 0, new Binary("2", TSFileConfig.STRING_CHARSET));
    t.addValue("s10", 0, "2");
    t.addValue("s1", 1, 1);
    t.addValue("s6", 1, "1");
    t.addValue("s8", 1, LocalDate.of(2024, 1, 1));
    t.addValue("s9", 1, new Binary("1", TSFileConfig.STRING_CHARSET));
    t.addValue("s10", 1, "1");

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      t.serialize(outputStream);
      ReadWriteIOUtils.write(false, outputStream);
      tabletBuffers.add(
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    }

    final PipeTransferTabletBatchReq req =
        PipeTransferTabletBatchReq.toTPipeTransferReq(insertNodeBuffers, tabletBuffers);

    final PipeTransferTabletBatchReq deserializedReq =
        PipeTransferTabletBatchReq.fromTPipeTransferReq(req);

    Assert.assertEquals(node, deserializedReq.getInsertNodeReqs().get(0).getInsertNode());
    Assert.assertEquals(t, deserializedReq.getTabletReqs().get(0).getTablet());
    Assert.assertFalse(deserializedReq.getTabletReqs().get(0).getIsAligned());
  }

  @Test
  public void testPipeTransferTabletBatchReqInternsRepeatedMeasurementNames() throws IOException {
    final List<ByteBuffer> tabletBuffers = new ArrayList<>();
    tabletBuffers.add(
        serializeTablet(createSingleValueTablet(new String("root.sg.d"), new String("s1")), false));
    tabletBuffers.add(
        serializeTablet(createSingleValueTablet(new String("root.sg.d"), new String("s1")), false));

    final PipeTransferTabletBatchReq deserializedReq =
        PipeTransferTabletBatchReq.fromTPipeTransferReq(
            PipeTransferTabletBatchReq.toTPipeTransferReq(Collections.emptyList(), tabletBuffers));
    final Pair<InsertRowsStatement, InsertMultiTabletsStatement> statements =
        deserializedReq.constructStatements();
    final List<InsertTabletStatement> insertTabletStatements =
        statements.getRight().getInsertTabletStatementList();

    Assert.assertEquals(2, insertTabletStatements.size());
    Assert.assertSame(
        insertTabletStatements.get(0).getMeasurements()[0],
        insertTabletStatements.get(1).getMeasurements()[0]);
  }

  @Test
  public void testPipeTransferTabletBatchReqWithLegacyTabletFormat() throws IOException {
    final List<ByteBuffer> tabletBuffers = new ArrayList<>();
    tabletBuffers.add(serializeLegacyTabletRawBuffer(false));
    tabletBuffers.add(serializeLegacyTabletRawBuffer(true));

    final PipeTransferTabletBatchReq req =
        PipeTransferTabletBatchReq.toTPipeTransferReq(Collections.emptyList(), tabletBuffers);

    final PipeTransferTabletBatchReq deserializedReq =
        PipeTransferTabletBatchReq.fromTPipeTransferReq(req);

    Assert.assertEquals(2, deserializedReq.getTabletReqs().size());
    Assert.assertFalse(deserializedReq.getTabletReqs().get(0).getIsAligned());
    Assert.assertTrue(deserializedReq.getTabletReqs().get(1).getIsAligned());

    assertLegacyTabletStatement(deserializedReq.getTabletReqs().get(0).constructStatement());
    assertLegacyTabletStatement(deserializedReq.getTabletReqs().get(1).constructStatement());
  }

  @Test
  public void testPipeTransferTabletBatchReqFromLegacyV13Body() throws IOException {
    final InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"root", "sg", "d"}),
            false,
            new String[] {"s"},
            new TSDataType[] {TSDataType.INT32},
            1,
            new Object[] {1},
            false);
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_TABLET_BATCH,
            serializeLegacyTabletBatchBody(
                Collections.singletonList(node.serializeToByteBuffer()),
                Collections.singletonList(serializeLegacyTabletRawBuffer(false))));

    final PipeTransferTabletBatchReq deserializedReq =
        PipeTransferTabletBatchReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializedReq.getVersion());
    Assert.assertEquals(req.getType(), deserializedReq.getType());
    Assert.assertEquals(1, deserializedReq.getInsertNodeReqs().size());
    Assert.assertEquals(1, deserializedReq.getTabletReqs().size());
    Assert.assertEquals(node, deserializedReq.getInsertNodeReqs().get(0).getInsertNode());
    assertLegacyTabletStatement(deserializedReq.getTabletReqs().get(0).constructStatement());
  }

  @Test
  public void testPipeTransferTabletBatchReqFromLegacyV13BodyWithBinaryReqs() throws IOException {
    final InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"root", "sg", "d"}),
            false,
            new String[] {"s"},
            new TSDataType[] {TSDataType.INT32},
            1,
            new Object[] {1},
            false);
    final ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[] {'a', 'b'});

    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_TABLET_BATCH,
            serializeLegacyTabletBatchBody(
                Collections.singletonList(binaryBuffer),
                Collections.singletonList(node.serializeToByteBuffer()),
                Collections.singletonList(serializeLegacyTabletRawBuffer(false))));

    final PipeTransferTabletBatchReq deserializedReq =
        PipeTransferTabletBatchReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializedReq.getVersion());
    Assert.assertEquals(req.getType(), deserializedReq.getType());
    Assert.assertEquals(1, deserializedReq.getBinaryReqs().size());
    Assert.assertArrayEquals(
        new byte[] {'a', 'b'},
        byteBufferToByteArray(deserializedReq.getBinaryReqs().get(0).getByteBuffer()));
    Assert.assertEquals(1, deserializedReq.getInsertNodeReqs().size());
    Assert.assertEquals(1, deserializedReq.getTabletReqs().size());
    Assert.assertEquals(node, deserializedReq.getInsertNodeReqs().get(0).getInsertNode());
    assertLegacyTabletStatement(deserializedReq.getTabletReqs().get(0).constructStatement());
  }

  @Test
  public void testPipeTransferTabletRawReqWithLegacyTabletFormat() throws IOException {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_RAW.getType();
    req.body = serializeLegacyTabletRawBuffer(true);

    final PipeTransferTabletRawReq deserializedReq =
        PipeTransferTabletRawReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializedReq.getVersion());
    Assert.assertEquals(req.getType(), deserializedReq.getType());
    Assert.assertTrue(deserializedReq.getIsAligned());
    assertLegacyTabletStatement(deserializedReq.constructStatement());
  }

  @Test
  public void testPipeTransferTabletRawReqWithSingleColumnLegacyTabletFormat() throws IOException {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_RAW.getType();
    req.body = serializeSingleColumnLegacyTabletRawBuffer(false);

    final PipeTransferTabletRawReq deserializedReq =
        PipeTransferTabletRawReq.fromTPipeTransferReq(req);

    Assert.assertFalse(deserializedReq.getIsAligned());
    final InsertTabletStatement statement = deserializedReq.constructStatement();
    Assert.assertEquals("root.sg.d", statement.getDevicePath().getFullPath());
    Assert.assertArrayEquals(new String[] {"s1"}, statement.getMeasurements());
    Assert.assertArrayEquals(new TSDataType[] {TSDataType.INT32}, statement.getDataTypes());
    Assert.assertEquals(2, statement.getRowCount());
    Assert.assertArrayEquals(new long[] {1700000000000L, 1700000000001L}, statement.getTimes());
    Assert.assertArrayEquals(new int[] {2, 1}, (int[]) statement.getColumns()[0]);
  }

  @Test
  public void testPipeTransferTabletBatchReqRejectsTruncatedRawTablet() throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(0, outputStream);
      ReadWriteIOUtils.write(0, outputStream);
      ReadWriteIOUtils.write(1, outputStream);
      outputStream.write(new byte[] {1, 0, 0, 0, 0, 0});

      final TPipeTransferReq req =
          legacyTransferReq(
              PipeRequestType.TRANSFER_TABLET_BATCH,
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));

      try {
        PipeTransferTabletBatchReq.fromTPipeTransferReq(req);
        Assert.fail("Expected IllegalArgumentException");
      } catch (final IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("Failed to deserialize raw tablet"));
        Assert.assertTrue(
            e.getCause().getMessage().contains("Failed to deserialize raw tablet request"));
      }
    }
  }

  @Test
  public void testPipeTransferTabletBatchReqV2() throws IOException {
    final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
    final List<ByteBuffer> tabletBuffers = new ArrayList<>();
    final List<String> insertDataBase = new ArrayList<>();
    final List<String> tabletDataBase = new ArrayList<>();

    final InsertRowNode node =
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
    insertDataBase.add("test");

    // Raw buffer
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s4", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));
    schemaList.add(new MeasurementSchema("s6", TSDataType.TEXT));
    schemaList.add(new MeasurementSchema("s7", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s8", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s9", TSDataType.BLOB));
    schemaList.add(new MeasurementSchema("s10", TSDataType.STRING));

    final Tablet t = new Tablet("root.sg.d", schemaList, 1024);
    t.addTimestamp(0, 2000);
    t.addTimestamp(1, 1000);
    t.addValue("s1", 0, 2);
    t.addValue("s6", 0, "2");
    t.addValue("s8", 0, LocalDate.of(2024, 2, 2));
    t.addValue("s9", 0, new Binary("2", TSFileConfig.STRING_CHARSET));
    t.addValue("s10", 0, "2");
    t.addValue("s1", 1, 1);
    t.addValue("s6", 1, "1");
    t.addValue("s8", 1, LocalDate.of(2024, 1, 1));
    t.addValue("s9", 1, new Binary("1", TSFileConfig.STRING_CHARSET));
    t.addValue("s10", 1, "1");

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      t.serialize(outputStream);
      ReadWriteIOUtils.write(true, outputStream);
      tabletBuffers.add(
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      tabletDataBase.add("test");
    }

    final PipeTransferTabletBatchReqV2 req =
        PipeTransferTabletBatchReqV2.toTPipeTransferReq(
            insertNodeBuffers, tabletBuffers, insertDataBase, tabletDataBase);

    final PipeTransferTabletBatchReqV2 deserializedReq =
        PipeTransferTabletBatchReqV2.fromTPipeTransferReq(req);

    Assert.assertEquals(node, deserializedReq.getInsertNodeReqs().get(0).getInsertNode());
    Assert.assertEquals(t, deserializedReq.getTabletReqs().get(0).getTablet());
    Assert.assertTrue(deserializedReq.getTabletReqs().get(0).getIsAligned());

    Assert.assertEquals("test", deserializedReq.getTabletReqs().get(0).getDataBaseName());
    Assert.assertEquals("test", deserializedReq.getInsertNodeReqs().get(0).getDataBaseName());
  }

  @Test
  public void testPipeTransferTabletBatchReqV2WithMultipleTreeModelDatabases() throws IOException {
    final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
    final List<ByteBuffer> tabletBuffers = new ArrayList<>();
    final List<String> insertDataBase = new ArrayList<>();
    final List<String> tabletDataBase = new ArrayList<>();

    insertNodeBuffers.add(
        new InsertRowNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "db1", "d"}),
                false,
                new String[] {"s"},
                new TSDataType[] {TSDataType.INT32},
                1,
                new Object[] {1},
                false)
            .serializeToByteBuffer());
    insertDataBase.add("root.db1");

    insertNodeBuffers.add(
        new InsertRowNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "db2", "d"}),
                false,
                new String[] {"s"},
                new TSDataType[] {TSDataType.INT32},
                2,
                new Object[] {2},
                false)
            .serializeToByteBuffer());
    insertDataBase.add("root.db2");

    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));

    final Tablet db1Tablet = new Tablet("root.db1.d", schemaList, 8);
    db1Tablet.addTimestamp(0, 1);
    db1Tablet.addValue("s1", 0, 1);
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      db1Tablet.serialize(outputStream);
      ReadWriteIOUtils.write(false, outputStream);
      tabletBuffers.add(
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      tabletDataBase.add("root.db1");
    }

    final Tablet db2Tablet = new Tablet("root.db2.d", schemaList, 8);
    db2Tablet.addTimestamp(0, 2);
    db2Tablet.addValue("s1", 0, 2);
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      db2Tablet.serialize(outputStream);
      ReadWriteIOUtils.write(false, outputStream);
      tabletBuffers.add(
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      tabletDataBase.add("root.db2");
    }

    final PipeTransferTabletBatchReqV2 req =
        PipeTransferTabletBatchReqV2.toTPipeTransferReq(
            insertNodeBuffers, tabletBuffers, insertDataBase, tabletDataBase);
    final PipeTransferTabletBatchReqV2 deserializedReq =
        PipeTransferTabletBatchReqV2.fromTPipeTransferReq(req);

    final List<InsertBaseStatement> statements = deserializedReq.constructStatements();
    final Set<String> insertRowsDatabases = new HashSet<>();
    final Set<String> insertTabletsDatabases = new HashSet<>();

    for (final InsertBaseStatement statement : statements) {
      Assert.assertFalse(statement.isWriteToTable());
      Assert.assertTrue(statement.getDatabaseName().isPresent());
      if (statement instanceof InsertRowsStatement) {
        insertRowsDatabases.add(statement.getDatabaseName().get());
      } else if (statement instanceof InsertMultiTabletsStatement) {
        insertTabletsDatabases.add(statement.getDatabaseName().get());
      } else {
        Assert.fail("Unexpected statement type: " + statement.getClass().getName());
      }
    }

    Assert.assertEquals(
        new HashSet<>(java.util.Arrays.asList("root.db1", "root.db2")), insertRowsDatabases);
    Assert.assertEquals(
        new HashSet<>(java.util.Arrays.asList("root.db1", "root.db2")), insertTabletsDatabases);
  }

  @Test
  public void testPipeTransferFilePieceReq() throws IOException {
    final byte[] body = "testPipeTransferFilePieceReq".getBytes();
    final String fileName = "1.tsfile";

    final PipeTransferTsFilePieceReq req =
        PipeTransferTsFilePieceReq.toTPipeTransferReq(fileName, 0, body);
    final PipeTransferTsFilePieceReq deserializeReq =
        PipeTransferTsFilePieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferFilePieceWithModReq() throws IOException {
    final byte[] body = "testPipeTransferFilePieceWithModReq".getBytes();
    final String fileName = "1.tsfile.mod";

    final PipeTransferTsFilePieceWithModReq req =
        PipeTransferTsFilePieceWithModReq.toTPipeTransferReq(fileName, 0, body);
    final PipeTransferTsFilePieceWithModReq deserializeReq =
        PipeTransferTsFilePieceWithModReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferSchemaSnapshotPieceReq() throws IOException {
    final byte[] body = "testPipeTransferSchemaSnapshotPieceReq".getBytes();
    final String fileName = "1.temp";

    final PipeTransferSchemaSnapshotPieceReq req =
        PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq(fileName, 0, body);
    final PipeTransferSchemaSnapshotPieceReq deserializeReq =
        PipeTransferSchemaSnapshotPieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferFilePieceReqsFromLegacyV13Bodies() throws IOException {
    final byte[] body = "legacyPiece".getBytes();

    final PipeTransferTsFilePieceReq tsFilePieceReq =
        PipeTransferTsFilePieceReq.fromTPipeTransferReq(
            legacyTransferReq(
                PipeRequestType.TRANSFER_TS_FILE_PIECE,
                serializeLegacyFilePieceBody("1.tsfile", 1L, body)));
    Assert.assertEquals("1.tsfile", tsFilePieceReq.getFileName());
    Assert.assertEquals(1L, tsFilePieceReq.getStartWritingOffset());
    Assert.assertArrayEquals(body, tsFilePieceReq.getFilePiece());

    final PipeTransferTsFilePieceWithModReq tsFilePieceWithModReq =
        PipeTransferTsFilePieceWithModReq.fromTPipeTransferReq(
            legacyTransferReq(
                PipeRequestType.TRANSFER_TS_FILE_PIECE_WITH_MOD,
                serializeLegacyFilePieceBody("1.tsfile.mod", 2L, body)));
    Assert.assertEquals("1.tsfile.mod", tsFilePieceWithModReq.getFileName());
    Assert.assertEquals(2L, tsFilePieceWithModReq.getStartWritingOffset());
    Assert.assertArrayEquals(body, tsFilePieceWithModReq.getFilePiece());

    final PipeTransferSchemaSnapshotPieceReq schemaSnapshotPieceReq =
        PipeTransferSchemaSnapshotPieceReq.fromTPipeTransferReq(
            legacyTransferReq(
                PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_PIECE,
                serializeLegacyFilePieceBody("schema.snapshot", 3L, body)));
    Assert.assertEquals("schema.snapshot", schemaSnapshotPieceReq.getFileName());
    Assert.assertEquals(3L, schemaSnapshotPieceReq.getStartWritingOffset());
    Assert.assertArrayEquals(body, schemaSnapshotPieceReq.getFilePiece());
  }

  @Test
  public void testPipeTransferTsFileSealReq() throws IOException {
    final String fileName = "1.tsfile";

    final PipeTransferTsFileSealReq req =
        PipeTransferTsFileSealReq.toTPipeTransferReq(fileName, 100);
    final PipeTransferTsFileSealReq deserializeReq =
        PipeTransferTsFileSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getFileLength(), deserializeReq.getFileLength());
  }

  @Test
  public void testPipeTransferTsFileSealReqFromLegacyV13Body() throws IOException {
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_TS_FILE_SEAL, serializeLegacyFileSealV1Body("1.tsfile", 100L));

    final PipeTransferTsFileSealReq deserializeReq =
        PipeTransferTsFileSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals("1.tsfile", deserializeReq.getFileName());
    Assert.assertEquals(100L, deserializeReq.getFileLength());
  }

  @Test
  public void testPipeTransferTsFileSealWithModReq() throws IOException {
    final String modFileName = "1.tsfile.mod";
    final String tsFileName = "1.tsfile";

    final PipeTransferTsFileSealWithModReq req =
        PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
            modFileName, 10, tsFileName, 100, "root.db");
    final PipeTransferTsFileSealWithModReq deserializeReq =
        PipeTransferTsFileSealWithModReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(Arrays.asList(modFileName, tsFileName), deserializeReq.getFileNames());
    Assert.assertEquals(Arrays.asList(10L, 100L), deserializeReq.getFileLengths());
    Assert.assertEquals("root.db", deserializeReq.getDatabaseNameByTsFileName());
  }

  @Test
  public void testPipeTransferTsFileSealWithModReqFromLegacyV13BodyWithoutDatabaseName()
      throws IOException {
    final String modFileName = "1.tsfile.mod";
    final String tsFileName = "1.tsfile";
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD,
            serializeLegacyFileSealV2Body(
                Arrays.asList(modFileName, tsFileName),
                Arrays.asList(10L, 100L),
                Collections.emptyMap()));

    final PipeTransferTsFileSealWithModReq deserializeReq =
        PipeTransferTsFileSealWithModReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(Arrays.asList(modFileName, tsFileName), deserializeReq.getFileNames());
    Assert.assertEquals(Arrays.asList(10L, 100L), deserializeReq.getFileLengths());
    Assert.assertTrue(deserializeReq.getParameters().isEmpty());
    Assert.assertNull(deserializeReq.getDatabaseNameByTsFileName());
  }

  @Test
  public void testPipeTransferTsFileSealWithModReqFromLegacyV13BodyWithNullDatabaseName()
      throws IOException {
    final String modFileName = "1.tsfile.mod";
    final String tsFileName = "1.tsfile";
    final Map<String, String> parameters = new HashMap<>();
    parameters.put("DATABASE_NAME_" + tsFileName, null);
    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD,
            serializeLegacyFileSealV2Body(
                Arrays.asList(modFileName, tsFileName), Arrays.asList(10L, 100L), parameters));

    final PipeTransferTsFileSealWithModReq deserializeReq =
        PipeTransferTsFileSealWithModReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(Arrays.asList(modFileName, tsFileName), deserializeReq.getFileNames());
    Assert.assertEquals(Arrays.asList(10L, 100L), deserializeReq.getFileLengths());
    Assert.assertEquals(parameters, deserializeReq.getParameters());
    Assert.assertNull(deserializeReq.getDatabaseNameByTsFileName());
  }

  @Test
  public void testPipeTransferSchemaSnapshotSealReq() throws IOException {
    final String mTreeSnapshotName = SchemaConstant.MTREE_SNAPSHOT;
    final String tLogName = SchemaConstant.TAG_LOG;
    final String attributeSnapshotName = SchemaConstant.DEVICE_ATTRIBUTE_SNAPSHOT;
    final String databaseName = "root.db";
    // CREATE_TIME_SERIES
    final String typeString = "19";

    final PipeTransferSchemaSnapshotSealReq req =
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferReq(
            "root.**",
            "db",
            "table",
            true,
            true,
            mTreeSnapshotName,
            100,
            tLogName,
            10,
            attributeSnapshotName,
            10,
            databaseName,
            typeString);
    final PipeTransferSchemaSnapshotSealReq deserializeReq =
        PipeTransferSchemaSnapshotSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());

    Assert.assertEquals(req.getFileNames(), deserializeReq.getFileNames());
    Assert.assertEquals(req.getFileLengths(), deserializeReq.getFileLengths());
    Assert.assertEquals(req.getParameters(), deserializeReq.getParameters());
  }

  @Test
  public void testPipeTransferSchemaSnapshotSealReqFromLegacyV13Body() throws IOException {
    final String mTreeSnapshotName = SchemaConstant.MTREE_SNAPSHOT;
    final String tLogName = SchemaConstant.TAG_LOG;
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(ColumnHeaderConstant.PATH_PATTERN, "root.**");
    parameters.put(ColumnHeaderConstant.DATABASE, "root.db");
    parameters.put(ColumnHeaderConstant.TYPE, "19");

    final TPipeTransferReq req =
        legacyTransferReq(
            PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL,
            serializeLegacyFileSealV2Body(
                Arrays.asList(mTreeSnapshotName, tLogName), Arrays.asList(100L, 10L), parameters));
    final PipeTransferSchemaSnapshotSealReq deserializeReq =
        PipeTransferSchemaSnapshotSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertEquals(Arrays.asList(mTreeSnapshotName, tLogName), deserializeReq.getFileNames());
    Assert.assertEquals(Arrays.asList(100L, 10L), deserializeReq.getFileLengths());
    Assert.assertEquals(parameters, deserializeReq.getParameters());
    Assert.assertTrue(
        PipeTransferFileSealReqV2.isTreeModelDataAllowedToBeCaptured(
            deserializeReq.getParameters()));
    Assert.assertFalse(
        PipeTransferFileSealReqV2.isTableModelDataAllowedToBeCaptured(
            deserializeReq.getParameters()));
  }

  @Test
  public void testPipeTransferFilePieceResp() throws IOException {
    final PipeTransferFilePieceResp resp =
        PipeTransferFilePieceResp.toTPipeTransferResp(RpcUtils.SUCCESS_STATUS, 100);
    final PipeTransferFilePieceResp deserializeResp =
        PipeTransferFilePieceResp.fromTPipeTransferResp(resp);

    Assert.assertEquals(resp.getStatus(), deserializeResp.getStatus());
    Assert.assertEquals(resp.getEndWritingOffset(), deserializeResp.getEndWritingOffset());
  }

  private static TPipeTransferReq legacyTransferReq(
      final PipeRequestType requestType, final ByteBuffer body) {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = requestType.getType();
    req.body = body;
    return req;
  }

  private static ByteBuffer serializeLegacyFileSealV2Body(
      final List<String> fileNames,
      final List<Long> fileLengths,
      final Map<String, String> parameters)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileNames.size(), outputStream);
      for (final String fileName : fileNames) {
        ReadWriteIOUtils.write(fileName, outputStream);
      }
      ReadWriteIOUtils.write(fileLengths.size(), outputStream);
      for (final Long fileLength : fileLengths) {
        ReadWriteIOUtils.write(fileLength, outputStream);
      }
      ReadWriteIOUtils.write(parameters.size(), outputStream);
      for (final Map.Entry<String, String> entry : parameters.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyHandshakeV1Body(final String timestampPrecision)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyHandshakeV2Body(final Map<String, String> params)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(params.size(), outputStream);
      for (final Map.Entry<String, String> entry : params.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyFilePieceBody(
      final String fileName, final long startWritingOffset, final byte[] filePiece)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(startWritingOffset, outputStream);
      ReadWriteIOUtils.write(new Binary(filePiece), outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyFileSealV1Body(
      final String fileName, final long fileLength) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(fileLength, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static Tablet createSingleValueTablet(final String deviceId, final String measurement) {
    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema(measurement, TSDataType.INT32));

    final Tablet tablet = new Tablet(deviceId, schemaList, 8);
    tablet.addTimestamp(0, 1);
    tablet.addValue(measurement, 0, 1);
    return tablet;
  }

  private static ByteBuffer serializeTablet(final Tablet tablet, final boolean isAligned)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      tablet.serialize(outputStream);
      ReadWriteIOUtils.write(isAligned, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyTabletRawBuffer(final boolean isAligned)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write("root.sg.d", outputStream);
      ReadWriteIOUtils.write(2, outputStream);

      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(2, outputStream);
      writeLegacyMeasurementSchema(outputStream, "s1", TSDataType.INT32);
      writeLegacyMeasurementSchema(outputStream, "s2", TSDataType.TEXT);

      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(1700000000000L, outputStream);
      ReadWriteIOUtils.write(1700000000001L, outputStream);

      ReadWriteIOUtils.write((byte) 0, outputStream);

      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(2, outputStream);
      ReadWriteIOUtils.write(1, outputStream);
      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(new Binary("2", TSFileConfig.STRING_CHARSET), outputStream);
      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(new Binary("1", TSFileConfig.STRING_CHARSET), outputStream);

      ReadWriteIOUtils.write(isAligned, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeSingleColumnLegacyTabletRawBuffer(final boolean isAligned)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write("root.sg.d", outputStream);
      ReadWriteIOUtils.write(2, outputStream);

      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(1, outputStream);
      writeLegacyMeasurementSchema(outputStream, "s1", TSDataType.INT32);

      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(1700000000000L, outputStream);
      ReadWriteIOUtils.write(1700000000001L, outputStream);

      ReadWriteIOUtils.write((byte) 0, outputStream);

      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write((byte) 1, outputStream);
      ReadWriteIOUtils.write(2, outputStream);
      ReadWriteIOUtils.write(1, outputStream);

      ReadWriteIOUtils.write(isAligned, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static ByteBuffer serializeLegacyTabletBatchBody(
      final List<ByteBuffer> insertNodeBuffers, final List<ByteBuffer> tabletBuffers)
      throws IOException {
    return serializeLegacyTabletBatchBody(
        Collections.emptyList(), insertNodeBuffers, tabletBuffers);
  }

  private static ByteBuffer serializeLegacyTabletBatchBody(
      final List<ByteBuffer> binaryBuffers,
      final List<ByteBuffer> insertNodeBuffers,
      final List<ByteBuffer> tabletBuffers)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(binaryBuffers.size(), outputStream);
      for (final ByteBuffer binaryBuffer : binaryBuffers) {
        ReadWriteIOUtils.write(binaryBuffer.limit(), outputStream);
        writeByteBuffer(outputStream, binaryBuffer);
      }

      ReadWriteIOUtils.write(insertNodeBuffers.size(), outputStream);
      for (final ByteBuffer insertNodeBuffer : insertNodeBuffers) {
        writeByteBuffer(outputStream, insertNodeBuffer);
      }

      ReadWriteIOUtils.write(tabletBuffers.size(), outputStream);
      for (final ByteBuffer tabletBuffer : tabletBuffers) {
        writeByteBuffer(outputStream, tabletBuffer);
      }

      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static void writeByteBuffer(
      final DataOutputStream outputStream, final ByteBuffer byteBuffer) throws IOException {
    outputStream.write(byteBufferToByteArray(byteBuffer));
  }

  private static byte[] byteBufferToByteArray(final ByteBuffer byteBuffer) {
    final ByteBuffer duplicatedBuffer = byteBuffer.duplicate();
    final byte[] bytes = new byte[duplicatedBuffer.remaining()];
    duplicatedBuffer.get(bytes);
    return bytes;
  }

  private static void writeLegacyMeasurementSchema(
      final DataOutputStream outputStream, final String measurement, final TSDataType dataType)
      throws IOException {
    ReadWriteIOUtils.write((byte) 1, outputStream);
    ReadWriteIOUtils.write(measurement, outputStream);
    ReadWriteIOUtils.write(dataType.serialize(), outputStream);
    ReadWriteIOUtils.write(TSEncoding.PLAIN.serialize(), outputStream);
    ReadWriteIOUtils.write(CompressionType.UNCOMPRESSED.serialize(), outputStream);
    ReadWriteIOUtils.write(0, outputStream);
  }

  private static void assertLegacyTabletStatement(final InsertTabletStatement statement) {
    Assert.assertEquals("root.sg.d", statement.getDevicePath().getFullPath());
    Assert.assertArrayEquals(new String[] {"s1", "s2"}, statement.getMeasurements());
    Assert.assertArrayEquals(
        new TSDataType[] {TSDataType.INT32, TSDataType.TEXT}, statement.getDataTypes());
    Assert.assertEquals(2, statement.getRowCount());
  }
}
