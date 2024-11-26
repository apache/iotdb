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
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PipeDataNodeThriftRequestTest {

  private static final String TIME_PRECISION = "ms";

  @Test
  public void testPipeTransferDataNodeHandshakeReq() throws IOException {
    final PipeTransferDataNodeHandshakeV1Req req =
        PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(TIME_PRECISION);
    final PipeTransferDataNodeHandshakeV1Req deserializeReq =
        PipeTransferDataNodeHandshakeV1Req.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
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
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getInsertNode(), deserializeReq.getInsertNode());

    final Statement statement = req.constructStatement();
    final List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath(new String[] {"root", "sg", "d", "s"}));
    Assert.assertEquals(statement.getPaths(), paths);
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
  public void testPipeTransferTabletBinaryReq() {
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    final PipeTransferTabletBinaryReq req =
        PipeTransferTabletBinaryReq.toTPipeTransferReq(ByteBuffer.wrap(new byte[] {'a', 'b'}));
    final PipeTransferTabletBinaryReq deserializeReq =
        PipeTransferTabletBinaryReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
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
  public void testPipeTransferSchemaPlanReq() {
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
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getPlanNode(), deserializeReq.getPlanNode());
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
      Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

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
      List<Tablet.ColumnCategory> columnTypes = new ArrayList<>();
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      columnTypes.add(Tablet.ColumnCategory.MEASUREMENT);
      final Tablet t =
          new Tablet(
              "root.sg.d",
              schemaList.stream()
                  .map(IMeasurementSchema::getMeasurementName)
                  .collect(Collectors.toList()),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              1024);
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

      Assert.assertTrue(statement.isWriteToTable());
      Assert.assertTrue(statement.getDatabaseName().isPresent());
      Assert.assertEquals(statement.getDatabaseName().get(), "test");
      Assert.assertEquals(t.getColumnTypes(), deserializeReq.getTablet().getColumnTypes());
    } catch (final IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPipeTransferTabletBatchReq() throws IOException {
    final List<ByteBuffer> binaryBuffers = new ArrayList<>();
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

    // Binary buffer
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    binaryBuffers.add(ByteBuffer.wrap(new byte[] {'a', 'b'}));

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
        PipeTransferTabletBatchReq.toTPipeTransferReq(
            binaryBuffers, insertNodeBuffers, tabletBuffers);

    final PipeTransferTabletBatchReq deserializedReq =
        PipeTransferTabletBatchReq.fromTPipeTransferReq(req);

    Assert.assertArrayEquals(
        new byte[] {'a', 'b'}, deserializedReq.getBinaryReqs().get(0).getBody());
    Assert.assertEquals(node, deserializedReq.getInsertNodeReqs().get(0).getInsertNode());
    Assert.assertEquals(t, deserializedReq.getTabletReqs().get(0).getTablet());
    Assert.assertFalse(deserializedReq.getTabletReqs().get(0).getIsAligned());
  }

  @Test
  public void testPipeTransferTabletBatchReqV2() throws IOException {
    final List<ByteBuffer> binaryBuffers = new ArrayList<>();
    final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
    final List<ByteBuffer> tabletBuffers = new ArrayList<>();
    final List<String> binaryDataBase = new ArrayList<>();
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

    // Binary buffer
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    binaryBuffers.add(ByteBuffer.wrap(new byte[] {'a', 'b'}));
    binaryDataBase.add("test");

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
      tabletDataBase.add("test");
    }

    final PipeTransferTabletBatchReqV2 req =
        PipeTransferTabletBatchReqV2.toTPipeTransferReq(
            binaryBuffers,
            insertNodeBuffers,
            tabletBuffers,
            binaryDataBase,
            insertDataBase,
            tabletDataBase);

    final PipeTransferTabletBatchReqV2 deserializedReq =
        PipeTransferTabletBatchReqV2.fromTPipeTransferReq(req);

    Assert.assertArrayEquals(
        new byte[] {'a', 'b'}, deserializedReq.getBinaryReqs().get(0).getByteBuffer().array());
    Assert.assertEquals(node, deserializedReq.getInsertNodeReqs().get(0).getInsertNode());
    Assert.assertEquals(t, deserializedReq.getTabletReqs().get(0).getTablet());
    Assert.assertFalse(deserializedReq.getTabletReqs().get(0).getIsAligned());

    Assert.assertEquals("test", deserializedReq.getBinaryReqs().get(0).getDataBaseName());
    Assert.assertEquals("test", deserializedReq.getTabletReqs().get(0).getDataBaseName());
    Assert.assertEquals("test", deserializedReq.getInsertNodeReqs().get(0).getDataBaseName());
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
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

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
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

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
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
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
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getFileLength(), deserializeReq.getFileLength());
  }

  @Test
  public void testPipeTransferSchemaSnapshotSealReq() throws IOException {
    final String mTreeSnapshotName = "mtree.snapshot";
    final String tLogName = "tlog.txt";
    final String databaseName = "root.db";
    // CREATE_TIME_SERIES
    final String typeString = "19";

    final PipeTransferSchemaSnapshotSealReq req =
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferReq(
            "root.**", mTreeSnapshotName, 100, tLogName, 10, databaseName, typeString);
    final PipeTransferSchemaSnapshotSealReq deserializeReq =
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
    final PipeTransferFilePieceResp resp =
        PipeTransferFilePieceResp.toTPipeTransferResp(RpcUtils.SUCCESS_STATUS, 100);
    final PipeTransferFilePieceResp deserializeResp =
        PipeTransferFilePieceResp.fromTPipeTransferResp(resp);

    Assert.assertEquals(resp.getStatus(), deserializeResp.getStatus());
    Assert.assertEquals(resp.getEndWritingOffset(), deserializeResp.getEndWritingOffset());
  }
}
