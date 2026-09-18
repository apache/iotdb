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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TabletInsertNodeReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PipeTransferSerializationSizeTest {

  @Test
  public void testTabletRequestLengths() throws Exception {
    final Tablet tablet = createTablet();
    final String database = "pipe_db";
    final PipeTransferTabletRawReq rawReq =
        PipeTransferTabletRawReq.toTPipeTransferReq(tablet, false);
    assertSerializedBodySize(PipeTransferTabletRawReq.calculateSerializedSize(tablet), rawReq.body);
    final PipeTransferTabletRawReqV2 rawReqV2 =
        PipeTransferTabletRawReqV2.toTPipeTransferReq(tablet, false, database);
    assertSerializedBodySize(
        PipeTransferTabletRawReqV2.calculateSerializedSize(tablet, database), rawReqV2.body);
    Assert.assertEquals(
        PipeTransferTabletRawReq.calculateAirGapSerializedSize(tablet),
        PipeTransferTabletRawReq.toTPipeTransferBytes(tablet, false).length);
    Assert.assertEquals(
        PipeTransferTabletRawReqV2.calculateAirGapSerializedSize(tablet, database),
        PipeTransferTabletRawReqV2.toTPipeTransferBytes(tablet, false, database).length);
  }

  @Test
  public void testBinaryRequestLengths() throws Exception {
    final ByteBuffer payload = createByteBufferWithOffsetAndPosition();
    final byte[] expectedPayload = getRemainingBytes(payload);
    final String database = "pipe_db_\u6d4b\u8bd5";
    final PipeTransferTabletBinaryReqV2 binaryReqV2 =
        PipeTransferTabletBinaryReqV2.toTPipeTransferReq(payload, database);
    assertSerializedBodySize(
        PipeTransferTabletBinaryReqV2.calculateSerializedSize(payload, database), binaryReqV2.body);
    final ByteBuffer thriftBody = binaryReqV2.body.duplicate();
    Assert.assertEquals(expectedPayload.length, ReadWriteIOUtils.readInt(thriftBody));
    assertNextBytes(thriftBody, expectedPayload);
    Assert.assertEquals(database, ReadWriteIOUtils.readString(thriftBody));
    Assert.assertFalse(thriftBody.hasRemaining());

    final byte[] airGapV2Bytes =
        PipeTransferTabletBinaryReqV2.toTPipeTransferBytes(payload, database);
    Assert.assertEquals(
        PipeTransferTabletBinaryReqV2.calculateAirGapSerializedSize(payload, database),
        airGapV2Bytes.length);
    final ByteBuffer airGapV2Body = ByteBuffer.wrap(airGapV2Bytes);
    airGapV2Body.position(Byte.BYTES + Short.BYTES);
    Assert.assertEquals(expectedPayload.length, ReadWriteIOUtils.readInt(airGapV2Body));
    assertNextBytes(airGapV2Body, expectedPayload);
    Assert.assertEquals(database, ReadWriteIOUtils.readString(airGapV2Body));
    Assert.assertFalse(airGapV2Body.hasRemaining());

    final byte[] airGapV1Bytes = PipeTransferTabletBinaryReq.toTPipeTransferBytes(payload);
    Assert.assertEquals(
        PipeTransferTabletBinaryReq.calculateSerializedSize(payload), airGapV1Bytes.length);
    final ByteBuffer airGapV1Body = ByteBuffer.wrap(airGapV1Bytes);
    airGapV1Body.position(Byte.BYTES + Short.BYTES);
    assertNextBytes(airGapV1Body, expectedPayload);
    Assert.assertFalse(airGapV1Body.hasRemaining());
  }

  @Test
  public void testBatchRequestLengths() throws Exception {
    final ByteBuffer insertNode = createByteBufferWithOffsetAndPosition();
    final ByteBuffer tablet = createByteBufferWithOffsetAndPosition();
    final byte[] expectedInsertNode = getRemainingBytes(insertNode);
    final byte[] expectedTablet = getRemainingBytes(tablet);
    final PipeTransferTabletBatchReq batchReq =
        PipeTransferTabletBatchReq.toTPipeTransferReq(
            Collections.singletonList(insertNode), Collections.singletonList(tablet));
    assertSerializedBodySize(
        PipeTransferTabletBatchReq.calculateSerializedSize(
            Collections.singletonList(insertNode), Collections.singletonList(tablet)),
        batchReq.body);
    final ByteBuffer batchBody = batchReq.body.duplicate();
    Assert.assertEquals(0, ReadWriteIOUtils.readInt(batchBody));
    Assert.assertEquals(1, ReadWriteIOUtils.readInt(batchBody));
    assertNextBytes(batchBody, expectedInsertNode);
    Assert.assertEquals(1, ReadWriteIOUtils.readInt(batchBody));
    assertNextBytes(batchBody, expectedTablet);
    Assert.assertFalse(batchBody.hasRemaining());

    final String database = "db_\u6d4b\u8bd5";
    final PipeTransferTabletBatchReqV2 batchReqV2 =
        PipeTransferTabletBatchReqV2.toTPipeTransferReq(
            Collections.singletonList(insertNode),
            Collections.singletonList(tablet),
            Collections.singletonList(database),
            Collections.singletonList(database));
    assertSerializedBodySize(
        PipeTransferTabletBatchReqV2.calculateSerializedSize(
            Collections.singletonList(insertNode),
            Collections.singletonList(tablet),
            Collections.singletonList(database),
            Collections.singletonList(database)),
        batchReqV2.body);
    final ByteBuffer batchV2Body = batchReqV2.body.duplicate();
    Assert.assertEquals(0, ReadWriteIOUtils.readInt(batchV2Body));
    Assert.assertEquals(1, ReadWriteIOUtils.readInt(batchV2Body));
    assertNextBytes(batchV2Body, expectedInsertNode);
    Assert.assertEquals(database, ReadWriteIOUtils.readString(batchV2Body));
    Assert.assertEquals(1, ReadWriteIOUtils.readInt(batchV2Body));
    assertNextBytes(batchV2Body, expectedTablet);
    Assert.assertEquals(database, ReadWriteIOUtils.readString(batchV2Body));
    Assert.assertFalse(batchV2Body.hasRemaining());
  }

  @Test
  public void testInsertNodeSerializedSize() throws Exception {
    assertInsertNodeRequestSizes(createInsertRowNode(0), "tree_db");
    assertInsertNodeRequestSizes(createInsertRowNodeWithSchemas(1), "tree_db");
    assertInsertNodeRequestSizes(createInsertRowNodeWithNullValue(), "tree_db");
    assertInsertNodeRequestSizes(createInsertRowNodeWithInferredType(), "tree_db");
    final InsertRowNode partiallyFailedRowNode = createInsertRowNodeWithSchemas(2);
    partiallyFailedRowNode.markFailedMeasurement(1);
    assertInsertNodeRequestSizes(partiallyFailedRowNode, "tree_db");

    final InsertTabletNode tabletNode = createInsertTabletNode();
    assertInsertNodeRequestSizes(tabletNode, "tree_db");
    assertInsertNodeRequestSizes(createInsertTabletNode(false, true), "tree_db");
    final InsertTabletNode partiallyFailedTabletNode = createInsertTabletNode(false, true);
    partiallyFailedTabletNode.markFailedMeasurement(1);
    assertInsertNodeRequestSizes(partiallyFailedTabletNode, "tree_db");

    final RelationalInsertRowNode relationalRowNode =
        new RelationalInsertRowNode(
            new PlanNodeId("relational-row"),
            new PartialPath("table"),
            false,
            measurements(),
            dataTypes(),
            1,
            rowValues(1),
            false,
            columnCategories());
    assertInsertNodeRequestSizes(relationalRowNode, "table_db_\u6d4b\u8bd5");

    final RelationalInsertTabletNode relationalTabletNode = createRelationalInsertTabletNode();
    assertInsertNodeRequestSizes(relationalTabletNode, "table_db");

    final List<InsertRowNode> rows = new ArrayList<>();
    final List<InsertRowNode> relationalRows = new ArrayList<>();
    for (int row = 0; row < 50; row++) {
      rows.add(createInsertRowNode(row));
      relationalRows.add(createRelationalInsertRowNode(row));
    }
    final InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId("rows"));
    insertRowsNode.setInsertRowNodeList(rows);
    insertRowsNode.setInsertRowNodeIndexList(indexes(rows.size()));
    assertInsertNodeRequestSizes(insertRowsNode, "tree_db");

    final InsertRowsOfOneDeviceNode oneDeviceNode =
        new InsertRowsOfOneDeviceNode(new PlanNodeId("one-device"));
    oneDeviceNode.setInsertRowNodeList(rows);
    oneDeviceNode.setInsertRowNodeIndexList(indexes(rows.size()));
    assertInsertNodeRequestSizes(oneDeviceNode, "tree_db");

    final InsertMultiTabletsNode multiTabletsNode =
        new InsertMultiTabletsNode(new PlanNodeId("multi-tablets"));
    multiTabletsNode.addInsertTabletNode(tabletNode, 0);
    multiTabletsNode.addInsertTabletNode(relationalTabletNode, 1);
    assertInsertNodeRequestSizes(multiTabletsNode, "tree_db");

    final RelationalInsertRowsNode relationalRowsNode =
        new RelationalInsertRowsNode(
            new PlanNodeId("relational-rows"), indexes(relationalRows.size()), relationalRows);
    assertInsertNodeRequestSizes(relationalRowsNode, "table_db");

    final PipeEnrichedInsertNode pipeEnrichedInsertNode =
        new PipeEnrichedInsertNode(createInsertRowNode(2));
    pipeEnrichedInsertNode.setPlanNodeId(new PlanNodeId("enriched-row"));
    assertInsertNodeRequestSizes(pipeEnrichedInsertNode, "tree_db");
  }

  private static void assertInsertNodeRequestSizes(
      final InsertNode insertNode, final String databaseName) throws Exception {
    final ByteBuffer serializedInsertNode = insertNode.serializeToByteBuffer();
    Assert.assertEquals(insertNode.serializeToByteBufferSize(), serializedInsertNode.capacity());
    Assert.assertEquals(insertNode.serializeToByteBufferSize(), serializedInsertNode.remaining());
    final PipeTransferTabletInsertNodeReq insertNodeReq =
        PipeTransferTabletInsertNodeReq.toTPipeTransferReq(insertNode);
    assertSerializedBodySize(
        PipeTransferTabletInsertNodeReq.calculateSerializedSize(insertNode), insertNodeReq.body);
    Assert.assertEquals(
        PipeTransferTabletInsertNodeReq.calculateAirGapSerializedSize(insertNode),
        PipeTransferTabletInsertNodeReq.toTPipeTransferBytes(insertNode).length);
    final PipeTransferTabletInsertNodeReqV2 insertNodeReqV2 =
        PipeTransferTabletInsertNodeReqV2.toTPipeTransferReq(insertNode, databaseName);
    assertSerializedBodySize(
        PipeTransferTabletInsertNodeReqV2.calculateSerializedSize(insertNode, databaseName),
        insertNodeReqV2.body);
    Assert.assertEquals(
        PipeTransferTabletInsertNodeReqV2.calculateAirGapSerializedSize(insertNode, databaseName),
        PipeTransferTabletInsertNodeReqV2.toTPipeTransferBytes(insertNode, databaseName).length);
    final IoTConsensusV2TabletInsertNodeReq iotConsensusReq =
        IoTConsensusV2TabletInsertNodeReq.toTIoTConsensusV2TransferReq(
            insertNode, null, null, MinimumProgressIndex.INSTANCE, 0);
    assertSerializedBodySize(
        IoTConsensusV2TabletInsertNodeReq.calculateSerializedSize(insertNode),
        iotConsensusReq.body);
  }

  private static List<Integer> indexes(final int size) {
    final List<Integer> indexes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      indexes.add(i);
    }
    return indexes;
  }

  private static InsertRowNode createInsertRowNode(final int row) throws IllegalPathException {
    return new InsertRowNode(
        new PlanNodeId("row-" + row),
        new PartialPath("root.sg.d"),
        false,
        measurements(),
        dataTypes(),
        row,
        rowValues(row),
        false);
  }

  private static InsertRowNode createInsertRowNodeWithSchemas(final int row)
      throws IllegalPathException {
    return new InsertRowNode(
        new PlanNodeId("row-with-schemas"),
        new PartialPath("root.sg.d"),
        false,
        measurements(),
        dataTypes(),
        measurementSchemas(),
        row,
        rowValues(row),
        false);
  }

  private static InsertRowNode createInsertRowNodeWithNullValue() throws IllegalPathException {
    return new InsertRowNode(
        new PlanNodeId("row-with-null"),
        new PartialPath("root.sg.d"),
        false,
        new String[] {"s"},
        new TSDataType[] {TSDataType.INT32},
        1,
        new Object[] {null},
        false);
  }

  private static InsertRowNode createInsertRowNodeWithInferredType() throws IllegalPathException {
    return new InsertRowNode(
        new PlanNodeId("row-with-inferred-type"),
        new PartialPath("root.sg.d"),
        false,
        new String[] {"s"},
        new TSDataType[] {null},
        1,
        new Object[] {"value"},
        true);
  }

  private static RelationalInsertRowNode createRelationalInsertRowNode(final int row)
      throws IllegalPathException {
    return new RelationalInsertRowNode(
        new PlanNodeId("relational-row-" + row),
        new PartialPath("table"),
        false,
        measurements(),
        dataTypes(),
        row,
        rowValues(row),
        false,
        columnCategories());
  }

  private static InsertTabletNode createInsertTabletNode() throws IllegalPathException {
    return createInsertTabletNode(false, false);
  }

  private static InsertTabletNode createInsertTabletNode(final boolean relational)
      throws IllegalPathException {
    return createInsertTabletNode(relational, false);
  }

  private static InsertTabletNode createInsertTabletNode(
      final boolean relational, final boolean withBitMaps) throws IllegalPathException {
    final String[] measurements = measurements();
    final TSDataType[] types = dataTypes();
    final MeasurementSchema[] schemas = measurementSchemas();
    final Object[] columns = new Object[types.length];
    final int rowCount = 50;
    final long[] times = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      times[i] = i;
    }
    for (int column = 0; column < types.length; column++) {
      switch (types[column]) {
        case BOOLEAN:
          final boolean[] booleanValues = new boolean[rowCount];
          for (int row = 0; row < rowCount; row++) {
            booleanValues[row] = row % 2 == 0;
          }
          columns[column] = booleanValues;
          break;
        case INT32:
        case DATE:
          final int[] intValues = new int[rowCount];
          for (int row = 0; row < rowCount; row++) {
            intValues[row] = row;
          }
          columns[column] = intValues;
          break;
        case INT64:
        case TIMESTAMP:
          final long[] longValues = new long[rowCount];
          for (int row = 0; row < rowCount; row++) {
            longValues[row] = row;
          }
          columns[column] = longValues;
          break;
        case FLOAT:
          final float[] floatValues = new float[rowCount];
          for (int row = 0; row < rowCount; row++) {
            floatValues[row] = row;
          }
          columns[column] = floatValues;
          break;
        case DOUBLE:
          final double[] doubleValues = new double[rowCount];
          for (int row = 0; row < rowCount; row++) {
            doubleValues[row] = row;
          }
          columns[column] = doubleValues;
          break;
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          Binary[] values = new Binary[rowCount];
          for (int row = 1; row < rowCount; row++) {
            values[row] = new Binary(("value-" + row).getBytes(StandardCharsets.UTF_8));
          }
          columns[column] = values;
          break;
        default:
          throw new AssertionError(types[column]);
      }
    }
    final BitMap[] bitMaps = withBitMaps ? createBitMaps(types.length, rowCount) : null;
    return relational
        ? new RelationalInsertTabletNode(
            new PlanNodeId("relational-tablet"),
            new PartialPath("table"),
            false,
            measurements,
            types,
            schemas,
            times,
            bitMaps,
            columns,
            rowCount,
            columnCategories())
        : new InsertTabletNode(
            new PlanNodeId("tablet"),
            new PartialPath("root.sg.d"),
            false,
            measurements,
            types,
            schemas,
            times,
            bitMaps,
            columns,
            rowCount);
  }

  private static RelationalInsertTabletNode createRelationalInsertTabletNode()
      throws IllegalPathException {
    return (RelationalInsertTabletNode) createInsertTabletNode(true);
  }

  private static String[] measurements() {
    return new String[] {"b", "i", "l", "f", "d", "t", "ts", "date", "blob", "string", "object"};
  }

  private static TSDataType[] dataTypes() {
    return new TSDataType[] {
      TSDataType.BOOLEAN,
      TSDataType.INT32,
      TSDataType.INT64,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.TEXT,
      TSDataType.TIMESTAMP,
      TSDataType.DATE,
      TSDataType.BLOB,
      TSDataType.STRING,
      TSDataType.OBJECT
    };
  }

  private static MeasurementSchema[] measurementSchemas() {
    final String[] measurements = measurements();
    final TSDataType[] types = dataTypes();
    final MeasurementSchema[] schemas = new MeasurementSchema[types.length];
    for (int i = 0; i < types.length; i++) {
      schemas[i] = new MeasurementSchema(measurements[i], types[i], TSEncoding.PLAIN);
    }
    return schemas;
  }

  private static BitMap[] createBitMaps(final int columnCount, final int rowCount) {
    final BitMap[] bitMaps = new BitMap[columnCount];
    bitMaps[0] = new BitMap(rowCount);
    bitMaps[0].mark(0);
    bitMaps[columnCount - 1] = new BitMap(rowCount);
    bitMaps[columnCount - 1].mark(rowCount - 1);
    return bitMaps;
  }

  private static Object[] rowValues(final int row) {
    return new Object[] {
      true,
      row,
      (long) row,
      (float) row,
      (double) row,
      new Binary(("text-" + row).getBytes(StandardCharsets.UTF_8)),
      (long) row,
      row,
      new Binary(("blob-" + row).getBytes(StandardCharsets.UTF_8)),
      new Binary(("string-" + row).getBytes(StandardCharsets.UTF_8)),
      new Binary(("object-" + row).getBytes(StandardCharsets.UTF_8))
    };
  }

  private static TsTableColumnCategory[] columnCategories() {
    final TsTableColumnCategory[] categories = new TsTableColumnCategory[dataTypes().length];
    Arrays.fill(categories, TsTableColumnCategory.FIELD);
    categories[0] = TsTableColumnCategory.TAG;
    return categories;
  }

  private static ByteBuffer createByteBufferWithOffsetAndPosition() {
    final ByteBuffer source = ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5});
    source.position(1);
    final ByteBuffer buffer = source.slice();
    buffer.position(1);
    buffer.limit(4);
    return buffer;
  }

  private static byte[] getRemainingBytes(final ByteBuffer buffer) {
    final ByteBuffer duplicate = buffer.duplicate();
    final byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }

  private static void assertSerializedBodySize(final int expectedSize, final ByteBuffer body) {
    Assert.assertEquals(expectedSize, body.remaining());
    Assert.assertEquals(expectedSize, body.capacity());
  }

  private static void assertNextBytes(final ByteBuffer buffer, final byte[] expectedBytes) {
    final byte[] actualBytes = new byte[expectedBytes.length];
    buffer.get(actualBytes);
    Assert.assertArrayEquals(expectedBytes, actualBytes);
  }

  private static Tablet createTablet() {
    final Tablet tablet =
        new Tablet(
            "table1", Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)), 1);
    tablet.setColumnCategories(Collections.singletonList(ColumnCategory.FIELD));
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, 1);
    tablet.setRowSize(1);
    return tablet;
  }
}
