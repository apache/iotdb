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
package org.apache.iotdb.db.queryengine.plan.parser;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTablePatternParser;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertNumericRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertNumericRow;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertNumericRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusLogToTabletConverter;
import org.apache.iotdb.db.utils.NumericRowValues;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Exercises one numeric row from the client codec through schema edits, WAL and memtable. */
public class NumericTableRowTest {
  private static final List<String> NAMES = Arrays.asList("b", "i", "l", "f", "d", "date", "ts");
  private static final List<TSDataType> TYPES =
      Arrays.asList(
          TSDataType.BOOLEAN,
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE,
          TSDataType.DATE,
          TSDataType.TIMESTAMP);
  private static final long[] BITS = {
    1, Integer.MIN_VALUE, Long.MIN_VALUE, 0x7fc01234, 0x7ff8000000001234L, 20260910, Long.MAX_VALUE
  };

  private TSInsertRecordReq request() throws Exception {
    return new TSInsertRecordReq()
        .setPrefixPath("numeric_table")
        .setTimestamp(42)
        .setMeasurements(NAMES)
        .setValues(
            SessionUtils.getValueBuffer(
                TYPES,
                BITS,
                NAMES,
                Collections.nCopies(NAMES.size(), ColumnCategory.FIELD),
                new String[0]))
        .setIsAligned(true)
        .setIsWriteToTable(true)
        .setIsNumericRow(true)
        .setColumnCategoryies(
            Collections.nCopies(NAMES.size(), (byte) ColumnCategory.FIELD.ordinal()));
  }

  @Test
  public void testRpcDecodeAndSchemaChangesRetainPrimitiveStorage() throws Exception {
    // Missing TAG insertion, column swaps and partial failure recovery must retain long[] values.
    InsertNumericRowStatement statement =
        (InsertNumericRowStatement) StatementGenerator.createStatement(request());
    assertArrayEquals(BITS, statement.getNumericValues().getValues());
    assertObjectStorageAbsent(InsertNumericRowStatement.class, statement);
    assertTrue(statement.isWriteToTable());
    assertTrue(statement.isAligned());
    assertEquals(StatementType.INSERT_NUMERIC_ROW, statement.getType());
    statement.semanticCheck();

    statement.insertColumn(
        0,
        new ColumnSchema(
            "tag", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    assertTrue(statement.getNumericValues().isNull(0));
    assertNull(statement.getFirstValueOfIndex(0));
    assertEquals(20260910, statement.getFirstValueOfIndex(6));
    statement.swapColumn(1, 2);
    assertEquals(Integer.MIN_VALUE, statement.getNumericValues().get(1));
    assertEquals(1, statement.getNumericValues().get(2));
    statement.markFailedMeasurement(1, new IllegalArgumentException());
    assertTrue(statement.getNumericValues().isNull(1));
    statement.removeAllFailedMeasurementMarks();
    assertEquals(Integer.MIN_VALUE, statement.getNumericValues().get(1));
    assertObjectStorageAbsent(InsertNumericRowStatement.class, statement);
    assertEquals("numeric_table", statement.getTableDeviceID().getTableName());
  }

  @Test
  public void testRpcRejectsNumericTreeAndNonStringTagRequests() throws Exception {
    // Raw RPC callers must use STRING TAGs and numeric FIELDs in the table model.
    TSInsertRecordReq tree = request().setIsWriteToTable(false);
    assertThrows(IllegalArgumentException.class, () -> StatementGenerator.createStatement(tree));
    TSInsertRecordReq tag =
        request()
            .setColumnCategoryies(
                Collections.nCopies(NAMES.size(), (byte) ColumnCategory.TAG.ordinal()));
    assertThrows(IllegalArgumentException.class, () -> StatementGenerator.createStatement(tag));
  }

  @Test
  public void testStringTagsSurviveSchemaChanges() throws Exception {
    // The client places TAG metadata before FIELD metadata while retaining dense value arrays.
    List<String> names = Arrays.asList("tag2", "tag1", "b", "l");
    List<TSDataType> types =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.BOOLEAN, TSDataType.INT64);
    List<ColumnCategory> categories =
        Arrays.asList(
            ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);
    TSInsertRecordReq req =
        request()
            .setMeasurements(names)
            .setValues(
                SessionUtils.getValueBuffer(
                    types,
                    new long[] {1, Long.MIN_VALUE},
                    names,
                    categories,
                    new String[] {"设备😀", ""}))
            .setColumnCategoryies(
                Arrays.asList(
                    (byte) ColumnCategory.TAG.ordinal(),
                    (byte) ColumnCategory.TAG.ordinal(),
                    (byte) ColumnCategory.FIELD.ordinal(),
                    (byte) ColumnCategory.FIELD.ordinal()));
    InsertNumericRowStatement statement =
        (InsertNumericRowStatement) StatementGenerator.createStatement(req);
    assertEquals("设备😀", statement.getNumericValues().getTag(0));
    assertEquals("", statement.getNumericValues().getTag(1));
    assertArrayEquals(new long[] {1, Long.MIN_VALUE}, statement.getValues());
    statement.rebuildArraysAfterExpansion(
        new int[] {-1, 1, 0, 2, 3}, new String[] {"missing", "tag1", "tag2", "b", "l"});
    assertArrayEquals(
        new String[] {"numeric_table", null, "", "设备😀"},
        statement.getTableDeviceID().getSegments());
    statement.swapColumn(1, 2);
    assertEquals("设备😀", statement.getNumericValues().getTag(1));
    statement.markFailedMeasurement(1, new IllegalArgumentException());
    statement.removeAllFailedMeasurementMarks();
    assertEquals("设备😀", statement.getNumericValues().getTag(1));
    assertEquals(1, statement.getValues()[0]);
    assertEquals(Long.MIN_VALUE, statement.getValues()[1]);
    assertObjectStorageAbsent(InsertNumericRowStatement.class, statement);
  }

  @Test
  public void testStringTagsSurviveWalConsensusAndTabletAdapters() throws Exception {
    // TAGs identify the device through WAL recovery and remain present in Pipe/subscription
    // Tablets.
    for (String tag : Arrays.asList("设备😀", "", null)) {
      RelationalInsertNumericRowNode node = nodeWithNullTag();
      node.getNumericValues().setTag(0, tag);
      assertRoundTrips(node);
      ByteBuffer bytes = node.serializeToByteBuffer();
      RelationalInsertNumericRowNode decoded =
          (RelationalInsertNumericRowNode) PlanNodeType.deserialize(bytes);
      assertEquals(tag, decoded.getNumericValues().getTag(0));
      assertEquals(node.getDeviceID(), decoded.getDeviceID());
      assertEquals(
          new Binary(tag == null ? "" : tag, TSFileConfig.STRING_CHARSET),
          tag == null ? Binary.EMPTY_VALUE : decoded.getValue(0));
      Tablet pipe =
          new TabletInsertionEventTablePatternParser(null, null, decoded, null).convertToTablet();
      Tablet subscription =
          new ConsensusLogToTabletConverter(null, null, null, "db").convert(decoded).get(0);
      for (Tablet tablet : Arrays.asList(pipe, subscription)) {
        if (tag == null) {
          assertTrue(tablet.getBitMaps()[0].isMarked(0));
        } else {
          assertEquals(
              tag,
              ((Binary[]) tablet.getValues()[0])[0].getStringValue(TSFileConfig.STRING_CHARSET));
        }
      }
    }
  }

  @Test
  public void testBulkTagExpansionKeepsValuesAndNulls() throws Exception {
    // A reversed schema mapping with two missing TAGs must preserve numeric bits and null slots.
    InsertNumericRowStatement statement =
        (InsertNumericRowStatement) StatementGenerator.createStatement(request());
    statement.rebuildArraysAfterExpansion(
        new int[] {-1, -1, 6, 5, 4, 3, 2, 1, 0},
        new String[] {"tag1", "tag2", "ts", "date", "d", "f", "l", "i", "b"});
    assertTrue(statement.getNumericValues().isNull(0));
    assertTrue(statement.getNumericValues().isNull(1));
    for (int i = 0; i < BITS.length; i++) {
      assertEquals(BITS[BITS.length - 1 - i], statement.getNumericValues().get(i + 2));
    }
    assertObjectStorageAbsent(InsertNumericRowStatement.class, statement);
  }

  @Test
  public void testNumericCastsKeepPrimitiveStorage() {
    // Widen INT32 to DOUBLE in place; reject STRING coercion while accepting an inserted null TAG.
    NumericRowValues values = new NumericRowValues(2);
    values.getValues()[0] = Integer.MIN_VALUE;
    values.setNull(1);
    assertTrue(values.cast(0, TSDataType.INT32, TSDataType.DOUBLE));
    assertEquals(Double.doubleToRawLongBits((double) Integer.MIN_VALUE), values.get(0));
    assertFalse(values.cast(0, TSDataType.DOUBLE, TSDataType.STRING));
    assertTrue(values.cast(1, null, TSDataType.STRING));
    assertTrue(values.isNull(1));
  }

  @Test
  public void testConsensusAndWalRetainPrimitiveStorage() throws Exception {
    // Both serializers and WAL readers must retain numeric storage, including partial failures.
    RelationalInsertNumericRowNode node = nodeWithNullTag();
    assertRoundTrips(node);
    node.markFailedMeasurement(2);
    assertRoundTrips(node);
    node.setMeasurementSchemas(null);
    assertRoundTrips(node);
  }

  @Test
  public void testWalEnvelopeRetainsTypeAndFrozenIndex() throws Exception {
    RelationalInsertNumericRowNode node = nodeWithNullTag();
    node.setSearchIndex(123);
    node.setLastFragment(true);
    WALInfoEntry entry = new WALInfoEntry(7, node);
    // WAL queuing freezes the consensus index even if the caller later reuses the node.
    node.setSearchIndex(456);
    node.setLastFragment(false);
    ByteBuffer buffer = ByteBuffer.allocate(entry.serializedSize());
    entry.serialize(new WALByteBufferForTest(buffer));
    assertFalse(buffer.hasRemaining());
    buffer.flip();
    RelationalInsertNumericRowNode consensus =
        (RelationalInsertNumericRowNode) WALEntry.deserializeForConsensus(buffer);
    assertEquals(123, consensus.getSearchIndex());
    assertTrue(consensus.isLastFragment());
    assertEquals(node.getNumericValues(), consensus.getNumericValues());
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(buffer.array()))) {
      WALEntry recovered = WALEntry.deserialize(input);
      assertEquals(7, recovered.getMemTableId());
      RelationalInsertNumericRowNode row = (RelationalInsertNumericRowNode) recovered.getValue();
      assertEquals(123, row.getSearchIndex());
      assertTrue(row.isLastFragment());
      assertEquals(node.getNumericValues(), row.getNumericValues());
      assertEquals(0, input.available());
      assertObjectStorageAbsent(RelationalInsertNumericRowNode.class, row);
    }
  }

  @Test
  public void testPipeBatchRetainsIndependentNumericStatements() throws Exception {
    RelationalInsertNumericRowNode node = nodeWithNullTag();
    PipeTransferTabletBatchReqV2 request =
        PipeTransferTabletBatchReqV2.toTPipeTransferReq(
            Arrays.asList(node.serializeToByteBuffer(), node.serializeToByteBuffer()),
            Collections.emptyList(),
            Arrays.asList("db", "db"),
            Collections.emptyList());
    List<InsertBaseStatement> statements =
        PipeTransferTabletBatchReqV2.fromTPipeTransferReq(request).constructStatements();
    // Batching must not replace numeric statements with the object-valued InsertRowsStatement.
    assertEquals(2, statements.size());
    for (InsertBaseStatement statement : statements) {
      assertEquals(StatementType.INSERT_NUMERIC_ROW, statement.getType());
      assertEquals("db", statement.getDatabaseName().get());
      assertEquals(
          node.getNumericValues(), ((InsertNumericRowStatement) statement).getNumericValues());
      assertObjectStorageAbsent(InsertNumericRowStatement.class, statement);
    }
  }

  @Test
  public void testNumericRowRedirectUsesSingleEndpoint() throws Exception {
    InsertNumericRowStatement statement =
        (InsertNumericRowStatement) StatementGenerator.createStatement(request());
    Analysis analysis = new Analysis(new InsertNumericRow(statement, null), Collections.emptyMap());
    TEndPoint local = new TEndPoint("127.0.0.1", 6667);
    TEndPoint leader = new TEndPoint("127.0.0.1", 6668);
    analysis.setRedirectNodeList(Collections.singletonList(leader));
    TableModelPlanner planner = Mockito.mock(TableModelPlanner.class, Mockito.CALLS_REAL_METHODS);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    planner.setRedirectInfo(analysis, local, status);
    assertEquals(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), status.getCode());
    assertEquals(leader, status.getRedirectNode());
    assertFalse(status.isSetSubStatus());
    TSStatus localStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    planner.setRedirectInfo(analysis, leader, localStatus);
    assertFalse(localStatus.isSetRedirectNode());
  }

  private void assertRoundTrips(RelationalInsertNumericRowNode node) throws Exception {
    ByteBuffer streamed = node.serializeToByteBuffer();
    ByteBuffer direct = ByteBuffer.allocate(streamed.remaining());
    node.serialize(direct);
    direct.flip();
    assertEquals(streamed, direct);
    RelationalInsertNumericRowNode decoded =
        (RelationalInsertNumericRowNode) PlanNodeType.deserialize(direct);
    assertObjectStorageAbsent(RelationalInsertNumericRowNode.class, decoded);
    assertEquals(node.getNumericValues().isNull(0), decoded.getNumericValues().isNull(0));
    assertEquals(node.getNumericValues().getTag(0), decoded.getNumericValues().getTag(0));
    assertEquals(node.getDeviceID(), decoded.getDeviceID());

    ByteBuffer wal = ByteBuffer.allocate(node.serializedSize());
    node.setSearchIndex(123);
    node.setLastFragment(true);
    node.serializeToWAL(new WALByteBufferForTest(wal));
    assertFalse(wal.hasRemaining());
    wal.flip();
    assertEquals(PlanNodeType.RELATIONAL_INSERT_NUMERIC_ROW.getNodeType(), wal.getShort());
    RelationalInsertNumericRowNode recovered =
        RelationalInsertNumericRowNode.deserializeFromWAL(wal);
    assertFalse(wal.hasRemaining());
    assertEquals(decoded.getNumericValues(), recovered.getNumericValues());
    assertArrayEquals(decoded.getMeasurements(), recovered.getMeasurements());
    assertArrayEquals(decoded.getColumnCategories(), recovered.getColumnCategories());
    assertObjectStorageAbsent(RelationalInsertNumericRowNode.class, recovered);
    assertEquals(123, recovered.getSearchIndex());
    assertTrue(recovered.isLastFragment());

    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(wal.array()))) {
      input.readShort();
      RelationalInsertNumericRowNode fromStream =
          RelationalInsertNumericRowNode.deserializeFromWAL(input);
      assertEquals(recovered.getNumericValues(), fromStream.getNumericValues());
      assertObjectStorageAbsent(RelationalInsertNumericRowNode.class, fromStream);
      assertEquals(0, input.available());
    }
  }

  @Test
  public void testMemtableAcceptsRecoveredRowWithoutBoxing() throws Exception {
    // A WAL-recovered row must reach primitive column arrays without boxing the row or its NaNs.
    RelationalInsertNumericRowNode original = nodeWithNullTag();
    ByteBuffer wal = ByteBuffer.allocate(original.serializedSize());
    original.serializeToWAL(new WALByteBufferForTest(wal));
    wal.flip();
    wal.getShort();
    RelationalInsertNumericRowNode recovered =
        RelationalInsertNumericRowNode.deserializeFromWAL(wal);
    // Fail immediately if the primitive memtable write path boxes even a single cell.
    RelationalInsertNumericRowNode row =
        new RelationalInsertNumericRowNode(new PlanNodeId("test")) {
          @Override
          public Object getValue(int index) {
            throw new AssertionError("Numeric memtable writes must not box the row");
          }
        };
    row.setTargetPath(recovered.getTargetPath());
    row.setTime(42);
    row.setAligned(true);
    row.setMeasurements(recovered.getMeasurements());
    row.setDataTypes(recovered.getDataTypes());
    row.setMeasurementSchemas(recovered.getMeasurementSchemas());
    row.setColumnCategories(recovered.getColumnCategories());
    row.setNumericValues(recovered.getNumericValues());
    PrimitiveMemTable memtable = new PrimitiveMemTable();
    try {
      assertEquals(7, memtable.insertAlignedRow(row));
      AlignedTVList list =
          ((AlignedWritableMemChunkGroup) memtable.getMemTableMap().get(row.getDeviceID()))
              .getAlignedMemChunk()
              .getWorkingTVList();
      assertEquals(42, list.getTime(0));
      assertTrue(list.getBooleanByValueIndex(0, 0));
      assertEquals(Integer.MIN_VALUE, list.getIntByValueIndex(0, 1));
      assertEquals(Long.MIN_VALUE, list.getLongByValueIndex(0, 2));
      assertEquals(0x7fc01234, Float.floatToRawIntBits(list.getFloatByValueIndex(0, 3)));
      assertEquals(
          0x7ff8000000001234L, Double.doubleToRawLongBits(list.getDoubleByValueIndex(0, 4)));
      assertEquals(20260910, list.getIntByValueIndex(0, 5));
      assertEquals(Long.MAX_VALUE, list.getLongByValueIndex(0, 6));
      assertObjectStorageAbsent(RelationalInsertNumericRowNode.class, row);
    } finally {
      memtable.release();
    }
  }

  @Test
  public void testRpcRejectsNonNumericTypes() throws Exception {
    // Raw RPC callers cannot smuggle unsupported values into numeric FIELD storage.
    for (TSDataType type :
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.BLOB,
            TSDataType.OBJECT,
            TSDataType.VECTOR,
            TSDataType.UNKNOWN)) {
      TSInsertRecordReq invalid =
          request().setValues(ByteBuffer.wrap(new byte[] {type.serialize()}));
      assertThrows(
          UnSupportedDataTypeException.class, () -> StatementGenerator.createStatement(invalid));
    }
    // STRING has a valid wire representation for TAGs, but must still be rejected as a FIELD.
    ByteBuffer stringValue = ByteBuffer.allocate(32);
    ReadWriteIOUtils.write(TSDataType.STRING, stringValue);
    ReadWriteIOUtils.write("invalid", stringValue);
    stringValue.flip();
    TSInsertRecordReq stringField =
        request()
            .setMeasurements(Collections.singletonList("s"))
            .setColumnCategoryies(Collections.singletonList((byte) ColumnCategory.FIELD.ordinal()))
            .setValues(stringValue);
    assertThrows(
        IllegalArgumentException.class, () -> StatementGenerator.createStatement(stringField));
  }

  @Test
  public void testTabletAdaptersDoNotMaterializeObjectRows() throws Exception {
    // Pipe and subscription must preserve values and null TAGs without boxing the entire row.
    RelationalInsertNumericRowNode node = Mockito.spy(nodeWithNullTag());
    Mockito.doThrow(new AssertionError("Numeric adapters must not box the row"))
        .when(node)
        .getValues();
    Tablet pipe =
        new TabletInsertionEventTablePatternParser(null, null, node, null).convertToTablet();
    List<Tablet> subscription =
        new ConsensusLogToTabletConverter(null, null, null, "db").convert(node);
    assertEquals(1, subscription.size());
    for (Tablet tablet : Arrays.asList(pipe, subscription.get(0))) {
      assertEquals(1, tablet.getRowSize());
      assertEquals("numeric_table", tablet.getTableName());
      assertEquals(42, tablet.getTimestamps()[0]);
      assertTrue(tablet.getBitMaps()[0].isMarked(0));
      assertTrue(((boolean[]) tablet.getValues()[1])[0]);
      assertEquals(Integer.MIN_VALUE, ((int[]) tablet.getValues()[2])[0]);
      assertEquals(Long.MIN_VALUE, ((long[]) tablet.getValues()[3])[0]);
      assertEquals(0x7fc01234, Float.floatToRawIntBits(((float[]) tablet.getValues()[4])[0]));
      assertEquals(
          0x7ff8000000001234L, Double.doubleToRawLongBits(((double[]) tablet.getValues()[5])[0]));
      assertEquals(LocalDate.of(2026, 9, 10), ((LocalDate[]) tablet.getValues()[6])[0]);
      assertEquals(Long.MAX_VALUE, ((long[]) tablet.getValues()[7])[0]);
    }
    assertObjectStorageAbsent(RelationalInsertNumericRowNode.class, node);
  }

  private RelationalInsertNumericRowNode nodeWithNullTag() throws Exception {
    InsertNumericRowStatement statement =
        (InsertNumericRowStatement) StatementGenerator.createStatement(request());
    statement.insertColumn(
        0,
        new ColumnSchema(
            "tag", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    MeasurementSchema[] schemas = new MeasurementSchema[statement.getValueCount()];
    for (int i = 0; i < schemas.length; i++) {
      schemas[i] = new MeasurementSchema(statement.getMeasurements()[i], statement.getDataType(i));
    }
    RelationalInsertNumericRowNode node =
        new RelationalInsertNumericRowNode(
            new PlanNodeId("numeric"),
            statement.getDevicePath(),
            statement.getMeasurements(),
            statement.getDataTypes(),
            schemas,
            statement.getTime(),
            statement.getNumericValues(),
            statement.getColumnCategories());
    node.setNumericValues(statement.getNumericValues());
    return node;
  }

  private void assertObjectStorageAbsent(Class<?> owner, Object row) throws Exception {
    // A dedicated numeric type must not inherit the object row or retain an Object[] payload.
    assertFalse(InsertRowStatement.class.isInstance(row));
    assertFalse(InsertRowNode.class.isInstance(row));
    for (Class<?> type = owner; type != null; type = type.getSuperclass()) {
      for (Field field : type.getDeclaredFields()) {
        assertFalse(field.getName(), field.getType() == Object[].class);
      }
    }
  }
}
