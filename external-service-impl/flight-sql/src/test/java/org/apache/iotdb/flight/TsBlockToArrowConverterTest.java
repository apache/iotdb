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

package org.apache.iotdb.flight;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/** Unit tests for TsBlockToArrowConverter. */
public class TsBlockToArrowConverterTest {

  private BufferAllocator allocator;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  // ===================== toArrowType Tests =====================

  @Test
  public void testToArrowTypeMappings() {
    assertTrue(TsBlockToArrowConverter.toArrowType(TSDataType.BOOLEAN) instanceof ArrowType.Bool);

    ArrowType int32Type = TsBlockToArrowConverter.toArrowType(TSDataType.INT32);
    assertTrue(int32Type instanceof ArrowType.Int);
    assertEquals(32, ((ArrowType.Int) int32Type).getBitWidth());
    assertTrue(((ArrowType.Int) int32Type).getIsSigned());

    ArrowType int64Type = TsBlockToArrowConverter.toArrowType(TSDataType.INT64);
    assertTrue(int64Type instanceof ArrowType.Int);
    assertEquals(64, ((ArrowType.Int) int64Type).getBitWidth());
    assertTrue(((ArrowType.Int) int64Type).getIsSigned());

    ArrowType floatType = TsBlockToArrowConverter.toArrowType(TSDataType.FLOAT);
    assertTrue(floatType instanceof ArrowType.FloatingPoint);
    assertEquals(
        FloatingPointPrecision.SINGLE, ((ArrowType.FloatingPoint) floatType).getPrecision());

    ArrowType doubleType = TsBlockToArrowConverter.toArrowType(TSDataType.DOUBLE);
    assertTrue(doubleType instanceof ArrowType.FloatingPoint);
    assertEquals(
        FloatingPointPrecision.DOUBLE, ((ArrowType.FloatingPoint) doubleType).getPrecision());

    assertTrue(TsBlockToArrowConverter.toArrowType(TSDataType.TEXT) instanceof ArrowType.Utf8);
    assertTrue(TsBlockToArrowConverter.toArrowType(TSDataType.STRING) instanceof ArrowType.Utf8);
    assertTrue(TsBlockToArrowConverter.toArrowType(TSDataType.BLOB) instanceof ArrowType.Binary);

    ArrowType tsType = TsBlockToArrowConverter.toArrowType(TSDataType.TIMESTAMP);
    assertTrue(tsType instanceof ArrowType.Timestamp);
    assertEquals(TimeUnit.MILLISECOND, ((ArrowType.Timestamp) tsType).getUnit());
    assertEquals("UTC", ((ArrowType.Timestamp) tsType).getTimezone());

    ArrowType dateType = TsBlockToArrowConverter.toArrowType(TSDataType.DATE);
    assertTrue(dateType instanceof ArrowType.Date);
    assertEquals(DateUnit.DAY, ((ArrowType.Date) dateType).getUnit());
  }

  // ===================== toArrowSchema Tests =====================

  @Test
  public void testToArrowSchema() {
    DatasetHeader header = buildHeader("col_bool", TSDataType.BOOLEAN, "col_int", TSDataType.INT32);
    Schema schema = TsBlockToArrowConverter.toArrowSchema(header);

    assertEquals(2, schema.getFields().size());
    assertEquals("col_bool", schema.getFields().get(0).getName());
    assertTrue(schema.getFields().get(0).getType() instanceof ArrowType.Bool);
    assertEquals("col_int", schema.getFields().get(1).getName());
    assertTrue(schema.getFields().get(1).getType() instanceof ArrowType.Int);
  }

  // ===================== INT32 Conversion =====================

  @Test
  public void testFillInt32Values() {
    DatasetHeader header = buildHeader("value", TSDataType.INT32);
    TsBlock tsBlock = buildTsBlock(TSDataType.INT32, new Object[] {10, 20, 30});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      IntVector vector = (IntVector) root.getVector(0);
      assertEquals(10, vector.get(0));
      assertEquals(20, vector.get(1));
      assertEquals(30, vector.get(2));
    }
  }

  @Test
  public void testFillInt32WithNulls() {
    DatasetHeader header = buildHeader("value", TSDataType.INT32);
    TsBlock tsBlock = buildTsBlock(TSDataType.INT32, new Object[] {10, null, 30});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      IntVector vector = (IntVector) root.getVector(0);
      assertEquals(10, vector.get(0));
      assertTrue(vector.isNull(1));
      assertEquals(30, vector.get(2));
    }
  }

  // ===================== INT64 Conversion =====================

  @Test
  public void testFillInt64Values() {
    DatasetHeader header = buildHeader("value", TSDataType.INT64);
    TsBlock tsBlock = buildTsBlock(TSDataType.INT64, new Object[] {100L, 200L, 300L});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      BigIntVector vector = (BigIntVector) root.getVector(0);
      assertEquals(100L, vector.get(0));
      assertEquals(200L, vector.get(1));
      assertEquals(300L, vector.get(2));
    }
  }

  // ===================== FLOAT Conversion =====================

  @Test
  public void testFillFloatValues() {
    DatasetHeader header = buildHeader("value", TSDataType.FLOAT);
    TsBlock tsBlock = buildTsBlock(TSDataType.FLOAT, new Object[] {1.1f, 2.2f, 3.3f});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      Float4Vector vector = (Float4Vector) root.getVector(0);
      assertEquals(1.1f, vector.get(0), 0.001f);
      assertEquals(2.2f, vector.get(1), 0.001f);
      assertEquals(3.3f, vector.get(2), 0.001f);
    }
  }

  // ===================== DOUBLE Conversion =====================

  @Test
  public void testFillDoubleValues() {
    DatasetHeader header = buildHeader("value", TSDataType.DOUBLE);
    TsBlock tsBlock = buildTsBlock(TSDataType.DOUBLE, new Object[] {1.11, 2.22, 3.33});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      Float8Vector vector = (Float8Vector) root.getVector(0);
      assertEquals(1.11, vector.get(0), 0.0001);
      assertEquals(2.22, vector.get(1), 0.0001);
      assertEquals(3.33, vector.get(2), 0.0001);
    }
  }

  // ===================== BOOLEAN Conversion =====================

  @Test
  public void testFillBooleanValues() {
    DatasetHeader header = buildHeader("value", TSDataType.BOOLEAN);
    TsBlock tsBlock = buildTsBlock(TSDataType.BOOLEAN, new Object[] {true, false, true});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      BitVector vector = (BitVector) root.getVector(0);
      assertEquals(1, vector.get(0));
      assertEquals(0, vector.get(1));
      assertEquals(1, vector.get(2));
    }
  }

  @Test
  public void testFillBooleanWithNulls() {
    DatasetHeader header = buildHeader("value", TSDataType.BOOLEAN);
    TsBlock tsBlock = buildTsBlock(TSDataType.BOOLEAN, new Object[] {true, null, false});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      BitVector vector = (BitVector) root.getVector(0);
      assertEquals(1, vector.get(0));
      assertTrue(vector.isNull(1));
      assertEquals(0, vector.get(2));
    }
  }

  // ===================== TEXT/STRING Conversion =====================

  @Test
  public void testFillTextValues() {
    DatasetHeader header = buildHeader("value", TSDataType.TEXT);
    TsBlock tsBlock =
        buildTsBlock(
            TSDataType.TEXT,
            new Object[] {
              new Binary("hello", StandardCharsets.UTF_8),
              new Binary("world", StandardCharsets.UTF_8),
              new Binary("IoTDB", StandardCharsets.UTF_8)
            });

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      VarCharVector vector = (VarCharVector) root.getVector(0);
      assertEquals("hello", new String(vector.get(0), StandardCharsets.UTF_8));
      assertEquals("world", new String(vector.get(1), StandardCharsets.UTF_8));
      assertEquals("IoTDB", new String(vector.get(2), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testFillTextWithNulls() {
    DatasetHeader header = buildHeader("value", TSDataType.TEXT);
    TsBlock tsBlock =
        buildTsBlock(
            TSDataType.TEXT,
            new Object[] {new Binary("hello", StandardCharsets.UTF_8), null, null});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      VarCharVector vector = (VarCharVector) root.getVector(0);
      assertEquals("hello", new String(vector.get(0), StandardCharsets.UTF_8));
      assertTrue(vector.isNull(1));
      assertTrue(vector.isNull(2));
    }
  }

  // ===================== BLOB Conversion =====================

  @Test
  public void testFillBlobValues() {
    DatasetHeader header = buildHeader("value", TSDataType.BLOB);
    byte[] bytes1 = {0x01, 0x02, 0x03};
    byte[] bytes2 = {0x04, 0x05};
    TsBlock tsBlock =
        buildTsBlock(TSDataType.BLOB, new Object[] {new Binary(bytes1), new Binary(bytes2)});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(2, root.getRowCount());
      VarBinaryVector vector = (VarBinaryVector) root.getVector(0);
      assertArrayEquals(bytes1, vector.get(0));
      assertArrayEquals(bytes2, vector.get(1));
    }
  }

  // ===================== TIMESTAMP Conversion =====================

  @Test
  public void testFillTimestampValues() {
    DatasetHeader header = buildHeader("value", TSDataType.TIMESTAMP);
    TsBlock tsBlock =
        buildTsBlock(
            TSDataType.TIMESTAMP, new Object[] {1609459200000L, 1609459260000L, 1609459320000L});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      TimeStampMilliTZVector vector = (TimeStampMilliTZVector) root.getVector(0);
      assertEquals(1609459200000L, vector.get(0));
      assertEquals(1609459260000L, vector.get(1));
      assertEquals(1609459320000L, vector.get(2));
    }
  }

  // ===================== DATE Conversion =====================

  @Test
  public void testFillDateValues() {
    DatasetHeader header = buildHeader("value", TSDataType.DATE);
    // Epoch days: 2021-01-01 = 18628
    TsBlock tsBlock = buildTsBlock(TSDataType.DATE, new Object[] {18628, 18629, 18630});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(3, root.getRowCount());
      DateDayVector vector = (DateDayVector) root.getVector(0);
      assertEquals(18628, vector.get(0));
      assertEquals(18629, vector.get(1));
      assertEquals(18630, vector.get(2));
    }
  }

  // ===================== Multi-Column Tests =====================

  @Test
  public void testFillMultipleColumns() {
    List<ColumnHeader> headers = new ArrayList<>();
    headers.add(new ColumnHeader("int_col", TSDataType.INT32));
    headers.add(new ColumnHeader("double_col", TSDataType.DOUBLE));
    headers.add(new ColumnHeader("text_col", TSDataType.TEXT));
    DatasetHeader header = new DatasetHeader(headers, true);
    List<String> colNames = header.getRespColumns();
    header.setTreeColumnToTsBlockIndexMap(colNames);

    List<TSDataType> types = Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE, TSDataType.TEXT);
    TsBlockBuilder builder = new TsBlockBuilder(types);
    ColumnBuilder[] cols = builder.getValueColumnBuilders();

    // Row 1
    cols[0].writeInt(42);
    cols[1].writeDouble(3.14);
    cols[2].writeBinary(new Binary("test", StandardCharsets.UTF_8));
    builder.declarePosition();

    // Row 2
    cols[0].writeInt(100);
    cols[1].writeDouble(2.71);
    cols[2].writeBinary(new Binary("hello", StandardCharsets.UTF_8));
    builder.declarePosition();

    TsBlock tsBlock =
        builder.build(
            new RunLengthEncodedColumn(
                new TimeColumn(1, new long[] {0L}), builder.getPositionCount()));

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);

      assertEquals(2, root.getRowCount());

      IntVector intVec = (IntVector) root.getVector("int_col");
      assertEquals(42, intVec.get(0));
      assertEquals(100, intVec.get(1));

      Float8Vector doubleVec = (Float8Vector) root.getVector("double_col");
      assertEquals(3.14, doubleVec.get(0), 0.001);
      assertEquals(2.71, doubleVec.get(1), 0.001);

      VarCharVector textVec = (VarCharVector) root.getVector("text_col");
      assertEquals("test", new String(textVec.get(0), StandardCharsets.UTF_8));
      assertEquals("hello", new String(textVec.get(1), StandardCharsets.UTF_8));
    }
  }

  // ===================== Empty TsBlock =====================

  @Test
  public void testFillEmptyTsBlock() {
    DatasetHeader header = buildHeader("value", TSDataType.INT32);
    TsBlock tsBlock = buildTsBlock(TSDataType.INT32, new Object[] {});

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(header, allocator)) {
      TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, header);
      assertEquals(0, root.getRowCount());
    }
  }

  // ===================== Helper Methods =====================

  /** Build a simple DatasetHeader with one or more columns (name, type pairs). */
  private DatasetHeader buildHeader(Object... nameTypePairs) {
    List<ColumnHeader> columnHeaders = new ArrayList<>();
    for (int i = 0; i < nameTypePairs.length; i += 2) {
      columnHeaders.add(
          new ColumnHeader((String) nameTypePairs[i], (TSDataType) nameTypePairs[i + 1]));
    }
    DatasetHeader header = new DatasetHeader(columnHeaders, true);
    List<String> colNames = header.getRespColumns();
    header.setTreeColumnToTsBlockIndexMap(colNames);
    return header;
  }

  /**
   * Build a single-column TsBlock with the given data type and values. Null values in the array
   * produce null entries in the TsBlock.
   */
  private TsBlock buildTsBlock(TSDataType type, Object[] values) {
    TsBlockBuilder builder = new TsBlockBuilder(java.util.Collections.singletonList(type));
    ColumnBuilder[] cols = builder.getValueColumnBuilders();

    for (Object value : values) {
      if (value == null) {
        cols[0].appendNull();
      } else {
        switch (type) {
          case BOOLEAN:
            cols[0].writeBoolean((Boolean) value);
            break;
          case INT32:
          case DATE:
            cols[0].writeInt((Integer) value);
            break;
          case INT64:
          case TIMESTAMP:
            cols[0].writeLong((Long) value);
            break;
          case FLOAT:
            cols[0].writeFloat((Float) value);
            break;
          case DOUBLE:
            cols[0].writeDouble((Double) value);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            cols[0].writeBinary((Binary) value);
            break;
          default:
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
      }
      builder.declarePosition();
    }

    return builder.build(
        new RunLengthEncodedColumn(new TimeColumn(1, new long[] {0L}), builder.getPositionCount()));
  }
}
