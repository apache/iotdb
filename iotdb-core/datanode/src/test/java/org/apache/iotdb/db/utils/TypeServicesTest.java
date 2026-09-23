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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TypeServicesTest {

  @Test
  public void testTabletBinarySerializationMatchesLegacyFormat() throws IOException {
    Binary[] values = {
      new Binary(new byte[] {1, 2, 3}),
      null,
      new Binary((byte[]) null),
      new Binary(new byte[0]),
      new Binary(new byte[100])
    };
    // Four active rows: length + payload, then three zero-length placeholders. No presence bytes.
    byte[] expected =
        ByteBuffer.allocate(19)
            .putInt(3)
            .put(new byte[] {1, 2, 3})
            .putInt(0)
            .putInt(0)
            .putInt(0)
            .array();
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Type type = Type.fromTsDataType(dataType);
      int size =
          TypeServices.StorageEngine.INSERT_TABLET_SERIALIZED_COLUMN_SIZE_SERVICE
              .call(type)
              .size(values, 4);
      assertEquals(expected.length, size);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      TypeServices.StorageEngine.RAW_ARRAY_BYTE_BUFFER_SERIALIZER_SERVICE
          .call(type)
          .serialize(values, 4, buffer);
      assertEquals(size, buffer.position());
      assertArrayEquals(expected, buffer.array());

      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream stream = new DataOutputStream(bytes)) {
        TypeServices.StorageEngine.RAW_ARRAY_OUTPUT_STREAM_SERIALIZER_SERVICE
            .call(type)
            .serialize(values, 4, stream);
      }
      assertArrayEquals(expected, bytes.toByteArray());
    }
  }

  @Test
  public void testWalBinaryRangeSizeMatchesWrittenBytes() {
    Binary[] values = {
      new Binary(new byte[100]),
      new Binary(new byte[] {1, 2, 3}),
      null,
      new Binary((byte[]) null),
      new Binary(new byte[0]),
      new Binary(new byte[100])
    };
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Type type = Type.fromTsDataType(dataType);
      int size =
          TypeServices.StorageEngine.INSERT_TABLET_SERIALIZED_COLUMN_SIZE_SERVICE
              .call(type)
              .size(values, 1, 5);
      assertEquals(19, size);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      TypeServices.StorageEngine.WAL_ARRAY_WRITER_SERVICE
          .call(type)
          .write(values, new WALByteBufferForTest(buffer), 1, 5);
      assertEquals(size, buffer.position());
      buffer.flip();
      assertEquals(3, buffer.getInt());
      byte[] payload = new byte[3];
      buffer.get(payload);
      assertArrayEquals(new byte[] {1, 2, 3}, payload);
      assertEquals(0, buffer.getInt());
      assertEquals(0, buffer.getInt());
      assertEquals(0, buffer.getInt());
      assertFalse(buffer.hasRemaining());
    }
  }

  @Test
  public void testMaxMinByReadsXWithoutModifyingInput() {
    // A selected timestamp must replace the result, including on reuse, without changing the input.
    for (TSDataType dataType : new TSDataType[] {TSDataType.INT64, TSDataType.TIMESTAMP}) {
      Type type = Type.fromTsDataType(dataType);
      Column input = new LongColumn(2, Optional.empty(), new long[] {2499, 8499});
      TsPrimitiveType result = type.getTsPrimitiveType();
      TypeServices.Aggregation.MaxMinByAccumulatorStrategy strategy =
          TypeServices.Aggregation.MAX_MIN_BY_ACCUMULATOR_STRATEGY_SERVICE.call(type);
      strategy.setXResult(result, input, 1);
      assertEquals(8499, result.getLong());
      strategy.setXResult(result, input, 0);
      assertEquals(2499, result.getLong());
      assertEquals(2499, input.getLong(0));
      assertEquals(8499, input.getLong(1));
    }
  }

  @Test
  public void testInsertSerializedSizesPreserveNullBinaryAndActiveRows() {
    Binary value = new Binary(new byte[] {1, 2, 3});
    Binary[] values = {value, null, new Binary((byte[]) null), new Binary(new byte[100])};
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Type type = Type.fromTsDataType(dataType);
      assertEquals(
          7,
          TypeServices.StorageEngine.INSERT_ROW_SERIALIZED_VALUE_SIZE_SERVICE
              .call(type)
              .applyAsInt(value));
      // The spare capacity must not contribute to the serialized size.
      assertEquals(
          15,
          TypeServices.StorageEngine.INSERT_TABLET_SERIALIZED_COLUMN_SIZE_SERVICE
              .call(type)
              .size(values, 3));
    }
    assertEquals(
        12,
        TypeServices.StorageEngine.INSERT_TABLET_SERIALIZED_COLUMN_SIZE_SERVICE
            .call(Type.fromTsDataType(TSDataType.DATE))
            .size(new int[5], 3));
    assertEquals(
        24,
        TypeServices.StorageEngine.INSERT_TABLET_SERIALIZED_COLUMN_SIZE_SERVICE
            .call(Type.fromTsDataType(TSDataType.TIMESTAMP))
            .size(new long[5], 3));
  }

  @Test
  public void testPlainFastPathExcludesObject() {
    for (TSDataType type :
        new TSDataType[] {
          TSDataType.BOOLEAN,
          TSDataType.INT32,
          TSDataType.DATE,
          TSDataType.INT64,
          TSDataType.TIMESTAMP,
          TSDataType.FLOAT,
          TSDataType.DOUBLE,
          TSDataType.TEXT,
          TSDataType.STRING,
          TSDataType.BLOB
        }) {
      assertTrue(
          TypeServices.StorageEngine.TABLET_PLAIN_FAST_PATH_SERVICE.call(
              Type.fromTsDataType(type)));
    }
    assertFalse(
        TypeServices.StorageEngine.TABLET_PLAIN_FAST_PATH_SERVICE.call(
            Type.fromTsDataType(TSDataType.OBJECT)));
  }

  @Test
  public void testAlteredDataTypeNumericConversion() {
    final Column intSource =
        new IntColumn(
            3,
            Optional.of(new boolean[] {false, true, false}),
            new int[] {1, 0, -2},
            TSDataType.INT32);
    final Column longSource = new LongColumn(1, Optional.of(new boolean[] {false}), new long[] {3});
    final Column floatSource =
        new FloatColumn(1, Optional.of(new boolean[] {false}), new float[] {4.5F});

    final Column int64Result = transform(intSource, TSDataType.INT64);
    final Column floatResult = transform(intSource, TSDataType.FLOAT);
    final Column doubleResult = transform(intSource, TSDataType.DOUBLE);

    assertEquals(1L, int64Result.getLong(0));
    assertTrue(int64Result.isNull(1));
    assertEquals(-2L, int64Result.getLong(2));
    assertEquals(1.0F, floatResult.getFloat(0), 0.0F);
    assertTrue(floatResult.isNull(1));
    assertEquals(-2.0F, floatResult.getFloat(2), 0.0F);
    assertEquals(1.0, doubleResult.getDouble(0), 0.0);
    assertTrue(doubleResult.isNull(1));
    assertEquals(-2.0, doubleResult.getDouble(2), 0.0);
    assertEquals(3.0, transform(longSource, TSDataType.DOUBLE).getDouble(0), 0.0);
    assertEquals(4.5, transform(floatSource, TSDataType.DOUBLE).getDouble(0), 0.0);
  }

  @Test
  public void testAlteredDataTypeTextConversion() {
    final int date = DateUtils.parseDateExpressionToInt(LocalDate.of(2026, 8, 12));
    final Column dateColumn =
        new IntColumn(1, Optional.of(new boolean[] {false}), new int[] {date}, TSDataType.DATE);
    final Column booleanColumn =
        new BooleanColumn(1, Optional.of(new boolean[] {false}), new boolean[] {true});

    assertEquals("2026-08-12", transform(dateColumn, TSDataType.TEXT).getBinary(0).toString());
    assertEquals("true", transform(booleanColumn, TSDataType.STRING).getBinary(0).toString());
  }

  @Test
  public void testAlteredDataTypeIncompatibleValuesBecomeNull() {
    final Column source =
        new BinaryColumn(
            2,
            Optional.of(new boolean[] {false, false}),
            new Binary[] {
              new Binary("1", StandardCharsets.UTF_8), new Binary("2", StandardCharsets.UTF_8)
            });

    final Column result = transform(source, TSDataType.INT32);

    assertTrue(result.isNull(0));
    assertTrue(result.isNull(1));
  }

  @Test
  public void testAlteredDataTypeCompatibleColumnIsReused() {
    final Column intSource =
        new IntColumn(1, Optional.of(new boolean[] {false}), new int[] {1}, TSDataType.INT32);
    final Column dateSource =
        new IntColumn(1, Optional.of(new boolean[] {false}), new int[] {1}, TSDataType.DATE);

    final Column sameTypeResult = transform(intSource, TSDataType.INT32);
    final Column intToDateResult = transform(intSource, TSDataType.DATE);
    final Column dateToIntResult = transform(dateSource, TSDataType.INT32);

    assertSame(intSource, sameTypeResult);
    assertSame(intSource, intToDateResult);
    assertFalse(sameTypeResult.isNull(0));
    assertTrue(dateToIntResult.isNull(0));
  }

  @Test
  public void testPreparedParameterLiteralConversion() {
    final Pair<Literal, String> stringResult = convertPreparedParameter(TSDataType.STRING, "it's");
    final Pair<Literal, String> blobResult =
        convertPreparedParameter(TSDataType.BLOB, new byte[] {(byte) 0xAB, 0x01});

    assertEquals("it's", ((StringLiteral) stringResult.left).getValue());
    assertEquals("'it''s'", stringResult.right);
    assertEquals("AB01", ((BinaryLiteral) blobResult.left).toHexString());
    assertEquals("X'AB01'", blobResult.right);
  }

  private static Column transform(final Column source, final TSDataType targetType) {
    return TypeServices.Transformation.ALTERED_DATA_TYPE_COLUMN_TRANSFORMER_SERVICE
        .call(Type.fromTsDataType(targetType))
        .transform(source, source.getPositionCount());
  }

  private static Pair<Literal, String> convertPreparedParameter(
      final TSDataType type, final Object value) {
    return TypeServices.ValueConversion.PREPARED_PARAMETER_LITERAL_SERVICE
        .call(Type.fromTsDataType(type))
        .apply(value);
  }
}
