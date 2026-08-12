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
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TypeServicesTest {

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
