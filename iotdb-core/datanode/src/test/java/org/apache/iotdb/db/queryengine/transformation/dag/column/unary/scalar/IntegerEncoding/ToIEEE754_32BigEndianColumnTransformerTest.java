/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.IntegerEncoding;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.FloatToBytesColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.NumericCodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BlobType.BLOB;

public class ToIEEE754_32BigEndianColumnTransformerTest {

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  // Helper method to convert a float to a big-endian 4-byte array.
  private byte[] floatToBigEndianBytes(float value) {
    return ByteBuffer.allocate(4).putFloat(value).array();
  }

  /** Test a positive float conversion using TO_IEEE754_32_BIG_ENDIAN strategy. */
  @Test
  public void testToIeee75432BigEndianPositive() {
    float input = 123.456f;
    float[] values = new float[] {input};
    Column floatColumn = new FloatColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(floatColumn);
    FloatToBytesColumnTransformer transformer =
        new FloatToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_32_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = floatToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test a negative float conversion. */
  @Test
  public void testToIeee75432BigEndianNegative() {
    float input = -987.654f;
    float[] values = new float[] {input};
    Column floatColumn = new FloatColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(floatColumn);
    FloatToBytesColumnTransformer transformer =
        new FloatToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_32_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = floatToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test float zero conversion. */
  @Test
  public void testToIeee75432BigEndianZero() {
    float input = 0.0f;
    float[] values = new float[] {input};
    Column floatColumn = new FloatColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(floatColumn);
    FloatToBytesColumnTransformer transformer =
        new FloatToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_32_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = floatToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test special float value: Positive Infinity. */
  @Test
  public void testToIeee75432BigEndianPositiveInfinity() {
    float input = Float.POSITIVE_INFINITY;
    float[] values = new float[] {input};
    Column floatColumn = new FloatColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(floatColumn);
    FloatToBytesColumnTransformer transformer =
        new FloatToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_32_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = floatToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test special float value: NaN. */
  @Test
  public void testToIeee75432BigEndianNaN() {
    float input = Float.NaN;
    float[] values = new float[] {input};
    Column floatColumn = new FloatColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(floatColumn);
    FloatToBytesColumnTransformer transformer =
        new FloatToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_32_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = floatToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test multi-row conversion with a null value. */
  @Test
  public void testToIeee75432BigEndianMultiRowsWithNull() {
    float[] values = new float[] {1.1f, 0f, -2.2f}; // 0f is placeholder for null
    boolean[] isNull = {false, true, false};
    Column floatColumn = new FloatColumn(values.length, Optional.of(isNull), values);

    ColumnTransformer child = mockChildColumnTransformer(floatColumn);
    FloatToBytesColumnTransformer transformer =
        new FloatToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_32_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expected1 = floatToBigEndianBytes(1.1f);
    byte[] expected3 = floatToBigEndianBytes(-2.2f);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }
}
