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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DoubleToBytesColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.NumericCodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BlobType.BLOB;

public class ToIEEE754_64BigEndianColumnTransformerTest {

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  // Helper method to convert a double to a big-endian 8-byte array.
  private byte[] doubleToBigEndianBytes(double value) {
    return ByteBuffer.allocate(8).putDouble(value).array();
  }

  /** Test a positive double conversion using TO_IEEE754_64_BIG_ENDIAN strategy. */
  @Test
  public void testToIeee75464BigEndianPositive() {
    double input = 123456.7890123;
    double[] values = new double[] {input};
    Column doubleColumn = new DoubleColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(doubleColumn);
    DoubleToBytesColumnTransformer transformer =
        new DoubleToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_64_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = doubleToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test a negative double conversion. */
  @Test
  public void testToIeee75464BigEndianNegative() {
    double input = -987654.3210987;
    double[] values = new double[] {input};
    Column doubleColumn = new DoubleColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(doubleColumn);
    DoubleToBytesColumnTransformer transformer =
        new DoubleToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_64_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = doubleToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test special double value: Positive Infinity. */
  @Test
  public void testToIeee75464BigEndianPositiveInfinity() {
    double input = Double.POSITIVE_INFINITY;
    double[] values = new double[] {input};
    Column doubleColumn = new DoubleColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(doubleColumn);
    DoubleToBytesColumnTransformer transformer =
        new DoubleToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_64_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = doubleToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test special double value: NaN. */
  @Test
  public void testToIeee75464BigEndianNaN() {
    double input = Double.NaN;
    double[] values = new double[] {input};
    Column doubleColumn = new DoubleColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(doubleColumn);
    DoubleToBytesColumnTransformer transformer =
        new DoubleToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_64_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = doubleToBigEndianBytes(input);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test multi-row conversion with a null value. */
  @Test
  public void testToIeee75464BigEndianMultiRowsWithNull() {
    double[] values = new double[] {1.12, 0.0, -2.23}; // 0.0 is placeholder for null
    boolean[] isNull = {false, true, false};
    Column doubleColumn = new DoubleColumn(values.length, Optional.of(isNull), values);

    ColumnTransformer child = mockChildColumnTransformer(doubleColumn);
    DoubleToBytesColumnTransformer transformer =
        new DoubleToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_IEEE754_64_BIG_ENDIAN);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expected1 = doubleToBigEndianBytes(1.12);
    byte[] expected3 = doubleToBigEndianBytes(-2.23);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }
}
