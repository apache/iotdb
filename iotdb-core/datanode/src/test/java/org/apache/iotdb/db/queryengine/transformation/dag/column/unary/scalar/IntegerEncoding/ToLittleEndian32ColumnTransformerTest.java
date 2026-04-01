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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.IntToBytesColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.NumericCodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BlobType.BLOB;

public class ToLittleEndian32ColumnTransformerTest {

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  // Helper method to convert an integer to a little-endian 4-byte array.
  private byte[] intToLittleEndianBytes(int value) {
    return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
  }

  /** Test a positive integer conversion using TO_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testToLittleEndian32Positive() {
    int input = 16909060; // 0x01020304
    int[] values = new int[] {input};
    Column intColumn = new IntColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(intColumn);
    IntToBytesColumnTransformer transformer =
        new IntToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_32);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = intToLittleEndianBytes(input); // {4, 3, 2, 1}
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test a negative integer conversion using TO_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testToLittleEndian32Negative() {
    int input = -1; // 0xFFFFFFFF
    int[] values = new int[] {input};
    Column intColumn = new IntColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(intColumn);
    IntToBytesColumnTransformer transformer =
        new IntToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_32);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = intToLittleEndianBytes(input); // {-1, -1, -1, -1}
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test integer zero conversion using TO_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testToLittleEndian32Zero() {
    int input = 0; // 0x00000000
    int[] values = new int[] {input};
    Column intColumn = new IntColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(intColumn);
    IntToBytesColumnTransformer transformer =
        new IntToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_32);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = intToLittleEndianBytes(input); // {0, 0, 0, 0}
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test Integer.MAX_VALUE conversion using TO_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testToLittleEndian32MaxValue() {
    int input = Integer.MAX_VALUE; // 0x7FFFFFFF
    int[] values = new int[] {input};
    Column intColumn = new IntColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(intColumn);
    IntToBytesColumnTransformer transformer =
        new IntToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_32);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = intToLittleEndianBytes(input); // {-1, -1, -1, 127}
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test multi-row conversion with a null value using TO_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testToLittleEndian32MultiRowsWithNull() {
    int[] values = new int[] {100, 0, -200}; // Use 0 as a placeholder for null
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column intColumn = new IntColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(intColumn);

    IntToBytesColumnTransformer transformer =
        new IntToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_32);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expected1 = intToLittleEndianBytes(100);
    byte[] expected3 = intToLittleEndianBytes(-200);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }

  /** Test conversion with a selection array using TO_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testToLittleEndian32WithSelection() {
    int[] values = {5, 10, 15};
    Column intColumn = new IntColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(intColumn);

    IntToBytesColumnTransformer transformer =
        new IntToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_32);
    transformer.addReferenceCount();

    // Select and process only the first and third rows.
    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    byte[] expected1 = intToLittleEndianBytes(5);
    byte[] expected3 = intToLittleEndianBytes(15);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    // The second row was not selected, so the result should be null.
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }
}
