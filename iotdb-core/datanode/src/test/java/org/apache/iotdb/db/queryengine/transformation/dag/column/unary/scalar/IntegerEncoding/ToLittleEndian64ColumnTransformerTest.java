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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.LongToBytesColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.NumericCodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BlobType.BLOB;

public class ToLittleEndian64ColumnTransformerTest {

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  // Helper method to convert a long to a little-endian 8-byte array.
  private byte[] longToLittleEndianBytes(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
  }

  /** Test a positive long conversion using TO_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testToLittleEndian64Positive() {
    long input = 72623859790382856L; // 0x0102030405060708L
    long[] values = new long[] {input};
    Column longColumn = new LongColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(longColumn);
    LongToBytesColumnTransformer transformer =
        new LongToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_64);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = longToLittleEndianBytes(input); // {8, 7, 6, 5, 4, 3, 2, 1}
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test a negative long conversion using TO_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testToLittleEndian64Negative() {
    long input = -1L; // 0xFFFFFFFFFFFFFFFFL
    long[] values = new long[] {input};
    Column longColumn = new LongColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(longColumn);
    LongToBytesColumnTransformer transformer =
        new LongToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_64);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = longToLittleEndianBytes(input); // {-1, -1, -1, -1, -1, -1, -1, -1}
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test long zero conversion using TO_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testToLittleEndian64Zero() {
    long input = 0L; // 0x0000000000000000L
    long[] values = new long[] {input};
    Column longColumn = new LongColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(longColumn);
    LongToBytesColumnTransformer transformer =
        new LongToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_64);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = longToLittleEndianBytes(input); // {0, 0, 0, 0, 0, 0, 0, 0}
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test Long.MAX_VALUE conversion using TO_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testToLittleEndian64MaxValue() {
    long input = Long.MAX_VALUE; // 0x7FFFFFFFFFFFFFFFL
    long[] values = new long[] {input};
    Column longColumn = new LongColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(longColumn);
    LongToBytesColumnTransformer transformer =
        new LongToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_64);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = longToLittleEndianBytes(input); // {-1, -1, -1, -1, -1, -1, -1, 127}
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /** Test multi-row conversion with a null value using TO_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testToLittleEndian64MultiRowsWithNull() {
    long[] values = new long[] {1000L, 0L, -2000L}; // Use 0L as a placeholder for null
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column longColumn = new LongColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(longColumn);

    LongToBytesColumnTransformer transformer =
        new LongToBytesColumnTransformer(
            BLOB, childColumnTransformer, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_64);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expected1 = longToLittleEndianBytes(1000L);
    byte[] expected3 = longToLittleEndianBytes(-2000L);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }

  /** Test conversion with a selection array using TO_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testToLittleEndian64WithSelection() {
    long[] values = {50L, 100L, 150L};
    Column longColumn = new LongColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(longColumn);

    LongToBytesColumnTransformer transformer =
        new LongToBytesColumnTransformer(
            BLOB, child, NumericCodecStrategiesFactory.TO_LITTLE_ENDIAN_64);
    transformer.addReferenceCount();

    // Select and process only the first and third rows.
    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    byte[] expected1 = longToLittleEndianBytes(50L);
    byte[] expected3 = longToLittleEndianBytes(150L);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    // The second row was not selected, so the result should be null.
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }
}
