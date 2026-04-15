package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.hashing;

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

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.GenericCodecColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.CodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;

public class SpookyHashV2_32ColumnTransformerTest {

  private static final String FUNCTION_NAME = "spooky_hash_v2_32";

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  // Helper method to convert an integer to a big-endian 4-byte array.
  private byte[] intToBytes(int value) {
    return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
  }

  /** Test spooky_hash_v2_32 on a TEXT/STRING column. */
  @Test
  public void testSpookyHashOnText() {
    // Expected hash for 'hello' is 0xd382e6ca
    int expectedHash = 0xd382e6ca;
    Binary[] values = new Binary[] {new Binary("hello".getBytes())};
    Column textColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(textColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB, child, CodecStrategiesFactory.spooky_hash_v2_32, FUNCTION_NAME, TEXT);

    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(intToBytes(expectedHash), result.getBinary(0).getValues());
  }

  /** Test spooky_hash_v2_32 on a BLOB column. */
  @Test
  public void testSpookyHashOnBlob() {
    // Expected hash for blob x'74657374' ('test') is 0xec0d8b75
    int expectedHash = 0xec0d8b75;
    byte[] inputBytes = new byte[] {(byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74};
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column blobColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(blobColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB, child, CodecStrategiesFactory.spooky_hash_v2_32, FUNCTION_NAME, BLOB);

    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(intToBytes(expectedHash), result.getBinary(0).getValues());
  }

  /** Test spooky_hash_v2_32 on a column with null values. */
  @Test
  public void testSpookyHashWithNull() {
    // Expected hash for 'world' is 0x4a0db65a
    int expectedHash = 0xaf3fbe25;
    Binary[] values = new Binary[] {null, new Binary("world".getBytes())};
    boolean[] isNull = {true, false};
    Column textColumn = new BinaryColumn(values.length, Optional.of(isNull), values);

    ColumnTransformer child = mockChildColumnTransformer(textColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB, child, CodecStrategiesFactory.spooky_hash_v2_32, FUNCTION_NAME, TEXT);

    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(2, result.getPositionCount());
    Assert.assertTrue(result.isNull(0));
    Assert.assertFalse(result.isNull(1));
    Assert.assertArrayEquals(intToBytes(expectedHash), result.getBinary(1).getValues());
  }

  /** Test spooky_hash_v2_32 with a selection array. */
  @Test
  public void testSpookyHashWithSelection() {
    // Hash for 'A' is 0x3d43503b
    // Hash for 'C' is 0xc1636130
    int hashA = 0xbec890ba;
    int hashC = 0x9bec4de2;
    Binary[] values = {
      new Binary("A".getBytes()), new Binary("B".getBytes()), new Binary("C".getBytes())
    };
    Column textColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(textColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB, child, CodecStrategiesFactory.spooky_hash_v2_32, FUNCTION_NAME, TEXT);

    transformer.addReferenceCount();

    // Select only the first and third elements
    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    // First element is processed
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(intToBytes(hashA), result.getBinary(0).getValues());
    // Second element is skipped (should be null)
    Assert.assertTrue(result.isNull(1));
    // Third element is processed
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(intToBytes(hashC), result.getBinary(2).getValues());
  }

  /** Test spooky_hash_v2_32 on an empty string. */
  @Test
  public void testSpookyHashEmptyString() {
    // Expected hash for '' (empty string) with default seed is 0x6bf50919
    int expectedHash = 0x6bf50919;
    Binary[] values = new Binary[] {new Binary("".getBytes())};
    Column textColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(textColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB, child, CodecStrategiesFactory.spooky_hash_v2_32, FUNCTION_NAME, TEXT);

    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(intToBytes(expectedHash), result.getBinary(0).getValues());
  }
}
