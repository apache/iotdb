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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.BytesToLongColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.NumericCodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.LongType.INT64;

public class FromLittleEndian64ColumnTransformerTest {

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

  /** Test a positive long conversion using FROM_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testFromLittleEndian64Positive() {
    long expected = 72623859790382856L; // 0x0102030405060708L
    Binary[] values = new Binary[] {new Binary(longToLittleEndianBytes(expected))};
    Column blobColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(blobColumn);
    BytesToLongColumnTransformer transformer =
        new BytesToLongColumnTransformer(
            INT64,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_64,
            "from_little_endian_64",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getLong(0));
  }

  /** Test a negative long conversion using FROM_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testFromLittleEndian64Negative() {
    long expected = -1L; // 0xFFFFFFFFFFFFFFFFL
    Binary[] values = new Binary[] {new Binary(longToLittleEndianBytes(expected))};
    Column blobColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(blobColumn);
    BytesToLongColumnTransformer transformer =
        new BytesToLongColumnTransformer(
            INT64,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_64,
            "from_little_endian_64",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getLong(0));
  }

  /** Test long zero conversion using FROM_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testFromLittleEndian64Zero() {
    long expected = 0L;
    Binary[] values = new Binary[] {new Binary(longToLittleEndianBytes(expected))};
    Column blobColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(blobColumn);
    BytesToLongColumnTransformer transformer =
        new BytesToLongColumnTransformer(
            INT64,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_64,
            "from_little_endian_64",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getLong(0));
  }

  /** Test Long.MAX_VALUE conversion using FROM_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testFromLittleEndian64MaxValue() {
    long expected = Long.MAX_VALUE;
    Binary[] values = new Binary[] {new Binary(longToLittleEndianBytes(expected))};
    Column blobColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(blobColumn);
    BytesToLongColumnTransformer transformer =
        new BytesToLongColumnTransformer(
            INT64,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_64,
            "from_little_endian_64",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getLong(0));
  }

  /** Test multi-row conversion with a null value using FROM_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testFromLittleEndian64MultiRowsWithNull() {
    long val1 = 1000L;
    long val3 = -2000L;
    Binary[] values =
        new Binary[] {
          new Binary(longToLittleEndianBytes(val1)), null, new Binary(longToLittleEndianBytes(val3))
        };
    boolean[] isNull = {false, true, false};
    Column blobColumn = new BinaryColumn(values.length, Optional.of(isNull), values);

    ColumnTransformer child = mockChildColumnTransformer(blobColumn);
    BytesToLongColumnTransformer transformer =
        new BytesToLongColumnTransformer(
            INT64,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_64,
            "from_little_endian_64",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(val1, result.getLong(0));
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(val3, result.getLong(2));
  }

  /** Test conversion with a selection array using FROM_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testFromLittleEndian64WithSelection() {
    long val1 = 50L;
    long val2 = 100L;
    long val3 = 150L;
    Binary[] values = {
      new Binary(longToLittleEndianBytes(val1)),
      new Binary(longToLittleEndianBytes(val2)),
      new Binary(longToLittleEndianBytes(val3))
    };
    Column blobColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(blobColumn);
    BytesToLongColumnTransformer transformer =
        new BytesToLongColumnTransformer(
            INT64,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_64,
            "from_little_endian_64",
            BLOB);
    transformer.addReferenceCount();

    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(val1, result.getLong(0));
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(val3, result.getLong(2));
  }

  /** Test exception for invalid input length using FROM_LITTLE_ENDIAN_64 strategy. */
  @Test
  public void testFromLittleEndian64InvalidLength() {
    // Input is a 4-byte array, but 8 is expected.
    Binary[] values = new Binary[] {new Binary(new byte[] {1, 2, 3, 4})};
    Column blobColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(blobColumn);
    BytesToLongColumnTransformer transformer =
        new BytesToLongColumnTransformer(
            INT64,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_64,
            "from_little_endian_64",
            BLOB);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException to be thrown");
    } catch (SemanticException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Failed to execute function 'from_little_endian_64' due to an invalid input format."));
    }
  }
}
