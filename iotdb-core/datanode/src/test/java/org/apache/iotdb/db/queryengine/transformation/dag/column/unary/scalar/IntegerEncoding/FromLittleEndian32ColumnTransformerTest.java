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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.BytesToIntColumnTransformer;
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
import static org.apache.tsfile.read.common.type.IntType.INT32;

public class FromLittleEndian32ColumnTransformerTest {

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

  /** Test a positive integer decoding using FROM_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testFromLittleEndian32Positive() {
    int expected = 16909060; // 0x01020304
    Binary[] values = new Binary[] {new Binary(intToLittleEndianBytes(expected))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    BytesToIntColumnTransformer transformer =
        new BytesToIntColumnTransformer(
            INT32,
            childColumnTransformer,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_32,
            "from_little_endian_32",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getInt(0));
  }

  /** Test a negative integer decoding using FROM_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testFromLittleEndian32Negative() {
    int expected = -1; // 0xFFFFFFFF
    Binary[] values = new Binary[] {new Binary(intToLittleEndianBytes(expected))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    BytesToIntColumnTransformer transformer =
        new BytesToIntColumnTransformer(
            INT32,
            childColumnTransformer,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_32,
            "from_little_endian_32",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getInt(0));
  }

  /** Test multi-row decoding with a null value using FROM_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testFromLittleEndian32MultiRowsWithNull() {
    Binary[] values =
        new Binary[] {
          new Binary(intToLittleEndianBytes(100)), null, new Binary(intToLittleEndianBytes(-200))
        };
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);

    BytesToIntColumnTransformer transformer =
        new BytesToIntColumnTransformer(
            INT32,
            childColumnTransformer,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_32,
            "from_little_endian_32",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(100, result.getInt(0));
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(-200, result.getInt(2));
  }

  /** Test decoding with a selection array using FROM_LITTLE_ENDIAN_32 strategy. */
  @Test
  public void testFromLittleEndian32WithSelection() {
    Binary[] values = {
      new Binary(intToLittleEndianBytes(5)),
      new Binary(intToLittleEndianBytes(10)),
      new Binary(intToLittleEndianBytes(15))
    };
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    BytesToIntColumnTransformer transformer =
        new BytesToIntColumnTransformer(
            INT32,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_32,
            "from_little_endian_32",
            BLOB);
    transformer.addReferenceCount();

    // Select and process only the first and third rows.
    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(5, result.getInt(0));
    // The second row was not selected, so the result should be null.
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(15, result.getInt(2));
  }

  /** Test decoding with a short binary input (< 4 bytes), expecting an exception. */
  @Test
  public void testFromLittleEndian32WithShortLength() {
    Binary[] values = {new Binary(new byte[] {1, 2, 3})}; // Too short
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    BytesToIntColumnTransformer transformer =
        new BytesToIntColumnTransformer(
            INT32,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_32,
            "from_little_endian_32",
            BLOB);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException was not thrown for short input.");
    } catch (SemanticException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Failed to execute function 'from_little_endian_32' due to an invalid input format."));
    }
  }

  /** Test decoding with a long binary input (> 4 bytes), expecting an exception. */
  @Test
  public void testFromLittleEndian32WithLongLength() {
    Binary[] values = {new Binary(new byte[] {1, 2, 3, 4, 5})}; // Too long
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    BytesToIntColumnTransformer transformer =
        new BytesToIntColumnTransformer(
            INT32,
            child,
            NumericCodecStrategiesFactory.FROM_LITTLE_ENDIAN_32,
            "from_little_endian_32",
            BLOB);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException was not thrown for long input.");
    } catch (SemanticException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Failed to execute function 'from_little_endian_32' due to an invalid input format."));
    }
  }
}
