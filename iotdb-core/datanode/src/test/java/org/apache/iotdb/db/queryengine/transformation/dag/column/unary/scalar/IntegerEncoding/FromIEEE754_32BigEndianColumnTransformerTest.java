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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.BytesToFloatColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.NumericCodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;

public class FromIEEE754_32BigEndianColumnTransformerTest {

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

  /** Test a positive float decoding using FROM_IEEE754_32_BIG_ENDIAN strategy. */
  @Test
  public void testFromIeee75432BigEndianPositive() {
    float expected = 123.456f;
    Binary[] values = new Binary[] {new Binary(floatToBigEndianBytes(expected))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);
    BytesToFloatColumnTransformer transformer =
        new BytesToFloatColumnTransformer(
            FLOAT,
            child,
            NumericCodecStrategiesFactory.FROM_IEEE754_32_BIG_ENDIAN,
            "from_ieee754_32",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getFloat(0), 0.0f);
  }

  /** Test a negative float decoding. */
  @Test
  public void testFromIeee75432BigEndianNegative() {
    float expected = -987.654f;
    Binary[] values = new Binary[] {new Binary(floatToBigEndianBytes(expected))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);
    BytesToFloatColumnTransformer transformer =
        new BytesToFloatColumnTransformer(
            FLOAT,
            child,
            NumericCodecStrategiesFactory.FROM_IEEE754_32_BIG_ENDIAN,
            "from_ieee754_32",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getFloat(0), 0.0f);
  }

  /** Test special float value decoding: Positive Infinity. */
  @Test
  public void testFromIeee75432BigEndianPositiveInfinity() {
    float expected = Float.POSITIVE_INFINITY;
    Binary[] values = new Binary[] {new Binary(floatToBigEndianBytes(expected))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);
    BytesToFloatColumnTransformer transformer =
        new BytesToFloatColumnTransformer(
            FLOAT,
            child,
            NumericCodecStrategiesFactory.FROM_IEEE754_32_BIG_ENDIAN,
            "from_ieee754_32",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expected, result.getFloat(0), 0.0f);
  }

  /** Test special float value decoding: NaN. */
  @Test
  public void testFromIeee75432BigEndianNaN() {
    float expected = Float.NaN;
    Binary[] values = new Binary[] {new Binary(floatToBigEndianBytes(expected))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);
    BytesToFloatColumnTransformer transformer =
        new BytesToFloatColumnTransformer(
            FLOAT,
            child,
            NumericCodecStrategiesFactory.FROM_IEEE754_32_BIG_ENDIAN,
            "from_ieee754_32",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertTrue(Float.isNaN(result.getFloat(0)));
  }

  /** Test multi-row decoding with a null value. */
  @Test
  public void testFromIeee75432BigEndianMultiRowsWithNull() {
    Binary[] values = {
      new Binary(floatToBigEndianBytes(1.1f)), null, new Binary(floatToBigEndianBytes(-2.2f))
    };
    boolean[] isNull = {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(isNull), values);

    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);
    BytesToFloatColumnTransformer transformer =
        new BytesToFloatColumnTransformer(
            FLOAT,
            child,
            NumericCodecStrategiesFactory.FROM_IEEE754_32_BIG_ENDIAN,
            "from_ieee754_32",
            BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(1.1f, result.getFloat(0), 0.0f);
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(-2.2f, result.getFloat(2), 0.0f);
  }

  /** Test decoding with a short binary input (< 4 bytes), expecting an exception. */
  @Test
  public void testFromIeee75432BigEndianInvalidLengthShort() {
    Binary[] values = {new Binary(new byte[] {1, 2, 3})}; // Too short
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    BytesToFloatColumnTransformer transformer =
        new BytesToFloatColumnTransformer(
            FLOAT,
            child,
            NumericCodecStrategiesFactory.FROM_IEEE754_32_BIG_ENDIAN,
            "from_ieee754_32",
            BLOB);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException was not thrown for short input.");
    } catch (SemanticException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Failed to execute function 'from_ieee754_32' due to an invalid input format."));
    }
  }

  /** Test decoding with a long binary input (> 4 bytes), expecting an exception. */
  @Test
  public void testFromIeee75432BigEndianInvalidLengthLong() {
    Binary[] values = {new Binary(new byte[] {1, 2, 3, 4, 5})}; // Too long
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    BytesToFloatColumnTransformer transformer =
        new BytesToFloatColumnTransformer(
            FLOAT,
            child,
            NumericCodecStrategiesFactory.FROM_IEEE754_32_BIG_ENDIAN,
            "from_ieee754_32",
            BLOB);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException was not thrown for long input.");
    } catch (SemanticException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Failed to execute function 'from_ieee754_32' due to an invalid input format."));
    }
  }
}
