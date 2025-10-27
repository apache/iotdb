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

// It's recommended to place this file in a new package:
// package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.hashing;
package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.hashing;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.GenericCodecColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.CodecStrategiesFactory;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.StringType.STRING;

public class XxHash64ColumnTransformerTest {

  private final XXHash64 xxHash64 = XXHashFactory.fastestInstance().hash64();
  private static final long SEED = 0L;

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  /**
   * Helper method to convert a hex string to a byte array. This is crucial for tests involving
   * binary data for two reasons: 1. It allows defining arbitrary binary inputs (like BLOBs) in a
   * human-readable format. 2. It allows defining expected binary outputs (like standard hash
   * values) from their readable hex representations for assertions.
   */
  private static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    if (len % 2 != 0) {
      throw new IllegalArgumentException("Hex string must have an even number of characters");
    }
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  /** Test XXHASH64 from a STRING input. Output should be BLOB. */
  @Test
  public void testXxHash64FromString() {
    String input = "Apache IoTDB";
    byte[] inputBytes = input.getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType
            childColumnTransformer,
            CodecStrategiesFactory.XXHASH64,
            "xxhash64",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    long hash = xxHash64.hash(inputBytes, 0, inputBytes.length, SEED);
    byte[] expectedHash = ByteBuffer.allocate(8).putLong(hash).array();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedHash, result.getBinary(0).getValues());
  }

  /** Test XXHASH64 from a BLOB input (represented by hex). Output should be BLOB. */
  @Test
  public void testXxHash64FromBlob() {
    String inputHex = "41706163686520496f544442";
    byte[] inputBytes = hexStringToByteArray(inputHex);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType
            childColumnTransformer,
            CodecStrategiesFactory.XXHASH64,
            "xxhash64",
            BlobType.BLOB // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    long hash = xxHash64.hash(inputBytes, 0, inputBytes.length, SEED);
    byte[] expectedHash = ByteBuffer.allocate(8).putLong(hash).array();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedHash, result.getBinary(0).getValues());
  }

  /** Test XXHASH64 with multiple rows, including a null value. */
  @Test
  public void testXxHash64MultiRowsWithNull() {
    byte[] bytes1 = "hello".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] bytes2 = "world".getBytes(TSFileConfig.STRING_CHARSET);

    Binary[] values = new Binary[] {new Binary(bytes1), null, new Binary(bytes2)};
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType
            childColumnTransformer,
            CodecStrategiesFactory.XXHASH64,
            "xxhash64",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    long hash1 = xxHash64.hash(bytes1, 0, bytes1.length, SEED);
    byte[] expected1 = ByteBuffer.allocate(8).putLong(hash1).array();
    long hash3 = xxHash64.hash(bytes2, 0, bytes2.length, SEED);
    byte[] expected3 = ByteBuffer.allocate(8).putLong(hash3).array();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }

  /** Test XXHASH64 with a selection array to process only a subset of rows. */
  @Test
  public void testXxHash64WithSelection() {
    byte[] bytes1 = "Apache".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] bytes2 = "IoTDB".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] bytes3 = "rocks".getBytes(TSFileConfig.STRING_CHARSET);

    Binary[] values = {new Binary(bytes1), new Binary(bytes2), new Binary(bytes3)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType
            child,
            CodecStrategiesFactory.XXHASH64,
            "xxhash64",
            STRING // inputType
            );
    transformer.addReferenceCount();

    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    long hash1 = xxHash64.hash(bytes1, 0, bytes1.length, SEED);
    byte[] expected1 = ByteBuffer.allocate(8).putLong(hash1).array();
    long hash3 = xxHash64.hash(bytes3, 0, bytes3.length, SEED);
    byte[] expected3 = ByteBuffer.allocate(8).putLong(hash3).array();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }

  /**
   * Test XXHASH64 with an empty binary input. The result is compared against the well-known,
   * standard hash for an empty string.
   */
  @Test
  public void testXxHash64EmptyInput() {
    byte[] inputBytes = "".getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType
            childColumnTransformer,
            CodecStrategiesFactory.XXHASH64,
            "xxhash64",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    // The known xxHash64 of an empty string with seed 0.
    long knownEmptyHashLong = 0xEF46DB3751D8E999L;
    byte[] expectedHash = ByteBuffer.allocate(8).putLong(knownEmptyHashLong).array();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedHash, result.getBinary(0).getValues());
  }
}
