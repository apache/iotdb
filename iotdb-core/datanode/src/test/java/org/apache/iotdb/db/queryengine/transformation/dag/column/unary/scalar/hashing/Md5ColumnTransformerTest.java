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

import com.google.common.hash.Hashing;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.apache.tsfile.read.common.type.StringType.STRING;

public class Md5ColumnTransformerTest {

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

  /** Test MD5 from a STRING input. Output should be BLOB. */
  @Test
  public void testMd5FromString() {
    String input = "Apache IoTDB";
    byte[] inputBytes = input.getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType
            childColumnTransformer,
            CodecStrategiesFactory.MD5,
            "md5",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedHash = Hashing.md5().hashBytes(inputBytes).asBytes();
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedHash, result.getBinary(0).getValues());
  }

  /** Test MD5 from a BLOB input (represented by hex). Output should be BLOB. */
  @Test
  public void testMd5FromBlob() {
    String inputHex = "41706163686520496f544442";
    byte[] inputBytes = hexStringToByteArray(inputHex);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType
            childColumnTransformer,
            CodecStrategiesFactory.MD5,
            "md5",
            BlobType.BLOB // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedHash = Hashing.md5().hashBytes(inputBytes).asBytes();
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedHash, result.getBinary(0).getValues());
  }

  /** Test MD5 with multiple rows, including a null value. */
  @Test
  public void testMd5MultiRowsWithNull() {
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
            CodecStrategiesFactory.MD5,
            "md5",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expected1 = Hashing.md5().hashBytes(bytes1).asBytes();
    byte[] expected3 = Hashing.md5().hashBytes(bytes2).asBytes();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }

  /** Test MD5 with a selection array to process only a subset of rows. */
  @Test
  public void testMd5WithSelection() {
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
            CodecStrategiesFactory.MD5,
            "md5",
            STRING // inputType
            );
    transformer.addReferenceCount();

    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    byte[] expected1 = Hashing.md5().hashBytes(bytes1).asBytes();
    byte[] expected3 = Hashing.md5().hashBytes(bytes3).asBytes();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expected1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expected3, result.getBinary(2).getValues());
  }

  /**
   * Test MD5 with an empty binary input. The result is compared against the well-known, standard
   * hash for an empty string.
   */
  @Test
  public void testMd5EmptyInput() {
    byte[] inputBytes = "".getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType
            childColumnTransformer,
            CodecStrategiesFactory.MD5,
            "md5",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    // The known MD5 hash of an empty string.
    String knownEmptyHashHex = "d41d8cd98f00b204e9800998ecf8427e";
    byte[] expectedHash = hexStringToByteArray(knownEmptyHashHex);

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedHash, result.getBinary(0).getValues());
  }
}
