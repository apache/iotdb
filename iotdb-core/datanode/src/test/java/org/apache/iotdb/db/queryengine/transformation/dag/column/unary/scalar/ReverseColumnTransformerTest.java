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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.CodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;

// Assuming this test file is for a GenericCodecColumnTransformer configured for REVERSE.
public class ReverseColumnTransformerTest {

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  // Helper method to convert a hex string to a byte array.
  private static byte[] hexStringToByteArray(String s) {
    if (s == null || s.isEmpty()) {
      return new byte[0];
    }
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

  /** Test character-wise reversal for TEXT type, including multi-byte characters. */
  @Test
  public void testReverseTextAsChars() {
    String originalString = "你好, world";
    String expectedReversed = "dlrow ,好你";

    Binary[] values =
        new Binary[] {new Binary(originalString.getBytes(TSFileConfig.STRING_CHARSET))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            TEXT, childColumnTransformer, CodecStrategiesFactory.REVERSE_CHARS, "reverse", TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expectedReversed, result.getBinary(0).toString());
  }

  /** Test byte-wise reversal for BLOB type. */
  @Test
  public void testReverseBlobAsBytes() {
    byte[] originalBytes = hexStringToByteArray("01020304AABB");
    byte[] expectedReversed = hexStringToByteArray("BBAA04030201");

    Binary[] values = new Binary[] {new Binary(originalBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB, childColumnTransformer, CodecStrategiesFactory.REVERSE_BYTES, "reverse", BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedReversed, result.getBinary(0).getValues());
  }

  /** Test REVERSE with multiple rows, including a null value. */
  @Test
  public void testReverseMultiRowsWithNull() {
    String original1 = "hello";
    String original2 = "Apache";
    String expected1 = "olleh";
    String expected2 = "ehcapA";

    Binary[] values =
        new Binary[] {
          new Binary(original1.getBytes(TSFileConfig.STRING_CHARSET)),
          null,
          new Binary(original2.getBytes(TSFileConfig.STRING_CHARSET))
        };
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            TEXT, childColumnTransformer, CodecStrategiesFactory.REVERSE_CHARS, "reverse", TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(expected2, result.getBinary(2).toString());
  }

  /** Test REVERSE with a selection array to process only a subset of rows. */
  @Test
  public void testReverseWithSelection() {
    String original1 = "one";
    String original2 = "two";
    String original3 = "three";
    String expected1 = "eno";
    String expected3 = "eerht";

    Binary[] values = {
      new Binary(original1.getBytes(TSFileConfig.STRING_CHARSET)),
      new Binary(original2.getBytes(TSFileConfig.STRING_CHARSET)),
      new Binary(original3.getBytes(TSFileConfig.STRING_CHARSET))
    };
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            TEXT, child, CodecStrategiesFactory.REVERSE_CHARS, "reverse", TEXT);
    transformer.addReferenceCount();

    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1)); // Not selected, so should be null
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(expected3, result.getBinary(2).toString());
  }

  /** Test REVERSE with an empty TEXT input. */
  @Test
  public void testReverseEmptyText() {
    String originalString = "";
    Binary[] values =
        new Binary[] {new Binary(originalString.getBytes(TSFileConfig.STRING_CHARSET))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            TEXT, childColumnTransformer, CodecStrategiesFactory.REVERSE_CHARS, "reverse", TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(originalString, result.getBinary(0).toString());
  }

  /** Test REVERSE with an empty BLOB input. */
  @Test
  public void testReverseEmptyBlob() {
    byte[] originalBytes = new byte[0];
    Binary[] values = new Binary[] {new Binary(originalBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB, childColumnTransformer, CodecStrategiesFactory.REVERSE_BYTES, "reverse", BLOB);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(originalBytes, result.getBinary(0).getValues());
  }
}
