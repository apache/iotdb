/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may not use this file except in compliance
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

// Note: The actual package may vary, adjust if CRC32Transformer is in a different location.
package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.hashing;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.CRC32Transformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.zip.CRC32;

public class Crc32ColumnTransformerTest {

  // Helper method to calculate the expected CRC32 value for a byte array.
  private long calculateCrc32(byte[] bytes) {
    CRC32 crc32 = new CRC32();
    crc32.update(bytes);
    return crc32.getValue();
  }

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  // Helper method to convert a hex string to a byte array for BLOB testing.
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

  /** Test CRC32 from a STRING-like input. Output should be INT64. */
  @Test
  public void testCrc32FromString() {
    String input = "Apache IoTDB";
    byte[] inputBytes = input.getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    // Instantiate the specific CRC32Transformer
    CRC32Transformer transformer = new CRC32Transformer(LongType.INT64, childColumnTransformer);

    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    long expectedCrc32 = calculateCrc32(inputBytes);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expectedCrc32, result.getLong(0));
  }

  /** Test CRC32 from a BLOB-like input (represented by hex). Output should be INT64. */
  @Test
  public void testCrc32FromBlob() {
    // Hex representation for the string "Apache IoTDB"
    String inputHex = "41706163686520496f544442";
    byte[] inputBytes = hexStringToByteArray(inputHex);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    // Instantiate the specific CRC32Transformer
    CRC32Transformer transformer = new CRC32Transformer(LongType.INT64, childColumnTransformer);

    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    long expectedCrc32 = calculateCrc32(inputBytes);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expectedCrc32, result.getLong(0));
  }

  /** Test CRC32 with multiple rows, including a null value. */
  @Test
  public void testCrc32MultiRowsWithNull() {
    byte[] bytes1 = "hello".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] bytes2 = "world".getBytes(TSFileConfig.STRING_CHARSET);

    Binary[] values = new Binary[] {new Binary(bytes1), null, new Binary(bytes2)};
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);

    // Instantiate the specific CRC32Transformer
    CRC32Transformer transformer = new CRC32Transformer(LongType.INT64, childColumnTransformer);

    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    long expected1 = calculateCrc32(bytes1);
    long expected3 = calculateCrc32(bytes2);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(expected1, result.getLong(0));
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(expected3, result.getLong(2));
  }

  /** Test CRC32 with a selection array to process only a subset of rows. */
  @Test
  public void testCrc32WithSelection() {
    byte[] bytes1 = "Apache".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] bytes2 = "IoTDB".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] bytes3 = "rocks".getBytes(TSFileConfig.STRING_CHARSET);

    Binary[] values = {new Binary(bytes1), new Binary(bytes2), new Binary(bytes3)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    // Instantiate the specific CRC32Transformer
    CRC32Transformer transformer = new CRC32Transformer(LongType.INT64, child);

    transformer.addReferenceCount();

    // Select only the first and third rows for processing.
    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    long expected1 = calculateCrc32(bytes1);
    long expected3 = calculateCrc32(bytes3);

    Assert.assertEquals(3, result.getPositionCount());
    // The first row was selected and processed.
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(expected1, result.getLong(0));
    // The second row was NOT selected, so its corresponding output should be null.
    Assert.assertTrue(result.isNull(1));
    // The third row was selected and processed.
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(expected3, result.getLong(2));
  }

  /** Test CRC32 with an empty binary input. The result should be 0. */
  @Test
  public void testCrc32EmptyInput() {
    byte[] inputBytes = "".getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    // Instantiate the specific CRC32Transformer
    CRC32Transformer transformer = new CRC32Transformer(LongType.INT64, childColumnTransformer);

    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    // The CRC32 of an empty input is always 0.
    long expectedCrc32 = 0L;

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expectedCrc32, result.getLong(0));
  }
}
