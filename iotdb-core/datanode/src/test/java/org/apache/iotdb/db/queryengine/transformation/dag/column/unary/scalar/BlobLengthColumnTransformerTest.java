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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.LongType.INT64;

public class BlobLengthColumnTransformerTest {

  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

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

  /**
   * Test blob length from string input, the length should be the byte length of the string in UTF-8
   * encoding.
   */
  @Test
  public void testBlobLengthFromString() {
    String input = "hello";
    Binary[] values = new Binary[] {new Binary(input, StandardCharsets.UTF_8)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    BlobLengthColumnTransformer blobLengthColumnTransformer =
        new BlobLengthColumnTransformer(INT64, childColumnTransformer);
    blobLengthColumnTransformer.addReferenceCount();
    blobLengthColumnTransformer.evaluate();
    Column result = blobLengthColumnTransformer.getColumn();

    int expectedLength = input.getBytes(StandardCharsets.UTF_8).length;
    Assert.assertEquals(expectedLength, result.getLong(0));
  }

  @Test
  public void testBlobLengthFromHex() {
    String input = "68656C6C6F";
    byte[] inputBytes = hexStringToByteArray(input);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    BlobLengthColumnTransformer blobLengthColumnTransformer =
        new BlobLengthColumnTransformer(INT64, childColumnTransformer);
    blobLengthColumnTransformer.addReferenceCount();
    blobLengthColumnTransformer.evaluate();
    Column result = blobLengthColumnTransformer.getColumn();

    int expectedLength = inputBytes.length;
    Assert.assertEquals(expectedLength, result.getLong(0));
  }

  @Test
  public void testBlobLengthMultiRowsWithNull() {
    String input1 = "68656C6C6F";
    String input2 = "1F3C";
    byte[] inputBytes1 = hexStringToByteArray(input1);
    byte[] inputBytes2 = hexStringToByteArray(input2);

    Binary[] values = new Binary[] {new Binary(inputBytes1), null, new Binary(inputBytes2)};
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);

    BlobLengthColumnTransformer blobLengthColumnTransformer =
        new BlobLengthColumnTransformer(INT64, childColumnTransformer);
    blobLengthColumnTransformer.addReferenceCount();
    blobLengthColumnTransformer.evaluate();
    Column result = blobLengthColumnTransformer.getColumn();
    Assert.assertEquals(inputBytes1.length, result.getLong(0));
    Assert.assertTrue(result.isNull(1));
    Assert.assertEquals(inputBytes2.length, result.getLong(2));
  }

  @Test
  public void testBlobLengthWithSelection() {
    String input1 = "68656C6C6F";
    String input2 = "1F3C";
    String input3 = "";

    byte[] bytes1 = hexStringToByteArray(input1);
    byte[] bytes2 = hexStringToByteArray(input2);
    byte[] bytes3 = hexStringToByteArray(input3);

    Binary[] values = {new Binary(bytes1), new Binary(bytes2), new Binary(bytes3)};
    boolean[] booleans = {false, true, true};
    ColumnTransformer child =
        mockChildColumnTransformer(new BinaryColumn(values.length, Optional.empty(), values));
    BlobLengthColumnTransformer blobLengthColumnTransformer =
        new BlobLengthColumnTransformer(INT64, child);
    blobLengthColumnTransformer.addReferenceCount();
    blobLengthColumnTransformer.evaluateWithSelection(booleans);
    Column result = blobLengthColumnTransformer.getColumn();

    int expectedValue2 = bytes2.length;
    int expectedValue3 = bytes3.length;

    Assert.assertTrue(result.isNull(0));
    Assert.assertEquals(expectedValue2, result.getLong(1));
    Assert.assertEquals(expectedValue3, result.getLong(2));
  }
}
