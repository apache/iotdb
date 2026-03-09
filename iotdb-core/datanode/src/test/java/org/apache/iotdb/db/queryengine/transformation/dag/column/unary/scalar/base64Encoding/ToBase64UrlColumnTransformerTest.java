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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.base64Encoding;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.GenericCodecColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.CodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Base64;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.StringType.STRING;

public class ToBase64UrlColumnTransformerTest {

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

  /** Test TO_BASE64URL from a string input. */
  @Test
  public void testToBase64UrlFromString() {
    String input = "Apache IoTDB";
    byte[] inputBytes = input.getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            STRING,
            childColumnTransformer,
            CodecStrategiesFactory.TO_BASE64URL,
            "to_base64url",
            STRING);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    String expectedOutput = Base64.getUrlEncoder().encodeToString(inputBytes);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expectedOutput, result.getBinary(0).toString());
  }

  /** Test TO_BASE64URL from a hex-represented binary input. */
  @Test
  public void testToBase64UrlFromHex() {
    // "Apache IoTDB" in hex
    String inputHex = "41706163686520496f544442";
    byte[] inputBytes = hexStringToByteArray(inputHex);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            STRING,
            childColumnTransformer,
            CodecStrategiesFactory.TO_BASE64URL,
            "to_base64url",
            STRING);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    String expectedOutput = Base64.getUrlEncoder().encodeToString(inputBytes); // "QXBhY2hlIElvVERC"
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expectedOutput, result.getBinary(0).toString());
  }

  /** Test specifically for URL-safe characters ('-' and '_'). */
  @Test
  public void testToBase64UrlSpecialCharacters() {
    // This specific byte sequence produces '+' and '/' in standard Base64.
    // Binary: 11111011 11111111 10111111
    // 6-bit groups: 111110 (62), 111111 (63), 111110 (62), 111111 (63)
    // Standard Base64: +/+/
    // URL-safe Base64: -_-_
    byte[] inputBytes = hexStringToByteArray("fbffbf");
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            TEXT,
            childColumnTransformer,
            CodecStrategiesFactory.TO_BASE64URL,
            "to_base64url",
            TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    String expectedOutput = "-_-_";
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expectedOutput, result.getBinary(0).toString());
  }

  /** Test TO_BASE64URL with multiple rows, including a null value. */
  @Test
  public void testToBase64UrlMultiRowsWithNull() {
    byte[] bytes1 = "hello?".getBytes(TSFileConfig.STRING_CHARSET); // Contains char that becomes /
    byte[] bytes2 = "world".getBytes(TSFileConfig.STRING_CHARSET);

    Binary[] values = new Binary[] {new Binary(bytes1), null, new Binary(bytes2)};
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            STRING,
            childColumnTransformer,
            CodecStrategiesFactory.TO_BASE64URL,
            "to_base64url",
            TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    String expected1 = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes1);
    String expected3 = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes2);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(expected3, result.getBinary(2).toString());
  }

  /** Test TO_BASE64URL with a selection array to process only a subset of rows. */
  @Test
  public void testToBase64UrlWithSelection() {
    byte[] bytes1 = "Apache".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] bytes2 = "IoTDB?".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] bytes3 = "rocks".getBytes(TSFileConfig.STRING_CHARSET);

    Binary[] values = {new Binary(bytes1), new Binary(bytes2), new Binary(bytes3)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            STRING, child, CodecStrategiesFactory.TO_BASE64URL, "to_base64url", TEXT);
    transformer.addReferenceCount();

    // Select only the second and third rows for processing.
    boolean[] selection = {false, true, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    String expected2 = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes2);
    String expected3 = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes3);

    Assert.assertEquals(3, result.getPositionCount());
    // The first row was not selected, so it should be null in the result.
    Assert.assertTrue(result.isNull(0));
    Assert.assertFalse(result.isNull(1));
    Assert.assertEquals(expected2, result.getBinary(1).toString());
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(expected3, result.getBinary(2).toString());
  }

  /** Test TO_BASE64URL with an empty binary input. */
  @Test
  public void testToBase64UrlEmptyInput() {
    byte[] inputBytes = "".getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            STRING,
            childColumnTransformer,
            CodecStrategiesFactory.TO_BASE64URL,
            "to_base64url",
            TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    String expectedOutput = Base64.getUrlEncoder().encodeToString(inputBytes); // ""
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(expectedOutput, result.getBinary(0).toString());
  }
}
