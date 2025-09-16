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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.GenericCodecColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.CodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Base64;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.StringType.STRING;

public class FromBase64UrlColumnTransformerTest {

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

  /** Test FROM_BASE64URL to a string output. */
  @Test
  public void testFromBase64UrlToString() {
    String originalString = "Apache IoTDB";
    String base64UrlInput =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(originalString.getBytes(TSFileConfig.STRING_CHARSET));

    Binary[] values =
        new Binary[] {new Binary(base64UrlInput.getBytes(TSFileConfig.STRING_CHARSET))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB,
            childColumnTransformer,
            CodecStrategiesFactory.FROM_BASE64URL,
            "from_base64url",
            STRING);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertEquals(originalString, result.getBinary(0).toString());
  }

  /** Test FROM_BASE64URL to a hex-represented binary output. */
  @Test
  public void testFromBase64UrlToHex() {
    String originalHex = "41706163686520496f544442";
    byte[] originalBytes = hexStringToByteArray(originalHex);
    String base64UrlInput = Base64.getUrlEncoder().withoutPadding().encodeToString(originalBytes);

    Binary[] values =
        new Binary[] {new Binary(base64UrlInput.getBytes(TSFileConfig.STRING_CHARSET))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB,
            childColumnTransformer,
            CodecStrategiesFactory.FROM_BASE64URL,
            "from_base64url",
            STRING);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(originalBytes, result.getBinary(0).getValues());
  }

  /** Test specifically for URL-safe characters ('-' and '_'). */
  @Test
  public void testFromBase64UrlSpecialCharacters() {
    String base64UrlInput = "-_-_";
    byte[] expectedBytes = hexStringToByteArray("fbffbf");

    Binary[] values =
        new Binary[] {new Binary(base64UrlInput.getBytes(TSFileConfig.STRING_CHARSET))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB,
            childColumnTransformer,
            CodecStrategiesFactory.FROM_BASE64URL,
            "from_base64url",
            TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedBytes, result.getBinary(0).getValues());
  }

  /** Test FROM_BASE64URL with multiple rows, including a null value. */
  @Test
  public void testFromBase64UrlMultiRowsWithNull() {
    String original1 = "hello?";
    String original2 = "rocks";
    String base64Url1 =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(original1.getBytes(TSFileConfig.STRING_CHARSET));
    String base64Url2 =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(original2.getBytes(TSFileConfig.STRING_CHARSET));

    Binary[] values =
        new Binary[] {
          new Binary(base64Url1.getBytes(TSFileConfig.STRING_CHARSET)),
          null,
          new Binary(base64Url2.getBytes(TSFileConfig.STRING_CHARSET))
        };
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB,
            childColumnTransformer,
            CodecStrategiesFactory.FROM_BASE64URL,
            "from_base64url",
            TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(original1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(original2, result.getBinary(2).toString());
  }

  /** Test FROM_BASE64URL with a selection array to process only a subset of rows. */
  @Test
  public void testFromBase64UrlWithSelection() {
    String original1 = "Apache";
    String original2 = "IoTDB?";
    String original3 = "rocks";
    String base64Url1 =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(original1.getBytes(TSFileConfig.STRING_CHARSET));
    String base64Url2 =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(original2.getBytes(TSFileConfig.STRING_CHARSET));
    String base64Url3 =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(original3.getBytes(TSFileConfig.STRING_CHARSET));

    Binary[] values = {
      new Binary(base64Url1.getBytes(TSFileConfig.STRING_CHARSET)),
      new Binary(base64Url2.getBytes(TSFileConfig.STRING_CHARSET)),
      new Binary(base64Url3.getBytes(TSFileConfig.STRING_CHARSET))
    };
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB, child, CodecStrategiesFactory.FROM_BASE64URL, "from_base64url", TEXT);
    transformer.addReferenceCount();

    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertEquals(original1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertEquals(original3, result.getBinary(2).toString());
  }

  /** Test FROM_BASE64URL with an empty binary input. */
  @Test
  public void testFromBase64UrlEmptyInput() {
    String base64UrlInput = "";
    Binary[] values =
        new Binary[] {new Binary(base64UrlInput.getBytes(TSFileConfig.STRING_CHARSET))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB,
            childColumnTransformer,
            CodecStrategiesFactory.FROM_BASE64URL,
            "from_base64url",
            TEXT);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedOutput = "".getBytes(TSFileConfig.STRING_CHARSET);
    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedOutput, result.getBinary(0).getValues());
  }

  /**
   * Test FROM_BASE64URL with an invalid Base64URL input string from a TEXT column. The error
   * message should display the original string.
   */
  @Test
  public void testInvalidBase64UrlInputFromText() {
    String invalidInput = "this is not base64url!";
    byte[] inputBytes = invalidInput.getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB,
            childColumnTransformer,
            CodecStrategiesFactory.FROM_BASE64URL,
            "from_base64url",
            TEXT);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException was not thrown.");
    } catch (SemanticException e) {
      String expectedErrorMessage =
          String.format(
              "Failed to execute function 'from_base64url' due to an invalid input format. Problematic value: %s",
              invalidInput);
      Assert.assertEquals(expectedErrorMessage, e.getMessage());
    }
  }

  /**
   * Test FROM_BASE64URL with an invalid Base64URL input from a BLOB column. The error message
   * should display the hex representation of the input bytes.
   */
  @Test
  public void testInvalidBase64UrlInputFromBlob() {
    byte[] invalidBytes = new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
    Binary[] values = new Binary[] {new Binary(invalidBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BLOB,
            childColumnTransformer,
            CodecStrategiesFactory.FROM_BASE64URL,
            "from_base64url",
            BlobType.BLOB);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException was not thrown.");
    } catch (SemanticException e) {
      String expectedProblematicValue = "0xdeadbeef";
      String expectedErrorMessage =
          String.format(
              "Failed to execute function 'from_base64url' due to an invalid input format. Problematic value: %s",
              expectedProblematicValue);
      Assert.assertEquals(expectedErrorMessage, e.getMessage());
    }
  }
}
