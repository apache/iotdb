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
// package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.string;
package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.hexEncoding;

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

import java.util.Optional;

import static org.apache.tsfile.read.common.type.StringType.STRING;

public class FromHexColumnTransformerTest {

  // Helper method to mock a child ColumnTransformer that returns a predefined Column.
  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }

  /** Test FROM_HEX from a valid hex STRING input. Output should be BLOB. */
  @Test
  public void testFromHexFromString() {
    String inputText = "Apache IoTDB";
    String inputHexString = "41706163686520496f544442"; // "Apache IoTDB" in hex

    byte[] inputHexBytes = inputHexString.getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputHexBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType is BLOB
            childColumnTransformer,
            CodecStrategiesFactory.FROM_HEX,
            "from_hex",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedBytes = inputText.getBytes(TSFileConfig.STRING_CHARSET);

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedBytes, result.getBinary(0).getValues());
  }

  /**
   * Test that FROM_HEX throws an exception for an input string with an odd number of characters.
   */
  @Test
  public void testFromHexWithOddLengthInput() {
    String invalidInput = "123";
    Binary[] values = {new Binary(invalidInput.getBytes(TSFileConfig.STRING_CHARSET))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, child, CodecStrategiesFactory.FROM_HEX, "from_hex", STRING);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected an exception to be thrown for odd-length hex string");
    } catch (RuntimeException e) {
      String expectedMessage =
          String.format(
              "Failed to execute function 'from_hex' due to an invalid input format. Problematic value: %s",
              invalidInput);
      Assert.assertEquals(expectedMessage, e.getMessage());
    }
  }

  /** Test that FROM_HEX throws an exception for an input string with non-hex characters. */
  @Test
  public void testFromHexWithNonHexCharsInput() {
    String invalidInput = "gg";
    Binary[] values = {new Binary(invalidInput.getBytes(TSFileConfig.STRING_CHARSET))};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, child, CodecStrategiesFactory.FROM_HEX, "from_hex", STRING);
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected an exception to be thrown for non-hex characters in input");
    } catch (RuntimeException e) {
      String expectedMessage =
          String.format(
              "Failed to execute function 'from_hex' due to an invalid input format. Problematic value: %s",
              invalidInput);
      Assert.assertEquals(expectedMessage, e.getMessage());
    }
  }

  /** Test FROM_HEX with multiple rows, including a null value. */
  @Test
  public void testFromHexMultiRowsWithNull() {
    String hex1 = "68656c6c6f"; // "hello"
    String hex2 = "776f726c64"; // "world"

    Binary[] values = {
      new Binary(hex1.getBytes(TSFileConfig.STRING_CHARSET)),
      null,
      new Binary(hex2.getBytes(TSFileConfig.STRING_CHARSET))
    };
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Column binaryColumn = new BinaryColumn(values.length, Optional.of(valueIsNull), values);
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType is BLOB
            childColumnTransformer,
            CodecStrategiesFactory.FROM_HEX,
            "from_hex",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    byte[] expectedBytes1 = "hello".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] expectedBytes3 = "world".getBytes(TSFileConfig.STRING_CHARSET);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expectedBytes1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expectedBytes3, result.getBinary(2).getValues());
  }

  /** Test FROM_HEX with a selection array to process only a subset of rows. */
  @Test
  public void testFromHexWithSelection() {
    String hex1 = "417061636865"; // "Apache"
    String hex2 = "496f544442"; // "IoTDB"
    String hex3 = "726f636b73"; // "rocks"

    Binary[] values = {
      new Binary(hex1.getBytes(TSFileConfig.STRING_CHARSET)),
      new Binary(hex2.getBytes(TSFileConfig.STRING_CHARSET)),
      new Binary(hex3.getBytes(TSFileConfig.STRING_CHARSET))
    };
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);
    ColumnTransformer child = mockChildColumnTransformer(binaryColumn);

    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType is BLOB
            child,
            CodecStrategiesFactory.FROM_HEX,
            "from_hex",
            STRING // inputType
            );
    transformer.addReferenceCount();

    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    byte[] expectedBytes1 = "Apache".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] expectedBytes3 = "rocks".getBytes(TSFileConfig.STRING_CHARSET);

    Assert.assertEquals(3, result.getPositionCount());
    Assert.assertFalse(result.isNull(0));
    Assert.assertArrayEquals(expectedBytes1, result.getBinary(0).getValues());
    Assert.assertTrue(result.isNull(1));
    Assert.assertFalse(result.isNull(2));
    Assert.assertArrayEquals(expectedBytes3, result.getBinary(2).getValues());
  }

  /** Test FROM_HEX with an empty string input. The result should be an empty BLOB. */
  @Test
  public void testFromHexEmptyInput() {
    String inputHexString = "";
    byte[] inputHexBytes = inputHexString.getBytes(TSFileConfig.STRING_CHARSET);
    Binary[] values = new Binary[] {new Binary(inputHexBytes)};
    Column binaryColumn = new BinaryColumn(values.length, Optional.empty(), values);

    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(binaryColumn);
    GenericCodecColumnTransformer transformer =
        new GenericCodecColumnTransformer(
            BlobType.BLOB, // returnType is BLOB
            childColumnTransformer,
            CodecStrategiesFactory.FROM_HEX,
            "from_hex",
            STRING // inputType
            );
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();

    // The decoded version of an empty string is an empty byte array.
    byte[] expectedBytes = new byte[0];

    Assert.assertEquals(1, result.getPositionCount());
    Assert.assertArrayEquals(expectedBytes, result.getBinary(0).getValues());
  }
}
