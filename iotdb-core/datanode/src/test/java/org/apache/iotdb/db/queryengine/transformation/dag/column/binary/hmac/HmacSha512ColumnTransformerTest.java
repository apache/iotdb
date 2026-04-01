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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary.hmac;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.HmacColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.factory.HmacStrategiesFactory;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.strategies.HmacStrategy;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.HmacConstantKeyColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertThrows;

public class HmacSha512ColumnTransformerTest {

  private static final Type returnType = BlobType.BLOB;

  /**
   * Helper method to calculate the expected HMAC-SHA512 hash using standard Java crypto libraries.
   *
   * @param data The message bytes.
   * @param key The key bytes.
   * @return The resulting HMAC-SHA512 hash.
   */
  private byte[] calculateHmacSha512(byte[] data, byte[] key)
      throws NoSuchAlgorithmException, InvalidKeyException {
    if (key == null || key.length == 0) {
      throw new InvalidKeyException("Key cannot be null or empty for HMAC-SHA512 calculation.");
    }
    Mac mac = Mac.getInstance("HmacSHA512");
    SecretKeySpec secretKeySpec = new SecretKeySpec(key, "HmacSHA512");
    mac.init(secretKeySpec);
    return mac.doFinal(data);
  }

  /** Helper method to create a mocked ColumnTransformer that returns a predefined Column. */
  private ColumnTransformer mockColumnTransformer(Column column) {
    ColumnTransformer mockTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockTransformer.getColumn()).thenReturn(column);
    // Ensure that tryEvaluate (or other evaluation methods) don't throw exceptions
    Mockito.doNothing().when(mockTransformer).tryEvaluate();
    Mockito.doNothing().when(mockTransformer).clearCache();
    Mockito.when(mockTransformer.getColumnCachePositionCount())
        .thenReturn(column.getPositionCount());
    return mockTransformer;
  }

  // region Tests for (Column, Column) scenario using HmacColumnTransformer

  /** Test case with standard STRING inputs for both data and key. */
  @Test
  public void testHmacSha512WithStringInputs() throws Exception {
    String dataStr = "Hello IoTDB";
    String keyStr = "secret_key";
    byte[] dataBytes = dataStr.getBytes(TSFileConfig.STRING_CHARSET);
    byte[] keyBytes = keyStr.getBytes(TSFileConfig.STRING_CHARSET);

    Column dataColumn = new BinaryColumnBuilder(null, 1).writeBinary(new Binary(dataBytes)).build();
    Column keyColumn = new BinaryColumnBuilder(null, 1).writeBinary(new Binary(keyBytes)).build();

    ColumnTransformer dataTransformer = mockColumnTransformer(dataColumn);
    ColumnTransformer keyTransformer = mockColumnTransformer(keyColumn);

    HmacColumnTransformer hmacTransformer =
        new HmacColumnTransformer(
            returnType,
            dataTransformer,
            keyTransformer,
            HmacStrategiesFactory.HMAC_SHA512,
            "hmac_sha512",
            StringType.STRING);

    hmacTransformer.addReferenceCount();
    hmacTransformer.evaluate();
    Column resultColumn = hmacTransformer.getColumn();

    byte[] expectedHash = calculateHmacSha512(dataBytes, keyBytes);

    Assert.assertEquals(1, resultColumn.getPositionCount());
    Assert.assertFalse(resultColumn.isNull(0));
    Assert.assertArrayEquals(expectedHash, resultColumn.getBinary(0).getValues());
  }

  /** Test case with multi-row inputs, including NULL values for data and key. */
  @Test
  public void testHmacSha512WithMultiRowsAndNulls() throws Exception {
    String[] dataStrings = {"data1", null, "data3", "data4"};
    String[] keyStrings = {"key1", "key2", null, "key4"};

    ColumnBuilder dataBuilder = new BinaryColumnBuilder(null, dataStrings.length);
    ColumnBuilder keyBuilder = new BinaryColumnBuilder(null, keyStrings.length);

    for (String s : dataStrings) {
      if (s != null) {
        dataBuilder.writeBinary(new Binary(s.getBytes(TSFileConfig.STRING_CHARSET)));
      } else {
        dataBuilder.appendNull();
      }
    }

    for (String s : keyStrings) {
      if (s != null) {
        keyBuilder.writeBinary(new Binary(s.getBytes(TSFileConfig.STRING_CHARSET)));
      } else {
        keyBuilder.appendNull();
      }
    }

    ColumnTransformer dataTransformer = mockColumnTransformer(dataBuilder.build());
    ColumnTransformer keyTransformer = mockColumnTransformer(keyBuilder.build());

    HmacColumnTransformer hmacTransformer =
        new HmacColumnTransformer(
            returnType,
            dataTransformer,
            keyTransformer,
            HmacStrategiesFactory.HMAC_SHA512,
            "hmac_sha512",
            StringType.STRING);

    hmacTransformer.addReferenceCount();
    hmacTransformer.evaluate();
    Column resultColumn = hmacTransformer.getColumn();

    Assert.assertEquals(4, resultColumn.getPositionCount());

    // Row 0: Valid data and key -> should have a valid hash
    byte[] expected0 = calculateHmacSha512(dataStrings[0].getBytes(), keyStrings[0].getBytes());
    Assert.assertFalse(resultColumn.isNull(0));
    Assert.assertArrayEquals(expected0, resultColumn.getBinary(0).getValues());

    // Row 1: Null data -> result should be null
    Assert.assertTrue(resultColumn.isNull(1));

    // Row 2: Null key -> result should be null
    Assert.assertTrue(resultColumn.isNull(2));

    // Row 3: Valid data and key -> should have a valid hash
    byte[] expected3 = calculateHmacSha512(dataStrings[3].getBytes(), keyStrings[3].getBytes());
    Assert.assertFalse(resultColumn.isNull(3));
    Assert.assertArrayEquals(expected3, resultColumn.getBinary(3).getValues());
  }

  /** An empty key is invalid for HMAC operations and should throw a SemanticException. */
  @Test
  public void testHmacSha512WithEmptyKeyThrowsException() {
    byte[] dataBytes = "some_data".getBytes(TSFileConfig.STRING_CHARSET);
    byte[] keyBytes = "".getBytes(TSFileConfig.STRING_CHARSET); // Empty key

    Column dataColumn = new BinaryColumnBuilder(null, 1).writeBinary(new Binary(dataBytes)).build();
    Column keyColumn = new BinaryColumnBuilder(null, 1).writeBinary(new Binary(keyBytes)).build();

    ColumnTransformer dataTransformer = mockColumnTransformer(dataColumn);
    ColumnTransformer keyTransformer = mockColumnTransformer(keyColumn);

    HmacColumnTransformer hmacTransformer =
        new HmacColumnTransformer(
            returnType,
            dataTransformer,
            keyTransformer,
            HmacStrategiesFactory.HMAC_SHA512,
            "hmac_sha512",
            StringType.STRING);

    hmacTransformer.addReferenceCount();

    // Assert that calling evaluate throws the expected exception
    SemanticException thrown = assertThrows(SemanticException.class, hmacTransformer::evaluate);

    Assert.assertTrue(
        "The exception message should indicate that an empty key is not allowed.",
        thrown.getMessage().contains("the empty key is not allowed in HMAC operation"));
  }

  /** Test HMAC-SHA512 with a selection array to process only a subset of rows. */
  @Test
  public void testHmacSha512WithSelection() throws Exception {
    String[] dataStrings = {"Apache", "IoTDB", "rocks"};
    String[] keyStrings = {"key1", "key2", "key3"};

    ColumnBuilder dataBuilder = new BinaryColumnBuilder(null, dataStrings.length);
    ColumnBuilder keyBuilder = new BinaryColumnBuilder(null, keyStrings.length);

    for (String s : dataStrings) {
      dataBuilder.writeBinary(new Binary(s.getBytes(TSFileConfig.STRING_CHARSET)));
    }
    for (String s : keyStrings) {
      keyBuilder.writeBinary(new Binary(s.getBytes(TSFileConfig.STRING_CHARSET)));
    }

    ColumnTransformer dataTransformer = mockColumnTransformer(dataBuilder.build());
    ColumnTransformer keyTransformer = mockColumnTransformer(keyBuilder.build());

    HmacColumnTransformer hmacTransformer =
        new HmacColumnTransformer(
            returnType,
            dataTransformer,
            keyTransformer,
            HmacStrategiesFactory.HMAC_SHA512,
            "hmac_sha512",
            StringType.STRING);

    hmacTransformer.addReferenceCount();

    // Select only the first and third rows for processing.
    boolean[] selection = {true, false, true};
    hmacTransformer.evaluateWithSelection(selection);
    Column resultColumn = hmacTransformer.getColumn();

    Assert.assertEquals(3, resultColumn.getPositionCount());

    // Row 0: Selected -> should have a valid hash
    byte[] expected0 = calculateHmacSha512(dataStrings[0].getBytes(), keyStrings[0].getBytes());
    Assert.assertFalse(resultColumn.isNull(0));
    Assert.assertArrayEquals(expected0, resultColumn.getBinary(0).getValues());

    // Row 1: Not selected -> result should be null
    Assert.assertTrue(resultColumn.isNull(1));

    // Row 2: Selected -> should have a valid hash
    byte[] expected2 = calculateHmacSha512(dataStrings[2].getBytes(), keyStrings[2].getBytes());
    Assert.assertFalse(resultColumn.isNull(2));
    Assert.assertArrayEquals(expected2, resultColumn.getBinary(2).getValues());
  }

  /**
   * Test case with multi-row inputs (including nulls) and a constant key. This tests the
   * HmacConstantKeyColumnTransformer path.
   */
  @Test
  public void testHmacSha512WithConstantKey_MultiRowAndNulls() throws Exception {
    // 1. Arrange
    String[] dataStrings = {"data1", null, "data3"};
    String keyStr = "constant_secret";
    byte[] keyBytes = keyStr.getBytes(TSFileConfig.STRING_CHARSET);

    ColumnBuilder dataBuilder = new BinaryColumnBuilder(null, dataStrings.length);
    for (String s : dataStrings) {
      if (s != null) {
        dataBuilder.writeBinary(new Binary(s.getBytes(TSFileConfig.STRING_CHARSET)));
      } else {
        dataBuilder.appendNull();
      }
    }
    ColumnTransformer dataTransformer = mockColumnTransformer(dataBuilder.build());

    // Get the optimized strategy for a constant key
    HmacStrategy strategy =
        HmacStrategiesFactory.createConstantKeyHmacSha512Strategy(
            keyStr.getBytes(TSFileConfig.STRING_CHARSET));
    HmacConstantKeyColumnTransformer hmacTransformer =
        new HmacConstantKeyColumnTransformer(returnType, dataTransformer, strategy);

    // 2. Act
    hmacTransformer.addReferenceCount();
    hmacTransformer.evaluate();
    Column resultColumn = hmacTransformer.getColumn();

    // 3. Assert
    Assert.assertEquals(3, resultColumn.getPositionCount());

    // Row 0: Valid data -> should have a valid hash
    byte[] expected0 = calculateHmacSha512(dataStrings[0].getBytes(), keyBytes);
    Assert.assertFalse(resultColumn.isNull(0));
    Assert.assertArrayEquals(expected0, resultColumn.getBinary(0).getValues());

    // Row 1: Null data -> result should be null
    Assert.assertTrue(resultColumn.isNull(1));

    // Row 2: Valid data -> should have a valid hash
    byte[] expected2 = calculateHmacSha512(dataStrings[2].getBytes(), keyBytes);
    Assert.assertFalse(resultColumn.isNull(2));
    Assert.assertArrayEquals(expected2, resultColumn.getBinary(2).getValues());
  }

  /**
   * Test HMAC-SHA512 with a constant key and a selection array. This tests the
   * HmacConstantKeyColumnTransformer path with selection.
   */
  @Test
  public void testHmacSha512WithConstantKey_WithSelection() throws Exception {
    // 1. Arrange
    String[] dataStrings = {"Apache", "IoTDB", "rocks"};
    String keyStr = "super_secret_key";
    byte[] keyBytes = keyStr.getBytes(TSFileConfig.STRING_CHARSET);

    ColumnBuilder dataBuilder = new BinaryColumnBuilder(null, dataStrings.length);
    for (String s : dataStrings) {
      dataBuilder.writeBinary(new Binary(s.getBytes(TSFileConfig.STRING_CHARSET)));
    }
    ColumnTransformer dataTransformer = mockColumnTransformer(dataBuilder.build());

    HmacStrategy strategy =
        HmacStrategiesFactory.createConstantKeyHmacSha512Strategy(
            keyStr.getBytes(TSFileConfig.STRING_CHARSET));
    HmacConstantKeyColumnTransformer hmacTransformer =
        new HmacConstantKeyColumnTransformer(returnType, dataTransformer, strategy);

    // 2. Act
    hmacTransformer.addReferenceCount();
    boolean[] selection = {true, false, true};
    hmacTransformer.evaluateWithSelection(selection);
    Column resultColumn = hmacTransformer.getColumn();

    // 3. Assert
    Assert.assertEquals(3, resultColumn.getPositionCount());

    // Row 0: Selected -> should have a valid hash
    byte[] expected0 = calculateHmacSha512(dataStrings[0].getBytes(), keyBytes);
    Assert.assertFalse(resultColumn.isNull(0));
    Assert.assertArrayEquals(expected0, resultColumn.getBinary(0).getValues());

    // Row 1: Not selected -> result should be null
    Assert.assertTrue(resultColumn.isNull(1));

    // Row 2: Selected -> should have a valid hash
    byte[] expected2 = calculateHmacSha512(dataStrings[2].getBytes(), keyBytes);
    Assert.assertFalse(resultColumn.isNull(2));
    Assert.assertArrayEquals(expected2, resultColumn.getBinary(2).getValues());
  }
}
