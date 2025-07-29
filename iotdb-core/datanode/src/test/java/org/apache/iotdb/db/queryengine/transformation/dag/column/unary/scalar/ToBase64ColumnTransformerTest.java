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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

public class ToBase64ColumnTransformerTest {

  private Column mockColumn(Binary[] values, boolean[] valueIsNull) {
    return new BinaryColumn(
        values.length, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), values);
  }

  private org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer
      mockChildTransformer(Column column) {
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer mock =
        Mockito.mock(
            org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer.class);
    Mockito.when(mock.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mock).tryEvaluate();
    Mockito.doNothing().when(mock).evaluateWithSelection(Mockito.any());
    Mockito.doNothing().when(mock).clearCache();
    return mock;
  }

  @Test
  public void testStringType() {
    String input = "iotdb";
    Binary[] values = new Binary[] {new Binary(input, StandardCharsets.UTF_8)};
    Column column = mockColumn(values, null);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    String expected = Base64.getEncoder().encodeToString(input.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(expected, result.getBinary(0).toString());
  }

  @Test
  public void testStringTypeNull() {
    boolean[] valueIsNull = new boolean[] {true};
    Binary[] values = new Binary[] {null};
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    Assert.assertTrue(result.isNull(0));
  }

  @Test
  public void testBlobType() {
    byte[] input = new byte[] {1, 2, 3, 4, 5};
    Binary[] values = new Binary[] {new Binary(input)};
    Column column = mockColumn(values, null);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    String expected = Base64.getEncoder().encodeToString(input);
    Assert.assertEquals(expected, result.getBinary(0).toString());
  }

  @Test
  public void testBlobTypeNull() {
    boolean[] valueIsNull = new boolean[] {true};
    Binary[] values = new Binary[] {null};
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    Assert.assertTrue(result.isNull(0));
  }

  @Test
  public void testTextType() {
    String input = "iotdb";
    Binary[] values = new Binary[] {new Binary(input, StandardCharsets.UTF_8)};
    Column column = mockColumn(values, null);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    String expected = Base64.getEncoder().encodeToString(input.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(expected, result.getBinary(0).toString());
  }

  @Test
  public void testTextTypeNull() {
    boolean[] valueIsNull = new boolean[] {true};
    Binary[] values = new Binary[] {null};
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    Assert.assertTrue(result.isNull(0));
  }

  @Test
  public void testStringTypeMultiRowsWithNull() {
    String input1 = "iotdb";
    String input2 = "iotdb2";
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Binary[] values =
        new Binary[] {
          new Binary(input1, StandardCharsets.UTF_8),
          null,
          new Binary(input2, StandardCharsets.UTF_8)
        };
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    String expected1 = Base64.getEncoder().encodeToString(input1.getBytes(StandardCharsets.UTF_8));
    String expected2 = Base64.getEncoder().encodeToString(input2.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertEquals(expected2, result.getBinary(2).toString());
  }

  @Test
  public void testBlobTypeMultiRowsWithNull() {
    byte[] input1 = new byte[] {1, 2, 3};
    byte[] input2 = new byte[] {4, 5, 6};
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Binary[] values = new Binary[] {new Binary(input1), null, new Binary(input2)};
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    String expected1 = Base64.getEncoder().encodeToString(input1);
    String expected2 = Base64.getEncoder().encodeToString(input2);
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertEquals(expected2, result.getBinary(2).toString());
  }

  @Test
  public void testTextTypeMultiRowsWithNull() {
    String input1 = "iotdb";
    String input2 = "iotdb2";
    boolean[] valueIsNull = new boolean[] {false, true, false};
    Binary[] values =
        new Binary[] {
          new Binary(input1, StandardCharsets.UTF_8),
          null,
          new Binary(input2, StandardCharsets.UTF_8)
        };
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    String expected1 = Base64.getEncoder().encodeToString(input1.getBytes(StandardCharsets.UTF_8));
    String expected2 = Base64.getEncoder().encodeToString(input2.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertEquals(expected2, result.getBinary(2).toString());
  }

  @Test
  public void testEvaluateWithSelection() {
    String input1 = "iotdb";
    String input2 = "iotdb2";
    boolean[] valueIsNull = new boolean[] {false, false, false};
    Binary[] values =
        new Binary[] {
          new Binary(input1, StandardCharsets.UTF_8),
          new Binary(input2, StandardCharsets.UTF_8),
          new Binary(input1, StandardCharsets.UTF_8)
        };
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();

    // Only select the first and third row
    boolean[] selection = new boolean[] {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    String expected1 = Base64.getEncoder().encodeToString(input1.getBytes(StandardCharsets.UTF_8));
    String expected2 = Base64.getEncoder().encodeToString(input2.getBytes(StandardCharsets.UTF_8));
    // Row 0: selected, should be encoded
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    // Row 1: not selected, should be null
    Assert.assertTrue(result.isNull(1));
    // Row 2: selected, should be encoded
    Assert.assertEquals(expected1, result.getBinary(2).toString());
  }

  @Test
  public void testStringTypeEvaluateWithSelection() {
    String input1 = "iotdb";
    String input2 = "iotdb2";
    boolean[] valueIsNull = new boolean[] {false, false, false};
    Binary[] values =
        new Binary[] {
          new Binary(input1, StandardCharsets.UTF_8),
          new Binary(input2, StandardCharsets.UTF_8),
          new Binary(input1, StandardCharsets.UTF_8)
        };
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();

    // Only select the first and third row
    boolean[] selection = new boolean[] {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    String expected1 = Base64.getEncoder().encodeToString(input1.getBytes(StandardCharsets.UTF_8));
    String expected2 = Base64.getEncoder().encodeToString(input2.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertEquals(expected1, result.getBinary(2).toString());
  }

  @Test
  public void testBlobTypeEvaluateWithSelection() {
    byte[] input1 = new byte[] {1, 2, 3};
    byte[] input2 = new byte[] {4, 5, 6};
    boolean[] valueIsNull = new boolean[] {false, false, false};
    Binary[] values = new Binary[] {new Binary(input1), new Binary(input2), new Binary(input1)};
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();

    boolean[] selection = new boolean[] {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    String expected1 = Base64.getEncoder().encodeToString(input1);
    String expected2 = Base64.getEncoder().encodeToString(input2);
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertEquals(expected1, result.getBinary(2).toString());
  }

  @Test
  public void testTextTypeEvaluateWithSelection() {
    String input1 = "iotdb";
    String input2 = "iotdb2";
    boolean[] valueIsNull = new boolean[] {false, false, false};
    Binary[] values =
        new Binary[] {
          new Binary(input1, StandardCharsets.UTF_8),
          new Binary(input2, StandardCharsets.UTF_8),
          new Binary(input1, StandardCharsets.UTF_8)
        };
    Column column = mockColumn(values, valueIsNull);
    org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer child =
        mockChildTransformer(column);
    ToBase64ColumnTransformer transformer = new ToBase64ColumnTransformer(StringType.STRING, child);
    transformer.addReferenceCount();

    boolean[] selection = new boolean[] {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column result = transformer.getColumn();

    String expected1 = Base64.getEncoder().encodeToString(input1.getBytes(StandardCharsets.UTF_8));
    String expected2 = Base64.getEncoder().encodeToString(input2.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(expected1, result.getBinary(0).toString());
    Assert.assertTrue(result.isNull(1));
    Assert.assertEquals(expected1, result.getBinary(2).toString());
  }
}
