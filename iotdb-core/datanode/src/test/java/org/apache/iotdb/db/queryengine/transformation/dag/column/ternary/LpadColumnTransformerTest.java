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
package org.apache.iotdb.db.queryengine.transformation.dag.column.ternary;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

// This unit test is for a hypothetical LpadColumnTransformer class.
// It assumes the class exists and follows a similar structure to other transformers.
public class LpadColumnTransformerTest {

  // Helper method to mock a ColumnTransformer to return a predefined Column.
  private ColumnTransformer mockColumnTransformer(Column column) {
    ColumnTransformer mockTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockTransformer.getColumn()).thenReturn(column);
    // Common mocking for evaluation methods
    Mockito.doNothing().when(mockTransformer).tryEvaluate();
    Mockito.doNothing().when(mockTransformer).clearCache();
    Mockito.doNothing().when(mockTransformer).evaluateWithSelection(Mockito.any());
    Mockito.when(mockTransformer.getColumnCachePositionCount())
        .thenReturn(column.getPositionCount());
    return mockTransformer;
  }

  // Helper method to convert a hex string to a byte array for creating Binary objects.
  private static byte[] hexToBytes(String s) {
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

  /** Test cases for padding logic (data length < size). */
  @Test
  public void testPaddingCases() {
    // Setup input columns
    Binary[] dataValues = {
      new Binary(hexToBytes("AABB")), // Simple padding
      new Binary(hexToBytes("FF")), // Full repetition of pad
      new Binary(hexToBytes("FF")), // Truncated repetition of pad
      new Binary(hexToBytes("")) // Padding an empty blob
    };
    int[] sizeValues = {5, 7, 6, 4};
    Binary[] padValues = {
      new Binary(hexToBytes("00")),
      new Binary(hexToBytes("123456")),
      new Binary(hexToBytes("123456")),
      new Binary(hexToBytes("AB"))
    };

    Column dataColumn = new BinaryColumn(dataValues.length, Optional.empty(), dataValues);
    Column sizeColumn = new IntColumn(sizeValues.length, Optional.empty(), sizeValues);
    Column padColumn = new BinaryColumn(padValues.length, Optional.empty(), padValues);

    // Mock transformers
    ColumnTransformer dataTransformer = mockColumnTransformer(dataColumn);
    ColumnTransformer sizeTransformer = mockColumnTransformer(sizeColumn);
    ColumnTransformer padTransformer = mockColumnTransformer(padColumn);

    // Assuming LpadColumnTransformer exists and has this constructor
    LpadColumnTransformer lpadTransformer =
        new LpadColumnTransformer(BlobType.BLOB, dataTransformer, sizeTransformer, padTransformer);
    lpadTransformer.addReferenceCount();
    lpadTransformer.evaluate();
    Column resultColumn = lpadTransformer.getColumn();

    // Expected results
    byte[][] expectedValues = {
      hexToBytes("000000AABB"),
      hexToBytes("123456123456FF"),
      hexToBytes("1234561234FF"),
      hexToBytes("ABABABAB")
    };

    // Assertions
    Assert.assertEquals(4, resultColumn.getPositionCount());
    for (int i = 0; i < 4; i++) {
      Assert.assertFalse(resultColumn.isNull(i));
      Assert.assertArrayEquals(expectedValues[i], resultColumn.getBinary(i).getValues());
    }
  }

  /** Test cases for truncation logic (data length > size). */
  @Test
  public void testTruncationCases() {
    Binary[] dataValues = {
      new Binary(hexToBytes("0102030405060708")), new Binary(hexToBytes("AABB"))
    };
    int[] sizeValues = {4, 0};
    Binary[] padValues = {new Binary(hexToBytes("FF")), new Binary(hexToBytes("00"))};

    Column dataColumn = new BinaryColumn(2, Optional.empty(), dataValues);
    Column sizeColumn = new IntColumn(2, Optional.empty(), sizeValues);
    Column padColumn = new BinaryColumn(2, Optional.empty(), padValues);

    LpadColumnTransformer transformer =
        new LpadColumnTransformer(
            BlobType.BLOB,
            mockColumnTransformer(dataColumn),
            mockColumnTransformer(sizeColumn),
            mockColumnTransformer(padColumn));
    transformer.addReferenceCount();
    transformer.evaluate();
    Column resultColumn = transformer.getColumn();

    byte[][] expectedValues = {hexToBytes("01020304"), hexToBytes("")};

    Assert.assertEquals(2, resultColumn.getPositionCount());
    Assert.assertFalse(resultColumn.isNull(0));
    Assert.assertArrayEquals(expectedValues[0], resultColumn.getBinary(0).getValues());
    Assert.assertFalse(resultColumn.isNull(1));
    Assert.assertArrayEquals(expectedValues[1], resultColumn.getBinary(1).getValues());
  }

  /** Test case where data length equals size. */
  @Test
  public void testEqualLengthCase() {
    Binary[] dataValues = {new Binary(hexToBytes("ABCDEF"))};
    int[] sizeValues = {3};
    Binary[] padValues = {new Binary(hexToBytes("00"))};

    Column dataColumn = new BinaryColumn(1, Optional.empty(), dataValues);
    Column sizeColumn = new IntColumn(1, Optional.empty(), sizeValues);
    Column padColumn = new BinaryColumn(1, Optional.empty(), padValues);

    LpadColumnTransformer transformer =
        new LpadColumnTransformer(
            BlobType.BLOB,
            mockColumnTransformer(dataColumn),
            mockColumnTransformer(sizeColumn),
            mockColumnTransformer(padColumn));
    transformer.addReferenceCount();
    transformer.evaluate();
    Column resultColumn = transformer.getColumn();

    Assert.assertEquals(1, resultColumn.getPositionCount());
    Assert.assertFalse(resultColumn.isNull(0));
    Assert.assertArrayEquals(dataValues[0].getValues(), resultColumn.getBinary(0).getValues());
  }

  /** Test cases where one of the inputs is NULL. */
  @Test
  public void testNullInputCases() {
    // One case for each argument being null
    Binary[] dataValues = {null, new Binary(hexToBytes("AA")), new Binary(hexToBytes("BB"))};
    boolean[] dataIsNull = {true, false, false};

    int[] sizeValues = {5, 0, 5}; // Using 0 for the second case to avoid NPE on null size
    boolean[] sizeIsNull = {false, true, false};

    Binary[] padValues = {new Binary(hexToBytes("00")), new Binary(hexToBytes("00")), null};
    boolean[] padIsNull = {false, false, true};

    Column dataColumn = new BinaryColumn(3, Optional.of(dataIsNull), dataValues);
    Column sizeColumn = new IntColumn(3, Optional.of(sizeIsNull), sizeValues);
    Column padColumn = new BinaryColumn(3, Optional.of(padIsNull), padValues);

    LpadColumnTransformer transformer =
        new LpadColumnTransformer(
            BlobType.BLOB,
            mockColumnTransformer(dataColumn),
            mockColumnTransformer(sizeColumn),
            mockColumnTransformer(padColumn));
    transformer.addReferenceCount();
    transformer.evaluate();
    Column resultColumn = transformer.getColumn();

    Assert.assertEquals(3, resultColumn.getPositionCount());
    Assert.assertTrue(resultColumn.isNull(0)); // data is null
    Assert.assertTrue(resultColumn.isNull(1)); // size is null
    Assert.assertTrue(resultColumn.isNull(2)); // pad is null
  }

  /** Test with a selection array to process only a subset of rows. */
  @Test
  public void testEvaluateWithSelection() {
    Binary[] dataValues = {
      new Binary(hexToBytes("AA")), // Should be processed
      new Binary(hexToBytes("BB")), // Should be skipped
      new Binary(hexToBytes("CC")) // Should be processed
    };
    int[] sizeValues = {4, 5, 2};
    Binary[] padValues = {
      new Binary(hexToBytes("00")), new Binary(hexToBytes("01")), new Binary(hexToBytes("02"))
    };

    Column dataColumn = new BinaryColumn(3, Optional.empty(), dataValues);
    Column sizeColumn = new IntColumn(3, Optional.empty(), sizeValues);
    Column padColumn = new BinaryColumn(3, Optional.empty(), padValues);

    LpadColumnTransformer transformer =
        new LpadColumnTransformer(
            BlobType.BLOB,
            mockColumnTransformer(dataColumn),
            mockColumnTransformer(sizeColumn),
            mockColumnTransformer(padColumn));
    transformer.addReferenceCount();

    boolean[] selection = {true, false, true};
    transformer.evaluateWithSelection(selection);
    Column resultColumn = transformer.getColumn();

    byte[] expectedRow1 = hexToBytes("000000AA");
    byte[] expectedRow3 = hexToBytes("02CC"); // Corrected: Padding case

    Assert.assertEquals(3, resultColumn.getPositionCount());
    Assert.assertFalse(resultColumn.isNull(0));
    Assert.assertArrayEquals(expectedRow1, resultColumn.getBinary(0).getValues());
    Assert.assertTrue(resultColumn.isNull(1));
    Assert.assertFalse(resultColumn.isNull(2));
    Assert.assertArrayEquals(expectedRow3, resultColumn.getBinary(2).getValues());
  }

  /** Test failure when size is negative. */
  @Test
  public void testNegativeSize() {
    Column dataColumn =
        new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(hexToBytes("AABB"))});
    Column sizeColumn = new IntColumn(1, Optional.empty(), new int[] {-1});
    Column padColumn =
        new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(hexToBytes("00"))});

    LpadColumnTransformer transformer =
        new LpadColumnTransformer(
            BlobType.BLOB,
            mockColumnTransformer(dataColumn),
            mockColumnTransformer(sizeColumn),
            mockColumnTransformer(padColumn));
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException was not thrown for negative size.");
    } catch (SemanticException e) {
      String expectedMessage =
          "Failed to execute function 'Lpad' due to the value 0xaabb corresponding to a invalid target size, the allowed range is [0, 2147483647].";
      Assert.assertEquals(expectedMessage, e.getMessage());
    }
  }

  /** Test failure when paddata is empty. */
  @Test
  public void testEmptyPadData() {
    Column dataColumn =
        new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(hexToBytes("AA"))});
    Column sizeColumn = new IntColumn(1, Optional.empty(), new int[] {5});
    Column padColumn =
        new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(hexToBytes(""))});

    LpadColumnTransformer transformer =
        new LpadColumnTransformer(
            BlobType.BLOB,
            mockColumnTransformer(dataColumn),
            mockColumnTransformer(sizeColumn),
            mockColumnTransformer(padColumn));
    transformer.addReferenceCount();

    try {
      transformer.evaluate();
      Assert.fail("Expected SemanticException was not thrown for empty pad data.");
    } catch (SemanticException e) {
      String expectedMessage =
          "Failed to execute function 'Lpad' due the value 0xaa corresponding to a empty padding string.";
      Assert.assertEquals(expectedMessage, e.getMessage());
    }
  }
}
