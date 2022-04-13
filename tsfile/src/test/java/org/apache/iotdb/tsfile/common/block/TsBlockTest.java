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
package org.apache.iotdb.tsfile.common.block;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.*;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class TsBlockTest {

  private static final double DELTA = 0.000001;

  @Test
  public void testBooleanTsBlock() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    boolean[] valueArray = {true, false, false, false, false};
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.BOOLEAN));
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      builder.getColumnBuilder(0).writeBoolean(valueArray[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(1, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof BooleanColumn);
    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertFalse(tsBlock.getColumn(0).isNull(i));
      assertEquals(valueArray[i], tsBlock.getColumn(0).getBoolean(i));
    }
  }

  @Test
  public void testIntTsBlock() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    int[] valueArray = {10, 20, 30, 40, 50};
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      builder.getColumnBuilder(0).writeInt(valueArray[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(1, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertFalse(tsBlock.getColumn(0).isNull(i));
      assertEquals(valueArray[i], tsBlock.getColumn(0).getInt(i));
    }
  }

  @Test
  public void testLongTsBlock() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    long[] valueArray = {10L, 20L, 30L, 40L, 50L};
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      builder.getColumnBuilder(0).writeLong(valueArray[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(1, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof LongColumn);
    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertFalse(tsBlock.getColumn(0).isNull(i));
      assertEquals(valueArray[i], tsBlock.getColumn(0).getLong(i));
    }
  }

  @Test
  public void testFloatTsBlock() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    float[] valueArray = {10.0f, 20.0f, 30.0f, 40.0f, 50.0f};
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.FLOAT));
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      builder.getColumnBuilder(0).writeFloat(valueArray[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(1, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof FloatColumn);
    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertFalse(tsBlock.getColumn(0).isNull(i));
      assertEquals(valueArray[i], tsBlock.getColumn(0).getFloat(i), DELTA);
    }
  }

  @Test
  public void testDoubleTsBlock() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    double[] valueArray = {10.0, 20.0, 30.0, 40.0, 50.0};
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.DOUBLE));
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      builder.getColumnBuilder(0).writeDouble(valueArray[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(1, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof DoubleColumn);
    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertFalse(tsBlock.getColumn(0).isNull(i));
      assertEquals(valueArray[i], tsBlock.getColumn(0).getDouble(i), DELTA);
    }
  }

  @Test
  public void testBinaryTsBlock() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    Binary[] valueArray = {
      new Binary("10"), new Binary("20"), new Binary("30"), new Binary("40"), new Binary("50")
    };
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      builder.getColumnBuilder(0).writeBinary(valueArray[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(1, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof BinaryColumn);
    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertFalse(tsBlock.getColumn(0).isNull(i));
      assertEquals(valueArray[i], tsBlock.getColumn(0).getBinary(i));
    }
  }

  @Test
  public void testIntTsBlockWithNull() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    int[] valueArray = {10, 20, 30, 40, 50};
    boolean[] isNull = {false, false, true, true, false};
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      if (isNull[i]) {
        builder.getColumnBuilder(0).appendNull();

      } else {
        builder.getColumnBuilder(0).writeInt(valueArray[i]);
      }
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(1, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertEquals(isNull[i], tsBlock.getColumn(0).isNull(i));
      if (!isNull[i]) {
        assertEquals(valueArray[i], tsBlock.getColumn(0).getInt(i));
      }
    }
  }

  @Test
  public void testIntTsBlockWithAllNull() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    for (long l : timeArray) {
      builder.getTimeColumnBuilder().writeLong(l);
      builder.getColumnBuilder(0).appendNull();
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(1, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof RunLengthEncodedColumn);
    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertTrue(tsBlock.getColumn(0).isNull(i));
    }
  }

  @Test
  public void testMultiColumnTsBlockWithNull() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    boolean[] booleanValueArray = {true, false, false, false, true};
    boolean[] booleanIsNull = {true, true, false, true, false};
    int[] intValueArray = {10, 20, 30, 40, 50};
    boolean[] intIsNull = {false, true, false, false, true};
    long[] longValueArray = {100L, 200L, 300L, 400, 500L};
    boolean[] longIsNull = {true, false, false, true, true};
    float[] floatValueArray = {1000.0f, 2000.0f, 3000.0f, 4000.0f, 5000.0f};
    boolean[] floatIsNull = {false, false, true, true, false};
    double[] doubleValueArray = {10000.0, 20000.0, 30000.0, 40000.0, 50000.0};
    boolean[] doubleIsNull = {true, false, false, true, false};
    Binary[] binaryValueArray = {
      new Binary("19970909"),
      new Binary("ty"),
      new Binary("love"),
      new Binary("zm"),
      new Binary("19950421")
    };
    boolean[] binaryIsNull = {false, false, false, false, false};

    TsBlockBuilder builder =
        new TsBlockBuilder(
            Arrays.asList(
                TSDataType.BOOLEAN,
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.DOUBLE,
                TSDataType.TEXT));
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      if (booleanIsNull[i]) {
        builder.getColumnBuilder(0).appendNull();
      } else {
        builder.getColumnBuilder(0).writeBoolean(booleanValueArray[i]);
      }
      if (intIsNull[i]) {
        builder.getColumnBuilder(1).appendNull();
      } else {
        builder.getColumnBuilder(1).writeInt(intValueArray[i]);
      }
      if (longIsNull[i]) {
        builder.getColumnBuilder(2).appendNull();
      } else {
        builder.getColumnBuilder(2).writeLong(longValueArray[i]);
      }
      if (floatIsNull[i]) {
        builder.getColumnBuilder(3).appendNull();
      } else {
        builder.getColumnBuilder(3).writeFloat(floatValueArray[i]);
      }
      if (doubleIsNull[i]) {
        builder.getColumnBuilder(4).appendNull();
      } else {
        builder.getColumnBuilder(4).writeDouble(doubleValueArray[i]);
      }
      if (binaryIsNull[i]) {
        builder.getColumnBuilder(5).appendNull();
      } else {
        builder.getColumnBuilder(5).writeBinary(binaryValueArray[i]);
      }
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    assertEquals(timeArray.length, tsBlock.getPositionCount());
    assertEquals(6, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(0) instanceof BooleanColumn);
    assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
    assertTrue(tsBlock.getColumn(2) instanceof LongColumn);
    assertTrue(tsBlock.getColumn(3) instanceof FloatColumn);
    assertTrue(tsBlock.getColumn(4) instanceof DoubleColumn);
    assertTrue(tsBlock.getColumn(5) instanceof BinaryColumn);

    for (int i = 0; i < timeArray.length; i++) {
      assertEquals(timeArray[i], tsBlock.getTimeByIndex(i));
      assertEquals(booleanIsNull[i], tsBlock.getColumn(0).isNull(i));
      if (!booleanIsNull[i]) {
        assertEquals(booleanValueArray[i], tsBlock.getColumn(0).getBoolean(i));
      }
      assertEquals(intIsNull[i], tsBlock.getColumn(1).isNull(i));
      if (!intIsNull[i]) {
        assertEquals(intValueArray[i], tsBlock.getColumn(1).getInt(i));
      }
      assertEquals(longIsNull[i], tsBlock.getColumn(2).isNull(i));
      if (!longIsNull[i]) {
        assertEquals(longValueArray[i], tsBlock.getColumn(2).getLong(i));
      }
      assertEquals(floatIsNull[i], tsBlock.getColumn(3).isNull(i));
      if (!floatIsNull[i]) {
        assertEquals(floatValueArray[i], tsBlock.getColumn(3).getFloat(i), DELTA);
      }
      assertEquals(doubleIsNull[i], tsBlock.getColumn(4).isNull(i));
      if (!doubleIsNull[i]) {
        assertEquals(doubleValueArray[i], tsBlock.getColumn(4).getDouble(i), DELTA);
      }
      assertEquals(binaryIsNull[i], tsBlock.getColumn(5).isNull(i));
      if (!binaryIsNull[i]) {
        assertEquals(binaryValueArray[i], tsBlock.getColumn(5).getBinary(i));
      }
    }
  }
}
