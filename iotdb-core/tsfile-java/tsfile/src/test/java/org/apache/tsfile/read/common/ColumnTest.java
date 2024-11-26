/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.read.common;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.BytesUtils;

import org.junit.Assert;
import org.junit.Test;

public class ColumnTest {

  @Test
  public void timeColumnSubColumnTest() {
    TimeColumnBuilder columnBuilder = new TimeColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeLong(i);
    }
    TimeColumn timeColumn1 = (TimeColumn) columnBuilder.build();
    timeColumn1 = (TimeColumn) timeColumn1.subColumn(5);
    Assert.assertEquals(5, timeColumn1.getPositionCount());
    Assert.assertEquals(5, timeColumn1.getLong(0));
    Assert.assertEquals(9, timeColumn1.getLong(4));

    TimeColumn timeColumn2 = (TimeColumn) timeColumn1.subColumn(3);
    Assert.assertEquals(2, timeColumn2.getPositionCount());
    Assert.assertEquals(8, timeColumn2.getLong(0));
    Assert.assertEquals(9, timeColumn2.getLong(1));

    Assert.assertSame(timeColumn1.getLongs(), timeColumn2.getLongs());
  }

  @Test
  public void timeColumnSubColumnCopyTest() {
    TimeColumnBuilder columnBuilder = new TimeColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeLong(i);
    }
    TimeColumn timeColumn1 = (TimeColumn) columnBuilder.build();
    timeColumn1 = (TimeColumn) timeColumn1.subColumnCopy(5);
    Assert.assertEquals(5, timeColumn1.getPositionCount());
    Assert.assertEquals(5, timeColumn1.getLong(0));
    Assert.assertEquals(9, timeColumn1.getLong(4));

    TimeColumn timeColumn2 = (TimeColumn) timeColumn1.subColumnCopy(3);
    Assert.assertEquals(2, timeColumn2.getPositionCount());
    Assert.assertEquals(8, timeColumn2.getLong(0));
    Assert.assertEquals(9, timeColumn2.getLong(1));

    Assert.assertNotSame(timeColumn1.getLongs(), timeColumn2.getLongs());
  }

  @Test
  public void binaryColumnSubColumnTest() {
    BinaryColumnBuilder columnBuilder = new BinaryColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeBinary(BytesUtils.valueOf(String.valueOf(i)));
    }
    BinaryColumn binaryColumn1 = (BinaryColumn) columnBuilder.build();
    binaryColumn1 = (BinaryColumn) binaryColumn1.subColumn(5);
    Assert.assertEquals(5, binaryColumn1.getPositionCount());
    Assert.assertEquals("5", binaryColumn1.getBinary(0).toString());
    Assert.assertEquals("9", binaryColumn1.getBinary(4).toString());

    BinaryColumn binaryColumn2 = (BinaryColumn) binaryColumn1.subColumn(3);
    Assert.assertEquals(2, binaryColumn2.getPositionCount());
    Assert.assertEquals("8", binaryColumn2.getBinary(0).toString());
    Assert.assertEquals("9", binaryColumn2.getBinary(1).toString());

    Assert.assertSame(binaryColumn1.getBinaries(), binaryColumn2.getBinaries());
  }

  @Test
  public void binaryColumnSubColumnCopyTest() {
    BinaryColumnBuilder columnBuilder = new BinaryColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeBinary(BytesUtils.valueOf(String.valueOf(i)));
    }
    BinaryColumn binaryColumn1 = (BinaryColumn) columnBuilder.build();
    binaryColumn1 = (BinaryColumn) binaryColumn1.subColumnCopy(5);
    Assert.assertEquals(5, binaryColumn1.getPositionCount());
    Assert.assertEquals("5", binaryColumn1.getBinary(0).toString());
    Assert.assertEquals("9", binaryColumn1.getBinary(4).toString());

    BinaryColumn binaryColumn2 = (BinaryColumn) binaryColumn1.subColumnCopy(3);
    Assert.assertEquals(2, binaryColumn2.getPositionCount());
    Assert.assertEquals("8", binaryColumn2.getBinary(0).toString());
    Assert.assertEquals("9", binaryColumn2.getBinary(1).toString());

    Assert.assertNotSame(binaryColumn1.getBinaries(), binaryColumn2.getBinaries());
  }

  @Test
  public void booleanColumnSubColumnTest() {
    BooleanColumnBuilder columnBuilder = new BooleanColumnBuilder(null, 10);
    // 0: true, 1: false
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeBoolean(i % 2 == 0);
    }
    BooleanColumn booleanColumn1 = (BooleanColumn) columnBuilder.build();
    booleanColumn1 = (BooleanColumn) booleanColumn1.subColumn(5);
    Assert.assertEquals(5, booleanColumn1.getPositionCount());
    Assert.assertFalse(booleanColumn1.getBoolean(0));
    Assert.assertFalse(booleanColumn1.getBoolean(4));

    BooleanColumn booleanColumn2 = (BooleanColumn) booleanColumn1.subColumn(3);
    Assert.assertEquals(2, booleanColumn2.getPositionCount());
    Assert.assertTrue(booleanColumn2.getBoolean(0));
    Assert.assertFalse(booleanColumn2.getBoolean(1));

    Assert.assertSame(booleanColumn1.getBooleans(), booleanColumn2.getBooleans());
  }

  @Test
  public void booleanColumnSubColumnCopyTest() {
    BooleanColumnBuilder columnBuilder = new BooleanColumnBuilder(null, 10);
    // 0: true, 1: false
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeBoolean(i % 2 == 0);
    }
    BooleanColumn booleanColumn1 = (BooleanColumn) columnBuilder.build();
    booleanColumn1 = (BooleanColumn) booleanColumn1.subColumnCopy(5);
    Assert.assertEquals(5, booleanColumn1.getPositionCount());
    Assert.assertFalse(booleanColumn1.getBoolean(0));
    Assert.assertFalse(booleanColumn1.getBoolean(4));

    BooleanColumn booleanColumn2 = (BooleanColumn) booleanColumn1.subColumnCopy(3);
    Assert.assertEquals(2, booleanColumn2.getPositionCount());
    Assert.assertTrue(booleanColumn2.getBoolean(0));
    Assert.assertFalse(booleanColumn2.getBoolean(1));

    Assert.assertNotSame(booleanColumn1.getBooleans(), booleanColumn2.getBooleans());
  }

  @Test
  public void doubleColumnSubColumnTest() {
    DoubleColumnBuilder columnBuilder = new DoubleColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeDouble(i);
    }
    DoubleColumn doubleColumn1 = (DoubleColumn) columnBuilder.build();
    doubleColumn1 = (DoubleColumn) doubleColumn1.subColumn(5);
    Assert.assertEquals(5, doubleColumn1.getPositionCount());
    Assert.assertEquals(5.0, doubleColumn1.getDouble(0), 0.001);
    Assert.assertEquals(9.0, doubleColumn1.getDouble(4), 0.001);

    DoubleColumn doubleColumn2 = (DoubleColumn) doubleColumn1.subColumn(3);
    Assert.assertEquals(2, doubleColumn2.getPositionCount());
    Assert.assertEquals(8.0, doubleColumn2.getDouble(0), 0.001);
    Assert.assertEquals(9.0, doubleColumn2.getDouble(1), 0.001);

    Assert.assertSame(doubleColumn1.getDoubles(), doubleColumn2.getDoubles());
  }

  @Test
  public void doubleColumnSubColumnCopyTest() {
    DoubleColumnBuilder columnBuilder = new DoubleColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeDouble(i);
    }
    DoubleColumn doubleColumn1 = (DoubleColumn) columnBuilder.build();
    doubleColumn1 = (DoubleColumn) doubleColumn1.subColumnCopy(5);
    Assert.assertEquals(5, doubleColumn1.getPositionCount());
    Assert.assertEquals(5.0, doubleColumn1.getDouble(0), 0.001);
    Assert.assertEquals(9.0, doubleColumn1.getDouble(4), 0.001);

    DoubleColumn doubleColumn2 = (DoubleColumn) doubleColumn1.subColumnCopy(3);
    Assert.assertEquals(2, doubleColumn2.getPositionCount());
    Assert.assertEquals(8.0, doubleColumn2.getDouble(0), 0.001);
    Assert.assertEquals(9.0, doubleColumn2.getDouble(1), 0.001);

    Assert.assertNotSame(doubleColumn1.getDoubles(), doubleColumn2.getDoubles());
  }

  @Test
  public void floatColumnSubColumnTest() {
    FloatColumnBuilder columnBuilder = new FloatColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeFloat(i);
    }
    FloatColumn floatColumn1 = (FloatColumn) columnBuilder.build();
    floatColumn1 = (FloatColumn) floatColumn1.subColumn(5);
    Assert.assertEquals(5, floatColumn1.getPositionCount());
    Assert.assertEquals(5.0, floatColumn1.getFloat(0), 0.001);
    Assert.assertEquals(9.0, floatColumn1.getFloat(4), 0.001);

    FloatColumn floatColumn2 = (FloatColumn) floatColumn1.subColumn(3);
    Assert.assertEquals(2, floatColumn2.getPositionCount());
    Assert.assertEquals(8.0, floatColumn2.getFloat(0), 0.001);
    Assert.assertEquals(9.0, floatColumn2.getFloat(1), 0.001);

    Assert.assertSame(floatColumn1.getFloats(), floatColumn2.getFloats());
  }

  @Test
  public void floatColumnSubColumnCopyTest() {
    FloatColumnBuilder columnBuilder = new FloatColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeFloat(i);
    }
    FloatColumn floatColumn1 = (FloatColumn) columnBuilder.build();
    floatColumn1 = (FloatColumn) floatColumn1.subColumnCopy(5);
    Assert.assertEquals(5, floatColumn1.getPositionCount());
    Assert.assertEquals(5.0, floatColumn1.getFloat(0), 0.001);
    Assert.assertEquals(9.0, floatColumn1.getFloat(4), 0.001);

    FloatColumn floatColumn2 = (FloatColumn) floatColumn1.subColumnCopy(3);
    Assert.assertEquals(2, floatColumn2.getPositionCount());
    Assert.assertEquals(8.0, floatColumn2.getFloat(0), 0.001);
    Assert.assertEquals(9.0, floatColumn2.getFloat(1), 0.001);

    Assert.assertNotSame(floatColumn1.getFloats(), floatColumn2.getFloats());
  }

  @Test
  public void intColumnSubColumnTest() {
    IntColumnBuilder columnBuilder = new IntColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeInt(i);
    }
    IntColumn intColumn1 = (IntColumn) columnBuilder.build();
    intColumn1 = (IntColumn) intColumn1.subColumn(5);
    Assert.assertEquals(5, intColumn1.getPositionCount());
    Assert.assertEquals(5, intColumn1.getInt(0));
    Assert.assertEquals(9, intColumn1.getInt(4));

    IntColumn intColumn2 = (IntColumn) intColumn1.subColumn(3);
    Assert.assertEquals(2, intColumn2.getPositionCount());
    Assert.assertEquals(8, intColumn2.getInt(0));
    Assert.assertEquals(9, intColumn2.getInt(1));

    Assert.assertSame(intColumn1.getInts(), intColumn2.getInts());
  }

  @Test
  public void intColumnSubColumnCopyTest() {
    IntColumnBuilder columnBuilder = new IntColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeInt(i);
    }
    IntColumn intColumn1 = (IntColumn) columnBuilder.build();
    intColumn1 = (IntColumn) intColumn1.subColumnCopy(5);
    Assert.assertEquals(5, intColumn1.getPositionCount());
    Assert.assertEquals(5, intColumn1.getInt(0));
    Assert.assertEquals(9, intColumn1.getInt(4));

    IntColumn intColumn2 = (IntColumn) intColumn1.subColumnCopy(3);
    Assert.assertEquals(2, intColumn2.getPositionCount());
    Assert.assertEquals(8, intColumn2.getInt(0));
    Assert.assertEquals(9, intColumn2.getInt(1));

    Assert.assertNotSame(intColumn1.getInts(), intColumn2.getInts());
  }

  @Test
  public void longColumnSubColumnTest() {
    LongColumnBuilder columnBuilder = new LongColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeLong(i);
    }
    LongColumn longColumn1 = (LongColumn) columnBuilder.build();
    longColumn1 = (LongColumn) longColumn1.subColumn(5);
    Assert.assertEquals(5, longColumn1.getPositionCount());
    Assert.assertEquals(5, longColumn1.getLong(0));
    Assert.assertEquals(9, longColumn1.getLong(4));

    LongColumn longColumn2 = (LongColumn) longColumn1.subColumn(3);
    Assert.assertEquals(2, longColumn2.getPositionCount());
    Assert.assertEquals(8, longColumn2.getLong(0));
    Assert.assertEquals(9, longColumn2.getLong(1));

    Assert.assertSame(longColumn1.getLongs(), longColumn2.getLongs());
  }

  @Test
  public void longColumnSubColumnCopyTest() {
    LongColumnBuilder columnBuilder = new LongColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeLong(i);
    }
    LongColumn longColumn1 = (LongColumn) columnBuilder.build();
    longColumn1 = (LongColumn) longColumn1.subColumnCopy(5);
    Assert.assertEquals(5, longColumn1.getPositionCount());
    Assert.assertEquals(5, longColumn1.getLong(0));
    Assert.assertEquals(9, longColumn1.getLong(4));

    LongColumn longColumn2 = (LongColumn) longColumn1.subColumnCopy(3);
    Assert.assertEquals(2, longColumn2.getPositionCount());
    Assert.assertEquals(8, longColumn2.getLong(0));
    Assert.assertEquals(9, longColumn2.getLong(1));

    Assert.assertNotSame(longColumn1.getLongs(), longColumn2.getLongs());
  }

  @Test
  public void nullColumnTest() {
    NullColumn nullColumn = new NullColumn(10);
    Column subRegion = nullColumn.getRegion(7, 2);
    Column subColumn = subRegion.subColumn(1);

    Assert.assertEquals(2, subRegion.getPositionCount());
    Assert.assertEquals(1, subColumn.getPositionCount());
  }

  @Test
  public void runLengthEncodedColumnSubColumnTest() {
    LongColumnBuilder longColumnBuilder = new LongColumnBuilder(null, 1);
    longColumnBuilder.writeLong(1);
    RunLengthEncodedColumn column = new RunLengthEncodedColumn(longColumnBuilder.build(), 10);
    column = (RunLengthEncodedColumn) column.subColumn(5);
    Assert.assertEquals(5, column.getPositionCount());
    Assert.assertEquals(1, column.getLong(0));
    Assert.assertEquals(1, column.getLong(4));

    column = (RunLengthEncodedColumn) column.subColumn(3);
    Assert.assertEquals(2, column.getPositionCount());
    Assert.assertEquals(1, column.getLong(0));
    Assert.assertEquals(1, column.getLong(1));
  }

  @Test
  public void runLengthEncodedColumnSubColumnCopyTest() {
    LongColumnBuilder longColumnBuilder = new LongColumnBuilder(null, 1);
    longColumnBuilder.writeLong(1);
    RunLengthEncodedColumn column = new RunLengthEncodedColumn(longColumnBuilder.build(), 10);
    column = (RunLengthEncodedColumn) column.subColumnCopy(5);
    Assert.assertEquals(5, column.getPositionCount());
    Assert.assertEquals(1, column.getLong(0));
    Assert.assertEquals(1, column.getLong(4));

    column = (RunLengthEncodedColumn) column.subColumnCopy(3);
    Assert.assertEquals(2, column.getPositionCount());
    Assert.assertEquals(1, column.getLong(0));
    Assert.assertEquals(1, column.getLong(1));
  }
}
