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

package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.NullColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

public class ColumnTest {

  @Test
  public void timeColumnSubColumnTest() {
    TimeColumnBuilder columnBuilder = new TimeColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeLong(i);
    }
    TimeColumn timeColumn = (TimeColumn) columnBuilder.build();
    timeColumn = (TimeColumn) timeColumn.subColumn(5);
    Assert.assertEquals(5, timeColumn.getPositionCount());
    Assert.assertEquals(5, timeColumn.getLong(0));
    Assert.assertEquals(9, timeColumn.getLong(4));

    timeColumn = (TimeColumn) timeColumn.subColumn(3);
    Assert.assertEquals(2, timeColumn.getPositionCount());
    Assert.assertEquals(8, timeColumn.getLong(0));
    Assert.assertEquals(9, timeColumn.getLong(1));
  }

  @Test
  public void binaryColumnSubColumnTest() {
    BinaryColumnBuilder columnBuilder = new BinaryColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeBinary(Binary.valueOf(String.valueOf(i)));
    }
    BinaryColumn binaryColumn = (BinaryColumn) columnBuilder.build();
    binaryColumn = (BinaryColumn) binaryColumn.subColumn(5);
    Assert.assertEquals(5, binaryColumn.getPositionCount());
    Assert.assertEquals("5", binaryColumn.getBinary(0).toString());
    Assert.assertEquals("9", binaryColumn.getBinary(4).toString());

    binaryColumn = (BinaryColumn) binaryColumn.subColumn(3);
    Assert.assertEquals(2, binaryColumn.getPositionCount());
    Assert.assertEquals("8", binaryColumn.getBinary(0).toString());
    Assert.assertEquals("9", binaryColumn.getBinary(1).toString());
  }

  @Test
  public void booleanColumnSubColumnTest() {
    BooleanColumnBuilder columnBuilder = new BooleanColumnBuilder(null, 10);
    // 0: true, 1: false
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeBoolean(i % 2 == 0);
    }
    BooleanColumn booleanColumn = (BooleanColumn) columnBuilder.build();
    booleanColumn = (BooleanColumn) booleanColumn.subColumn(5);
    Assert.assertEquals(5, booleanColumn.getPositionCount());
    Assert.assertFalse(booleanColumn.getBoolean(0));
    Assert.assertFalse(booleanColumn.getBoolean(4));

    booleanColumn = (BooleanColumn) booleanColumn.subColumn(3);
    Assert.assertEquals(2, booleanColumn.getPositionCount());
    Assert.assertTrue(booleanColumn.getBoolean(0));
    Assert.assertFalse(booleanColumn.getBoolean(1));
  }

  @Test
  public void doubleColumnSubColumnTest() {
    DoubleColumnBuilder columnBuilder = new DoubleColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeDouble(i);
    }
    DoubleColumn doubleColumn = (DoubleColumn) columnBuilder.build();
    doubleColumn = (DoubleColumn) doubleColumn.subColumn(5);
    Assert.assertEquals(5, doubleColumn.getPositionCount());
    Assert.assertEquals(5.0, doubleColumn.getDouble(0), 0.001);
    Assert.assertEquals(9.0, doubleColumn.getDouble(4), 0.001);

    doubleColumn = (DoubleColumn) doubleColumn.subColumn(3);
    Assert.assertEquals(2, doubleColumn.getPositionCount());
    Assert.assertEquals(8.0, doubleColumn.getDouble(0), 0.001);
    Assert.assertEquals(9.0, doubleColumn.getDouble(1), 0.001);
  }

  @Test
  public void floatColumnSubColumnTest() {
    FloatColumnBuilder columnBuilder = new FloatColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeFloat(i);
    }
    FloatColumn floatColumn = (FloatColumn) columnBuilder.build();
    floatColumn = (FloatColumn) floatColumn.subColumn(5);
    Assert.assertEquals(5, floatColumn.getPositionCount());
    Assert.assertEquals(5.0, floatColumn.getFloat(0), 0.001);
    Assert.assertEquals(9.0, floatColumn.getFloat(4), 0.001);

    floatColumn = (FloatColumn) floatColumn.subColumn(3);
    Assert.assertEquals(2, floatColumn.getPositionCount());
    Assert.assertEquals(8.0, floatColumn.getFloat(0), 0.001);
    Assert.assertEquals(9.0, floatColumn.getFloat(1), 0.001);
  }

  @Test
  public void intColumnSubColumnTest() {
    IntColumnBuilder columnBuilder = new IntColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeInt(i);
    }
    IntColumn intColumn = (IntColumn) columnBuilder.build();
    intColumn = (IntColumn) intColumn.subColumn(5);
    Assert.assertEquals(5, intColumn.getPositionCount());
    Assert.assertEquals(5, intColumn.getInt(0));
    Assert.assertEquals(9, intColumn.getInt(4));

    intColumn = (IntColumn) intColumn.subColumn(3);
    Assert.assertEquals(2, intColumn.getPositionCount());
    Assert.assertEquals(8, intColumn.getInt(0));
    Assert.assertEquals(9, intColumn.getInt(1));
  }

  @Test
  public void longColumnSubColumnTest() {
    LongColumnBuilder columnBuilder = new LongColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      columnBuilder.writeLong(i);
    }
    LongColumn longColumn = (LongColumn) columnBuilder.build();
    longColumn = (LongColumn) longColumn.subColumn(5);
    Assert.assertEquals(5, longColumn.getPositionCount());
    Assert.assertEquals(5, longColumn.getLong(0));
    Assert.assertEquals(9, longColumn.getLong(4));

    longColumn = (LongColumn) longColumn.subColumn(3);
    Assert.assertEquals(2, longColumn.getPositionCount());
    Assert.assertEquals(8, longColumn.getLong(0));
    Assert.assertEquals(9, longColumn.getLong(1));
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
}
