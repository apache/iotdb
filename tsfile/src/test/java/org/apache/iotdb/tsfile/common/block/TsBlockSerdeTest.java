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
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoding;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TsBlockSerdeTest {
  @Test
  public void testSerializeAndDeserialize() {
    final int positionCount = 10;

    // TODO: test more data types
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.DOUBLE);
    dataTypes.add(TSDataType.BOOLEAN);
    dataTypes.add(TSDataType.TEXT);
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(dataTypes);
    ColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder intColumnBuilder = tsBlockBuilder.getColumnBuilder(0);
    ColumnBuilder floatColumnBuilder = tsBlockBuilder.getColumnBuilder(1);
    ColumnBuilder longColumnBuilder = tsBlockBuilder.getColumnBuilder(2);
    ColumnBuilder doubleColumnBuilder = tsBlockBuilder.getColumnBuilder(3);
    ColumnBuilder booleanColumnBuilder = tsBlockBuilder.getColumnBuilder(4);
    ColumnBuilder binaryColumnBuilder = tsBlockBuilder.getColumnBuilder(5);
    for (int i = 0; i < positionCount; i++) {
      timeColumnBuilder.writeLong(i);
      intColumnBuilder.writeInt(i);
      floatColumnBuilder.writeFloat(i + i / 10F);
      longColumnBuilder.writeLong(i);
      doubleColumnBuilder.writeDouble(i + i / 10D);
      booleanColumnBuilder.writeBoolean(true);
      binaryColumnBuilder.writeBinary(new Binary("foo"));
      tsBlockBuilder.declarePosition();
    }

    TsBlockSerde tsBlockSerde = new TsBlockSerde();
    try {
      ByteBuffer output = tsBlockSerde.serialize(tsBlockBuilder.build());
      output.rewind();
      int valueColumnCount = output.getInt();
      assertEquals(6, valueColumnCount);
      assertEquals(TSDataType.INT32, TSDataType.deserialize(output.get()));
      assertEquals(TSDataType.FLOAT, TSDataType.deserialize(output.get()));
      assertEquals(TSDataType.INT64, TSDataType.deserialize(output.get()));
      assertEquals(TSDataType.DOUBLE, TSDataType.deserialize(output.get()));
      assertEquals(TSDataType.BOOLEAN, TSDataType.deserialize(output.get()));
      assertEquals(TSDataType.TEXT, TSDataType.deserialize(output.get()));
      assertEquals(positionCount, output.getInt());
      assertEquals(ColumnEncoding.INT64_ARRAY, ColumnEncoding.deserializeFrom(output));
      assertEquals(ColumnEncoding.INT32_ARRAY, ColumnEncoding.deserializeFrom(output));
      assertEquals(ColumnEncoding.INT32_ARRAY, ColumnEncoding.deserializeFrom(output));
      assertEquals(ColumnEncoding.INT64_ARRAY, ColumnEncoding.deserializeFrom(output));
      assertEquals(ColumnEncoding.INT64_ARRAY, ColumnEncoding.deserializeFrom(output));
      assertEquals(ColumnEncoding.BYTE_ARRAY, ColumnEncoding.deserializeFrom(output));
      assertEquals(ColumnEncoding.BINARY_ARRAY, ColumnEncoding.deserializeFrom(output));

      output.rewind();
      TsBlock tsBlock = tsBlockSerde.deserialize(output);
      assertEquals(valueColumnCount, tsBlock.getValueColumnCount());
      assertEquals(TSDataType.INT32, tsBlock.getColumn(0).getDataType());
      assertEquals(TSDataType.FLOAT, tsBlock.getColumn(1).getDataType());
      assertEquals(TSDataType.INT64, tsBlock.getColumn(2).getDataType());
      assertEquals(TSDataType.DOUBLE, tsBlock.getColumn(3).getDataType());
      assertEquals(TSDataType.BOOLEAN, tsBlock.getColumn(4).getDataType());
      assertEquals(TSDataType.TEXT, tsBlock.getColumn(5).getDataType());
      assertEquals(positionCount, tsBlock.getPositionCount());
      assertEquals(ColumnEncoding.INT32_ARRAY, tsBlock.getColumn(0).getEncoding());
      assertEquals(ColumnEncoding.INT32_ARRAY, tsBlock.getColumn(1).getEncoding());
      assertEquals(ColumnEncoding.INT64_ARRAY, tsBlock.getColumn(2).getEncoding());
      assertEquals(ColumnEncoding.INT64_ARRAY, tsBlock.getColumn(3).getEncoding());
      assertEquals(ColumnEncoding.BYTE_ARRAY, tsBlock.getColumn(4).getEncoding());
      assertEquals(ColumnEncoding.BINARY_ARRAY, tsBlock.getColumn(5).getEncoding());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializeAndDeserialize2() {

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      // to indicate this binary data is database info
      ReadWriteIOUtils.write((byte) 0, outputStream);

      ReadWriteIOUtils.write(1, outputStream);
      ReadWriteIOUtils.write("root.test.g_0", outputStream);
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
      fail(e.getMessage());
    }

    TsBlock tsBlock =
        new TsBlock(
            new TimeColumn(1, new long[] {0}),
            new BinaryColumn(
                1, Optional.empty(), new Binary[] {new Binary(outputStream.toByteArray())}));

    TsBlockSerde tsBlockSerde = new TsBlockSerde();
    try {
      ByteBuffer output = tsBlockSerde.serialize(tsBlock);
      output.rewind();

      TsBlock deserializedTsBlock = tsBlockSerde.deserialize(output);
      assertEquals(tsBlock.getRetainedSizeInBytes(), deserializedTsBlock.getRetainedSizeInBytes());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializeAndDeserialize3() {

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      // to indicate this binary data is database info
      ReadWriteIOUtils.write((byte) 0, outputStream);

      ReadWriteIOUtils.write(1, outputStream);
      ReadWriteIOUtils.write("root.test.g_0", outputStream);
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
      fail(e.getMessage());
    }

    TsBlock tsBlock =
        new TsBlock(
            new TimeColumn(1, new long[] {0}),
            new BinaryColumn(
                1,
                Optional.of(new boolean[] {false}),
                new Binary[] {new Binary(outputStream.toByteArray())}));

    TsBlockSerde tsBlockSerde = new TsBlockSerde();
    try {
      ByteBuffer output = tsBlockSerde.serialize(tsBlock);
      output.rewind();

      TsBlock deserializedTsBlock = tsBlockSerde.deserialize(output);
      assertEquals(tsBlock.getRetainedSizeInBytes(), deserializedTsBlock.getRetainedSizeInBytes());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
