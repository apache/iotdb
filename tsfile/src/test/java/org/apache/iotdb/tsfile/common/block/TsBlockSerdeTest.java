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
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoding;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(dataTypes);
    ColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder intColumnBuilder = tsBlockBuilder.getColumnBuilder(0);
    ColumnBuilder floatColumnBuilder = tsBlockBuilder.getColumnBuilder(1);
    ColumnBuilder longColumnBuilder = tsBlockBuilder.getColumnBuilder(2);
    ColumnBuilder doubleColumnBuilder = tsBlockBuilder.getColumnBuilder(3);
    for (int i = 0; i < positionCount; i++) {
      timeColumnBuilder.writeLong(i);
      intColumnBuilder.writeInt(i);
      floatColumnBuilder.writeFloat(i + i / 10F);
      longColumnBuilder.writeLong(i);
      doubleColumnBuilder.writeDouble(i + i / 10D);
      tsBlockBuilder.declarePosition();
    }

    TsBlockSerde tsBlockSerde = new TsBlockSerde();
    try {
      ByteBuffer output = tsBlockSerde.serialize(tsBlockBuilder.build());
      output.rewind();
      int valueColumnCount = output.getInt();
      Assert.assertEquals(4, valueColumnCount);
      Assert.assertEquals(TSDataType.INT32, TSDataType.deserialize(output.get()));
      Assert.assertEquals(TSDataType.FLOAT, TSDataType.deserialize(output.get()));
      Assert.assertEquals(TSDataType.INT64, TSDataType.deserialize(output.get()));
      Assert.assertEquals(TSDataType.DOUBLE, TSDataType.deserialize(output.get()));
      Assert.assertEquals(positionCount, output.getInt());
      Assert.assertEquals(ColumnEncoding.INT64_ARRAY, ColumnEncoding.deserializeFrom(output));
      Assert.assertEquals(ColumnEncoding.INT32_ARRAY, ColumnEncoding.deserializeFrom(output));
      Assert.assertEquals(ColumnEncoding.INT32_ARRAY, ColumnEncoding.deserializeFrom(output));
      Assert.assertEquals(ColumnEncoding.INT64_ARRAY, ColumnEncoding.deserializeFrom(output));
      Assert.assertEquals(ColumnEncoding.INT64_ARRAY, ColumnEncoding.deserializeFrom(output));

      output.rewind();
      TsBlock tsBlock = tsBlockSerde.deserialize(output);
      Assert.assertEquals(valueColumnCount, tsBlock.getValueColumnCount());
      Assert.assertEquals(TSDataType.INT32, tsBlock.getColumn(0).getDataType());
      Assert.assertEquals(TSDataType.FLOAT, tsBlock.getColumn(1).getDataType());
      Assert.assertEquals(TSDataType.INT64, tsBlock.getColumn(2).getDataType());
      Assert.assertEquals(TSDataType.DOUBLE, tsBlock.getColumn(3).getDataType());
      Assert.assertEquals(positionCount, tsBlock.getPositionCount());
      Assert.assertEquals(ColumnEncoding.INT32_ARRAY, tsBlock.getColumn(0).getEncoding());
      Assert.assertEquals(ColumnEncoding.INT32_ARRAY, tsBlock.getColumn(1).getEncoding());
      Assert.assertEquals(ColumnEncoding.INT64_ARRAY, tsBlock.getColumn(2).getEncoding());
      Assert.assertEquals(ColumnEncoding.INT64_ARRAY, tsBlock.getColumn(3).getEncoding());
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
