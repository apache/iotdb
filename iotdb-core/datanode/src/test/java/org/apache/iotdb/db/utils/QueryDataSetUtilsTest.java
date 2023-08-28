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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryDataSetUtilsTest {

  private static final String BINARY_STR = "ty love zm";

  @Test
  public void testConvertTsBlockByFetchSize() throws IoTDBException, IOException {

    Pair<TSQueryDataSet, Boolean> res =
        QueryDataSetUtils.convertTsBlockByFetchSize(buildQueryExecution(), 10);

    compareRes(res);
  }

  private IQueryExecution buildQueryExecution() throws IoTDBException, IOException {
    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Mockito.when(queryExecution.getOutputValueColumnCount()).thenReturn(6);
    TsBlockBuilder builder =
        new TsBlockBuilder(
            Arrays.asList(
                TSDataType.BOOLEAN,
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.DOUBLE,
                TSDataType.TEXT));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeBoolean(true);
    builder.getColumnBuilder(1).writeInt(1);
    builder.getColumnBuilder(2).writeLong(1L);
    builder.getColumnBuilder(3).writeFloat(1.1f);
    builder.getColumnBuilder(4).appendNull();
    builder.getColumnBuilder(5).writeBinary(new Binary(BINARY_STR));
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).appendNull();
    builder.getColumnBuilder(1).appendNull();
    builder.getColumnBuilder(2).appendNull();
    builder.getColumnBuilder(3).appendNull();
    builder.getColumnBuilder(4).writeDouble(3.14d);
    builder.getColumnBuilder(5).appendNull();
    builder.declarePosition();
    Mockito.when(queryExecution.getBatchResult())
        .thenReturn(Optional.of(builder.build()), Optional.empty());
    Mockito.when(queryExecution.getByteBufferBatchResult())
        .thenReturn(Optional.of(new TsBlockSerde().serialize(builder.build())), Optional.empty());
    return queryExecution;
  }

  private void compareRes(Pair<TSQueryDataSet, Boolean> res) {
    final double delta = 0.00001d;
    assertEquals(true, res.right);
    assertEquals(Long.BYTES * 2, res.left.time.limit());
    assertEquals(1L, res.left.time.getLong());
    assertEquals(2L, res.left.time.getLong());
    assertEquals(6, res.left.valueList.size());
    assertEquals(6, res.left.bitmapList.size());

    assertEquals(Byte.BYTES, res.left.valueList.get(0).limit());
    assertEquals(Byte.BYTES, res.left.bitmapList.get(0).limit());
    assertEquals((byte) 1, res.left.valueList.get(0).get());
    assertEquals((byte) 0x80, res.left.bitmapList.get(0).get());

    assertEquals(Integer.BYTES, res.left.valueList.get(1).limit());
    assertEquals(Byte.BYTES, res.left.bitmapList.get(1).limit());
    assertEquals(1, res.left.valueList.get(1).getInt());
    assertEquals((byte) 0x80, res.left.bitmapList.get(1).get());

    assertEquals(Long.BYTES, res.left.valueList.get(2).limit());
    assertEquals(Byte.BYTES, res.left.bitmapList.get(2).limit());
    assertEquals(1L, res.left.valueList.get(2).getLong());
    assertEquals((byte) 0x80, res.left.bitmapList.get(2).get());

    assertEquals(Float.BYTES, res.left.valueList.get(3).limit());
    assertEquals(Byte.BYTES, res.left.bitmapList.get(3).limit());
    assertEquals(1.1f, res.left.valueList.get(3).getFloat(), delta);
    assertEquals((byte) 0x80, res.left.bitmapList.get(3).get());

    assertEquals(Double.BYTES, res.left.valueList.get(4).limit());
    assertEquals(Byte.BYTES, res.left.bitmapList.get(4).limit());
    assertEquals(3.14d, res.left.valueList.get(4).getDouble(), delta);
    assertEquals((byte) 0x40, res.left.bitmapList.get(4).get());

    assertEquals(Byte.BYTES, res.left.bitmapList.get(5).limit());
    assertEquals(Integer.BYTES + 10, res.left.valueList.get(5).limit());
    assertEquals(10, res.left.valueList.get(5).getInt());
    byte[] bytes = new byte[10];
    res.left.valueList.get(5).get(bytes);
    assertEquals(BINARY_STR, new String(bytes));
    assertEquals((byte) 0x80, res.left.bitmapList.get(5).get());
  }

  @Test
  public void testConvertQueryResultByFetchSize() throws IoTDBException, IOException {

    Pair<List<ByteBuffer>, Boolean> res =
        QueryDataSetUtils.convertQueryResultByFetchSize(buildQueryExecution(), 10);

    compareTsBlock(res);
  }

  private void compareTsBlock(Pair<List<ByteBuffer>, Boolean> res) {
    final double delta = 0.00001d;

    assertEquals(true, res.right);
    assertEquals(1, res.left.size());

    TsBlockSerde serde = new TsBlockSerde();
    TsBlock tsBlock = serde.deserialize(res.left.get(0));

    assertEquals(2, tsBlock.getPositionCount());

    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(tsBlock.getColumn(0).isNull(0));
    assertTrue(tsBlock.getColumn(0).getBoolean(0));
    assertTrue(tsBlock.getColumn(0).isNull(1));

    assertFalse(tsBlock.getColumn(1).isNull(0));
    assertEquals(1, tsBlock.getColumn(1).getInt(0));
    assertTrue(tsBlock.getColumn(1).isNull(1));

    assertFalse(tsBlock.getColumn(2).isNull(0));
    assertEquals(1L, tsBlock.getColumn(2).getLong(0));
    assertTrue(tsBlock.getColumn(2).isNull(1));

    assertFalse(tsBlock.getColumn(3).isNull(0));
    assertEquals(1.1f, tsBlock.getColumn(3).getFloat(0), delta);
    assertTrue(tsBlock.getColumn(3).isNull(1));

    assertTrue(tsBlock.getColumn(4).isNull(0));
    assertFalse(tsBlock.getColumn(4).isNull(1));
    assertEquals(3.14d, tsBlock.getColumn(4).getDouble(1), delta);

    assertFalse(tsBlock.getColumn(5).isNull(0));
    assertEquals(new Binary(BINARY_STR), tsBlock.getColumn(5).getBinary(0));
    assertTrue(tsBlock.getColumn(5).isNull(1));
  }
}
