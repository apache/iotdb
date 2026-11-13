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
import org.apache.iotdb.db.queryengine.plan.execution.PendingResultFragment;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryDataSetUtilsTest {

  private static final String BINARY_STR = "ty love zm";

  @Test
  public void testReadTabletValues() throws IOException {
    // Exercise the RPC length-prefixed binary format in both readers, including empty binaries.
    TSDataType[] dataTypes = {
      TSDataType.BOOLEAN,
      TSDataType.INT32,
      TSDataType.DATE,
      TSDataType.INT64,
      TSDataType.TIMESTAMP,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.TEXT,
      TSDataType.BLOB,
      TSDataType.STRING,
      TSDataType.OBJECT
    };
    Object[] expected = {
      new boolean[] {true, false},
      new int[] {1, 2},
      new int[] {20260721, 20260722},
      new long[] {3L, 4L},
      new long[] {5L, 6L},
      new float[] {1.25F, 2.5F},
      new double[] {3.75D, 4.5D},
      new Binary[] {
        new Binary("text-1", TSFileConfig.STRING_CHARSET),
        new Binary("text-2", TSFileConfig.STRING_CHARSET)
      },
      new Binary[] {Binary.EMPTY_VALUE, new Binary(new byte[] {2})},
      new Binary[] {
        new Binary("string-1", TSFileConfig.STRING_CHARSET),
        new Binary("string-2", TSFileConfig.STRING_CHARSET)
      },
      new Binary[] {
        new Binary("object-1", TSFileConfig.STRING_CHARSET),
        new Binary("object-2", TSFileConfig.STRING_CHARSET)
      }
    };

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (DataOutputStream stream = new DataOutputStream(byteArrayOutputStream)) {
      for (int i = 0; i < dataTypes.length; i++) {
        if (expected[i] instanceof Binary[] binaries) {
          for (Binary binary : binaries) {
            stream.writeInt(binary.getLength());
            stream.write(binary.getValues());
          }
        } else if (expected[i] instanceof int[] integers) {
          // DATE is an integer in tablet RPC, not the LocalDate[] used by the Type API.
          for (int value : integers) {
            stream.writeInt(value);
          }
        } else {
          Type.fromTsDataType(dataTypes[i]).serializeArray(expected[i], 2, stream);
        }
      }
    }
    byte[] serializedValues = byteArrayOutputStream.toByteArray();

    ByteBuffer buffer = ByteBuffer.wrap(serializedValues);
    assertTrue(
        Arrays.deepEquals(
            expected,
            QueryDataSetUtils.readTabletValuesFromBuffer(buffer, dataTypes, dataTypes.length, 2)));
    assertFalse(buffer.hasRemaining());
    try (DataInputStream stream = new DataInputStream(new ByteArrayInputStream(serializedValues))) {
      assertTrue(
          Arrays.deepEquals(
              expected,
              QueryDataSetUtils.readTabletValuesFromStream(
                  stream, dataTypes, dataTypes.length, 2)));
      assertEquals(-1, stream.read());
    }
  }

  @Test
  public void testConvertTsBlockByFetchSize() throws IoTDBException, IOException {

    Pair<TSQueryDataSet, Boolean> res =
        QueryDataSetUtils.convertTsBlockByFetchSize(buildQueryExecution(), 10);

    compareRes(res);
  }

  @Test
  public void testConvertTsBlockListByFetchSize() throws IOException {
    TSQueryDataSet result = QueryDataSetUtils.convertTsBlockByFetchSize(List.of(buildTsBlock()));

    assertEquals(Long.BYTES * 2, result.time.limit());
    assertEquals(1L, result.time.getLong());
    assertEquals(2L, result.time.getLong());
    assertEquals(1, result.valueList.size());
    assertEquals(1, result.bitmapList.size());
    assertEquals(Byte.BYTES, result.valueList.get(0).limit());
    assertEquals((byte) 1, result.valueList.get(0).get());
    assertEquals((byte) 0x80, result.bitmapList.get(0).get());
  }

  private IQueryExecution buildQueryExecution() throws IoTDBException, IOException {
    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Mockito.when(queryExecution.getOutputValueColumnCount()).thenReturn(6);
    TsBlock tsBlock = buildTsBlock();
    Mockito.when(queryExecution.getBatchResult())
        .thenReturn(Optional.of(tsBlock), Optional.empty());
    Mockito.when(queryExecution.getByteBufferBatchResult(false))
        .thenReturn(Optional.of(new TsBlockSerde().serialize(tsBlock)), Optional.empty());
    return queryExecution;
  }

  private TsBlock buildTsBlock() {
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
    builder.getColumnBuilder(5).writeBinary(new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET));
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).appendNull();
    builder.getColumnBuilder(1).appendNull();
    builder.getColumnBuilder(2).appendNull();
    builder.getColumnBuilder(3).appendNull();
    builder.getColumnBuilder(4).writeDouble(3.14d);
    builder.getColumnBuilder(5).appendNull();
    builder.declarePosition();
    return builder.build();
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
    assertEquals(
        new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET), tsBlock.getColumn(5).getBinary(0));
    assertTrue(tsBlock.getColumn(5).isNull(1));
  }

  @Test
  public void testConvertQueryResultByFetchSizeWithSingleOversizedBlock()
      throws IoTDBException, IOException {
    TsBlock tsBlock = buildTsBlock();
    ByteBuffer serialized = new TsBlockSerde().serialize(tsBlock);
    int totalLength = serialized.remaining();
    // one byte smaller than the block so that the block has to be split into two fragments
    int payloadSize = totalLength - 1;

    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Runnable memoryReleaser = Mockito.mock(Runnable.class);
    PendingResultFragment pendingFragment = new PendingResultFragment(memoryReleaser);
    Mockito.when(queryExecution.getPendingResultFragment()).thenReturn(pendingFragment);
    Mockito.when(queryExecution.getByteBufferBatchResult(true))
        .thenReturn(Optional.of(serialized.duplicate()), Optional.empty());
    Mockito.when(queryExecution.hasNextResult()).thenReturn(false);

    QueryDataSetUtils.ResultChunk firstChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, 10, 0, payloadSize);
    assertFalse(firstChunk.isFinished());
    assertTrue(firstChunk.hasNextOffset());
    assertTrue(firstChunk.hasTotalLength());
    assertEquals(totalLength, firstChunk.getTotalLength());
    assertEquals(payloadSize, firstChunk.getNextOffset());
    assertEquals(1, firstChunk.getQueryResult().size());
    assertEquals(payloadSize, firstChunk.getQueryResult().get(0).remaining());
    assertTrue(pendingFragment.getBlock() != null);
    // the memory of the pending block is still retained
    Mockito.verify(memoryReleaser, Mockito.never()).run();

    QueryDataSetUtils.ResultChunk secondChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(
            queryExecution, 10, firstChunk.getNextOffset(), payloadSize);
    assertTrue(secondChunk.isFinished());
    assertFalse(secondChunk.hasNextOffset());
    assertFalse(secondChunk.hasTotalLength());
    assertTrue(pendingFragment.getBlock() == null);
    // the memory is released once the fragmented block has been fully sent
    Mockito.verify(memoryReleaser, Mockito.times(1)).run();

    TsBlock result = deserialize(firstChunk.getQueryResult().get(0), secondChunk.getQueryResult());
    assertEquals(2, result.getPositionCount());
    assertEquals(1L, result.getTimeColumn().getLong(0));
    assertEquals(2L, result.getTimeColumn().getLong(1));
  }

  @Test
  public void testConvertQueryResultByFetchSizeWithTrailingFragment()
      throws IoTDBException, IOException {
    TsBlock tsBlock = buildTsBlock();
    ByteBuffer serialized = new TsBlockSerde().serialize(tsBlock);
    int blockSize = serialized.remaining();
    // a small block that fits into the payload, followed by the oversized block
    ByteBuffer smallBlock = serialized.duplicate();
    int payloadSize = smallBlock.remaining() + blockSize / 2;

    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Runnable memoryReleaser = Mockito.mock(Runnable.class);
    PendingResultFragment pendingFragment = new PendingResultFragment(memoryReleaser);
    Mockito.when(queryExecution.getPendingResultFragment()).thenReturn(pendingFragment);
    Mockito.when(queryExecution.getByteBufferBatchResult(true))
        .thenReturn(Optional.of(smallBlock), Optional.of(serialized.duplicate()), Optional.empty());
    Mockito.when(queryExecution.hasNextResult()).thenReturn(false);

    QueryDataSetUtils.ResultChunk firstChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, 10, 0, payloadSize);
    assertFalse(firstChunk.isFinished());
    assertTrue(firstChunk.hasNextOffset());
    assertEquals(2, firstChunk.getQueryResult().size());
    // the last element is the fragment of the second block
    assertEquals(blockSize / 2, firstChunk.getQueryResult().get(1).remaining());
    assertTrue(firstChunk.hasTotalLength());
    assertEquals(blockSize, firstChunk.getTotalLength());
    // the complete block is released at once, the fragmented one is released when it completes
    Mockito.verify(memoryReleaser, Mockito.times(1)).run();

    QueryDataSetUtils.ResultChunk secondChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(
            queryExecution, 10, firstChunk.getNextOffset(), payloadSize);
    assertTrue(secondChunk.isFinished());
    assertFalse(secondChunk.hasNextOffset());
    // only the rest of the oversized block remains
    assertEquals(1, secondChunk.getQueryResult().size());
    assertEquals(blockSize - blockSize / 2, secondChunk.getQueryResult().get(0).remaining());
    Mockito.verify(memoryReleaser, Mockito.times(2)).run();
  }

  @Test
  public void testContinuationCompletionDoesNotFetchNewBlocks() throws IoTDBException, IOException {
    TsBlock tsBlock = buildTsBlock();
    ByteBuffer serialized = new TsBlockSerde().serialize(tsBlock);
    int blockSize = serialized.remaining();
    int payloadSize = blockSize / 2;

    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Runnable memoryReleaser = Mockito.mock(Runnable.class);
    PendingResultFragment pendingFragment = new PendingResultFragment(memoryReleaser);
    Mockito.when(queryExecution.getPendingResultFragment()).thenReturn(pendingFragment);
    Mockito.when(queryExecution.getByteBufferBatchResult(true))
        .thenReturn(
            Optional.of(serialized.duplicate()),
            Optional.of(serialized.duplicate()),
            Optional.empty());
    Mockito.when(queryExecution.hasNextResult()).thenReturn(true, false);

    QueryDataSetUtils.ResultChunk firstChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, 10, 0, payloadSize);
    assertFalse(firstChunk.isFinished());
    assertEquals(payloadSize, firstChunk.getNextOffset());
    Mockito.verify(memoryReleaser, Mockito.never()).run();

    // The response that completes the fragmented block must not fetch new blocks, otherwise its
    // row count would exceed fetchSize (the tail of the big block is not accounted).
    QueryDataSetUtils.ResultChunk continuationChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(
            queryExecution, 10, firstChunk.getNextOffset(), blockSize);
    assertFalse(continuationChunk.isFinished());
    assertFalse(continuationChunk.hasNextOffset());
    assertEquals(1, continuationChunk.getQueryResult().size());
    assertEquals(blockSize - payloadSize, continuationChunk.getQueryResult().get(0).remaining());
    assertTrue(pendingFragment.getBlock() == null);
    Mockito.verify(memoryReleaser, Mockito.times(1)).run();

    // New blocks are delivered by the following request only
    QueryDataSetUtils.ResultChunk nextChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, 10, 0, blockSize);
    assertTrue(nextChunk.isFinished());
    assertFalse(nextChunk.hasNextOffset());
    assertEquals(1, nextChunk.getQueryResult().size());
    assertEquals(blockSize, nextChunk.getQueryResult().get(0).remaining());
    Mockito.verify(memoryReleaser, Mockito.times(2)).run();
  }

  @Test
  public void testEmptyBlockReleasesItsMemory() throws IoTDBException, IOException {
    TsBlock emptyTsBlock = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64)).build();
    ByteBuffer emptyBlock = new TsBlockSerde().serialize(emptyTsBlock);
    TsBlock tsBlock = buildTsBlock();
    ByteBuffer serialized = new TsBlockSerde().serialize(tsBlock);

    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Runnable memoryReleaser = Mockito.mock(Runnable.class);
    Mockito.when(queryExecution.getPendingResultFragment())
        .thenReturn(new PendingResultFragment(memoryReleaser));
    Mockito.when(queryExecution.getByteBufferBatchResult(true))
        .thenReturn(Optional.of(emptyBlock), Optional.of(serialized), Optional.empty());
    Mockito.when(queryExecution.hasNextResult()).thenReturn(false);

    QueryDataSetUtils.ResultChunk chunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(
            queryExecution, 10, 0, serialized.remaining());
    assertTrue(chunk.isFinished());
    assertEquals(1, chunk.getQueryResult().size());
    // the empty block and the complete block are both released immediately
    Mockito.verify(memoryReleaser, Mockito.times(2)).run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertQueryResultByFetchSizeRejectsMismatchedOffset() throws IoTDBException {
    PendingResultFragment pendingFragment = new PendingResultFragment(() -> {});
    pendingFragment.reset(ByteBuffer.wrap(new byte[10]), 5);
    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Mockito.when(queryExecution.getPendingResultFragment()).thenReturn(pendingFragment);

    QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, 10, 3, 8);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertQueryResultByFetchSizeRejectsOffsetWithoutPendingFragment()
      throws IoTDBException {
    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Mockito.when(queryExecution.getPendingResultFragment())
        .thenReturn(new PendingResultFragment(() -> {}));

    QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, 10, 5, 8);
  }

  private TsBlock deserialize(ByteBuffer head, List<ByteBuffer> tail) {
    int size = head.remaining();
    for (ByteBuffer buffer : tail) {
      size += buffer.remaining();
    }
    ByteBuffer assembled = ByteBuffer.allocate(size);
    assembled.put(head.duplicate());
    for (ByteBuffer buffer : tail) {
      assembled.put(buffer.duplicate());
    }
    assembled.flip();
    return new TsBlockSerde().deserialize(assembled);
  }
}
