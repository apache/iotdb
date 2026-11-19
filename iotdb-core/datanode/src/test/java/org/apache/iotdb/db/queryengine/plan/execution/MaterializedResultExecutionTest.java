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

package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.utils.QueryDataSetUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MaterializedResultExecutionTest {

  private static final long POOL_CAPACITY = 16L * 1024 * 1024;

  @Test
  public void testFragmentedTransferAndMemoryAccounting() throws Exception {
    ByteBuffer serialized = new TsBlockSerde().serialize(buildTsBlock());
    int totalLength = serialized.remaining();
    int payloadSize = totalLength / 2;
    MemoryPool memoryPool = new MemoryPool("test", new MemoryManager(POOL_CAPACITY), POOL_CAPACITY);
    LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);

    MaterializedResultExecution execution =
        new MaterializedResultExecution(
            new QueryId("20261119_050035_03054_1"),
            serialized.duplicate(),
            DatasetHeaderFactory.getLastQueryHeader(),
            "FAST_LAST_QUERY",
            "SELECT LAST root.sg.d1.s1",
            60_000L,
            "root",
            "127.0.0.1",
            localMemoryManager);
    assertEquals(0L, memoryPool.getReservedBytes());

    // the result is served once; the first response carries a fragment of it, and the serialized
    // bytes of the pending block are reserved from the query pool
    QueryDataSetUtils.ResultChunk firstChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(execution, 10, 0, payloadSize);
    assertFalse(firstChunk.isFinished());
    assertTrue(firstChunk.hasNextOffset());
    assertEquals(totalLength, firstChunk.getTotalLength());
    assertEquals(payloadSize, firstChunk.getQueryResult().get(0).remaining());
    assertFalse(execution.hasNextResult());
    assertTrue(execution.getByteBufferBatchResult().isEmpty());
    assertEquals(totalLength, memoryPool.getReservedBytes());

    // the following fetch completes the block and releases the reservation
    QueryDataSetUtils.ResultChunk secondChunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(
            execution, 10, firstChunk.getNextOffset(), totalLength);
    assertTrue(secondChunk.isFinished());
    assertFalse(secondChunk.hasNextOffset());
    assertEquals(0L, memoryPool.getReservedBytes());

    // cleanup is idempotent
    execution.stopAndCleanup(null);
    execution.stopAndCleanup(null);
  }

  @Test
  public void testCleanupReleasesReservationWithoutFetch() throws Exception {
    ByteBuffer serialized = new TsBlockSerde().serialize(buildTsBlock());
    MemoryPool memoryPool = new MemoryPool("test", new MemoryManager(POOL_CAPACITY), POOL_CAPACITY);
    LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);

    MaterializedResultExecution execution =
        new MaterializedResultExecution(
            new QueryId("20261119_050035_03054_2"),
            serialized.duplicate(),
            DatasetHeaderFactory.getLastQueryHeader(),
            "FAST_LAST_QUERY",
            "SELECT LAST root.sg.d1.s1",
            60_000L,
            "root",
            "127.0.0.1",
            localMemoryManager);

    QueryDataSetUtils.convertQueryResultByFetchSize(execution, 10, 0, 64);
    assertEquals(serialized.remaining(), memoryPool.getReservedBytes());

    // the client never fetches the rest
    execution.stopAndCleanup(null);
    assertEquals(0L, memoryPool.getReservedBytes());
  }

  @Test
  public void testPartialReservationWhenResultExceedsPool() throws Exception {
    ByteBuffer serialized = new TsBlockSerde().serialize(buildTsBlock());
    // a pool smaller than the result still serves it, with a partial reservation
    long smallCapacity = 1024L;
    MemoryPool memoryPool = new MemoryPool("test", new MemoryManager(smallCapacity), smallCapacity);
    LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);

    MaterializedResultExecution execution =
        new MaterializedResultExecution(
            new QueryId("20261119_050035_03054_3"),
            serialized.duplicate(),
            DatasetHeaderFactory.getLastQueryHeader(),
            "FAST_LAST_QUERY",
            "SELECT LAST root.sg.d1.s1",
            60_000L,
            "root",
            "127.0.0.1",
            localMemoryManager);

    QueryDataSetUtils.ResultChunk chunk =
        QueryDataSetUtils.convertQueryResultByFetchSize(execution, 10, 0, 64);
    assertTrue(chunk.hasNextOffset());
    assertEquals(smallCapacity, memoryPool.getReservedBytes());
    execution.stopAndCleanup(null);
    assertEquals(0L, memoryPool.getReservedBytes());
  }

  private static TsBlock buildTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    for (int i = 0; i < 100; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      builder.getColumnBuilder(0).writeLong(i * 10L);
      builder.declarePosition();
    }
    return builder.build();
  }
}
