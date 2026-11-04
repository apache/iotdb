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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.splitter.NonAlignedChunkData;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

public class PieceDispatcherTest {

  @Test
  public void testEvictionQueueStaysBoundedAcrossChunksAndFlushes() throws Exception {
    final MemoryBoundedBuffer buffer = Mockito.mock(MemoryBoundedBuffer.class);
    final AtomicLong usedBytes = new AtomicLong();
    final long budget = chunk(0L).getDataSize() * 5;
    when(buffer.isMemoryEnough()).thenAnswer(ignored -> usedBytes.get() <= budget);
    Mockito.doAnswer(
            invocation -> {
              usedBytes.addAndGet(-invocation.<Long>getArgument(0));
              return null;
            })
        .when(buffer)
        .release(anyLong());
    final AtomicInteger dispatchedChunks = new AtomicInteger();
    final PieceDispatcher dispatcher =
        new PieceDispatcher(
            loadNode(),
            buffer,
            (piece, region) -> {
              dispatchedChunks.addAndGet(piece.getAllTsFileData().size());
              return true;
            });
    final TRegionReplicaSet replicaSet =
        new TRegionReplicaSet()
            .setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    final Field queueField = PieceDispatcher.class.getDeclaredField("largestPieceRegions");
    queueField.setAccessible(true);

    for (int i = 0; i < 128; i++) {
      final NonAlignedChunkData chunk = chunk(i % 2);
      usedBytes.addAndGet(chunk.getDataSize());
      dispatcher.offerChunk(chunk, replicaSet);
      assertTrue(dispatcher.dispatchLargestUntilMemoryEnough());
      assertTrue(
          "Eviction tracking must be bounded by the two live partition buffers",
          ((Collection<?>) queueField.get(dispatcher)).size() <= 2);
    }
    assertTrue(dispatcher.flushAll());
    assertEquals(128, dispatchedChunks.get());
    assertEquals(0L, usedBytes.get());
    assertTrue(
        "Flushed pieces must leave no eviction entries",
        ((Collection<?>) queueField.get(dispatcher)).isEmpty());
  }

  @Test
  public void testFailedEvictionCanBeRetried() throws Exception {
    final MemoryBoundedBuffer buffer = Mockito.mock(MemoryBoundedBuffer.class);
    final AtomicBoolean memoryEnough = new AtomicBoolean(false);
    when(buffer.isMemoryEnough()).thenAnswer(ignored -> memoryEnough.get());
    Mockito.doAnswer(
            ignored -> {
              memoryEnough.set(true);
              return null;
            })
        .when(buffer)
        .release(anyLong());
    final AtomicInteger attempts = new AtomicInteger();
    final PieceDispatcher dispatcher =
        new PieceDispatcher(loadNode(), buffer, (piece, region) -> attempts.incrementAndGet() > 1);
    final NonAlignedChunkData chunk = chunk(0L);
    dispatcher.offerChunk(
        chunk,
        new TRegionReplicaSet()
            .setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1)));

    assertFalse(dispatcher.dispatchLargestUntilMemoryEnough());
    assertTrue(dispatcher.dispatchLargestUntilMemoryEnough());
    assertEquals(2, attempts.get());
    Mockito.verify(buffer).release(chunk.getDataSize());
  }

  private static LoadSingleTsFileNode loadNode() {
    final LoadSingleTsFileNode node = Mockito.mock(LoadSingleTsFileNode.class);
    when(node.getPlanNodeId()).thenReturn(new PlanNodeId("bounded-queue"));
    final TsFileResource resource = Mockito.mock(TsFileResource.class);
    when(resource.getTsFile()).thenReturn(new File("bounded-queue.tsfile"));
    when(node.getTsFileResource()).thenReturn(resource);
    return node;
  }

  private static NonAlignedChunkData chunk(final long partition) {
    final NonAlignedChunkData chunk =
        new NonAlignedChunkData(
            new StringArrayDeviceID("root", "test", "d1"),
            new ChunkHeader(
                "s1", 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0),
            new TTimePartitionSlot(partition));
    chunk.writeDecodePage(new long[] {partition + 1}, new Object[] {1}, 1);
    chunk.endChunk();
    return chunk;
  }
}
