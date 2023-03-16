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

package org.apache.iotdb.db.mpp.execution.exchange;

import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.util.concurrent.SettableFuture;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class Utils {
  public static List<TsBlock> createMockTsBlocks(int numOfTsBlocks, long mockTsBlockSize) {
    List<TsBlock> mockTsBlocks = new ArrayList<>(numOfTsBlocks);
    for (int i = 0; i < numOfTsBlocks; i++) {
      TsBlock mockTsBlock = Mockito.mock(TsBlock.class);
      Mockito.when(mockTsBlock.getRetainedSizeInBytes()).thenReturn(mockTsBlockSize);
      mockTsBlocks.add(mockTsBlock);
    }

    return mockTsBlocks;
  }

  public static TsBlock createMockTsBlock(long mockTsBlockSize) {
    TsBlock mockTsBlock = Mockito.mock(TsBlock.class);
    Mockito.when(mockTsBlock.getRetainedSizeInBytes()).thenReturn(mockTsBlockSize);
    return mockTsBlock;
  }

  public static MemoryPool createMockBlockedMemoryPool(
      String queryId,
      String fragmentInstanceId,
      String planNodeId,
      int numOfMockTsBlock,
      long mockTsBlockSize) {
    long capacityInBytes = numOfMockTsBlock * mockTsBlockSize;
    MemoryPool mockMemoryPool = Mockito.mock(MemoryPool.class);
    AtomicReference<SettableFuture<Void>> settableFuture = new AtomicReference<>();
    settableFuture.set(SettableFuture.create());
    settableFuture.get().set(null);
    AtomicReference<Long> reservedBytes = new AtomicReference<>(0L);
    Mockito.when(
            mockMemoryPool.reserve(
                Mockito.eq(queryId),
                Mockito.eq(fragmentInstanceId),
                Mockito.eq(planNodeId),
                Mockito.anyLong(),
                Mockito.anyLong()))
        .thenAnswer(
            invocation -> {
              long bytesToReserve = invocation.getArgument(3);
              if (reservedBytes.get() + bytesToReserve <= capacityInBytes) {
                reservedBytes.updateAndGet(v -> v + (long) invocation.getArgument(3));
                return new Pair<>(settableFuture.get(), true);
              } else {
                settableFuture.set(SettableFuture.create());
                return new Pair<>(settableFuture.get(), false);
              }
            });
    Mockito.doAnswer(
            (Answer<Void>)
                invocation -> {
                  reservedBytes.updateAndGet(v -> v - (long) invocation.getArgument(3));
                  if (reservedBytes.get() <= 0) {
                    settableFuture.get().set(null);
                    reservedBytes.updateAndGet(v -> v + mockTsBlockSize);
                  }
                  return null;
                })
        .when(mockMemoryPool)
        .free(
            Mockito.eq(queryId),
            Mockito.eq(fragmentInstanceId),
            Mockito.eq(planNodeId),
            Mockito.anyLong());
    Mockito.when(
            mockMemoryPool.tryReserveForTest(
                Mockito.eq(queryId),
                Mockito.eq(fragmentInstanceId),
                Mockito.eq(planNodeId),
                Mockito.anyLong(),
                Mockito.anyLong()))
        .thenAnswer(
            invocation -> {
              long bytesToReserve = invocation.getArgument(3);
              if (reservedBytes.get() + bytesToReserve > capacityInBytes) {
                return false;
              } else {
                reservedBytes.updateAndGet(v -> v + bytesToReserve);
                return true;
              }
            });
    return mockMemoryPool;
  }

  public static MemoryPool createMockNonBlockedMemoryPool() {
    MemoryPool mockMemoryPool = Mockito.mock(MemoryPool.class);
    Mockito.when(
            mockMemoryPool.reserve(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyLong(),
                Mockito.anyLong()))
        .thenReturn(new Pair<>(immediateFuture(null), true));
    Mockito.when(
            mockMemoryPool.tryReserveForTest(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyLong(),
                Mockito.anyLong()))
        .thenReturn(true);
    return mockMemoryPool;
  }

  public static TsBlockSerde createMockTsBlockSerde(long mockTsBlockSize) {
    TsBlockSerde mockTsBlockSerde = Mockito.mock(TsBlockSerde.class);
    TsBlock mockTsBlock = Mockito.mock(TsBlock.class);
    Mockito.when(mockTsBlock.getRetainedSizeInBytes()).thenReturn(mockTsBlockSize);
    Mockito.when(mockTsBlockSerde.deserialize(Mockito.any(ByteBuffer.class)))
        .thenReturn(mockTsBlock);
    return mockTsBlockSerde;
  }
}
