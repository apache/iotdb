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

package org.apache.iotdb.consensus.natraft.protocol.log.recycle;

import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class EntryAllocator<T extends Entry> {
  private BlockingQueue<T> entryPool;
  private Supplier<T> entryFactory;
  private BlockingQueue<T> recyclingEntries;
  private Supplier<Long> safeIndexProvider;
  private AtomicLong allocatorSize = new AtomicLong();
  private AtomicLong allocatorGetCnt = new AtomicLong();
  private AtomicLong allocatorGetMissCnt = new AtomicLong();
  private AtomicLong allocatorRecycleCnt = new AtomicLong();
  private AtomicLong allocatorRecycleMissCnt = new AtomicLong();

  public EntryAllocator(
      RaftConfig config, Supplier<T> entryFactory, Supplier<Long> safeIndexProvider) {
    this.entryPool = new ArrayBlockingQueue<>(config.getEntryAllocatorCapacity());
    this.recyclingEntries = new ArrayBlockingQueue<>(config.getEntryAllocatorCapacity());
    this.entryFactory = entryFactory;
    this.safeIndexProvider = safeIndexProvider;
  }

  public T Allocate() {
    allocatorGetCnt.incrementAndGet();
    T entry = entryPool.poll();
    if (entry == null) {
      entry = entryFactory.get();
      allocatorGetMissCnt.incrementAndGet();
    } else {
      allocatorSize.addAndGet(-entry.estimateSize());
    }
    return entry;
  }

  public void recycle(T entry) {
    Long safeIndex = safeIndexProvider.get();
    allocatorRecycleCnt.incrementAndGet();
    if (entry.getCurrLogIndex() <= safeIndex) {
      entry.recycle();
      if (entryPool.offer(entry)) {
        allocatorSize.addAndGet(entry.estimateSize());
      } else {
        allocatorRecycleMissCnt.incrementAndGet();
      }
    } else {
      if (recyclingEntries.offer(entry)) {
        allocatorSize.addAndGet(entry.estimateSize());
      } else {
        allocatorRecycleMissCnt.incrementAndGet();
      }
    }

    checkRecyclingEntries();
  }

  public void checkRecyclingEntries() {
    Long safeIndex = safeIndexProvider.get();
    while (!recyclingEntries.isEmpty()) {
      T recyclingEntry = recyclingEntries.poll();
      if (recyclingEntry != null && recyclingEntry.getCurrLogIndex() <= safeIndex) {
        recyclingEntry.recycle();
        entryPool.offer(recyclingEntry);
      } else {
        if (recyclingEntry != null) {
          recyclingEntries.offer(recyclingEntry);
        }
        break;
      }
    }
  }

  public long getAllocatorSize() {
    return allocatorSize.get();
  }

  public int cachedEntryNum() {
    return entryPool.size() + recyclingEntries.size();
  }

  public double allocateHitRatio() {
    return 1.0 - allocatorGetMissCnt.get() * 1.0 / allocatorGetCnt.get();
  }

  public double recycleHitRatio() {
    return 1.0 - allocatorRecycleMissCnt.get() * 1.0 / allocatorRecycleCnt.get();
  }

  @Override
  public String toString() {
    return "EntryAllocator{"
        + "size="
        + getAllocatorSize()
        + ","
        + "entryNum="
        + cachedEntryNum()
        + ","
        + "allocateRatio="
        + allocateHitRatio()
        + ","
        + "recycleRatio="
        + recycleHitRatio()
        + "}";
  }
}
