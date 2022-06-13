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

package org.apache.iotdb.db.mpp.execution.datatransfer;

import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.Validate;

import javax.annotation.concurrent.GuardedBy;

import java.util.LinkedList;
import java.util.Queue;

public class SharedTsBlockQueue {

  private final TFragmentInstanceId localFragmentInstanceId;
  private final LocalMemoryManager localMemoryManager;

  @GuardedBy("this")
  private boolean noMoreTsBlocks = false;

  @GuardedBy("this")
  private long bufferRetainedSizeInBytes = 0L;

  @GuardedBy("this")
  private final Queue<TsBlock> queue = new LinkedList<>();

  @GuardedBy("this")
  private SettableFuture<Void> blocked = SettableFuture.create();

  @GuardedBy("this")
  private ListenableFuture<Void> blockedOnMemory;

  @GuardedBy("this")
  private boolean destroyed = false;

  public SharedTsBlockQueue(
      TFragmentInstanceId fragmentInstanceId, LocalMemoryManager localMemoryManager) {
    this.localFragmentInstanceId =
        Validate.notNull(fragmentInstanceId, "fragment instance ID cannot be null");
    this.localMemoryManager =
        Validate.notNull(localMemoryManager, "local memory manager cannot be null");
  }

  public synchronized boolean hasNoMoreTsBlocks() {
    return noMoreTsBlocks;
  }

  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  public ListenableFuture<Void> isBlocked() {
    return blocked;
  }

  public synchronized boolean isEmpty() {
    return queue.isEmpty();
  }

  /** Notify no more tsblocks will be added to the queue. */
  public synchronized void setNoMoreTsBlocks(boolean noMoreTsBlocks) {
    if (destroyed) {
      throw new IllegalStateException("queue has been destroyed");
    }
    this.noMoreTsBlocks = noMoreTsBlocks;
  }

  /**
   * Remove a tsblock from the head of the queue and return. Should be invoked only when the future
   * returned by {@link #isBlocked()} completes.
   */
  public synchronized TsBlock remove() {
    if (destroyed) {
      throw new IllegalStateException("queue has been destroyed");
    }
    TsBlock tsBlock = queue.remove();
    localMemoryManager
        .getQueryPool()
        .free(localFragmentInstanceId.getQueryId(), tsBlock.getRetainedSizeInBytes());
    bufferRetainedSizeInBytes -= tsBlock.getRetainedSizeInBytes();
    if (blocked.isDone() && queue.isEmpty()) {
      blocked = SettableFuture.create();
    }
    return tsBlock;
  }

  /**
   * Add tsblocks to the queue. Except the first invocation, this method should be invoked only when
   * the returned future of last invocation completes.
   */
  public synchronized ListenableFuture<Void> add(TsBlock tsBlock) {
    if (destroyed) {
      throw new IllegalStateException("queue has been destroyed");
    }

    Validate.notNull(tsBlock, "tsblock cannot be null");
    Validate.isTrue(blockedOnMemory == null || blockedOnMemory.isDone(), "queue is full");
    blockedOnMemory =
        localMemoryManager
            .getQueryPool()
            .reserve(localFragmentInstanceId.getQueryId(), tsBlock.getRetainedSizeInBytes());
    bufferRetainedSizeInBytes += tsBlock.getRetainedSizeInBytes();
    queue.add(tsBlock);
    if (!blocked.isDone()) {
      blocked.set(null);
    }
    return blockedOnMemory;
  }

  /** Destroy the queue and cancel the future. */
  public synchronized void destroy() {
    if (destroyed) {
      return;
    }
    destroyed = true;
    if (!blocked.isDone()) {
      blocked.set(null);
    }
    if (blockedOnMemory != null) {
      bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
    }
    queue.clear();
    if (bufferRetainedSizeInBytes > 0L) {
      localMemoryManager
          .getQueryPool()
          .free(localFragmentInstanceId.getQueryId(), bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
  }
}
