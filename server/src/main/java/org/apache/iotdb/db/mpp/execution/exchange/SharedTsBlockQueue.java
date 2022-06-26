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

import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedList;
import java.util.Queue;

/** This is not thread safe class, the caller should ensure multi-threads safety. */
@NotThreadSafe
public class SharedTsBlockQueue {

  private static final Logger logger = LoggerFactory.getLogger(SharedTsBlockQueue.class);

  private final TFragmentInstanceId localFragmentInstanceId;
  private final LocalMemoryManager localMemoryManager;

  private boolean noMoreTsBlocks = false;

  private long bufferRetainedSizeInBytes = 0L;

  private final Queue<TsBlock> queue = new LinkedList<>();

  private SettableFuture<Void> blocked = SettableFuture.create();

  private ListenableFuture<Void> blockedOnMemory;

  private boolean destroyed = false;

  private LocalSourceHandle sourceHandle;
  private LocalSinkHandle sinkHandle;

  public SharedTsBlockQueue(
      TFragmentInstanceId fragmentInstanceId, LocalMemoryManager localMemoryManager) {
    this.localFragmentInstanceId =
        Validate.notNull(fragmentInstanceId, "fragment instance ID cannot be null");
    this.localMemoryManager =
        Validate.notNull(localMemoryManager, "local memory manager cannot be null");
  }

  public boolean hasNoMoreTsBlocks() {
    return noMoreTsBlocks;
  }

  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  public ListenableFuture<Void> isBlocked() {
    return blocked;
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public void setSinkHandle(LocalSinkHandle sinkHandle) {
    this.sinkHandle = sinkHandle;
  }

  public void setSourceHandle(LocalSourceHandle sourceHandle) {
    this.sourceHandle = sourceHandle;
  }

  /** Notify no more tsblocks will be added to the queue. */
  public void setNoMoreTsBlocks(boolean noMoreTsBlocks) {
    logger.info("SharedTsBlockQueue receive no more TsBlocks signal.");
    if (destroyed) {
      throw new IllegalStateException("queue has been destroyed");
    }
    this.noMoreTsBlocks = noMoreTsBlocks;
    if (!blocked.isDone()) {
      blocked.set(null);
    }
    if (this.sourceHandle != null) {
      this.sourceHandle.checkAndInvokeOnFinished();
    }
  }

  /**
   * Remove a tsblock from the head of the queue and return. Should be invoked only when the future
   * returned by {@link #isBlocked()} completes.
   */
  public TsBlock remove() {
    if (destroyed) {
      throw new IllegalStateException("queue has been destroyed");
    }
    TsBlock tsBlock = queue.remove();
    // Every time LocalSourceHandle consumes a TsBlock, it needs to send the event to
    // corresponding LocalSinkHandle.
    if (sinkHandle != null) {
      sinkHandle.checkAndInvokeOnFinished();
    }
    localMemoryManager
        .getQueryPool()
        .free(localFragmentInstanceId.getQueryId(), tsBlock.getRetainedSizeInBytes());
    bufferRetainedSizeInBytes -= tsBlock.getRetainedSizeInBytes();
    if (blocked.isDone() && queue.isEmpty() && !noMoreTsBlocks) {
      blocked = SettableFuture.create();
    }
    return tsBlock;
  }

  /**
   * Add tsblocks to the queue. Except the first invocation, this method should be invoked only when
   * the returned future of last invocation completes.
   */
  public ListenableFuture<Void> add(TsBlock tsBlock) {
    if (destroyed) {
      throw new IllegalStateException("queue has been destroyed");
    }

    Validate.notNull(tsBlock, "TsBlock cannot be null");
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
  public void destroy() {
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
