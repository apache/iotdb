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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.mpp.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedList;
import java.util.Queue;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/** This is not thread safe class, the caller should ensure multi-threads safety. */
@NotThreadSafe
public class SharedTsBlockQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SharedTsBlockQueue.class);

  private final TFragmentInstanceId localFragmentInstanceId;

  private final String localPlanNodeId;

  private final String fullFragmentInstanceId;

  private final LocalMemoryManager localMemoryManager;

  private boolean noMoreTsBlocks = false;

  private long bufferRetainedSizeInBytes = 0L;

  private final Queue<TsBlock> queue = new LinkedList<>();

  private SettableFuture<Void> blocked = SettableFuture.create();

  /**
   * this is completed after calling isBlocked for the first time which indicates this queue needs
   * to output data
   */
  private final SettableFuture<Void> canAddTsBlock = SettableFuture.create();

  private ListenableFuture<Void> blockedOnMemory;

  private boolean closed = false;

  private LocalSourceHandle sourceHandle;
  private LocalSinkChannel sinkChannel;

  private long maxBytesCanReserve =
      IoTDBDescriptor.getInstance().getConfig().getMaxBytesPerFragmentInstance();

  public SharedTsBlockQueue(
      TFragmentInstanceId fragmentInstanceId,
      String planNodeId,
      LocalMemoryManager localMemoryManager) {
    this.localFragmentInstanceId =
        Validate.notNull(fragmentInstanceId, "fragment instance ID cannot be null");
    this.fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId);
    this.localPlanNodeId = Validate.notNull(planNodeId, "PlanNode ID cannot be null");
    this.localMemoryManager =
        Validate.notNull(localMemoryManager, "local memory manager cannot be null");
  }

  public boolean hasNoMoreTsBlocks() {
    return noMoreTsBlocks;
  }

  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  public SettableFuture<Void> getCanAddTsBlock() {
    return canAddTsBlock;
  }

  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    this.maxBytesCanReserve = maxBytesCanReserve;
  }

  public long getMaxBytesCanReserve() {
    return maxBytesCanReserve;
  }

  /** Allow adding data to queue manually. */
  public void allowAddingTsBlock() {
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
    }
  }

  public ListenableFuture<Void> isBlocked() {
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
    }
    return blocked;
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public int getNumOfBufferedTsBlocks() {
    return queue.size();
  }

  public void setSinkChannel(LocalSinkChannel sinkChannel) {
    this.sinkChannel = sinkChannel;
  }

  public void setSourceHandle(LocalSourceHandle sourceHandle) {
    this.sourceHandle = sourceHandle;
  }

  /** Notify no more tsblocks will be added to the queue. */
  public void setNoMoreTsBlocks(boolean noMoreTsBlocks) {
    LOGGER.debug("[SignalNoMoreTsBlockOnQueue]");
    if (closed) {
      LOGGER.warn("queue has been destroyed");
      return;
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
    if (closed) {
      throw new IllegalStateException("queue has been destroyed");
    }
    TsBlock tsBlock = queue.remove();
    // Every time LocalSourceHandle consumes a TsBlock, it needs to send the event to
    // corresponding LocalSinkChannel.
    if (sinkChannel != null) {
      sinkChannel.checkAndInvokeOnFinished();
    }
    localMemoryManager
        .getQueryPool()
        .free(
            localFragmentInstanceId.getQueryId(),
            fullFragmentInstanceId,
            localPlanNodeId,
            tsBlock.getRetainedSizeInBytes());
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
    if (closed) {
      LOGGER.warn("queue has been destroyed");
      return immediateVoidFuture();
    }

    Validate.notNull(tsBlock, "TsBlock cannot be null");
    Validate.isTrue(blockedOnMemory == null || blockedOnMemory.isDone(), "queue is full");
    Pair<ListenableFuture<Void>, Boolean> pair =
        localMemoryManager
            .getQueryPool()
            .reserve(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                tsBlock.getRetainedSizeInBytes(),
                maxBytesCanReserve);
    blockedOnMemory = pair.left;
    bufferRetainedSizeInBytes += tsBlock.getRetainedSizeInBytes();

    // reserve memory failed, we should wait until there is enough memory
    if (!pair.right) {
      blockedOnMemory.addListener(
          () -> {
            synchronized (this) {
              queue.add(tsBlock);
              if (!blocked.isDone()) {
                blocked.set(null);
              }
            }
          },
          directExecutor());
    } else { // reserve memory succeeded, add the TsBlock directly
      queue.add(tsBlock);
      if (!blocked.isDone()) {
        blocked.set(null);
      }
    }

    return blockedOnMemory;
  }

  /** Destroy the queue and complete the future. Should only be called in normal case */
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
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
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    localMemoryManager
        .getQueryPool()
        .clearMemoryReservationMap(
            localFragmentInstanceId.getQueryId(), fullFragmentInstanceId, localPlanNodeId);
  }

  /** Destroy the queue and cancel the future. Should only be called in abnormal case */
  public void abort() {
    if (closed) {
      return;
    }
    closed = true;
    if (!blocked.isDone()) {
      blocked.cancel(true);
    }
    if (blockedOnMemory != null) {
      bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
    }
    queue.clear();
    if (bufferRetainedSizeInBytes > 0L) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    localMemoryManager
        .getQueryPool()
        .clearMemoryReservationMap(
            localFragmentInstanceId.getQueryId(), fullFragmentInstanceId, localPlanNodeId);
  }

  /** Destroy the queue and cancel the future. Should only be called in abnormal case */
  public void abort(Throwable t) {
    if (closed) {
      return;
    }
    closed = true;
    if (!blocked.isDone()) {
      blocked.setException(t);
    }
    if (blockedOnMemory != null) {
      bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
    }
    queue.clear();
    if (bufferRetainedSizeInBytes > 0L) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
  }
}
