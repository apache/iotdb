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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

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
   * this is completed after calling isBlocked for the first time which indicates that this queue
   * needs to output data.
   */
  private final SettableFuture<Void> canAddTsBlock = SettableFuture.create();

  private ListenableFuture<Void> blockedOnMemory;

  private boolean closed = false;
  private boolean alreadyRegistered = false;

  private LocalSourceHandle sourceHandle;
  private LocalSinkChannel sinkChannel;

  private long maxBytesCanReserve =
      IoTDBDescriptor.getInstance().getConfig().getMaxBytesPerFragmentInstance();

  // used for SharedTsBlockQueue listener
  private final ExecutorService executorService;

  public SharedTsBlockQueue(
      TFragmentInstanceId fragmentInstanceId,
      String planNodeId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService) {
    this.localFragmentInstanceId =
        Validate.notNull(fragmentInstanceId, "fragment instance ID cannot be null");
    this.fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId);
    this.localPlanNodeId = Validate.notNull(planNodeId, "PlanNode ID cannot be null");
    this.localMemoryManager =
        Validate.notNull(localMemoryManager, "local memory manager cannot be null");
    this.executorService = Validate.notNull(executorService, "ExecutorService can not be null.");
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

  public boolean isClosed() {
    return closed;
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

  /** Notify no more TsBlocks will be added to the queue. */
  public void setNoMoreTsBlocks(boolean noMoreTsBlocks) {
    LOGGER.debug("[SignalNoMoreTsBlockOnQueue]");
    if (closed) {
      LOGGER.debug("The queue has been destroyed when calling setNoMoreTsBlocks.");
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
   * Remove a TsBlock from the head of the queue and return. Should be invoked only when the future
   * returned by {@link #isBlocked()} completes.
   *
   * @throws IllegalStateException Cannot remove TsBlock from closed queue.
   */
  public TsBlock remove() {
    if (closed) {
      // try throw underlying exception instead of "Source handle is aborted."
      try {
        blocked.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(e.getCause() == null ? e : e.getCause());
      }
      throw new IllegalStateException("queue has been destroyed");
    }
    TsBlock tsBlock = queue.remove();
    localMemoryManager
        .getQueryPool()
        .free(
            localFragmentInstanceId.getQueryId(),
            fullFragmentInstanceId,
            localPlanNodeId,
            tsBlock.getRetainedSizeInBytes());
    bufferRetainedSizeInBytes -= tsBlock.getRetainedSizeInBytes();
    // Every time LocalSourceHandle consumes a TsBlock, it needs to send the event to
    // corresponding LocalSinkChannel.
    if (sinkChannel != null) {
      sinkChannel.checkAndInvokeOnFinished();
    }
    if (blocked.isDone() && queue.isEmpty() && !noMoreTsBlocks) {
      blocked = SettableFuture.create();
    }
    return tsBlock;
  }

  /**
   * Add TsBlocks to the queue. Except the first invocation, this method should be invoked only when
   * the returned future of last invocation completes.
   */
  public ListenableFuture<Void> add(TsBlock tsBlock) {
    if (closed) {
      // queue may have been closed
      return immediateVoidFuture();
    }

    Validate.notNull(tsBlock, "TsBlock cannot be null");
    Validate.isTrue(
        blockedOnMemory == null || blockedOnMemory.isDone(), "SharedTsBlockQueue is full");
    if (!alreadyRegistered) {
      localMemoryManager
          .getQueryPool()
          .registerPlanNodeIdToQueryMemoryMap(
              localFragmentInstanceId.queryId, fullFragmentInstanceId, localPlanNodeId);
      alreadyRegistered = true;
    }
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
    if (!Boolean.TRUE.equals(pair.right)) {
      SettableFuture<Void> channelBlocked = SettableFuture.create();
      blockedOnMemory.addListener(
          () -> {
            synchronized (this) {
              queue.add(tsBlock);
              if (!blocked.isDone()) {
                blocked.set(null);
              }
              channelBlocked.set(null);
            }
          },
          // Use directExecutor() here could lead to deadlock. Thread A holds lock of
          // SharedTsBlockQueueA and tries to invoke the listener of
          // SharedTsBlockQueueB(when freeing memory to complete MemoryReservationFuture) while
          // Thread B holds lock of SharedTsBlockQueueB and tries to invoke the listener of
          // SharedTsBlockQueueA
          executorService);
      return channelBlocked;
    } else { // reserve memory succeeded, add the TsBlock directly
      queue.add(tsBlock);
      if (!blocked.isDone()) {
        blocked.set(null);
      }
      return blockedOnMemory;
    }
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
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
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
    if (sinkChannel != null) {
      // attention: LocalSinkChannel of this SharedTsBlockQueue could be null when we close
      // LocalSourceHandle(with limit clause it's possible) before constructing the corresponding
      // LocalSinkChannel.
      // If this close method is invoked by LocalSourceHandle, listener of LocalSourceHandle will
      // remove the LocalSourceHandle from the map of MppDataExchangeManager and later when
      // LocalSinkChannel is initialized, it will construct a new SharedTsBlockQueue.
      // It is still safe that we let the LocalSourceHandle close successfully in this case. Because
      // the QueryTerminator will do the final cleaning logic.
      sinkChannel.close();
    }
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
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
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

  /** Destroy the queue and cancel the future. Should only be called in abnormal case */
  public void abort(Throwable t) {
    if (closed) {
      return;
    }
    closed = true;
    if (!blocked.isDone()) {
      blocked.setException(t);
    }
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
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
