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

package org.apache.iotdb.db.queryengine.execution.exchange.sink;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.exception.exchange.GetTsBlockFromClosedOrAbortedChannelException;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.queryengine.common.FragmentInstanceId.createFullId;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SINK_HANDLE_SEND_TSBLOCK_REMOTE;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet.SEND_NEW_DATA_BLOCK_NUM_CALLER;

public class SinkChannel implements ISinkChannel {

  private static final Logger LOGGER = LoggerFactory.getLogger(SinkChannel.class);

  public static final int MAX_ATTEMPT_TIMES = 3;
  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = 1000L;

  private final TEndPoint remoteEndpoint;
  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final String remotePlanNodeId;

  private final String localPlanNodeId;
  private final TFragmentInstanceId localFragmentInstanceId;

  private final String fullFragmentInstanceId;
  private final LocalMemoryManager localMemoryManager;
  private final ExecutorService executorService;
  private final TsBlockSerde serde;
  private final SinkListener sinkListener;
  private final String threadName;
  private long retryIntervalInMs;

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  // Use LinkedHashMap to meet 2 needs,
  //   1. Predictable iteration order so that removing buffered TsBlocks can be efficient.
  //   2. Fast lookup.
  private final LinkedHashMap<Integer, Pair<TsBlock, Long>> sequenceIdToTsBlock =
      new LinkedHashMap<>();

  // size for current TsBlock to reserve and free
  private long currentTsBlockSize;

  private final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
      mppDataExchangeServiceClientManager;

  @SuppressWarnings("squid:S3077")
  private volatile ListenableFuture<Void> blocked;

  private int nextSequenceId = 0;

  /** The actual buffered memory in bytes, including the amount of memory being reserved. */
  private long bufferRetainedSizeInBytes;

  private boolean aborted = false;

  private boolean closed = false;

  private boolean noMoreTsBlocks = false;

  private final AtomicBoolean invokedOnFinished = new AtomicBoolean(false);

  /** max bytes this SinkChannel can reserve. */
  private long maxBytesCanReserve =
      IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();

  private static final DataExchangeCostMetricSet DATA_EXCHANGE_COST_METRIC_SET =
      DataExchangeCostMetricSet.getInstance();
  private static final DataExchangeCountMetricSet DATA_EXCHANGE_COUNT_METRIC_SET =
      DataExchangeCountMetricSet.getInstance();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SinkChannel.class)
          + RamUsageEstimator.shallowSizeOfInstance(TFragmentInstanceId.class) * 2;

  @SuppressWarnings("squid:S107")
  public SinkChannel(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId,
      String localPlanNodeId,
      TFragmentInstanceId localFragmentInstanceId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SinkListener sinkListener,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this.remoteEndpoint = Validate.notNull(remoteEndpoint, "remoteEndPoint can not be null.");
    this.remoteFragmentInstanceId =
        Validate.notNull(remoteFragmentInstanceId, "remoteFragmentInstanceId can not be null.");
    this.remotePlanNodeId = Validate.notNull(remotePlanNodeId, "remotePlanNodeId can not be null.");
    this.localPlanNodeId = Validate.notNull(localPlanNodeId, "localPlanNodeId can not be null.");
    this.localFragmentInstanceId =
        Validate.notNull(localFragmentInstanceId, "localFragmentInstanceId can not be null.");
    this.fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId);
    this.localMemoryManager =
        Validate.notNull(localMemoryManager, "localMemoryManager can not be null.");
    this.executorService = Validate.notNull(executorService, "executorService can not be null.");
    this.serde = Validate.notNull(serde, "serde can not be null.");
    this.sinkListener = Validate.notNull(sinkListener, "sinkListener can not be null.");
    this.mppDataExchangeServiceClientManager = mppDataExchangeServiceClientManager;
    this.retryIntervalInMs = DEFAULT_RETRY_INTERVAL_IN_MS;
    this.threadName =
        createFullId(
            localFragmentInstanceId.queryId,
            localFragmentInstanceId.fragmentId,
            localFragmentInstanceId.instanceId);
    this.bufferRetainedSizeInBytes = 0;
    this.currentTsBlockSize = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    localMemoryManager
        .getQueryPool()
        .registerPlanNodeIdToQueryMemoryMap(
            localFragmentInstanceId.queryId, fullFragmentInstanceId, localPlanNodeId);
  }

  @Override
  public synchronized ListenableFuture<?> isFull() {
    checkState();
    // blocked could be null if this channel is closed before it is opened by ShuffleSinkHandle
    // return immediateVoidFuture() to avoid NPE
    if (closed) {
      return immediateVoidFuture();
    }
    return nonCancellationPropagating(blocked);
  }

  private void submitSendNewDataBlockEventTask(int startSequenceId, List<Long> blockSizes) {
    executorService.submit(new SendNewDataBlockEventTask(startSequenceId, blockSizes));
  }

  @Override
  public synchronized void send(TsBlock tsBlock) {
    long startTime = System.nanoTime();
    try {
      Validate.notNull(tsBlock, "tsBlocks is null");
      if (closed) {
        // SinkChannel may have been closed by its downstream SourceHandle
        return;
      }
      checkState();
      if (!blocked.isDone()) {
        throw new IllegalStateException("Sink handle is blocked.");
      }
      if (noMoreTsBlocks) {
        return;
      }
      long sizeInBytes = tsBlock.getSizeInBytes();
      int startSequenceId;
      startSequenceId = nextSequenceId;
      blocked =
          localMemoryManager
              .getQueryPool()
              .reserve(
                  localFragmentInstanceId.getQueryId(),
                  fullFragmentInstanceId,
                  localPlanNodeId,
                  sizeInBytes,
                  maxBytesCanReserve)
              .left;
      bufferRetainedSizeInBytes += sizeInBytes;

      sequenceIdToTsBlock.put(nextSequenceId, new Pair<>(tsBlock, currentTsBlockSize));
      nextSequenceId += 1;
      currentTsBlockSize = sizeInBytes;

      submitSendNewDataBlockEventTask(startSequenceId, ImmutableList.of(sizeInBytes));
    } finally {
      DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
          SINK_HANDLE_SEND_TSBLOCK_REMOTE, System.nanoTime() - startTime);
    }
  }

  @Override
  public synchronized void setNoMoreTsBlocks() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[StartSetNoMoreTsBlocks]");
    }
    if (aborted || closed) {
      return;
    }
    executorService.submit(new SendEndOfDataBlockEventTask());
  }

  @Override
  public synchronized boolean abort() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[StartAbortSinkChannel]");
    }
    if (aborted || closed) {
      return false;
    }
    sequenceIdToTsBlock.clear();
    if (blocked != null) {
      bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blocked);
    }
    if (bufferRetainedSizeInBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    sinkListener.onAborted(this);
    aborted = true;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[EndAbortSinkChannel]");
    }
    return true;
  }

  @Override
  public synchronized boolean close() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[StartCloseSinkChannel]");
    }
    if (closed || aborted) {
      return false;
    }
    sequenceIdToTsBlock.clear();
    if (blocked != null) {
      bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blocked);
    }
    if (bufferRetainedSizeInBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    invokeOnFinished();
    closed = true;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[EndCloseSinkChannel]");
    }
    return true;
  }

  private void invokeOnFinished() {
    if (invokedOnFinished.compareAndSet(false, true)) {
      sinkListener.onFinish(this);
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public synchronized boolean isAborted() {
    return aborted;
  }

  @Override
  public synchronized boolean isFinished() {
    return noMoreTsBlocks && sequenceIdToTsBlock.isEmpty();
  }

  @Override
  public synchronized long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOf(threadName)
        + RamUsageEstimator.sizeOf(localPlanNodeId)
        + RamUsageEstimator.sizeOf(remotePlanNodeId)
        + RamUsageEstimator.sizeOf(fullFragmentInstanceId);
  }

  public ByteBuffer getSerializedTsBlock(int partition, int sequenceId) {
    throw new UnsupportedOperationException();
  }

  public synchronized ByteBuffer getSerializedTsBlock(int sequenceId) throws IOException {
    if (aborted || closed) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "SinkChannel still receive getting TsBlock request after being aborted={} or closed={}",
            aborted,
            closed);
      }
      throw new GetTsBlockFromClosedOrAbortedChannelException("SinkChannel is aborted or closed. ");
    }
    Pair<TsBlock, Long> pair = sequenceIdToTsBlock.get(sequenceId);
    if (pair == null || pair.left == null) {
      LOGGER.warn(
          "The TsBlock doesn't exist. Sequence ID is {}, remaining map is {}",
          sequenceId,
          sequenceIdToTsBlock.entrySet());
      throw new IllegalStateException("The data block doesn't exist. Sequence ID: " + sequenceId);
    }
    return serde.serialize(pair.left);
  }

  public void acknowledgeTsBlock(int startSequenceId, int endSequenceId) {
    long freedBytes = 0L;
    synchronized (this) {
      if (aborted || closed) {
        return;
      }
      Iterator<Entry<Integer, Pair<TsBlock, Long>>> iterator =
          sequenceIdToTsBlock.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<Integer, Pair<TsBlock, Long>> entry = iterator.next();
        if (entry.getKey() < startSequenceId) {
          continue;
        }
        if (entry.getKey() >= endSequenceId) {
          break;
        }

        freedBytes += entry.getValue().right;
        bufferRetainedSizeInBytes -= entry.getValue().right;
        iterator.remove();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[ACKTsBlock] {}.", entry.getKey());
        }
      }

      // there may exist duplicate ack message in network caused by caller retrying, if so duplicate
      // ack message's freedBytes may be zero
      if (freedBytes > 0) {
        localMemoryManager
            .getQueryPool()
            .free(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                freedBytes);
      }
    }
    if (isFinished()) {
      invokeOnFinished();
    }
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    this.maxBytesCanReserve = Math.min(this.maxBytesCanReserve, maxBytesCanReserve);
  }

  @Override
  public String toString() {
    return String.format(
        "Query[%s]-[%s-%s-SinkChannel]:",
        localFragmentInstanceId.queryId,
        localFragmentInstanceId.fragmentId,
        localFragmentInstanceId.instanceId);
  }

  @Override
  public void checkState() {
    if (aborted) {
      throw new IllegalStateException("SinkChannel is aborted.");
    }
  }

  // region ============ ISinkChannel related ============

  @Override
  public synchronized void open() {
    if (aborted || closed) {
      return;
    }
    // SinkChannel is opened when ShuffleSinkHandle choose it as the next channel
    this.blocked =
        localMemoryManager
            .getQueryPool()
            .reserve(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
                maxBytesCanReserve) // actually we only know maxBytesCanReserve after
            // the handle is created, so we use DEFAULT here. It is ok to use DEFAULT here because
            // at first this SinkChannel has not reserved memory.
            .left;
    this.bufferRetainedSizeInBytes = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public boolean isNoMoreTsBlocks() {
    return noMoreTsBlocks;
  }

  @Override
  public int getNumOfBufferedTsBlocks() {
    return sequenceIdToTsBlock.size();
  }

  // endregion

  // region ============ TestOnly ============
  @TestOnly
  public void setRetryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
  }

  // endregion

  // region ============ inner class ============
  /**
   * Send a {@link org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent} to downstream fragment
   * instance.
   */
  class SendNewDataBlockEventTask implements Runnable {

    private final int startSequenceId;
    private final List<Long> blockSizes;

    SendNewDataBlockEventTask(int startSequenceId, List<Long> blockSizes) {
      Validate.isTrue(
          startSequenceId >= 0,
          "Start sequence ID should be greater than or equal to zero, but was: "
              + startSequenceId
              + ".");
      this.startSequenceId = startSequenceId;
      this.blockSizes = Validate.notNull(blockSizes);
    }

    @Override
    public void run() {
      try (SetThreadName sinkChannelName = new SetThreadName(threadName)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "[NotifyNewTsBlock] [{}, {}) to {}.{}",
              startSequenceId,
              startSequenceId + blockSizes.size(),
              remoteFragmentInstanceId,
              remotePlanNodeId);
        }
        int attempt = 0;
        TNewDataBlockEvent newDataBlockEvent =
            new TNewDataBlockEvent(
                remoteFragmentInstanceId,
                remotePlanNodeId,
                localFragmentInstanceId,
                startSequenceId,
                blockSizes);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          long startTime = System.nanoTime();
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            client.onNewDataBlockEvent(newDataBlockEvent);
            break;
          } catch (Exception e) {
            LOGGER.warn("Failed to send new data block event, attempt times: {}", attempt, e);
            if (attempt == MAX_ATTEMPT_TIMES) {
              sinkListener.onFailure(SinkChannel.this, e);
            }
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              sinkListener.onFailure(SinkChannel.this, e);
            }
          } finally {
            DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
                SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER, System.nanoTime() - startTime);
            DATA_EXCHANGE_COUNT_METRIC_SET.recordDataBlockNum(
                SEND_NEW_DATA_BLOCK_NUM_CALLER, blockSizes.size());
          }
        }
      }
    }
  }

  /**
   * Send a {@link org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent} to downstream fragment
   * instance.
   */
  class SendEndOfDataBlockEventTask implements Runnable {

    @Override
    public void run() {
      try (SetThreadName sinkChannelName = new SetThreadName(threadName)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[NotifyNoMoreTsBlock]");
        }
        int attempt = 0;
        TEndOfDataBlockEvent endOfDataBlockEvent =
            new TEndOfDataBlockEvent(
                remoteFragmentInstanceId,
                remotePlanNodeId,
                localFragmentInstanceId,
                nextSequenceId - 1);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            client.onEndOfDataBlockEvent(endOfDataBlockEvent);
            break;
          } catch (Exception e) {
            LOGGER.warn("Failed to send end of data block event, attempt times: {}", attempt, e);
            if (attempt == MAX_ATTEMPT_TIMES) {
              LOGGER.warn("Failed to send end of data block event after all retry", e);
              sinkListener.onFailure(SinkChannel.this, e);
              return;
            }
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              sinkListener.onFailure(SinkChannel.this, e);
            }
          }
        }
        noMoreTsBlocks = true;
        if (isFinished()) {
          invokeOnFinished();
        }
        sinkListener.onEndOfBlocks(SinkChannel.this);
      }
    }
  }
  // endregion

}
