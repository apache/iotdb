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

package org.apache.iotdb.db.mpp.execution.exchange.sink;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkHandleListener;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.mpp.common.FragmentInstanceId.createFullId;
import static org.apache.iotdb.db.mpp.metric.DataExchangeCostMetricSet.SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER;
import static org.apache.iotdb.db.mpp.metric.DataExchangeCostMetricSet.SINK_HANDLE_SEND_TSBLOCK_REMOTE;
import static org.apache.iotdb.db.mpp.metric.DataExchangeCountMetricSet.SEND_NEW_DATA_BLOCK_NUM_CALLER;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SinkHandle implements ISinkHandle {

  private static final Logger LOGGER = LoggerFactory.getLogger(SinkHandle.class);

  public static final int MAX_ATTEMPT_TIMES = 3;
  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = 1000L;

  private final List<DownStreamChannelLocation> downStreamChannelLocationList;

  private final DownStreamChannelIndex downStreamChannelIndex;

  private final List<DownStreamChannel> downStreamChannelList;

  private final int channelNum;

  private final ShuffleStrategy shuffleStrategy;

  private final String localPlanNodeId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final LocalMemoryManager localMemoryManager;
  private final ExecutorService executorService;
  private final TsBlockSerde serde;
  private final SinkHandleListener sinkHandleListener;
  private final String threadName;
  private long retryIntervalInMs;

  private final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
      mppDataExchangeServiceClientManager;

  private volatile ListenableFuture<Void> blocked;

  /** The actual buffered memory in bytes, including the amount of memory being reserved. */
  private long bufferRetainedSizeInBytes;

  // size for current TsBlock to reserve and free
  private long currentTsBlockSize = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

  private boolean aborted = false;

  private boolean closed = false;

  /** max bytes this SinkHandle can reserve. */
  private long maxBytesCanReserve =
      IoTDBDescriptor.getInstance().getConfig().getMaxBytesPerFragmentInstance();

  /** startSequenceID of each channel = channelIndex * CHANNEL_ID_GAP */
  private static final int CHANNEL_ID_GAP = 10000;

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public SinkHandle(
      List<DownStreamChannelLocation> downStreamChannelLocationList,
      DownStreamChannelIndex downStreamChannelIndex,
      ShuffleStrategyEnum shuffleStrategyEnum,
      String localPlanNodeId,
      TFragmentInstanceId localFragmentInstanceId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SinkHandleListener sinkHandleListener,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this.downStreamChannelLocationList = Validate.notNull(downStreamChannelLocationList);
    this.downStreamChannelIndex = Validate.notNull(downStreamChannelIndex);
    this.channelNum = downStreamChannelLocationList.size();
    // Init downStreamChannel
    this.downStreamChannelList = new ArrayList<>();
    for (int i = 0; i < channelNum; i++) {
      downStreamChannelList.add(new DownStreamChannel(i));
    }
    this.shuffleStrategy = getShuffleStrategy(shuffleStrategyEnum);
    this.localPlanNodeId = Validate.notNull(localPlanNodeId);
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    this.executorService = Validate.notNull(executorService);
    this.serde = Validate.notNull(serde);
    this.sinkHandleListener = Validate.notNull(sinkHandleListener);
    this.mppDataExchangeServiceClientManager = mppDataExchangeServiceClientManager;
    this.retryIntervalInMs = DEFAULT_RETRY_INTERVAL_IN_MS;
    this.threadName =
        createFullId(
            localFragmentInstanceId.queryId,
            localFragmentInstanceId.fragmentId,
            localFragmentInstanceId.instanceId);
    this.blocked =
        localMemoryManager
            .getQueryPool()
            .reserve(
                localFragmentInstanceId.getQueryId(),
                localFragmentInstanceId.getInstanceId(),
                localPlanNodeId,
                DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
                DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES) // actually we only know maxBytesCanReserve after
            // the handle is created, so we use DEFAULT here. It is ok to use DEFAULT here because
            // at first this SinkHandle has not reserved memory.
            .left;
    this.bufferRetainedSizeInBytes = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public synchronized ListenableFuture<?> isFull() {
    checkState();
    return nonCancellationPropagating(blocked);
  }

  private void submitSendNewDataBlockEventTask(
      int startSequenceId, List<Long> blockSizes, int currentDownStreamIndex) {
    executorService.submit(
        new SendNewDataBlockEventTask(startSequenceId, blockSizes, currentDownStreamIndex));
  }

  @Override
  public synchronized void send(TsBlock tsBlock) {
    long startTime = System.nanoTime();
    try {
      // pre check
      Validate.notNull(tsBlock, "tsBlocks is null");
      checkState();
      if (!blocked.isDone()) {
        throw new IllegalStateException("Sink handle is blocked.");
      }

      // choose one channel
      int currentDownStreamIndex = downStreamChannelIndex.getCurrentIndex();
      DownStreamChannel currentChannel = downStreamChannelList.get(currentDownStreamIndex);

      if (currentChannel.noMoreTsBlocks) {
        return;
      }
      long retainedSizeInBytes = tsBlock.getRetainedSizeInBytes();
      int startSequenceId;
      startSequenceId = currentChannel.nextSequenceId;
      blocked =
          localMemoryManager
              .getQueryPool()
              .reserve(
                  localFragmentInstanceId.getQueryId(),
                  localFragmentInstanceId.getInstanceId(),
                  localPlanNodeId,
                  retainedSizeInBytes,
                  maxBytesCanReserve)
              .left;
      bufferRetainedSizeInBytes += retainedSizeInBytes;

      currentChannel.sequenceIdToTsBlock.put(
          currentChannel.nextSequenceId, new Pair<>(tsBlock, currentTsBlockSize));
      currentChannel.nextSequenceId += 1;
      currentTsBlockSize = retainedSizeInBytes;

      // TODO: consider merge multiple NewDataBlockEvent for less network traffic.
      submitSendNewDataBlockEventTask(
          startSequenceId, ImmutableList.of(retainedSizeInBytes), currentDownStreamIndex);
    } finally {
      switchChannelIfNecessary();
      QUERY_METRICS.recordDataExchangeCost(
          SINK_HANDLE_SEND_TSBLOCK_REMOTE, System.nanoTime() - startTime);
    }
  }

  private synchronized void switchChannelIfNecessary() {
    shuffleStrategy.shuffle();
  }

  @Override
  public synchronized void send(int partition, List<TsBlock> tsBlocks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void setNoMoreTsBlocks() {
    LOGGER.debug("[StartSetNoMoreTsBlocks]");
    if (aborted || closed) {
      return;
    }
    downStreamChannelList.forEach(
        (downStreamChannel -> {
          if (!downStreamChannel.closed) {
            executorService.submit(new SendEndOfDataBlockEventTask(downStreamChannel.channelIndex));
            downStreamChannel.setNoMoreTsBlocks();
          }
        }));
  }

  @Override
  public synchronized void setNoMoreTsBlocksOfOneChannel(int channelIndex) {
    LOGGER.debug("[StartSetNoMoreTsBlocksOfChannel: {}]", channelIndex);
    if (aborted || closed) {
      return;
    }
    executorService.submit(new SendEndOfDataBlockEventTask(channelIndex));
    downStreamChannelList.get(channelIndex).setNoMoreTsBlocks();
  }

  @Override
  public synchronized void abort() {
    LOGGER.debug("[StartAbortSinkHandle]");
    downStreamChannelList.forEach(DownStreamChannel::clear);
    aborted = true;
    bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blocked);
    if (bufferRetainedSizeInBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              localFragmentInstanceId.getInstanceId(),
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    sinkHandleListener.onAborted(this);
    LOGGER.debug("[EndAbortSinkHandle]");
  }

  @Override
  public synchronized void close() {
    LOGGER.debug("[StartCloseSinkHandle]");
    downStreamChannelList.forEach(DownStreamChannel::clear);
    closed = true;
    bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryComplete(blocked);
    if (bufferRetainedSizeInBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              localFragmentInstanceId.getInstanceId(),
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    sinkHandleListener.onFinish(this);
    LOGGER.debug("[EndCloseSinkHandle]");
  }

  @Override
  public synchronized boolean isAborted() {
    return aborted;
  }

  @Override
  public synchronized boolean isFinished() {
    for (DownStreamChannel channel : downStreamChannelList) {
      // SinkHandle is not finished if at least one channel still has data.
      if (!channel.noMoreTsBlocks || !channel.sequenceIdToTsBlock.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public synchronized long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  public synchronized ByteBuffer getSerializedTsBlock(int sequenceId) throws IOException {
    if (aborted || closed) {
      LOGGER.warn(
          "SinkHandle still receive getting TsBlock request after being aborted={} or closed={}",
          aborted,
          closed);
      throw new IllegalStateException("Sink handle is aborted or closed. ");
    }
    int channelOfCurrentSequenceId = sequenceId / CHANNEL_ID_GAP;
    Pair<TsBlock, Long> pair =
        downStreamChannelList.get(channelOfCurrentSequenceId).sequenceIdToTsBlock.get(sequenceId);
    if (pair == null || pair.left == null) {
      LOGGER.warn(
          "The TsBlock doesn't exist. Sequence ID is {}, remaining map is {}, channelIndex is {}",
          sequenceId,
          downStreamChannelList.get(channelOfCurrentSequenceId).sequenceIdToTsBlock.entrySet(),
          channelOfCurrentSequenceId);
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
      int channelOfCurrentSequenceId = startSequenceId / CHANNEL_ID_GAP;
      Iterator<Entry<Integer, Pair<TsBlock, Long>>> iterator =
          downStreamChannelList
              .get(channelOfCurrentSequenceId)
              .sequenceIdToTsBlock
              .entrySet()
              .iterator();
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
        LOGGER.debug("[ACKTsBlock] {} of channel {}.", entry.getKey(), channelOfCurrentSequenceId);
      }
    }
    if (isFinished()) {
      sinkHandleListener.onFinish(this);
    }
    // there may exist duplicate ack message in network caused by caller retrying, if so duplicate
    // ack message's freedBytes may be zero
    if (freedBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              localFragmentInstanceId.getInstanceId(),
              localPlanNodeId,
              freedBytes);
    }
  }

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
        "Query[%s]-[%s-%s-SinkHandle]:",
        localFragmentInstanceId.queryId,
        localFragmentInstanceId.fragmentId,
        localFragmentInstanceId.instanceId);
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("Sink handle is aborted.");
    } else if (closed) {
      throw new IllegalStateException("SinkHandle is closed.");
    }
  }

  // region ============ Shuffle Related ============

  public enum ShuffleStrategyEnum {
    PLAIN,
    SIMPLE_ROUND_ROBIN,
  }

  @FunctionalInterface
  interface ShuffleStrategy {
    /*
     SinkHandle may have multiple channels, we need to choose the next channel each time we send a TsBlock.
    */
    void shuffle();
  }

  class PlainShuffleStrategy implements ShuffleStrategy {

    @Override
    public void shuffle() {
      // do nothing
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PlainShuffleStrategy needs to do nothing, current channel index is {}",
            downStreamChannelIndex.getCurrentIndex());
      }
    }
  }

  class SimpleRoundRobinStrategy implements ShuffleStrategy {

    private final long channelMemoryThreshold = maxBytesCanReserve / channelNum * 3;

    @Override
    public void shuffle() {
      int currentIndex = downStreamChannelIndex.getCurrentIndex();
      for (int i = 1; i < channelNum; i++) {
        int nextIndex = (currentIndex + i) % channelNum;
        if (satisfy(nextIndex)) {
          downStreamChannelIndex.setCurrentIndex(nextIndex);
          return;
        }
      }
    }

    private boolean satisfy(int channelIndex) {
      DownStreamChannel channel = downStreamChannelList.get(channelIndex);
      if (channel.closed || channel.noMoreTsBlocks) {
        return false;
      }
      return channel.getRetainedTsBlockSize() <= channelMemoryThreshold
          && channel.sequenceIdToTsBlock.size() < 3;
    }
  }

  private ShuffleStrategy getShuffleStrategy(ShuffleStrategyEnum strategyEnum) {
    switch (strategyEnum) {
      case PLAIN:
        return new PlainShuffleStrategy();
      case SIMPLE_ROUND_ROBIN:
        return new SimpleRoundRobinStrategy();
      default:
        throw new UnsupportedOperationException("Unsupported type of shuffle strategy");
    }
  }

  // endregion

  // region ============ inner class ============
  static class DownStreamChannel {
    /**
     * Use LinkedHashMap to meet 2 needs, 1. Predictable iteration order so that removing buffered
     * TsBlocks can be efficient. 2. Fast lookup.
     */
    private final LinkedHashMap<Integer, Pair<TsBlock, Long>> sequenceIdToTsBlock =
        new LinkedHashMap<>();

    /** true if this channel has no more data */
    private boolean noMoreTsBlocks = false;

    private boolean closed = false;

    /** Index of the channel */
    private final int channelIndex;

    /** Next sequence ID for each downstream ISourceHandle */
    private int nextSequenceId;

    public DownStreamChannel(int channelIndex) {
      this.channelIndex = channelIndex;
      this.nextSequenceId = channelIndex * CHANNEL_ID_GAP;
    }

    void close() {
      if (closed) {
        return;
      }
      sequenceIdToTsBlock.clear();
      noMoreTsBlocks = true;
      closed = true;
    }

    void clear() {
      sequenceIdToTsBlock.clear();
    }

    void setNoMoreTsBlocks() {
      noMoreTsBlocks = true;
    }

    long getRetainedTsBlockSize() {
      return sequenceIdToTsBlock.values().stream()
          .map(pair -> pair.right)
          .reduce(Long::sum)
          .orElse(0L);
    }
  }

  /**
   * Send a {@link TNewDataBlockEvent} to downstream fragment specified by downStreamIndex instance.
   */
  class SendNewDataBlockEventTask implements Runnable {

    private final int startSequenceId;
    private final List<Long> blockSizes;

    private final int downStreamIndex;

    SendNewDataBlockEventTask(int startSequenceId, List<Long> blockSizes, int downStreamIndex) {
      Validate.isTrue(
          startSequenceId >= 0,
          "Start sequence ID should be greater than or equal to zero, but was: "
              + startSequenceId
              + ".");
      Validate.isTrue(downStreamIndex >= 0, "downstreamIndex can not be negative.");
      this.startSequenceId = startSequenceId;
      this.blockSizes = Validate.notNull(blockSizes);
      this.downStreamIndex = downStreamIndex;
    }

    @Override
    public void run() {
      try (SetThreadName sinkHandleName = new SetThreadName(threadName)) {
        LOGGER.debug(
            "[NotifyNewTsBlock] [{}, {})", startSequenceId, startSequenceId + blockSizes.size());
        int attempt = 0;
        DownStreamChannelLocation location = downStreamChannelLocationList.get(downStreamIndex);
        TNewDataBlockEvent newDataBlockEvent =
            new TNewDataBlockEvent(
                location.getRemoteFragmentInstanceId(),
                location.getRemotePlanNodeId(),
                localFragmentInstanceId,
                startSequenceId,
                blockSizes);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          long startTime = System.nanoTime();
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(location.getRemoteEndpoint())) {
            client.onNewDataBlockEvent(newDataBlockEvent);
            break;
          } catch (Exception e) {
            LOGGER.warn("Failed to send new data block event, attempt times: {}", attempt, e);
            if (attempt == MAX_ATTEMPT_TIMES) {
              sinkHandleListener.onFailure(SinkHandle.this, e);
            }
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              sinkHandleListener.onFailure(SinkHandle.this, e);
            }
          } finally {
            QUERY_METRICS.recordDataExchangeCost(
                SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER, System.nanoTime() - startTime);
            QUERY_METRICS.recordDataBlockNum(SEND_NEW_DATA_BLOCK_NUM_CALLER, blockSizes.size());
          }
        }
      }
    }
  }

  /**
   * Send a {@link TEndOfDataBlockEvent} to downstream fragment specified by downStreamIndex
   * instance.
   */
  class SendEndOfDataBlockEventTask implements Runnable {
    private final int downStreamIndex;

    public SendEndOfDataBlockEventTask(int downStreamIndex) {
      Validate.isTrue(downStreamIndex >= 0, "downstreamIndex can not be negative.");
      this.downStreamIndex = downStreamIndex;
    }

    @Override
    public void run() {
      try (SetThreadName sinkHandleName = new SetThreadName(threadName)) {
        LOGGER.debug("[NotifyNoMoreTsBlock]");
        int attempt = 0;
        DownStreamChannelLocation location = downStreamChannelLocationList.get(downStreamIndex);
        DownStreamChannel channel = downStreamChannelList.get(downStreamIndex);
        TEndOfDataBlockEvent endOfDataBlockEvent =
            new TEndOfDataBlockEvent(
                location.getRemoteFragmentInstanceId(),
                location.getRemotePlanNodeId(),
                localFragmentInstanceId,
                channel.nextSequenceId - 1);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(location.getRemoteEndpoint())) {
            client.onEndOfDataBlockEvent(endOfDataBlockEvent);
            break;
          } catch (Exception e) {
            LOGGER.warn("Failed to send end of data block event, attempt times: {}", attempt, e);
            if (attempt == MAX_ATTEMPT_TIMES) {
              LOGGER.warn("Failed to send end of data block event after all retry", e);
              sinkHandleListener.onFailure(SinkHandle.this, e);
              return;
            }
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              sinkHandleListener.onFailure(SinkHandle.this, e);
            }
          }
        }
        channel.noMoreTsBlocks = true;
        if (isFinished()) {
          sinkHandleListener.onFinish(SinkHandle.this);
        }
        sinkHandleListener.onEndOfBlocks(SinkHandle.this);
      }
    }
  }

  // endregion

  // region ============ TestOnly ============
  @TestOnly
  public int getNumOfBufferedTsBlocks() {
    return downStreamChannelList.stream()
        .map(downStreamChannel -> downStreamChannel.sequenceIdToTsBlock.size())
        .reduce(Integer::sum)
        .orElse(0);
  }

  @TestOnly
  public void setRetryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
  }
  // endregion
}
