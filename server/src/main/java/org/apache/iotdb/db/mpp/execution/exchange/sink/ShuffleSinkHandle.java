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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.iotdb.db.mpp.metric.DataExchangeCostMetricSet.SINK_HANDLE_SEND_TSBLOCK_REMOTE;

public class ShuffleSinkHandle implements ISinkHandle {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleSinkHandle.class);

  /** Each ISinkHandle in the list matches one downStream ISourceHandle */
  private final List<ISinkChannel> downStreamChannelList;

  private final boolean[] hasSetNoMoreTsBlocks;

  private final boolean[] channelOpened;

  private final DownStreamChannelIndex downStreamChannelIndex;

  private final int channelNum;

  private final ShuffleStrategy shuffleStrategy;

  private final String localPlanNodeId;

  private final TFragmentInstanceId localFragmentInstanceId;

  private final MPPDataExchangeManager.SinkListener sinkListener;

  private boolean aborted = false;

  private boolean closed = false;

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  /** max bytes this ShuffleSinkHandle can reserve. */
  private long maxBytesCanReserve =
      IoTDBDescriptor.getInstance().getConfig().getMaxBytesPerFragmentInstance();

  public ShuffleSinkHandle(
      TFragmentInstanceId localFragmentInstanceId,
      List<ISinkChannel> downStreamChannelList,
      DownStreamChannelIndex downStreamChannelIndex,
      ShuffleStrategyEnum shuffleStrategyEnum,
      String localPlanNodeId,
      MPPDataExchangeManager.SinkListener sinkListener) {
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.downStreamChannelList = Validate.notNull(downStreamChannelList);
    this.downStreamChannelIndex = Validate.notNull(downStreamChannelIndex);
    this.localPlanNodeId = Validate.notNull(localPlanNodeId);
    this.sinkListener = Validate.notNull(sinkListener);
    this.channelNum = downStreamChannelList.size();
    this.shuffleStrategy = getShuffleStrategy(shuffleStrategyEnum);
    this.hasSetNoMoreTsBlocks = new boolean[channelNum];
    this.channelOpened = new boolean[channelNum];
    // open first channel
    tryOpenChannel(0);
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  public ISinkChannel getChannel(int index) {
    return downStreamChannelList.get(index);
  }

  @Override
  public synchronized ListenableFuture<?> isFull() {
    // It is safe to use currentChannel.isFull() to judge whether we can send a TsBlock only when
    // downStreamChannelIndex will not be changed between we call isFull() and send() of
    // ShuffleSinkHandle
    ISinkChannel currentChannel =
        downStreamChannelList.get(downStreamChannelIndex.getCurrentIndex());
    return currentChannel.isFull();
  }

  @Override
  public synchronized void send(TsBlock tsBlock) {
    long startTime = System.nanoTime();
    try {
      ISinkChannel currentChannel =
          downStreamChannelList.get(downStreamChannelIndex.getCurrentIndex());
      checkState();
      currentChannel.send(tsBlock);
    } finally {
      switchChannelIfNecessary();
      QUERY_METRICS.recordDataExchangeCost(
          SINK_HANDLE_SEND_TSBLOCK_REMOTE, System.nanoTime() - startTime);
    }
  }

  @Override
  public synchronized void setNoMoreTsBlocks() {
    for (int i = 0; i < downStreamChannelList.size(); i++) {
      if (!hasSetNoMoreTsBlocks[i]) {
        downStreamChannelList.get(i).setNoMoreTsBlocks();
        hasSetNoMoreTsBlocks[i] = true;
      }
    }
    sinkListener.onEndOfBlocks(this);
  }

  @Override
  public synchronized void setNoMoreTsBlocksOfOneChannel(int channelIndex) {
    if (!hasSetNoMoreTsBlocks[channelIndex]) {
      downStreamChannelList.get(channelIndex).setNoMoreTsBlocks();
      hasSetNoMoreTsBlocks[channelIndex] = true;
    }
  }

  @Override
  public synchronized boolean isAborted() {
    return aborted;
  }

  @Override
  public synchronized boolean isFinished() {
    for (ISink channel : downStreamChannelList) {
      if (!channel.isFinished()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public synchronized void abort() {
    if (aborted) {
      return;
    }
    LOGGER.debug("[StartAbortShuffleSinkHandle]");
    for (ISink channel : downStreamChannelList) {
      try {
        channel.abort();
      } catch (Exception e) {
        LOGGER.warn("Error occurred when try to abort channel.");
      }
    }
    aborted = true;
    sinkListener.onAborted(this);
    LOGGER.debug("[EndAbortShuffleSinkHandle]");
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    LOGGER.debug("[StartCloseShuffleSinkHandle]");
    for (ISink channel : downStreamChannelList) {
      try {
        channel.close();
      } catch (Exception e) {
        LOGGER.warn("Error occurred when try to abort channel.");
      }
    }
    closed = true;
    sinkListener.onFinish(this);
    LOGGER.debug("[EndCloseShuffleSinkHandle]");
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    this.maxBytesCanReserve = maxBytesCanReserve;
    downStreamChannelList.forEach(
        sinkHandle -> sinkHandle.setMaxBytesCanReserve(maxBytesCanReserve));
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("ShuffleSinkHandle is aborted.");
    } else if (closed) {
      throw new IllegalStateException("ShuffleSinkHandle is closed.");
    }
  }

  private void switchChannelIfNecessary() {
    shuffleStrategy.shuffle();
    tryOpenChannel(downStreamChannelIndex.getCurrentIndex());
  }

  public void tryOpenChannel(int channelIndex) {
    if (!channelOpened[channelIndex]) {
      downStreamChannelList.get(channelIndex).open();
      channelOpened[channelIndex] = true;
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
      // downStreamChannel is always an ISinkChannel
      ISinkChannel channel = downStreamChannelList.get(channelIndex);
      if (channel.isNoMoreTsBlocks()) {
        return false;
      }
      return channel.getBufferRetainedSizeInBytes() <= channelMemoryThreshold
          && channel.getNumOfBufferedTsBlocks() < 3;
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

  // region ============= Test Only =============
  @TestOnly
  @Override
  public long getBufferRetainedSizeInBytes() {
    return downStreamChannelList.stream()
        .map(ISink::getBufferRetainedSizeInBytes)
        .reduce(Long::sum)
        .orElse(0L);
  }
  // endregion
}
