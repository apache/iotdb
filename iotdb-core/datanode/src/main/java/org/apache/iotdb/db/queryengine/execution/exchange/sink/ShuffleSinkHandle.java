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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SINK_HANDLE_SEND_TSBLOCK_REMOTE;

public class ShuffleSinkHandle implements ISinkHandle {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleSinkHandle.class);

  /** Each ISinkChannel in the list matches one downstream ISourceHandle. */
  private final List<ISinkChannel> downStreamChannelList;

  private final boolean[] hasSetNoMoreTsBlocks;

  private final boolean[] channelOpened;

  private final DownStreamChannelIndex downStreamChannelIndex;

  private final int channelNum;

  private final ShuffleStrategy shuffleStrategy;

  private final TFragmentInstanceId localFragmentInstanceId;

  private final MPPDataExchangeManager.SinkListener sinkListener;

  private volatile boolean aborted = false;

  private volatile boolean closed = false;

  private static final DataExchangeCostMetricSet DATA_EXCHANGE_COST_METRIC_SET =
      DataExchangeCostMetricSet.getInstance();
  private final Lock lock = new ReentrantLock();

  /** max bytes this ShuffleSinkHandle can reserve. */
  private long maxBytesCanReserve =
      IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ShuffleSinkHandle.class)
          + RamUsageEstimator.shallowSizeOfInstance(TFragmentInstanceId.class)
          + RamUsageEstimator.shallowSizeOfInstance(DownStreamChannelIndex.class);

  public ShuffleSinkHandle(
      TFragmentInstanceId localFragmentInstanceId,
      List<ISinkChannel> downStreamChannelList,
      DownStreamChannelIndex downStreamChannelIndex,
      ShuffleStrategyEnum shuffleStrategyEnum,
      MPPDataExchangeManager.SinkListener sinkListener) {
    this.localFragmentInstanceId =
        Validate.notNull(localFragmentInstanceId, "localFragmentInstanceId can not be null.");
    this.downStreamChannelList =
        Validate.notNull(downStreamChannelList, "downStreamChannelList can not be null.");
    this.downStreamChannelIndex =
        Validate.notNull(downStreamChannelIndex, "downStreamChannelIndex can not be null.");
    this.sinkListener = Validate.notNull(sinkListener, "sinkListener can not be null.");
    this.channelNum = downStreamChannelList.size();
    this.shuffleStrategy = getShuffleStrategy(shuffleStrategyEnum);
    this.hasSetNoMoreTsBlocks = new boolean[channelNum];
    this.channelOpened = new boolean[channelNum];
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
    int currentIndex = downStreamChannelIndex.getCurrentIndex();
    // try open channel
    tryOpenChannel(currentIndex);
    // It is safe to use currentChannel.isFull() to judge whether we can send a TsBlock only when
    // downStreamChannelIndex will not be changed between we call isFull() and send() of
    // ShuffleSinkHandle
    return downStreamChannelList.get(currentIndex).isFull();
  }

  @Override
  public synchronized void send(TsBlock tsBlock) {
    long startTime = System.nanoTime();
    try {
      checkState();
      if (closed) {
        return;
      }
      ISinkChannel currentChannel =
          downStreamChannelList.get(downStreamChannelIndex.getCurrentIndex());
      currentChannel.send(tsBlock);
    } finally {
      switchChannelIfNecessary();
      DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
          SINK_HANDLE_SEND_TSBLOCK_REMOTE, System.nanoTime() - startTime);
    }
  }

  @Override
  public void setNoMoreTsBlocks() {
    if (closed || aborted) {
      return;
    }
    try {
      lock.lock();
      for (int i = 0; i < downStreamChannelList.size(); i++) {
        if (!hasSetNoMoreTsBlocks[i]) {
          downStreamChannelList.get(i).setNoMoreTsBlocks();
          hasSetNoMoreTsBlocks[i] = true;
        }
      }
      sinkListener.onEndOfBlocks(this);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void setNoMoreTsBlocksOfOneChannel(int channelIndex) {
    if (closed || aborted) {
      // if this ShuffleSinkHandle has been closed, Driver.close() will attempt to setNoMoreTsBlocks
      // for all the channels
      return;
    }
    try {
      lock.lock();
      if (!hasSetNoMoreTsBlocks[channelIndex]) {
        downStreamChannelList.get(channelIndex).setNoMoreTsBlocks();
        hasSetNoMoreTsBlocks[channelIndex] = true;
      }
    } finally {
      lock.unlock();
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
    for (ISink channel : downStreamChannelList) {
      if (!channel.isFinished()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean abort() {
    if (aborted || closed) {
      return false;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[StartAbortShuffleSinkHandle]");
    }
    boolean meetError = false;
    Exception firstException = null;
    boolean selfAborted = true;
    for (ISink channel : downStreamChannelList) {
      try {
        selfAborted = channel.abort();
      } catch (Exception e) {
        if (!meetError) {
          firstException = e;
          meetError = true;
        }
      }
    }
    if (meetError) {
      LOGGER.warn("Error occurred when try to abort channel.", firstException);
    }
    if (selfAborted) {
      sinkListener.onAborted(this);
      aborted = true;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[EndAbortShuffleSinkHandle]");
      }
      return true;
    } else {
      return false;
    }
  }

  // Add synchronized on this method may lead to Dead Lock
  // It is possible that when LocalSinkChannel revokes this close method and try to get Lock
  // ShuffleSinkHandle while synchronized methods of ShuffleSinkHandle
  // Lock ShuffleSinkHandle and wait to lock LocalSinkChannel
  @Override
  public boolean close() {
    if (closed || aborted) {
      return false;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[StartCloseShuffleSinkHandle]");
    }
    boolean meetError = false;
    Exception firstException = null;
    boolean selfClosed = true;
    for (ISink channel : downStreamChannelList) {
      try {
        selfClosed = channel.close();
      } catch (Exception e) {
        if (!meetError) {
          firstException = e;
          meetError = true;
        }
      }
    }
    if (meetError) {
      LOGGER.warn("Error occurred when try to close channel.", firstException);
    }
    if (selfClosed) {
      sinkListener.onFinish(this);
      closed = true;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[EndCloseShuffleSinkHandle]");
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    this.maxBytesCanReserve = maxBytesCanReserve;
    downStreamChannelList.forEach(
        sinkHandle -> sinkHandle.setMaxBytesCanReserve(maxBytesCanReserve));
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + downStreamChannelList.stream().map(ISink::ramBytesUsed).reduce(Long::sum).orElse(0L)
        + RamUsageEstimator.sizeOf(channelOpened)
        + RamUsageEstimator.sizeOf(hasSetNoMoreTsBlocks);
  }

  private void checkState() {
    if (aborted) {
      for (ISinkChannel channel : downStreamChannelList) {
        channel.checkState();
      }
      throw new IllegalStateException("ShuffleSinkHandle is aborted.");
    }
  }

  private void switchChannelIfNecessary() {
    shuffleStrategy.shuffle();
  }

  public void tryOpenChannel(int channelIndex) {
    if (!channelOpened[channelIndex]) {
      downStreamChannelList.get(channelIndex).open();
      channelOpened[channelIndex] = true;
    }
  }

  @Override
  public boolean isChannelClosed(int index) {
    return downStreamChannelList.get(index).isClosed();
  }

  // region ============ Shuffle Related ============
  public enum ShuffleStrategyEnum {
    PLAIN,
    SIMPLE_ROUND_ROBIN,
  }

  @FunctionalInterface
  interface ShuffleStrategy {
    /**
     * SinkHandle may have multiple channels. we need to choose the next channel each time we send a
     * TsBlock.
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
      if (channel.isNoMoreTsBlocks() || channel.isClosed()) {
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
  @Override
  public long getBufferRetainedSizeInBytes() {
    return downStreamChannelList.stream()
        .map(ISink::getBufferRetainedSizeInBytes)
        .reduce(Long::sum)
        .orElse(0L);
  }
}
