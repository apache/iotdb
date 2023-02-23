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

import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.mpp.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.mpp.metric.DataExchangeCostMetricSet.SINK_HANDLE_SEND_TSBLOCK_LOCAL;

public class LocalSinkChannel implements ISinkChannel {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalSinkChannel.class);

  private TFragmentInstanceId localFragmentInstanceId;
  private final SinkListener sinkListener;

  private final SharedTsBlockQueue queue;
  private volatile ListenableFuture<Void> blocked;
  private boolean aborted = false;
  private boolean closed = false;

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public LocalSinkChannel(SharedTsBlockQueue queue, SinkListener sinkListener) {
    this.sinkListener = Validate.notNull(sinkListener);
    this.queue = Validate.notNull(queue);
    this.queue.setSinkChannel(this);
    blocked = queue.getCanAddTsBlock();
  }

  public LocalSinkChannel(
      TFragmentInstanceId localFragmentInstanceId,
      SharedTsBlockQueue queue,
      SinkListener sinkListener) {
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.sinkListener = Validate.notNull(sinkListener);
    this.queue = Validate.notNull(queue);
    this.queue.setSinkChannel(this);
    // SinkChannel can send data after SourceHandle asks it to
    blocked = queue.getCanAddTsBlock();
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    synchronized (queue) {
      return queue.getBufferRetainedSizeInBytes();
    }
  }

  @Override
  public synchronized ListenableFuture<?> isFull() {
    checkState();
    return nonCancellationPropagating(blocked);
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public boolean isFinished() {
    synchronized (queue) {
      return queue.hasNoMoreTsBlocks() && queue.isEmpty();
    }
  }

  public void checkAndInvokeOnFinished() {
    synchronized (queue) {
      if (isFinished()) {
        synchronized (this) {
          sinkListener.onFinish(this);
        }
      }
    }
  }

  @Override
  public void send(TsBlock tsBlock) {
    long startTime = System.nanoTime();
    try {
      Validate.notNull(tsBlock, "tsBlocks is null");
      synchronized (this) {
        checkState();
        if (!blocked.isDone()) {
          throw new IllegalStateException("Sink handle is blocked.");
        }
      }

      synchronized (queue) {
        if (queue.hasNoMoreTsBlocks()) {
          return;
        }
        LOGGER.debug("[StartSendTsBlockOnLocal]");
        synchronized (this) {
          blocked = queue.add(tsBlock);
        }
      }
    } finally {
      QUERY_METRICS.recordDataExchangeCost(
          SINK_HANDLE_SEND_TSBLOCK_LOCAL, System.nanoTime() - startTime);
    }
  }

  @Override
  public void setNoMoreTsBlocks() {
    synchronized (queue) {
      synchronized (this) {
        LOGGER.debug("[StartSetNoMoreTsBlocksOnLocal]");
        if (aborted || closed) {
          return;
        }
        queue.setNoMoreTsBlocks(true);
        sinkListener.onEndOfBlocks(this);
      }
    }
    checkAndInvokeOnFinished();
    LOGGER.debug("[EndSetNoMoreTsBlocksOnLocal]");
  }

  @Override
  public void abort() {
    LOGGER.debug("[StartAbortLocalSinkChannel]");
    synchronized (queue) {
      synchronized (this) {
        if (aborted || closed) {
          return;
        }
        aborted = true;
        Optional<Throwable> t = sinkListener.onAborted(this);
        if (t.isPresent()) {
          queue.abort(t.get());
        } else {
          queue.abort();
        }
      }
    }
    LOGGER.debug("[EndAbortLocalSinkChannel]");
  }

  @Override
  public void close() {
    LOGGER.debug("[StartCloseLocalSinkChannel]");
    synchronized (queue) {
      synchronized (this) {
        if (aborted || closed) {
          return;
        }
        closed = true;
        queue.close();
        sinkListener.onFinish(this);
      }
    }
    LOGGER.debug("[EndCloseLocalSinkChannel]");
  }

  public SharedTsBlockQueue getSharedTsBlockQueue() {
    return queue;
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("LocalSinkChannel is aborted.");
    } else if (closed) {
      throw new IllegalStateException("LocalSinkChannel is closed.");
    }
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    if (maxBytesCanReserve < queue.getMaxBytesCanReserve()) {
      queue.setMaxBytesCanReserve(maxBytesCanReserve);
    }
  }

  // region ============ ISinkChannel related ============

  @Override
  public void open() {}

  @Override
  public boolean isNoMoreTsBlocks() {
    synchronized (queue) {
      return queue.hasNoMoreTsBlocks();
    }
  }

  @Override
  public int getNumOfBufferedTsBlocks() {
    synchronized (queue) {
      return queue.getNumOfBufferedTsBlocks();
    }
  }

  // end region
}
