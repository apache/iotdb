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

import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkHandleListener;
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

public class LocalSinkHandle implements ISinkHandle, ISinkChannel {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalSinkHandle.class);

  private TFragmentInstanceId localFragmentInstanceId;
  private final SinkHandleListener sinkHandleListener;

  private final SharedTsBlockQueue queue;
  private volatile ListenableFuture<Void> blocked;
  private boolean aborted = false;
  private boolean closed = false;

  private boolean noMoreTsBlocks = false;

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public LocalSinkHandle(SharedTsBlockQueue queue, SinkHandleListener sinkHandleListener) {
    this.sinkHandleListener = Validate.notNull(sinkHandleListener);
    this.queue = Validate.notNull(queue);
    this.queue.setSinkHandle(this);
    blocked = queue.getCanAddTsBlock();
  }

  public LocalSinkHandle(
      TFragmentInstanceId localFragmentInstanceId,
      SharedTsBlockQueue queue,
      SinkHandleListener sinkHandleListener) {
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.sinkHandleListener = Validate.notNull(sinkHandleListener);
    this.queue = Validate.notNull(queue);
    this.queue.setSinkHandle(this);
    // SinkHandle can send data after SourceHandle asks it to
    blocked = queue.getCanAddTsBlock();
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return queue.getBufferRetainedSizeInBytes();
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
          sinkHandleListener.onFinish(this);
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
        noMoreTsBlocks = true;
        if (aborted || closed) {
          return;
        }
        queue.setNoMoreTsBlocks(true);
        sinkHandleListener.onEndOfBlocks(this);
      }
    }
    checkAndInvokeOnFinished();
    LOGGER.debug("[EndSetNoMoreTsBlocksOnLocal]");
  }

  @Override
  public void abort() {
    LOGGER.debug("[StartAbortLocalSinkHandle]");
    synchronized (queue) {
      synchronized (this) {
        if (aborted || closed) {
          return;
        }
        aborted = true;
        Optional<Throwable> t = sinkHandleListener.onAborted(this);
        if (t.isPresent()) {
          queue.abort(t.get());
        } else {
          queue.abort();
        }
      }
    }
    LOGGER.debug("[EndAbortLocalSinkHandle]");
  }

  @Override
  public void close() {
    LOGGER.debug("[StartCloseLocalSinkHandle]");
    synchronized (queue) {
      synchronized (this) {
        if (aborted || closed) {
          return;
        }
        closed = true;
        queue.close();
        sinkHandleListener.onFinish(this);
      }
    }
    LOGGER.debug("[EndCloseLocalSinkHandle]");
  }

  public SharedTsBlockQueue getSharedTsBlockQueue() {
    return queue;
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("Sink handle is aborted.");
    } else if (closed) {
      throw new IllegalStateException("Sink Handle is closed.");
    }
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    // do nothing, the maxBytesCanReserve of SharedTsBlockQueue should be set by corresponding
    // LocalSourceHandle
  }

  // region ============ ISinkChannel related ============

  @Override
  public void open() {}

  @Override
  public boolean isNoMoreTsBlocks() {
    return noMoreTsBlocks;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return queue.getBufferRetainedSizeInBytes();
  }

  @Override
  public int getNumOfBufferedTsBlocks() {
    return queue.getNumOfBufferedTsBlocks();
  }

  // end region
}
