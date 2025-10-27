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

import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SINK_HANDLE_SEND_TSBLOCK_LOCAL;

public class LocalSinkChannel implements ISinkChannel {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalSinkChannel.class);

  private TFragmentInstanceId localFragmentInstanceId;
  private final SinkListener sinkListener;

  private final SharedTsBlockQueue queue;

  @SuppressWarnings("squid:S3077")
  private volatile ListenableFuture<Void> blocked;

  private volatile boolean aborted = false;
  private volatile boolean closed = false;

  private boolean invokedOnFinished = false;

  private static final DataExchangeCostMetricSet DATA_EXCHANGE_COST_METRIC_SET =
      DataExchangeCostMetricSet.getInstance();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LocalSinkChannel.class)
          + +RamUsageEstimator.shallowSizeOfInstance(TFragmentInstanceId.class)
          + RamUsageEstimator.shallowSizeOfInstance(SharedTsBlockQueue.class);

  public LocalSinkChannel(SharedTsBlockQueue queue, SinkListener sinkListener) {
    this.sinkListener = Validate.notNull(sinkListener, "sinkListener can not be null.");
    this.queue = Validate.notNull(queue, "queue can not be null.");
    this.queue.setSinkChannel(this);
    blocked = queue.getCanAddTsBlock();
  }

  public LocalSinkChannel(
      TFragmentInstanceId localFragmentInstanceId,
      SharedTsBlockQueue queue,
      SinkListener sinkListener) {
    this.localFragmentInstanceId =
        Validate.notNull(localFragmentInstanceId, "localFragmentInstanceId can not be null.");
    this.sinkListener = Validate.notNull(sinkListener, "sinkListener can not be null.");
    this.queue = Validate.notNull(queue, "queue can not be null.");
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
    if (closed) {
      return immediateVoidFuture();
    }
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
          if (!invokedOnFinished) {
            sinkListener.onFinish(this);
            invokedOnFinished = true;
          }
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
        if (closed) {
          return;
        }
        if (!blocked.isDone()) {
          throw new IllegalStateException("Sink handle is blocked.");
        }
      }

      synchronized (queue) {
        if (queue.hasNoMoreTsBlocks()) {
          return;
        }
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[StartSendTsBlockOnLocal]");
        }
        synchronized (this) {
          blocked = queue.add(tsBlock);
        }
      }
    } finally {
      DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
          SINK_HANDLE_SEND_TSBLOCK_LOCAL, System.nanoTime() - startTime);
    }
  }

  @Override
  public void setNoMoreTsBlocks() {
    synchronized (queue) {
      synchronized (this) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[StartSetNoMoreTsBlocksOnLocal]");
        }
        if (aborted || closed) {
          return;
        }
        queue.setNoMoreTsBlocks(true);
        sinkListener.onEndOfBlocks(this);
      }
    }
    checkAndInvokeOnFinished();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[EndSetNoMoreTsBlocksOnLocal]");
    }
  }

  @Override
  public boolean abort() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[StartAbortLocalSinkChannel]");
    }
    synchronized (queue) {
      synchronized (this) {
        if (aborted || closed) {
          return false;
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[EndAbortLocalSinkChannel]");
    }
    return true;
  }

  @Override
  public boolean close() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[StartCloseLocalSinkChannel]");
    }
    synchronized (queue) {
      synchronized (this) {
        if (aborted || closed) {
          return false;
        }
        closed = true;
        queue.close();
        if (!invokedOnFinished) {
          sinkListener.onFinish(this);
          invokedOnFinished = true;
        }
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[EndCloseLocalSinkChannel]");
    }
    return true;
  }

  public SharedTsBlockQueue getSharedTsBlockQueue() {
    return queue;
  }

  @Override
  public void checkState() {
    if (aborted) {
      Optional<Throwable> abortedCause = queue.getAbortedCause();
      if (abortedCause.isPresent()) {
        throw new IllegalStateException(abortedCause.get());
      }
      if (queue.isBlocked().isDone()) {
        // try throw underlying exception
        try {
          queue.isBlocked().get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        } catch (ExecutionException e) {
          throw new IllegalStateException(e.getCause() == null ? e : e.getCause());
        }
      }
      throw new IllegalStateException("LocalSinkChannel is ABORTED.");
    }
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    if (maxBytesCanReserve < queue.getMaxBytesCanReserve()) {
      queue.setMaxBytesCanReserve(maxBytesCanReserve);
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }

  // region ============ ISinkChannel related ============

  @Override
  public void open() {
    // do nothing
  }

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

  @Override
  public boolean isClosed() {
    synchronized (queue) {
      return queue.isClosed();
    }
  }

  // end region
}
