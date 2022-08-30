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

import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkHandleListener;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

public class LocalSinkHandle implements ISinkHandle {

  private static final Logger logger = LoggerFactory.getLogger(LocalSinkHandle.class);

  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final String remotePlanNodeId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final SinkHandleListener sinkHandleListener;

  private final SharedTsBlockQueue queue;
  private volatile ListenableFuture<Void> blocked = immediateFuture(null);
  private boolean aborted = false;
  private boolean closed = false;

  public LocalSinkHandle(
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId,
      TFragmentInstanceId localFragmentInstanceId,
      SharedTsBlockQueue queue,
      SinkHandleListener sinkHandleListener) {
    this.remoteFragmentInstanceId = Validate.notNull(remoteFragmentInstanceId);
    this.remotePlanNodeId = Validate.notNull(remotePlanNodeId);
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.sinkHandleListener = Validate.notNull(sinkHandleListener);
    this.queue = Validate.notNull(queue);
    this.queue.setSinkHandle(this);
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
      logger.debug("send TsBlocks");
      synchronized (this) {
        blocked = queue.add(tsBlock);
      }
    }
  }

  @Override
  public synchronized void send(int partition, List<TsBlock> tsBlocks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNoMoreTsBlocks() {
    synchronized (queue) {
      synchronized (this) {
        logger.debug("set noMoreTsBlocks.");
        if (aborted || closed) {
          logger.debug("SinkHandle has been aborted={} or closed={}.", aborted, closed);
          return;
        }
        queue.setNoMoreTsBlocks(true);
        sinkHandleListener.onEndOfBlocks(this);
      }
    }
    checkAndInvokeOnFinished();
    logger.debug("noMoreTsBlocks has been set.");
  }

  @Override
  public void abort() {
    logger.debug("Sink handle is being aborted.");
    synchronized (queue) {
      synchronized (this) {
        if (aborted || closed) {
          return;
        }
        aborted = true;
        queue.abort();
        sinkHandleListener.onAborted(this);
      }
    }
    logger.debug("Sink handle is aborted");
  }

  @Override
  public void close() {
    logger.debug("Sink handle is being closed.");
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
    logger.debug("Sink handle is closed");
  }

  public TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId;
  }

  public String getRemotePlanNodeId() {
    return remotePlanNodeId;
  }

  SharedTsBlockQueue getSharedTsBlockQueue() {
    return queue;
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("Sink handle is aborted.");
    } else if (closed) {
      throw new IllegalStateException("Sink Handle is closed.");
    }
  }
}
