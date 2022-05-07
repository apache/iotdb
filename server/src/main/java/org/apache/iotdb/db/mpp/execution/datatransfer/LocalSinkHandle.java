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

package org.apache.iotdb.db.mpp.execution.datatransfer;

import org.apache.iotdb.db.mpp.execution.datatransfer.DataBlockManager.SinkHandleListener;
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
  public synchronized ListenableFuture<Void> isFull() {
    if (aborted) {
      throw new IllegalStateException("Sink handle is closed.");
    }
    return nonCancellationPropagating(blocked);
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public boolean isFinished() {
    return queue.hasNoMoreTsBlocks() && queue.isEmpty();
  }

  @Override
  public synchronized void send(List<TsBlock> tsBlocks) {
    Validate.notNull(tsBlocks, "tsBlocks is null");
    if (aborted) {
      throw new IllegalStateException("Sink handle is aborted.");
    }
    if (!blocked.isDone()) {
      throw new IllegalStateException("Sink handle is blocked.");
    }
    if (queue.hasNoMoreTsBlocks()) {
      return;
    }
    for (TsBlock tsBlock : tsBlocks) {
      blocked = queue.add(tsBlock);
    }
  }

  @Override
  public synchronized void send(int partition, List<TsBlock> tsBlocks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void setNoMoreTsBlocks() {
    logger.info("Set no-more-tsblocks to {}.", this);
    if (aborted) {
      return;
    }
    queue.setNoMoreTsBlocks(true);
    sinkHandleListener.onEndOfBlocks(this);
    if (isFinished()) {
      sinkHandleListener.onFinish(this);
    }
    logger.info("No-more-tsblocks has been set to {}.", this);
  }

  @Override
  public synchronized void abort() {
    logger.info("Sink handle {} is being aborted.", this);
    aborted = true;
    queue.destroy();
    sinkHandleListener.onAborted(this);
    logger.info("Sink handle {} is aborted", this);
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
}
