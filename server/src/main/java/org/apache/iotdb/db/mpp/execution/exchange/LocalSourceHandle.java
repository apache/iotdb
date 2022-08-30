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

import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.createFullIdFrom;

public class LocalSourceHandle implements ISourceHandle {

  private static final Logger logger = LoggerFactory.getLogger(LocalSourceHandle.class);

  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final String localPlanNodeId;
  private final SourceHandleListener sourceHandleListener;
  private final SharedTsBlockQueue queue;
  private boolean aborted = false;

  private boolean closed = false;

  private int currSequenceId;

  private final String threadName;

  public LocalSourceHandle(
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      SharedTsBlockQueue queue,
      SourceHandleListener sourceHandleListener) {
    this.remoteFragmentInstanceId = Validate.notNull(remoteFragmentInstanceId);
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.localPlanNodeId = Validate.notNull(localPlanNodeId);
    this.queue = Validate.notNull(queue);
    this.queue.setSourceHandle(this);
    this.sourceHandleListener = Validate.notNull(sourceHandleListener);
    this.threadName =
        createFullIdFrom(localFragmentInstanceId, localPlanNodeId + "." + "SourceHandle");
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  @Override
  public String getLocalPlanNodeId() {
    return localPlanNodeId;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return queue.getBufferRetainedSizeInBytes();
  }

  @Override
  public TsBlock receive() {
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      checkState();

      if (!queue.isBlocked().isDone()) {
        throw new IllegalStateException("Source handle is blocked.");
      }
      TsBlock tsBlock;
      synchronized (queue) {
        tsBlock = queue.remove();
      }
      if (tsBlock != null) {
        currSequenceId++;
        logger.debug(
            "Receive {} TsdBlock, size is {}", currSequenceId, tsBlock.getRetainedSizeInBytes());
      }
      checkAndInvokeOnFinished();
      return tsBlock;
    }
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
        // Putting synchronized here rather than marking in method is to avoid deadlock.
        // There are two locks need to invoke this method. One is lock of SharedTsBlockQueue,
        // the other is lock of LocalSourceHandle.
        synchronized (this) {
          sourceHandleListener.onFinished(this);
        }
      }
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    checkState();
    return nonCancellationPropagating(queue.isBlocked());
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public void abort() {
    if (aborted || closed) {
      return;
    }
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      logger.debug("Source handle is being aborted.");
      synchronized (queue) {
        synchronized (this) {
          if (aborted || closed) {
            return;
          }
          queue.abort();
          aborted = true;
          sourceHandleListener.onAborted(this);
        }
      }
      logger.debug("Source handle is aborted");
    }
  }

  @Override
  public void close() {
    if (aborted || closed) {
      return;
    }
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      logger.debug("Source handle is being closed.");
      synchronized (queue) {
        synchronized (this) {
          if (aborted || closed) {
            return;
          }
          queue.close();
          closed = true;
          sourceHandleListener.onFinished(this);
        }
      }
      logger.debug("Source handle is closed");
    }
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("Source handle is aborted.");
    } else if (closed) {
      throw new IllegalStateException("Source Handle is closed.");
    }
  }

  public TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId;
  }

  SharedTsBlockQueue getSharedTsBlockQueue() {
    return queue;
  }
}
