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

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.mpp.buffer.DataBlockManager.SourceHandleListener;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

public class LocalSourceHandle implements ISourceHandle {

  private static final Logger logger = LoggerFactory.getLogger(LocalSourceHandle.class);

  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final String localPlanNodeId;
  private final SourceHandleListener sourceHandleListener;
  private final SharedTsBlockQueue queue;
  private boolean aborted = false;

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
    this.sourceHandleListener = Validate.notNull(sourceHandleListener);
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
    if (aborted) {
      throw new IllegalStateException("Source handle is aborted.");
    }
    if (!queue.isBlocked().isDone()) {
      throw new IllegalStateException("Source handle is blocked.");
    }
    TsBlock tsBlock;
    synchronized (this) {
      tsBlock = queue.remove();
    }
    if (isFinished()) {
      sourceHandleListener.onFinished(this);
    }
    return tsBlock;
  }

  @Override
  public boolean isFinished() {
    return queue.hasNoMoreTsBlocks() && queue.isEmpty();
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    if (aborted) {
      throw new IllegalStateException("Source handle is closed.");
    }
    return nonCancellationPropagating(queue.isBlocked());
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public synchronized void abort() {
    if (aborted) {
      return;
    }
    queue.destroy();
    aborted = true;
    sourceHandleListener.onAborted(this);
  }

  public TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId;
  }

  SharedTsBlockQueue getSharedTsBlockQueue() {
    return queue;
  }
}
