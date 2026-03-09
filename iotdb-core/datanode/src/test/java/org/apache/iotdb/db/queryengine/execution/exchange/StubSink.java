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
package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public class StubSink implements ISink {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(StubSink.class);

  private final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

  private final List<TsBlock> tsBlocks = new ArrayList<>();

  private final FragmentInstanceContext instanceContext;

  private boolean closed = false;

  public StubSink(FragmentInstanceContext instanceContext) {
    this.instanceContext = instanceContext;
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return null;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return 0;
  }

  @Override
  public ListenableFuture<?> isFull() {
    return NOT_BLOCKED;
  }

  @Override
  public void send(TsBlock tsBlock) {
    this.tsBlocks.add(tsBlock);
  }

  @Override
  public void setNoMoreTsBlocks() {
    if (closed) {
      return;
    }
    closed = true;
    instanceContext.transitionToFlushing();
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public boolean isAborted() {
    return closed;
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  @Override
  public boolean abort() {
    closed = true;
    tsBlocks.clear();
    return true;
  }

  @Override
  public boolean close() {
    closed = true;
    tsBlocks.clear();
    return true;
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {}

  public List<TsBlock> getTsBlocks() {
    return tsBlocks;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }
}
