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

import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public class StubSinkHandle implements ISinkHandle {

  private final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

  private final List<TsBlock> tsBlocks = new ArrayList<>();

  @Override
  public long getBufferRetainedSizeInBytes() {
    return 0;
  }

  @Override
  public int getNumOfBufferedTsBlocks() {
    return 0;
  }

  @Override
  public ListenableFuture<Void> isFull() {
    return NOT_BLOCKED;
  }

  @Override
  public void send(List<TsBlock> tsBlocks) throws IOException {}

  @Override
  public void send(int partition, List<TsBlock> tsBlocks) throws IOException {}

  @Override
  public void setNoMoreTsBlocks() {}

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  @Override
  public void close() {
    tsBlocks.clear();
  }

  @Override
  public void abort() {
    tsBlocks.clear();
  }

  public List<TsBlock> getTsBlocks() {
    return tsBlocks;
  }
}
