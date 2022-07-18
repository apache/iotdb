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

package org.apache.iotdb.db.mpp.plan.execution.memory;

import org.apache.iotdb.db.mpp.execution.exchange.ISourceHandle;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class MemorySourceHandle implements ISourceHandle {

  private final TsBlock result;
  private boolean hasNext;

  public MemorySourceHandle(TsBlock result) {
    Validate.notNull(result, "the TsBlock should not be null when constructing MemorySourceHandle");
    this.result = result;
    this.hasNext = true;
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getLocalPlanNodeId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized TsBlock receive() {
    hasNext = false;
    return result;
  }

  @Override
  public synchronized boolean isFinished() {
    return !hasNext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return immediateFuture(null);
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public void abort() {}
}
