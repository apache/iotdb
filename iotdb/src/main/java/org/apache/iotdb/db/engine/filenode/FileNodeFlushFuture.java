/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.filenode;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iotdb.db.utils.ImmediateFuture;

public class FileNodeFlushFuture implements Future<Boolean> {
  Future<Boolean> bufferWriteFlushFuture;
  Future<Boolean> overflowFlushFuture;
  boolean hasOverflowFlushTask;

  public FileNodeFlushFuture(Future<Boolean> bufferWriteFlushFuture, Future<Boolean> overflowFlushFuture){
    if(bufferWriteFlushFuture != null) {
      this.bufferWriteFlushFuture = bufferWriteFlushFuture;
    } else {
      this.bufferWriteFlushFuture = new ImmediateFuture<>(true);
    }
    if(overflowFlushFuture !=null) {
      this.overflowFlushFuture = overflowFlushFuture;
      hasOverflowFlushTask = true;
    } else {
      this.overflowFlushFuture = new ImmediateFuture<>(true);
      hasOverflowFlushTask = false;
    }
  }

  /**
   * @param mayInterruptIfRunning true if the thread executing this task should be interrupted;
   * otherwise, in-progress tasks are allowed to complete
   * @return true if both of the two sub-future are canceled successfully.
   * @see Future#cancel(boolean) The difference is that this Future consists of two sub-Futures. If
   * the first sub-future is canceled successfully but the second sub-future fails, the result is
   * false.
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    boolean result = bufferWriteFlushFuture.cancel(mayInterruptIfRunning);
    result = result & overflowFlushFuture.cancel(mayInterruptIfRunning);
    return result;
  }

  @Override
  public boolean isCancelled() {
    return bufferWriteFlushFuture.isCancelled() && overflowFlushFuture.isCancelled();
  }

  @Override
  public boolean isDone() {
    return bufferWriteFlushFuture.isDone() && overflowFlushFuture.isDone();
  }

  @Override
  public Boolean get() throws InterruptedException, ExecutionException {
    boolean result = bufferWriteFlushFuture.get();
    result = result & overflowFlushFuture.get();
    return result;
  }

  @Override
  public Boolean get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    boolean result = bufferWriteFlushFuture.get(timeout, unit);
    result = result && overflowFlushFuture.get(timeout, unit);
    return result;
  }

  public boolean isHasOverflowFlushTask() {
    return hasOverflowFlushTask;
  }
}
