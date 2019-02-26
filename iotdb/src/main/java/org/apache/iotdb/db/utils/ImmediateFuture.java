/**
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

package org.apache.iotdb.db.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ImmediateFuture<T> implements Future<T> {

  T result;
  public ImmediateFuture(T result){
    this.result = result;
  }
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return true;
  }

  @Override
  public boolean isCancelled() {
    return true;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    return result;
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return result;
  }
}
