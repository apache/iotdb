/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.util.concurrent;

import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;

/**
 * Abstract {@link ListeningExecutorService} implementation that creates {@link ListenableFuture}
 * instances for each {@link Runnable} and {@link Callable} submitted to it. These tasks are run
 * with the abstract {@link #execute execute(Runnable)} method.
 *
 * <p>In addition to {@link #execute}, subclasses must implement all methods related to shutdown and
 * termination.
 *
 * @author Chris Povirk
 * @since 14.0
 */
public abstract class AbstractListeningExecutorService extends AbstractExecutorService
    implements ListeningExecutorService {

  /**
   * @since 19.0 (present with return type {@code ListenableFutureTask} since 14.0)
   */
  @Override
  protected final <T extends Object> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return TrustedListenableFutureTask.create(runnable, value);
  }

  /**
   * @since 19.0 (present with return type {@code ListenableFutureTask} since 14.0)
   */
  @Override
  protected final <T extends Object> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    return TrustedListenableFutureTask.create(callable);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return (ListenableFuture<?>) super.submit(task);
  }

  @Override
  public <T extends Object> ListenableFuture<T> submit(Runnable task, T result) {
    return (ListenableFuture<T>) super.submit(task, result);
  }

  @Override
  public <T extends Object> ListenableFuture<T> submit(Callable<T> task) {
    return (ListenableFuture<T>) super.submit(task);
  }
}
