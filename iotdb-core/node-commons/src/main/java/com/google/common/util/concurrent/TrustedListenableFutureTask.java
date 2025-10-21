/*
 * Copyright (C) 2014 The Guava Authors
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

import javax.annotation.CheckForNull;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link RunnableFuture} that also implements the {@link ListenableFuture} interface.
 *
 * <p>This should be used in preference to {@link ListenableFutureTask} when possible for
 * performance reasons.
 */
class TrustedListenableFutureTask<V extends Object> extends FluentFuture.TrustedFuture<V>
    implements RunnableFuture<V> {

  static <V extends Object> TrustedListenableFutureTask<V> create(AsyncCallable<V> callable) {
    return new TrustedListenableFutureTask<>(callable);
  }

  static <V extends Object> TrustedListenableFutureTask<V> create(Callable<V> callable) {
    return new TrustedListenableFutureTask<>(callable);
  }

  /**
   * Creates a {@code ListenableFutureTask} that will upon running, execute the given {@code
   * Runnable}, and arrange that {@code get} will return the given result on successful completion.
   *
   * @param runnable the runnable task
   * @param result the result to return on successful completion. If you don't need a particular
   *     result, consider using constructions of the form: {@code ListenableFuture<?> f =
   *     ListenableFutureTask.create(runnable, null)}
   */
  static <V extends Object> TrustedListenableFutureTask<V> create(Runnable runnable, V result) {
    return new TrustedListenableFutureTask<>(Executors.callable(runnable, result));
  }

  /*
   * In certain circumstances, this field might theoretically not be visible to an afterDone() call
   * triggered by cancel(). For details, see the comments on the fields of TimeoutFuture.
   *
   * <p>{@code volatile} is required for j2objc transpiling:
   * https://developers.google.com/j2objc/guides/j2objc-memory-model#atomicity
   */
  @CheckForNull private volatile InterruptibleTask<?> task;

  TrustedListenableFutureTask(Callable<V> callable) {
    this.task = new TrustedFutureInterruptibleTask(callable);
  }

  TrustedListenableFutureTask(AsyncCallable<V> callable) {
    this.task = new TrustedFutureInterruptibleAsyncTask(callable);
  }

  @Override
  public void run() {
    InterruptibleTask<?> localTask = task;
    if (localTask != null) {
      localTask.run();
    }
    /*
     * In the Async case, we may have called setFuture(pendingFuture), in which case afterDone()
     * won't have been called yet.
     */
    this.task = null;
  }

  @Override
  protected void afterDone() {
    super.afterDone();

    if (wasInterrupted()) {
      InterruptibleTask<?> localTask = task;
      if (localTask != null) {
        localTask.interruptTask();
      }
    }

    this.task = null;
  }

  @Override
  @CheckForNull
  protected String pendingToString() {
    InterruptibleTask<?> localTask = task;
    if (localTask != null) {
      return "task=[" + localTask + "]";
    }
    return super.pendingToString();
  }

  private final class TrustedFutureInterruptibleTask extends InterruptibleTask<V> {
    private final Callable<V> callable;

    TrustedFutureInterruptibleTask(Callable<V> callable) {
      this.callable = checkNotNull(callable);
    }

    @Override
    final boolean isDone() {
      return TrustedListenableFutureTask.this.isDone();
    }

    @Override
    V runInterruptibly() throws Exception {
      return callable.call();
    }

    @Override
    void afterRanInterruptiblySuccess(V result) {
      TrustedListenableFutureTask.this.set(result);
    }

    @Override
    void afterRanInterruptiblyFailure(Throwable error) {
      setException(error);
    }

    @Override
    String toPendingString() {
      return callable.toString();
    }
  }

  private final class TrustedFutureInterruptibleAsyncTask
      extends InterruptibleTask<ListenableFuture<V>> {
    private final AsyncCallable<V> callable;

    TrustedFutureInterruptibleAsyncTask(AsyncCallable<V> callable) {
      this.callable = checkNotNull(callable);
    }

    @Override
    final boolean isDone() {
      return TrustedListenableFutureTask.this.isDone();
    }

    @Override
    ListenableFuture<V> runInterruptibly() throws Exception {
      return checkNotNull(
          callable.call(),
          "AsyncCallable.call returned null instead of a Future. "
              + "Did you mean to return immediateFuture(null)? %s",
          callable);
    }

    @Override
    void afterRanInterruptiblySuccess(ListenableFuture<V> result) {
      setFuture(result);
    }

    @Override
    void afterRanInterruptiblyFailure(Throwable error) {
      setException(error);
    }

    @Override
    String toPendingString() {
      return callable.toString();
    }
  }
}
