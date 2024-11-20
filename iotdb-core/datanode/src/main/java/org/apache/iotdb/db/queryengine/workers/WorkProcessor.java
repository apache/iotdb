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

package org.apache.iotdb.db.queryengine.workers;

import org.apache.iotdb.db.queryengine.workers.state.ProcessState;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

// WorkProcessor is essentially a stream with state.
public class WorkProcessor<T> {
  Processor<T> process;
  ProcessState<T> state;

  public WorkProcessor(Processor<T> processor) {
    this(processor, ProcessState.yielded());
  }

  public WorkProcessor(Processor<T> process, ProcessState<T> initialState) {
    this.process = process;
    this.state = initialState;
  }

  static <T> WorkProcessor<T> create(Processor<T> process) {
    return WorkProcessorUtils.create(process);
  }

  static <T> WorkProcessor<T> fromIterable(Iterable<T> iterable) {
    return WorkProcessorUtils.fromIterator(iterable.iterator());
  }

  /**
   * Does some work and return whether it exits normally or not
   *
   * @return if the worker yields or blocking, return true. Otherwise, it returns false
   */
  public boolean process() {
    if (isBlocked()) {
      return false;
    }
    if (isFinished()) {
      return true;
    }
    state = process.process();

    if (state.getType() == ProcessState.Type.FINISHED) {
      process = null;
      return true;
    }

    return state.getType() == ProcessState.Type.RESULT;
  }

  public boolean isBlocked() {
    return state.getType() == ProcessState.Type.BLOCKED && !state.getBlocked().isDone();
  }

  public ListenableFuture<Void> getBlockedFuture() {
    if (state.getType() != ProcessState.Type.BLOCKED) {
      throw new IllegalStateException("Must be blocked to get blocked future");
    }

    return state.getBlocked();
  }

  public boolean isFinished() {
    return state.getType() == ProcessState.Type.FINISHED;
  }

  public T getResult() {
    if (state.getType() != ProcessState.Type.RESULT) {
      throw new IllegalStateException("process() must return true and must not be finished");
    }

    return state.getResult();
  }

  /** Return a WorkProcessor that is blocking forever until futureSupplier is done. */
  public WorkProcessor<T> blocking(Supplier<ListenableFuture<Void>> futureSupplier) {
    return WorkProcessorUtils.blocking(this, futureSupplier);
  }

  /** Return a WorkProcessor that yields at each process until futureSupplier is done. */
  WorkProcessor<T> yielding(BooleanSupplier yieldSignal) {
    return WorkProcessorUtils.yielding(this, yieldSignal);
  }

  /** Create a new WorkProcessor which returns finish state when the signal is set. */
  public WorkProcessor<T> finishWhen(BooleanSupplier signal) {
    return WorkProcessorUtils.finishWhen(this, signal);
  }

  /**
   * Accept an input elements, and flat outputs generated from inner WorkProcessor, then wrap it
   * into a brand-new WorkProcessor.
   */
  public <R> WorkProcessor<R> transform(Transformer<T, R> transformation) {
    return WorkProcessorUtils.transform(this, transformation);
  }

  /**
   * Syntactic sugar of transform. Each input has a corresponding output element, There is no
   * immediate state like NEEDS_MORE_DATA.
   */
  public <R> WorkProcessor<R> map(Function<T, R> mapper) {
    return WorkProcessorUtils.map(this, mapper);
  }

  /**
   * Accept an input elements, and flat outputs generated from inner WorkProcessor, then wrap it
   * into a brand-new WorkProcessor.
   */
  public <R> WorkProcessor<R> flatTransform(Transformer<T, WorkProcessor<R>> transformation) {
    return WorkProcessorUtils.flatTransform(this, transformation);
  }

  /**
   * Syntactic sugar of flatTransform. Each input has a corresponding output WorkProcessor. There is
   * no immediate state like NEEDS_MORE_DATA.
   */
  public <R> WorkProcessor<R> flatMap(Function<T, WorkProcessor<R>> mapper) {
    return WorkProcessorUtils.flatMap(this, mapper);
  }

  public <R> WorkProcessor<R> transformProcessor(
      Function<WorkProcessor<T>, WorkProcessor<R>> transformation) {
    return transformation.apply(this);
  }
}
