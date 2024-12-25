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

package org.apache.iotdb.db.queryengine.common.transformation;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TransformationState<T> {
  private static final TransformationState<?> NEEDS_MORE_DATA_STATE =
      new TransformationState<>(Type.NEEDS_MORE_DATA, true, null, null);
  private static final TransformationState<?> YIELD_STATE =
      new TransformationState<>(Type.YIELD, false, null, null);
  private static final TransformationState<?> FINISHED_STATE =
      new TransformationState<>(Type.FINISHED, false, null, null);

  public enum Type {
    NEEDS_MORE_DATA,
    BLOCKED,
    YIELD,
    RESULT,
    FINISHED
  }

  private final Type type;
  private final boolean needsMoreData;
  // Nullable
  private final T result;
  // Nullable
  private final ListenableFuture<Void> blocked;

  private TransformationState(
      Type type, boolean needsMoreData, T result, ListenableFuture<Void> blocked) {
    this.type = requireNonNull(type, "type is null");
    this.needsMoreData = needsMoreData;
    this.result = result;
    this.blocked = blocked;
  }

  /**
   * Signals that transformation requires more data in order to continue and no result has been
   * produced. {@link Transformation#process} will be called with a new input element or with {@link
   * Optional#empty()} if there are no more elements.
   */
  @SuppressWarnings("unchecked")
  public static <T> TransformationState<T> needsMoreData() {
    return (TransformationState<T>) NEEDS_MORE_DATA_STATE;
  }

  /**
   * Signals that transformation is blocked. {@link Transformation#process} will be called again
   * with the same input element after {@code blocked} future is done.
   */
  public static <T> TransformationState<T> blocked(ListenableFuture<Void> blocked) {
    return new TransformationState<>(
        Type.BLOCKED, false, null, requireNonNull(blocked, "blocked is null"));
  }

  /**
   * Signals that transformation has yielded. {@link Transformation#process} will be called again
   * with the same input element.
   */
  @SuppressWarnings("unchecked")
  public static <T> TransformationState<T> yielded() {
    return (TransformationState<T>) YIELD_STATE;
  }

  /**
   * Signals that transformation has produced a result from its input. {@link
   * Transformation#process} will be called again with a new element.
   */
  public static <T> TransformationState<T> ofResult(T result) {
    return ofResult(result, true);
  }

  /**
   * Signals that transformation has produced a result. If {@code needsMoreData}, {@link
   * Transformation#process} will be called again with a new element. If not @{code needsMoreData},
   * {@link Transformation#process} will be called again with the same element.
   */
  public static <T> TransformationState<T> ofResult(T result, boolean needsMoreData) {
    return new TransformationState<>(
        Type.RESULT, needsMoreData, requireNonNull(result, "result is null"), null);
  }

  /**
   * Signals that transformation has finished. {@link Transformation#process} method will not be
   * called again.
   */
  @SuppressWarnings("unchecked")
  public static <T> TransformationState<T> finished() {
    return (TransformationState<T>) FINISHED_STATE;
  }

  public Type getType() {
    return type;
  }

  public boolean isNeedsMoreData() {
    return needsMoreData;
  }

  // Nullable
  public T getResult() {
    return result;
  }

  // Nullable
  public ListenableFuture<Void> getBlocked() {
    return blocked;
  }
}
