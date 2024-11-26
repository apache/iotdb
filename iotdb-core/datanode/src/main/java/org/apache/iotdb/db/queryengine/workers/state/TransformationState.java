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

package org.apache.iotdb.db.queryengine.workers.state;

import com.google.common.util.concurrent.ListenableFuture;

public final class TransformationState<T> {
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
  private final T result;
  private final ListenableFuture<?> blocked;

  public TransformationState(
      Type type, boolean needsMoreData, T result, ListenableFuture<?> blocked) {
    this.type = type;
    this.needsMoreData = needsMoreData;
    this.result = result;
    this.blocked = blocked;
  }

  @SuppressWarnings("unchecked")
  public static <T> TransformationState<T> needsMoreData() {
    return (TransformationState<T>) NEEDS_MORE_DATA_STATE;
  }

  public static <T> TransformationState<T> blocked(ListenableFuture<?> blocked) {
    return new TransformationState<>(Type.BLOCKED, false, null, blocked);
  }

  @SuppressWarnings("unchecked")
  public static <T> TransformationState<T> yielded() {
    return (TransformationState<T>) YIELD_STATE;
  }

  public static <T> TransformationState<T> ofResult(T result) {
    return ofResult(result, true);
  }

  public static <T> TransformationState<T> ofResult(T result, boolean needsMoreData) {
    return new TransformationState<>(Type.RESULT, needsMoreData, result, null);
  }

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

  public T getResult() {
    return result;
  }

  public ListenableFuture<?> getBlocked() {
    return blocked;
  }
}
