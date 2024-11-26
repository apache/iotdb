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

public class ProcessState<T> {
  private static final ProcessState<?> YIELD_STATE = new ProcessState<>(Type.YIELD, null, null);
  private static final ProcessState<?> FINISHED_STATE =
      new ProcessState<>(Type.FINISHED, null, null);

  public enum Type {
    BLOCKED,
    YIELD,
    RESULT,
    FINISHED
  }

  private final Type type;
  private final T result;
  private final ListenableFuture<?> blocked;

  private ProcessState(Type type, T result, ListenableFuture<?> blocked) {
    this.type = type;
    this.result = result;
    this.blocked = blocked;
  }

  public static <T> ProcessState<T> blocked(ListenableFuture<?> blocked) {
    return new ProcessState<>(Type.BLOCKED, null, blocked);
  }

  @SuppressWarnings("unchecked")
  public static <T> ProcessState<T> yielded() {
    return (ProcessState<T>) YIELD_STATE;
  }

  public static <T> ProcessState<T> ofResult(T result) {
    return new ProcessState<>(Type.RESULT, result, null);
  }

  @SuppressWarnings("unchecked")
  public static <T> ProcessState<T> finished() {
    return (ProcessState<T>) FINISHED_STATE;
  }

  public Type getType() {
    return type;
  }

  public T getResult() {
    return result;
  }

  public ListenableFuture<?> getBlocked() {
    return blocked;
  }
}
