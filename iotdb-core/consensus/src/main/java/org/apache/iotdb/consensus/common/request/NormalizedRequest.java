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

package org.apache.iotdb.consensus.common.request;

import org.apache.iotdb.common.rpc.thrift.TSStatus;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

/**
 * One application request's ordered logical entries. A child keeps an entire row together; the
 * collection is not a transaction. Child construction may be lazy so a tablet need not be copied in
 * full. The caller must not mutate the parent while the write is in progress.
 */
public final class NormalizedRequest {
  public enum FailureSelection {
    FIRST,
    LAST
  }

  private final int size;
  private final IntFunction<IConsensusRequest> requests;
  private final IntUnaryOperator sourceIndexes;
  private final FailureSelection failureSelection;
  private final BiConsumer<Integer, TSStatus> results;

  public NormalizedRequest(
      int size,
      IntFunction<IConsensusRequest> requests,
      IntUnaryOperator sourceIndexes,
      FailureSelection failureSelection,
      BiConsumer<Integer, TSStatus> results) {
    if (size <= 0) {
      throw new IllegalArgumentException("A normalized request must contain at least one entry");
    }
    this.size = size;
    this.requests = Objects.requireNonNull(requests, "requests");
    this.sourceIndexes = Objects.requireNonNull(sourceIndexes, "sourceIndexes");
    this.failureSelection = Objects.requireNonNull(failureSelection, "failureSelection");
    this.results = Objects.requireNonNull(results, "results");
  }

  /** Keep one operation intact; its own DATA/BARRIER classification still applies. */
  public static NormalizedRequest single(IConsensusRequest request) {
    Objects.requireNonNull(request, "request");
    return new NormalizedRequest(1, i -> request, i -> 0, FailureSelection.FIRST, (i, s) -> {});
  }

  public int size() {
    return size;
  }

  public IConsensusRequest requestAt(int index) {
    checkIndex(index);
    return Objects.requireNonNull(requests.apply(index), "normalized child");
  }

  /** The row's position in the original request, before DataRegion routing split it. */
  public int sourceIndex(int index) {
    checkIndex(index);
    return sourceIndexes.applyAsInt(index);
  }

  public FailureSelection failureSelection() {
    return failureSelection;
  }

  /** Records an actually completed child by its position within this routed request. */
  public void recordResult(int index, TSStatus status) {
    checkIndex(index);
    results.accept(index, Objects.requireNonNull(status, "status"));
  }

  private void checkIndex(int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("row " + index + " of " + size);
    }
  }
}
