/*
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
package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public interface Operator extends AutoCloseable {

  ListenableFuture<?> NOT_BLOCKED = immediateVoidFuture();

  OperatorContext getOperatorContext();

  /**
   * Returns a future that will be completed when the operator becomes unblocked. If the operator is
   * not blocked, this method should return {@code NOT_BLOCKED}.
   */
  default ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  default TsBlock nextWithTimer() {
    OperatorContext context = getOperatorContext();
    long startTime = System.nanoTime();

    try {
      return next();
    } finally {
      context.recordExecutionTime(System.nanoTime() - startTime);
      context.recordNextCalled();
    }
  }

  /** Gets next tsBlock from this operator. If no data is currently available, return null. */
  TsBlock next();

  default boolean hasNextWithTimer() {
    OperatorContext context = getOperatorContext();
    long startTime = System.nanoTime();

    try {
      return hasNext();
    } finally {
      context.recordExecutionTime(System.nanoTime() - startTime);
    }
  }

  /** @return true if the operator has more data, otherwise false */
  boolean hasNext();

  /** This method will always be called before releasing the Operator reference. */
  @Override
  default void close() throws Exception {}

  /**
   * Is this operator completely finished processing and no more output TsBlock will be produced.
   */
  boolean isFinished();

  /**
   * We should also consider the memory used by its children operator, so the calculation logic may
   * be like: long estimatedOfCurrentOperator = XXXXX; return max(estimatedOfCurrentOperator,
   * child1.calculateMaxPeekMemory(), child2.calculateMaxPeekMemory(), ....)
   *
   * <p>Each operator's MaxPeekMemory should also take retained size of each child operator into
   * account.
   *
   * @return estimated max memory footprint that the Operator Tree(rooted from this operator) will
   *     use while doing its own query processing
   */
  long calculateMaxPeekMemory();

  /** @return estimated max memory footprint for returned TsBlock when calling operator.next() */
  long calculateMaxReturnSize();

  /**
   * @return each operator's retained size(including all its children's retained size) after calling
   *     its next() method
   */
  long calculateRetainedSizeAfterCallingNext();
}
