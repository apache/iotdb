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

package org.apache.iotdb.db.queryengine.execution.operator.process.last;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class LastQueryTransformOperator implements ProcessOperator {

  private String viewPath;

  private String dataType;

  private final OperatorContext operatorContext;

  private final List<Operator> children;

  private int currentIndex;

  private TsBlockBuilder tsBlockBuilder;

  public LastQueryTransformOperator(
      String viewPath, String dataType, OperatorContext operatorContext, List<Operator> children) {
    this.viewPath = viewPath;
    this.dataType = dataType;
    this.operatorContext = operatorContext;
    this.children = children;
    this.currentIndex = 0;
    this.tsBlockBuilder = LastQueryUtil.createTsBlockBuilder(0);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return this.operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (currentIndex < 1) {
      List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
      for (int i = currentIndex; i < 1; i++) {
        ListenableFuture<?> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
      return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
    } else {
      return Futures.immediateVoidFuture();
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (currentIndex >= 1) {
      TsBlock res = tsBlockBuilder.build();
      tsBlockBuilder.reset();
      return res;
    }

    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    int endIndex = 1;

    while ((System.nanoTime() - start < maxRuntime)
        && (currentIndex < endIndex)
        && !tsBlockBuilder.isFull()) {
      if (children.get(currentIndex).hasNextWithTimer()) {
        TsBlock tsBlock = children.get(currentIndex).nextWithTimer();
        if (tsBlock == null) {
          return null;
        } else if (!tsBlock.isEmpty()) {
          if (tsBlock.getColumn(1).isNull(0)) {
            return null;
          }
          LastQueryUtil.appendLastValue(
              tsBlockBuilder,
              tsBlock.getColumn(0).getLong(0),
              viewPath,
              tsBlock.getColumn(1).getTsPrimitiveType(0).getStringValue(),
              dataType);
        }
      } else {
        children.get(currentIndex).close();
        children.set(currentIndex, null);
      }

      currentIndex++;
    }

    TsBlock res = tsBlockBuilder.build();
    tsBlockBuilder.reset();
    return res;
  }

  @Override
  public boolean hasNext() throws Exception {
    return currentIndex < 1;
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    for (Operator child : children) {
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemory());
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateRetainedSizeAfterCallingNext());
    }
    return maxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long maxReturnMemory = 0;
    for (Operator child : children) {
      maxReturnMemory = Math.max(maxReturnMemory, child.calculateMaxReturnSize());
    }
    return maxReturnMemory;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long sum = 0;
    for (Operator operator : children) {
      sum += operator.calculateRetainedSizeAfterCallingNext();
    }
    return sum;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      if (child != null) {
        child.close();
      }
    }
    tsBlockBuilder = null;
  }
}
