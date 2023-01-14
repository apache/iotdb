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

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class CountGroupByLevelMergeOperator implements ProcessOperator {

  private final PlanNodeId planNodeId;
  private final OperatorContext operatorContext;

  private final List<Operator> children;

  private final boolean[] childrenHasNext;

  private final Map<String, Long> countMap = new HashMap<>();

  private List<TsBlock> resultTsBlockList;

  private int currentIndex = 0;

  public CountGroupByLevelMergeOperator(
      PlanNodeId planNodeId, OperatorContext operatorContext, List<Operator> children) {
    this.planNodeId = planNodeId;
    this.operatorContext = operatorContext;
    this.children = children;

    childrenHasNext = new boolean[children.size()];
    Arrays.fill(childrenHasNext, true);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutureList = new ArrayList<>(children.size());
    for (int i = 0; i < children.size(); i++) {
      if (childrenHasNext[i]) {
        ListenableFuture<?> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutureList.add(blocked);
        }
      }
    }

    return listenableFutureList.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutureList);
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (resultTsBlockList != null) {
      currentIndex++;
      return resultTsBlockList.get(currentIndex - 1);
    }

    boolean allChildrenConsumed = true;
    for (int i = 0; i < children.size(); i++) {
      if (childrenHasNext[i]) {
        // when this operator is not blocked, it means all children that have remaining TsBlock is
        // not blocked.
        if (children.get(i).hasNextWithTimer()) {
          allChildrenConsumed = false;
          TsBlock tsBlock = children.get(i).nextWithTimer();
          if (tsBlock != null && !tsBlock.isEmpty()) {
            consumeChildrenTsBlock(tsBlock);
          }
        } else {
          childrenHasNext[i] = false;
        }
      }
    }
    if (allChildrenConsumed) {
      generateResultTsBlockList();
      currentIndex++;
      return resultTsBlockList.get(currentIndex - 1);
    } else {
      return null;
    }
  }

  private void consumeChildrenTsBlock(TsBlock tsBlock) {
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      String columnName = tsBlock.getColumn(0).getBinary(i).getStringValue();
      long count = tsBlock.getColumn(1).getLong(i);
      countMap.put(columnName, countMap.getOrDefault(columnName, 0L) + count);
    }
  }

  private void generateResultTsBlockList() {
    this.resultTsBlockList =
        SchemaTsBlockUtil.transferSchemaResultToTsBlockList(
            countMap.entrySet().iterator(),
            Arrays.asList(TSDataType.TEXT, TSDataType.INT64),
            (entry, tsBlockBuilder) -> {
              tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
              tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(entry.getKey()));
              tsBlockBuilder.getColumnBuilder(1).writeLong(entry.getValue());
              tsBlockBuilder.declarePosition();
            });
    if (resultTsBlockList.isEmpty()) {
      TsBlockBuilder tsBlockBuilder =
          new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.INT64));
      resultTsBlockList.add(tsBlockBuilder.build());
    }
  }

  @Override
  public boolean hasNext() {
    return resultTsBlockList == null || currentIndex < resultTsBlockList.size();
  }

  @Override
  public boolean isFinished() {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory = Math.max(childrenMaxPeekMemory, child.calculateMaxPeekMemory());
    }

    return childrenMaxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long childrenMaxReturnSize = 0;
    for (Operator child : children) {
      childrenMaxReturnSize = Math.max(childrenMaxReturnSize, child.calculateMaxReturnSize());
    }

    return childrenMaxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long retainedSize = 0L;
    for (Operator child : children) {
      retainedSize += child.calculateRetainedSizeAfterCallingNext();
    }
    return retainedSize;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }
}
