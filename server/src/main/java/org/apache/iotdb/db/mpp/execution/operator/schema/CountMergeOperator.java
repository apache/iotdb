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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class CountMergeOperator implements ProcessOperator {
  private final PlanNodeId planNodeId;
  private final OperatorContext operatorContext;

  private List<TsBlock> tsBlockList = new ArrayList<>();
  private int currentIndex = 0;

  private final List<Operator> children;

  private final boolean isGroupByLevel;

  public CountMergeOperator(
      PlanNodeId planNodeId, OperatorContext operatorContext, List<Operator> children) {
    this.planNodeId = planNodeId;
    this.operatorContext = operatorContext;
    this.children = children;
    isGroupByLevel = children.get(0) instanceof LevelTimeSeriesCountOperator;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    for (Operator child : children) {
      while (!child.isFinished()) {
        ListenableFuture<?> blocked = child.isBlocked();
        if (!blocked.isDone()) {
          return blocked;
        }
        if (child.hasNext()) {
          TsBlock tsBlock = child.next();
          if (null != tsBlock && !tsBlock.isEmpty()) {
            tsBlockList.add(tsBlock);
          }
        }
      }
    }

    generateResultTsBlockList();

    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    currentIndex++;
    return tsBlockList.get(currentIndex - 1);
  }

  private void generateResultTsBlockList() {
    if (isGroupByLevel) {
      generateResultWithGroupByLevel();
    } else {
      generateResultWithoutGroupByLevel();
    }
  }

  private void generateResultWithoutGroupByLevel() {
    int totalCount = 0;
    for (TsBlock tsBlock : tsBlockList) {
      int count = tsBlock.getColumn(0).getInt(0);
      totalCount += count;
    }
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeInt(totalCount);
    tsBlockBuilder.declarePosition();
    this.tsBlockList = Collections.singletonList(tsBlockBuilder.build());
  }

  private void generateResultWithGroupByLevel() {
    Map<String, Integer> countMap = new HashMap<>();
    for (TsBlock tsBlock : tsBlockList) {
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        String columnName = tsBlock.getColumn(0).getBinary(i).getStringValue();
        int count = tsBlock.getColumn(1).getInt(i);
        countMap.put(columnName, countMap.getOrDefault(columnName, 0) + count);
      }
    }
    this.tsBlockList =
        SchemaTsBlockUtil.transferSchemaResultToTsBlockList(
            countMap.entrySet().iterator(),
            Arrays.asList(TSDataType.TEXT, TSDataType.INT32),
            (entry, tsBlockBuilder) -> {
              tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
              tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(entry.getKey()));
              tsBlockBuilder.getColumnBuilder(1).writeInt(entry.getValue());
              tsBlockBuilder.declarePosition();
            });
  }

  @Override
  public boolean hasNext() {
    return currentIndex < tsBlockList.size();
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
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
}
