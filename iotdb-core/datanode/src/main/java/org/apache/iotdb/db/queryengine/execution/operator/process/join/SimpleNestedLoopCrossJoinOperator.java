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

package org.apache.iotdb.db.queryengine.execution.operator.process.join;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.AbstractOperator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableInnerJoinOperator.buildResultTsBlock;

/**
 * This Operator is used to implement the simple nested loop join algorithm for Cartesian product.
 * It is used to join two tables, one is the probe table and the other is the build table. For now,
 * the build table is assumed to be small enough to be cached in memory.(Produced by a scalar
 * subquery.) Scalar subquery is always the right child of PlanNode, so we can use right child of
 * JoinNode as the build table.
 */
public class SimpleNestedLoopCrossJoinOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SimpleNestedLoopCrossJoinOperator.class);

  private final Operator probeSource;

  // cache the result of buildSource, for now, we assume that the buildChild produces a small number
  // of TsBlocks
  private final Operator buildSource;

  private final List<TsBlock> buildBlocks;

  private final TsBlockBuilder resultBuilder;

  private final MemoryReservationManager memoryReservationManager;

  private final int[] leftOutputSymbolIdx;

  private final int[] rightOutputSymbolIdx;

  private TsBlock cachedProbeBlock;

  private int probeIndex;

  private boolean buildFinished = false;

  public SimpleNestedLoopCrossJoinOperator(
      OperatorContext operatorContext,
      Operator probeSource,
      Operator buildSource,
      int[] leftOutputSymbolIdx,
      int[] rightOutputSymbolIdx,
      List<TSDataType> dataTypes) {
    this.operatorContext = operatorContext;
    this.probeSource = probeSource;
    this.buildSource = buildSource;
    this.leftOutputSymbolIdx = leftOutputSymbolIdx;
    this.rightOutputSymbolIdx = rightOutputSymbolIdx;
    this.buildBlocks = new ArrayList<>();
    this.resultBuilder = new TsBlockBuilder(dataTypes);
    this.memoryReservationManager =
        operatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getMemoryReservationContext();
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      getResultFromRetainedTsBlock();
    }
    resultBuilder.reset();
    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();
    if (!buildFinished) {
      TsBlock block = buildSource.next();
      if (block != null && !block.isEmpty()) {
        buildBlocks.add(block);
        memoryReservationManager.reserveMemoryCumulatively(block.getRetainedSizeInBytes());
      }
      if (!buildSource.hasNext()) {
        buildFinished = true;
      }
      // probeSource could still be blocked by now, so we need to check it again
      return null;
    }
    cachedProbeBlock = cachedProbeBlock == null ? probeSource.next() : cachedProbeBlock;
    if (cachedProbeBlock == null || cachedProbeBlock.isEmpty()) {
      // TsBlock returned by probeSource is null or empty, we need to wait for another round
      cachedProbeBlock = null;
      return null;
    }
    while (probeIndex < cachedProbeBlock.getPositionCount()
        && System.nanoTime() - start < maxRuntime) {
      for (TsBlock buildBlock : buildBlocks) {
        for (int i = 0; i < buildBlock.getPositionCount(); i++) {
          appendValueToResult(probeIndex, buildBlock);
        }
      }
      probeIndex++;
    }
    if (probeIndex == cachedProbeBlock.getPositionCount()) {
      probeIndex = 0;
      cachedProbeBlock = null;
    }
    if (resultBuilder.isEmpty()) {
      return null;
    }

    resultTsBlock = buildResultTsBlock(resultBuilder);
    return checkTsBlockSizeAndGetResult();
  }

  private void appendValueToResult(int probeIndex, TsBlock buildBlock) {
    for (int i = 0; i < buildBlock.getPositionCount(); i++) {
      for (int j = 0; j < leftOutputSymbolIdx.length; j++) {
        ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(j);
        if (cachedProbeBlock.getColumn(leftOutputSymbolIdx[j]).isNull(probeIndex)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(cachedProbeBlock.getColumn(leftOutputSymbolIdx[j]), probeIndex);
        }
      }
      for (int j = 0; j < rightOutputSymbolIdx.length; j++) {
        ColumnBuilder columnBuilder =
            resultBuilder.getColumnBuilder(leftOutputSymbolIdx.length + j);
        if (buildBlock.getColumn(rightOutputSymbolIdx[j]).isNull(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(buildBlock.getColumn(rightOutputSymbolIdx[j]), i);
        }
      }
    }
    resultBuilder.declarePosition();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    if (!buildFinished) {
      return buildSource.hasNext();
    }
    return probeSource.hasNext();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (buildFinished) {
      return probeSource.isBlocked();
    }
    return buildSource.isBlocked();
  }

  @Override
  public void close() throws Exception {
    if (probeSource != null) {
      probeSource.close();
    }
    if (buildSource != null) {
      buildSource.close();
    }
    for (TsBlock block : buildBlocks) {
      memoryReservationManager.releaseMemoryCumulatively(block.getRetainedSizeInBytes());
    }
    buildBlocks.clear();
    cachedProbeBlock = null;
    resultTsBlock = null;
    retainedTsBlock = null;
  }

  @Override
  public boolean isFinished() throws Exception {
    if (retainedTsBlock != null) {
      return false;
    }

    return (cachedProbeBlock != null && !cachedProbeBlock.isEmpty())
        && probeSource.isFinished()
        && buildFinished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        Math.max(
            probeSource.calculateMaxPeekMemoryWithCounter(),
            buildSource.calculateMaxPeekMemoryWithCounter()),
        calculateRetainedSizeAfterCallingNext() + calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return probeSource.calculateRetainedSizeAfterCallingNext()
        + buildSource.calculateRetainedSizeAfterCallingNext()
        // cachedProbeBlock + one build block assumed
        + maxReturnSize * 2;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(probeSource)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(buildSource)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultBuilder.getRetainedSizeInBytes();
  }
}
