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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class AggregationOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AggregationOperator.class);

  private final OperatorContext operatorContext;

  private final Operator child;

  private final List<TableAggregator> aggregators;

  private final TsBlockBuilder resultBuilder;

  private final ColumnBuilder[] resultColumnsBuilder;

  private final long maxReturnSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  protected MemoryReservationManager memoryReservationManager;

  private boolean finished = false;

  public AggregationOperator(
      OperatorContext operatorContext, Operator child, List<TableAggregator> aggregators) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.aggregators = aggregators;
    this.resultBuilder =
        new TsBlockBuilder(
            aggregators.stream().map(TableAggregator::getType).collect(toImmutableList()));
    this.resultColumnsBuilder = resultBuilder.getValueColumnBuilders();
    this.memoryReservationManager =
        operatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getMemoryReservationContext();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished;
  }

  @Override
  public TsBlock next() throws Exception {
    // Each call only calculate at most once, no need to check time slice.
    TsBlock block;
    if (child.hasNextWithTimer()) {
      block = child.nextWithTimer();
      if (block == null) {
        return null;
      }

      for (TableAggregator aggregator : aggregators) {
        aggregator.processBlock(block);
      }

      return null;
    } else {
      // evaluate output
      Column[] valueColumns = new Column[resultColumnsBuilder.length];
      for (int i = 0; i < aggregators.size(); i++) {
        aggregators.get(i).evaluate(resultColumnsBuilder[i]);
        valueColumns[i] = resultColumnsBuilder[i].build();
      }

      finished = true;
      return TsBlock.wrapBlocksWithoutCopy(
          1, new RunLengthEncodedColumn(TableScanOperator.TIME_COLUMN_TEMPLATE, 1), valueColumns);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        child.calculateMaxPeekMemoryWithCounter(),
        calculateRetainedSizeAfterCallingNext() + calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + aggregators.stream().mapToLong(TableAggregator::getEstimatedSize).sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultBuilder.getRetainedSizeInBytes();
  }
}
