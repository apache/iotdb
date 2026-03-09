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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;

public abstract class AbstractDefaultAggTableScanOperator extends AbstractAggTableScanOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AbstractDefaultAggTableScanOperator.class);

  protected AbstractDefaultAggTableScanOperator(AbstractAggTableScanOperatorParameter parameter) {
    super(parameter);
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    return timeIterator.hasCachedTimeRange() || timeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    // optimize for sql: select count(*) from (select count(s1), sum(s1) from table)
    if (tableAggregators.isEmpty()
        && timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.SINGLE_TIME_ITERATOR
        && resultTsBlockBuilder.getValueColumnBuilders().length == 0) {
      resultTsBlockBuilder.reset();
      currentDeviceIndex = deviceCount;
      timeIterator.setFinished();
      Column[] valueColumns = new Column[0];
      return new TsBlock(1, new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, 1), valueColumns);
    }

    // start stopwatch, reset leftRuntimeOfOneNextCall
    long start = System.nanoTime();
    leftRuntimeOfOneNextCall = 1000 * operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long maxRuntime = leftRuntimeOfOneNextCall;

    while (System.nanoTime() - start < maxRuntime
        && (timeIterator.hasCachedTimeRange() || timeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {

      // calculate aggregation result on current time window
      // return true if current time window is calc finished
      Optional<Boolean> b = calculateAggregationResultForCurrentTimeRange();
      if (b.isPresent() && b.get()) {
        timeIterator.resetCurTimeRange();
      }
    }

    if (resultTsBlockBuilder.isEmpty()) {
      return null;
    }

    buildResultTsBlock();
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult();
    // after appendAggregationResult invoked, aggregators must be cleared
    resetTableAggregators();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes())
        + RamUsageEstimator.sizeOfCollection(deviceEntries);
  }
}
