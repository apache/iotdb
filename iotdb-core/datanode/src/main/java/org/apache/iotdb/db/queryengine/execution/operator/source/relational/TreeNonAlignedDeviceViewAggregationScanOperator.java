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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING;
import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.TIME_COLUMN_TEMPLATE;

public class TreeNonAlignedDeviceViewAggregationScanOperator
    extends AbstractDefaultAggTableScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(
          TreeNonAlignedDeviceViewAggregationScanOperator.class);

  private final IDeviceID.TreeDeviceIdColumnValueExtractor extractor;
  private final DeviceIteratorScanOperator.DeviceChildOperatorTreeGenerator childOperatorGenerator;

  private Operator child;
  private List<Operator> dataSourceOperators;

  public TreeNonAlignedDeviceViewAggregationScanOperator(
      AbstractAggTableScanOperatorParameter parameter,
      IDeviceID.TreeDeviceIdColumnValueExtractor extractor,
      DeviceIteratorScanOperator.DeviceChildOperatorTreeGenerator childOperatorGenerator) {
    super(parameter);
    this.extractor = extractor;
    this.childOperatorGenerator = childOperatorGenerator;
    constructCurrentDeviceOperatorTree();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    // Don't call isBlocked of child if the device has been consumed up, because the child operator
    // may have been closed
    return currentDeviceIndex >= deviceCount ? NOT_BLOCKED : child.isBlocked();
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

    // calculate aggregation result on current time window
    // return true if current time window is calc finished
    Optional<Boolean> b = calculateAggregationResultForCurrentTimeRange();
    if (b.isPresent() && b.get()) {
      timeIterator.resetCurTimeRange();
      buildResultTsBlock();
    }

    if (resultTsBlockBuilder.isFull()) {
      buildResultTsBlock();
    }

    if (resultTsBlock == null) {
      return null;
    }
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  public void close() throws Exception {
    if (child != null) {
      child.close();
    }
  }

  @Override
  String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    return (String) extractor.extract(deviceEntry.getDeviceID(), idColumnIndex);
  }

  @Override
  protected Optional<Boolean> calculateAggregationResultForCurrentTimeRange() throws Exception {
    // Try to calculate from cached data
    if (calcFromCachedData()) {
      updateResultTsBlock();
      checkIfAllAggregatorHasFinalResult();
      return Optional.of(true);
    }

    // Read from child operator
    if (readAndCalcFromChild()) {
      updateResultTsBlock();
      checkIfAllAggregatorHasFinalResult();
      return Optional.of(true);
    }

    // No more data from child, finish the current device
    if (!child.hasNext()) {
      updateResultTsBlock();
      timeIterator.resetCurTimeRange();
      nextDevice();

      if (currentDeviceIndex >= deviceCount) {
        // All devices consumed
        timeIterator.setFinished();
        return Optional.of(true);
      } else {
        // More devices to process, child should provide next device's data
        return Optional.of(false);
      }
    }

    return Optional.of(false);
  }

  /** Read data from child operator and calculate aggregation. */
  private boolean readAndCalcFromChild() throws Exception {
    if (child.hasNext()) {
      // Get next TsBlock from child
      TsBlock tsBlock = child.nextWithTimer();
      if (tsBlock == null || tsBlock.isEmpty()) {
        return false;
      }
      // Calculate aggregation from raw data
      return calcUsingRawData(tsBlock);
    }
    return false;
  }

  @Override
  protected void nextDevice() throws Exception {
    currentDeviceIndex++;
    childOperatorGenerator.getCurrentDeviceStartCloseOperator().close();
    if (currentDeviceIndex >= deviceEntries.size()) {
      return;
    }
    constructCurrentDeviceOperatorTree();
    queryDataSource.reset();
    initQueryDataSource(queryDataSource);
    this.operatorContext.recordSpecifiedInfo(
        CURRENT_DEVICE_INDEX_STRING, Integer.toString(currentDeviceIndex));
  }

  private void constructCurrentDeviceOperatorTree() {
    if (this.deviceEntries.isEmpty()) {
      return;
    }
    if (this.deviceEntries.get(this.currentDeviceIndex) == null) {
      throw new IllegalStateException(
          "Device entries of index " + this.currentDeviceIndex + " is empty");
    }
    DeviceEntry deviceEntry = this.deviceEntries.get(this.currentDeviceIndex);

    childOperatorGenerator.generateCurrentDeviceOperatorTree(deviceEntry, false);
    child = childOperatorGenerator.getCurrentDeviceRootOperator();
    dataSourceOperators = childOperatorGenerator.getCurrentDeviceDataSourceOperators();
  }

  /** same with {@link DeviceIteratorScanOperator#initQueryDataSource(IQueryDataSource)} */
  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    if (resultTsBlockBuilder == null) {
      // only need to do this when init firstly
      this.queryDataSource = (QueryDataSource) dataSource;
      this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
    }

    if (dataSourceOperators == null || dataSourceOperators.isEmpty()) {
      return;
    }

    for (Operator operator : dataSourceOperators) {
      ((AbstractDataSourceOperator) operator).initQueryDataSource(dataSource);
    }
  }

  @Override
  protected void checkIfAllAggregatorHasFinalResult() throws Exception {
    if (allAggregatorsHasFinalResult
        && (timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.SINGLE_TIME_ITERATOR
            || tableAggregators.isEmpty())) {
      nextDevice();
      inputTsBlock = null;

      if (currentDeviceIndex >= deviceCount) {
        // all devices have been consumed
        timeIterator.setFinished();
      }

      allAggregatorsHasFinalResult = false;
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes())
        + RamUsageEstimator.sizeOfCollection(deviceEntries)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(childOperatorGenerator);
  }
}
