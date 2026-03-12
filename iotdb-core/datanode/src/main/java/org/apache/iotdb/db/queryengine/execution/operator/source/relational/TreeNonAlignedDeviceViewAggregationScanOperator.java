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
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.CURRENT_DEVICE_INDEX_STRING;

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
    return child.isBlocked();
  }

  @Override
  String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    return (String) extractor.extract(deviceEntry.getDeviceID(), idColumnIndex);
  }

  @Override
  protected Optional<Boolean> calculateAggregationResultForCurrentTimeRange() {
    try {
      // First try to calculate from cached data
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
    } catch (Exception e) {
      throw new RuntimeException("Error while processing aggregation from child operator", e);
    }
  }

  /** Read data from child operator and calculate aggregation. */
  private boolean readAndCalcFromChild() throws Exception {
    long start = System.nanoTime();

    while (child.hasNext()) {
      // Get next TsBlock from child
      TsBlock tsBlock = child.nextWithTimer();
      if (tsBlock == null || tsBlock.isEmpty()) {
        continue;
      }

      // Calculate aggregation from raw data
      if (calcUsingRawData(tsBlock)) {
        return true;
      }

      // If not finished, continue reading from child
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

    this.queryDataSource = (QueryDataSource) dataSource;
    this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
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
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child);
  }
}
