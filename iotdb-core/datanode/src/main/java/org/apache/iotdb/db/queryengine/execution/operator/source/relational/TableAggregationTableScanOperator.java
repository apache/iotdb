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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSeriesAggregationScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;

public class TableAggregationTableScanOperator extends AbstractSeriesAggregationScanOperator {

  List<Aggregator> aggregators;

  public static final LongColumn TIME_COLUMN_TEMPLATE =
      new LongColumn(1, Optional.empty(), new long[] {0});

  private final List<ColumnSchema> columnSchemas;

  private final int[] columnsIndexArray;

  private final int measurementColumnCount;

  private final List<DeviceEntry> deviceEntries;

  private final int deviceCount;

  private final Ordering scanOrder;
  private final SeriesScanOptions seriesScanOptions;

  private final List<String> measurementColumnNames;

  private final List<IMeasurementSchema> measurementSchemas;

  private final List<TSDataType> measurementColumnTSDataTypes;

  private TsBlockBuilder measurementDataBuilder;

  // TODO calc maxTsBlockLineNum using date_bin
  private final int maxTsBlockLineNum;

  private TsBlock measurementDataBlock;

  private QueryDataSource queryDataSource;

  private int currentDeviceIndex;

  public TableAggregationTableScanOperator(
      PlanNodeId sourceId,
      OperatorContext context,
      List<ColumnSchema> columnSchemas,
      int[] columnsIndexArray,
      int measurementColumnCount,
      List<DeviceEntry> deviceEntries,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      List<IMeasurementSchema> measurementSchemas,
      int maxTsBlockLineNum,
      int subSensorSize,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter,
      long maxReturnSize,
      boolean canUseStatistics) {

    super(
        sourceId,
        context,
        null,
        subSensorSize,
        aggregators,
        timeRangeIterator,
        ascending,
        false,
        groupByTimeParameter,
        maxReturnSize,
        canUseStatistics);

    this.sourceId = sourceId;
    this.operatorContext = context;
    this.columnSchemas = columnSchemas;
    this.columnsIndexArray = columnsIndexArray;
    this.measurementColumnCount = measurementColumnCount;
    this.deviceEntries = deviceEntries;
    this.deviceCount = deviceEntries.size();
    this.scanOrder = scanOrder;
    this.seriesScanOptions = seriesScanOptions;
    this.measurementColumnNames = measurementColumnNames;
    this.measurementSchemas = measurementSchemas;
    this.measurementColumnTSDataTypes =
        measurementSchemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    this.currentDeviceIndex = 0;

    this.maxReturnSize = maxReturnSize;
    this.maxTsBlockLineNum = maxTsBlockLineNum;

    this.seriesScanUtil = constructAlignedSeriesScanUtil(deviceEntries.get(currentDeviceIndex));
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished();
  }

  @Override
  public TsBlock next() throws Exception {
    // start stopwatch, reset leftRuntimeOfOneNextCall
    long start = System.nanoTime();
    // leftRuntimeOfOneNextCall = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    // long maxRuntime = leftRuntimeOfOneNextCall;

    while (
    // System.nanoTime() - start < maxRuntime&&
    (curTimeRange != null || timeRangeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {
      if (curTimeRange == null) {
        // move to the next time window
        curTimeRange = timeRangeIterator.nextTimeRange();
        // clear previous aggregation result
        for (Aggregator aggregator : aggregators) {
          aggregator.reset();
        }
      }

      // calculate aggregation result on current time window
      // Keep curTimeRange if the calculation of this timeRange is not done
      if (calculateAggregationResultForCurrentTimeRange()) {
        curTimeRange = null;
      }
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      TsBlock resultTsBlock = resultTsBlockBuilder.build();
      resultTsBlockBuilder.reset();
      return resultTsBlock;
    } else {
      return null;
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return (retainedTsBlock == null)
        && (currentDeviceIndex >= deviceCount || seriesScanOptions.limitConsumedUp());
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  private AlignedSeriesScanUtil constructAlignedSeriesScanUtil(DeviceEntry deviceEntry) {
    AlignedFullPath alignedPath =
        constructAlignedPath(deviceEntry, measurementColumnNames, measurementSchemas);

    return new AlignedSeriesScanUtil(
        alignedPath,
        scanOrder,
        seriesScanOptions,
        operatorContext.getInstanceContext(),
        true,
        measurementColumnTSDataTypes);
  }

  /** Return true if we have the result of this timeRange. */
  protected boolean calculateAggregationResultForCurrentTimeRange() {
    try {
      if (calcFromCachedData()) {
        updateResultTsBlock();
        return true;
      }

      if (readAndCalcFromPage()) {
        updateResultTsBlock();
        return true;
      }

      // only when all the page data has been consumed, we need to read the chunk data
      if (!seriesScanUtil.hasNextPage() && readAndCalcFromChunk()) {
        updateResultTsBlock();
        return true;
      }

      // only when all the page and chunk data has been consumed, we need to read the file data
      if (!seriesScanUtil.hasNextPage()
          && !seriesScanUtil.hasNextChunk()
          && readAndCalcFromFile()) {
        updateResultTsBlock();
        return true;
      }

      // If the TimeRange is (Long.MIN_VALUE, Long.MAX_VALUE), for Aggregators like countAggregator,
      // we have to consume all the data before we finish the aggregation calculation.
      if (seriesScanUtil.hasNextPage()
          || seriesScanUtil.hasNextChunk()
          || seriesScanUtil.hasNextFile()) {
        return false;
      } else {
        currentDeviceIndex++;
      }
      updateResultTsBlock();
      if (currentDeviceIndex < deviceCount) {
        // construct AlignedSeriesScanUtil for next device
        this.seriesScanUtil = constructAlignedSeriesScanUtil(deviceEntries.get(currentDeviceIndex));

        // reset QueryDataSource
        queryDataSource.reset();
        this.seriesScanUtil.initQueryDataSource(queryDataSource);
      }
      return currentDeviceIndex >= deviceCount;
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSource) dataSource;
    this.seriesScanUtil.initQueryDataSource(queryDataSource);
    this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
    this.resultTsBlockBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
    this.measurementDataBuilder = new TsBlockBuilder(this.measurementColumnTSDataTypes);
    this.measurementDataBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult(
        resultTsBlockBuilder, aggregators, timeRangeIterator.currentOutputTime());
  }
}
