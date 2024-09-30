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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.window.IWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.TimeWindow;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.calculateAggregationFromRawData;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.isAllAggregatorsHasFinalResult;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.process;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.satisfiedTimeRange;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.tsfile.read.common.block.TsBlockUtil.skipPointsOutOfTimeRange;

public class TableAggregationTableScanOperator extends AbstractSeriesAggregationScanOperator {

  private final List<TableAggregator> aggregators;

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
      List<TableAggregator> aggregators,
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
        null,
        timeRangeIterator,
        ascending,
        false,
        groupByTimeParameter,
        maxReturnSize,
        canUseStatistics);

    this.aggregators = aggregators;

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
        for (TableAggregator aggregator : aggregators) {
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
    appendAggregationResult(resultTsBlockBuilder, aggregators);
  }

  protected boolean calcFromCachedData() {
    return calcFromRawData(inputTsBlock);
  }

  private boolean calcFromRawData(TsBlock tsBlock) {
    Pair<Boolean, TsBlock> calcResult =
        calculateAggregationFromRawData(tsBlock, aggregators, curTimeRange, ascending);
    inputTsBlock = calcResult.getRight();
    return calcResult.getLeft();
  }

  /**
   * Calculate aggregation value on the time range from the tsBlock containing raw data.
   *
   * @return left - whether the aggregation calculation of the current time range has done; right -
   *     remaining tsBlock
   */
  public static Pair<Boolean, TsBlock> calculateAggregationFromRawData(
      TsBlock inputTsBlock,
      List<TableAggregator> aggregators,
      TimeRange curTimeRange,
      boolean ascending) {
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return new Pair<>(false, inputTsBlock);
    }

    // check if the tsBlock does not contain points in current interval
    if (satisfiedTimeRange(inputTsBlock, curTimeRange, ascending)) {
      // skip points that cannot be calculated
      if ((ascending && inputTsBlock.getStartTime() < curTimeRange.getMin())
          || (!ascending && inputTsBlock.getStartTime() > curTimeRange.getMax())) {
        inputTsBlock = skipPointsOutOfTimeRange(inputTsBlock, curTimeRange, ascending);
      }

      inputTsBlock = process(inputTsBlock, curTimeRange, aggregators);
    }

    // judge whether the calculation finished
    boolean isTsBlockOutOfBound =
        inputTsBlock != null
            && (ascending
                ? inputTsBlock.getEndTime() > curTimeRange.getMax()
                : inputTsBlock.getEndTime() < curTimeRange.getMin());
    return new Pair<>(
        isAllAggregatorsHasFinalResult(aggregators) || isTsBlockOutOfBound, inputTsBlock);
  }

  private static TsBlock process(
      TsBlock inputTsBlock, TimeRange curTimeRange, List<TableAggregator> aggregators) {
    // Get the row which need to be processed by aggregator
    IWindow curWindow = new TimeWindow(curTimeRange);
    Column timeColumn = inputTsBlock.getTimeColumn();
    int lastIndexToProcess = 0;
    for (int i = 0; i < inputTsBlock.getPositionCount(); i++) {
      if (!curWindow.satisfy(timeColumn, i)) {
        break;
      }
      lastIndexToProcess = i;
    }

    TsBlock inputRegion = inputTsBlock.getRegion(0, lastIndexToProcess + 1);
    for (TableAggregator aggregator : aggregators) {
      // current agg method has been calculated
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processBlock(inputRegion);
    }
    int lastReadRowIndex = lastIndexToProcess + 1;
    if (lastReadRowIndex >= inputTsBlock.getPositionCount()) {
      return null;
    } else {
      return inputTsBlock.subTsBlock(lastReadRowIndex);
    }
  }

  public static boolean isAllAggregatorsHasFinalResult(List<TableAggregator> aggregators) {
    for (TableAggregator aggregator : aggregators) {
      if (!aggregator.hasFinalResult()) {
        return false;
      }
    }
    return true;
  }

  protected void calcFromStatistics(Statistics timeStatistics, Statistics[] valueStatistics) {
    for (TableAggregator aggregator : aggregators) {
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processStatistics(valueStatistics);
    }
  }

  boolean canUseStatistics = true;

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromFile() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (seriesScanUtil.hasNextFile()) {
      if (canUseStatistics && seriesScanUtil.canUseCurrentFileStatistics()) {
        Statistics fileTimeStatistics = seriesScanUtil.currentFileTimeStatistics();
        if (fileTimeStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentFile();
            continue;
          }
        }
        // calc from fileMetaData
        if (curTimeRange.contains(
            fileTimeStatistics.getStartTime(), fileTimeStatistics.getEndTime())) {
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentFileStatistics(i);
          }
          calcFromStatistics(fileTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentFile();
          if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read chunk
      if (readAndCalcFromChunk()) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromChunk() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (seriesScanUtil.hasNextChunk()) {
      if (canUseStatistics && seriesScanUtil.canUseCurrentChunkStatistics()) {
        Statistics chunkTimeStatistics = seriesScanUtil.currentChunkTimeStatistics();
        if (chunkTimeStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentChunk();
            continue;
          }
        }
        // calc from chunkMetaData
        if (curTimeRange.contains(
            chunkTimeStatistics.getStartTime(), chunkTimeStatistics.getEndTime())) {
          // calc from chunkMetaData
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentChunkStatistics(i);
          }
          calcFromStatistics(chunkTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentChunk();
          if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read page
      if (readAndCalcFromPage()) {
        return true;
      }
    }
    return false;
  }

  long leftRuntimeOfOneNextCall = Long.MAX_VALUE;

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromPage() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    try {
      while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextPage()) {
        if (canUseStatistics && seriesScanUtil.canUseCurrentPageStatistics()) {
          Statistics pageTimeStatistics = seriesScanUtil.currentPageTimeStatistics();
          // There is no more eligible points in current time range
          if (pageTimeStatistics.getStartTime() > curTimeRange.getMax()) {
            if (ascending) {
              return true;
            } else {
              seriesScanUtil.skipCurrentPage();
              continue;
            }
          }
          // can use pageHeader
          if (curTimeRange.contains(
              pageTimeStatistics.getStartTime(), pageTimeStatistics.getEndTime())) {
            Statistics[] statisticsList = new Statistics[subSensorSize];
            for (int i = 0; i < subSensorSize; i++) {
              statisticsList[i] = seriesScanUtil.currentPageStatistics(i);
            }
            calcFromStatistics(pageTimeStatistics, statisticsList);
            seriesScanUtil.skipCurrentPage();
            if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
              return true;
            } else {
              continue;
            }
          }
        }

        // calc from page data
        TsBlock tsBlock = seriesScanUtil.nextPage();
        if (tsBlock == null || tsBlock.isEmpty()) {
          continue;
        }

        // calc from raw data
        if (calcFromRawData(tsBlock)) {
          return true;
        }
      }
      return false;
    } finally {
      leftRuntimeOfOneNextCall -= (System.nanoTime() - start);
    }
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    //    List<TSDataType> dataTypes = new ArrayList<>();
    //    for (TableAggregator aggregator : aggregators) {
    //      dataTypes.add(aggregator.getType());
    //    }
    //    return dataTypes;
    return aggregators.stream().map(TableAggregator::getType).collect(Collectors.toList());
  }

  /** Append a row of aggregation results to the result tsBlock. */
  public static void appendAggregationResult(
      TsBlockBuilder tsBlockBuilder, List<? extends TableAggregator> aggregators) {
    // TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    // Use start time of current time range as time column
    // timeColumnBuilder.writeLong(outputTime);
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    int columnIndex = 0;
    //    if (endTime != INVALID_END_TIME) {
    //      columnBuilders[columnIndex].writeLong(endTime);
    //      columnIndex++;
    //    }
    for (int i = 0; i < aggregators.size(); i++) {
      //      TableAggregator aggregator = aggregators.get(i);
      //      ColumnBuilder[] columnBuilder = new ColumnBuilder[aggregator.get, length];
      //      columnBuilder[0] = columnBuilders[columnIndex++];
      //      if (columnBuilder.length > 1) {
      //        columnBuilder[1] = columnBuilders[columnIndex++];
      //      }
      aggregators.get(i).evaluate(columnBuilders[i]);
    }
    tsBlockBuilder.declarePosition();
  }
}
