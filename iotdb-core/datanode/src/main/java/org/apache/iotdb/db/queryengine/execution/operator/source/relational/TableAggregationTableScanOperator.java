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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
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

import org.apache.commons.lang3.stream.Streams;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.satisfiedTimeRange;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.tsfile.read.common.block.TsBlockUtil.skipPointsOutOfTimeRange;

public class TableAggregationTableScanOperator extends AbstractSeriesAggregationScanOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableAggregationTableScanOperator.class);

  private final List<TableAggregator> tableAggregators;

  private final List<ColumnSchema> groupingKeySchemas;
  private final int[] groupingKeyIndex;

  public static final LongColumn TIME_COLUMN_TEMPLATE =
      new LongColumn(1, Optional.empty(), new long[] {0});

  private final List<ColumnSchema> columnSchemas;

  private final int[] columnsIndexArray;

  private final List<DeviceEntry> deviceEntries;

  private final int deviceCount;

  private final Ordering scanOrder;
  private final SeriesScanOptions seriesScanOptions;

  private final List<String> measurementColumnNames;

  private final List<IMeasurementSchema> measurementSchemas;

  private final List<TSDataType> measurementColumnTSDataTypes;

  // TODO calc maxTsBlockLineNum using date_bin
  private final int maxTsBlockLineNum;

  // for different aggregations aiming to same column, use this variable to point to same column
  private final int[] layoutArray;

  private QueryDataSource queryDataSource;

  private int currentDeviceIndex;

  public TableAggregationTableScanOperator(
      PlanNodeId sourceId,
      OperatorContext context,
      List<ColumnSchema> columnSchemas,
      int[] columnsIndexArray,
      List<DeviceEntry> deviceEntries,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      List<IMeasurementSchema> measurementSchemas,
      int maxTsBlockLineNum,
      int subSensorSize,
      List<TableAggregator> tableAggregators,
      List<ColumnSchema> groupingKeySchemas,
      int[] groupingKeyIndex,
      ITimeRangeIterator timeRangeIterator,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter,
      long maxReturnSize,
      boolean canUseStatistics,
      int[] layoutArray) {

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

    this.tableAggregators = tableAggregators;
    this.groupingKeySchemas = groupingKeySchemas;
    this.groupingKeyIndex = groupingKeyIndex;

    this.sourceId = sourceId;
    this.operatorContext = context;
    this.columnSchemas = columnSchemas;
    this.columnsIndexArray = columnsIndexArray;
    this.deviceEntries = deviceEntries;
    this.deviceCount = deviceEntries.size();
    this.scanOrder = scanOrder;
    this.seriesScanOptions = seriesScanOptions;
    this.measurementColumnNames = measurementColumnNames;
    this.measurementSchemas = measurementSchemas;
    this.measurementColumnTSDataTypes =
        measurementSchemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    this.currentDeviceIndex = 0;
    this.layoutArray = layoutArray;

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

    // optimize for sql: select count(*) from (select count(s1), sum(s1) from table)
    if (tableAggregators.isEmpty()) {
      retainedTsBlock = null;
      currentDeviceIndex = deviceCount;
      Column[] valueColumns = new Column[0];
      return new TsBlock(1, new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, 1), valueColumns);
    }

    // start stopwatch, reset leftRuntimeOfOneNextCall
    long start = System.nanoTime();
    leftRuntimeOfOneNextCall = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long maxRuntime = leftRuntimeOfOneNextCall;

    while (System.nanoTime() - start < maxRuntime
        && (curTimeRange != null || timeRangeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {
      if (curTimeRange == null) {
        // move to the next time window
        curTimeRange = timeRangeIterator.nextTimeRange();
        // clear previous aggregation result
        for (TableAggregator aggregator : tableAggregators) {
          aggregator.reset();
        }
      }

      // calculate aggregation result on current time window
      // Keep curTimeRange if the calculation of this timeRange is not done
      if (calculateAggregationResultForCurrentTimeRange()) {
        // updateResultTsBlock();
        curTimeRange = null;
      }
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      int declaredPositions = resultTsBlockBuilder.getPositionCount();
      ColumnBuilder[] valueColumnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
      Column[] valueColumns = new Column[valueColumnBuilders.length];
      for (int i = 0; i < valueColumns.length; i++) {
        valueColumns[i] = valueColumnBuilders[i].build();
        if (valueColumns[i].getPositionCount() != declaredPositions) {
          throw new IllegalStateException(
              format(
                  "Declared positions (%s) does not match column %s's number of entries (%s)",
                  declaredPositions, i, valueColumns[i].getPositionCount()));
        }
      }

      this.resultTsBlock =
          new TsBlock(
              resultTsBlockBuilder.getPositionCount(),
              new RunLengthEncodedColumn(
                  TIME_COLUMN_TEMPLATE, resultTsBlockBuilder.getPositionCount()),
              valueColumns);
      resultTsBlockBuilder.reset();
      return this.resultTsBlock;
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
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes())
        + RamUsageEstimator.sizeOfCollection(deviceEntries);
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
        updateResultTsBlock();
        for (TableAggregator aggregator : tableAggregators) {
          aggregator.reset();
        }
        currentDeviceIndex++;
      }
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
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult(resultTsBlockBuilder, tableAggregators);
  }

  protected boolean calcFromCachedData() {
    return calcFromRawData(inputTsBlock);
  }

  private boolean calcFromRawData(TsBlock tsBlock) {
    Pair<Boolean, TsBlock> calcResult =
        calculateAggregationFromRawData(tsBlock, tableAggregators, curTimeRange, ascending);
    inputTsBlock = calcResult.getRight();
    return calcResult.getLeft();
  }

  /**
   * Calculate aggregation value on the time range from the tsBlock containing raw data.
   *
   * @return left - whether the aggregation calculation of the current time range has done; right -
   *     remaining tsBlock
   */
  public Pair<Boolean, TsBlock> calculateAggregationFromRawData(
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

  private TsBlock process(
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
    for (int i = 0; i < aggregators.size(); i++) {
      if (isNullIdOrAttribute(i)) {
        continue;
      }

      TableAggregator aggregator = aggregators.get(i);
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

  /** ID or Attribute is null, skip this aggregation logic */
  private boolean isNullIdOrAttribute(int i) {
    if (TsTableColumnCategory.ID == columnSchemas.get(layoutArray[i]).getColumnCategory()
        && deviceEntries.get(currentDeviceIndex).getNthSegment(columnsIndexArray[i] + 1) == null) {
      return true;
    }

    return TsTableColumnCategory.ATTRIBUTE == columnSchemas.get(layoutArray[i]).getColumnCategory()
        && deviceEntries
                .get(currentDeviceIndex)
                .getAttributeColumnValues()
                .get(columnsIndexArray[i])
            == null;
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
    for (int i = 0; i < tableAggregators.size(); i++) {
      if (isNullIdOrAttribute(i)) {
        continue;
      }

      TableAggregator aggregator = tableAggregators.get(i);
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processStatistics(
          columnSchemas.get(layoutArray[i]).getColumnCategory() == TsTableColumnCategory.MEASUREMENT
              ? valueStatistics[columnsIndexArray[i]]
              : timeStatistics);
    }
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromFile() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextFile()) {
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
          if (isAllAggregatorsHasFinalResult(tableAggregators) && !isGroupByQuery) {
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
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextChunk()) {
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
          if (isAllAggregatorsHasFinalResult(tableAggregators) && !isGroupByQuery) {
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
            if (isAllAggregatorsHasFinalResult(tableAggregators) && !isGroupByQuery) {
              return true;
            } else {
              continue;
            }
          }
        }

        // calc from page data
        TsBlock originalTsBlock = seriesScanUtil.nextPage();
        if (originalTsBlock == null) {
          continue;
        }
        // TODO(beyyes) add optimization: if only agg measurement columns, no need to transfer
        Column[] valueColumns = new Column[tableAggregators.size()];
        for (int i = 0; i < tableAggregators.size(); i++) {
          ColumnSchema columnSchema = columnSchemas.get(layoutArray[i]);
          if (Streams.of(
                  TsTableColumnCategory.ID,
                  TsTableColumnCategory.ATTRIBUTE,
                  TsTableColumnCategory.TIME)
              .anyMatch(columnSchema.getColumnCategory()::equals)) {
            valueColumns[i] = originalTsBlock.getTimeColumn();
          } else {
            valueColumns[i] = originalTsBlock.getColumn(i);
          }
        }
        TsBlock tsBlock =
            new TsBlock(
                originalTsBlock.getPositionCount(),
                new RunLengthEncodedColumn(
                    TIME_COLUMN_TEMPLATE, originalTsBlock.getPositionCount()),
                valueColumns);
        if (tsBlock.isEmpty()) {
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
    List<TSDataType> resultDataTypes =
        new ArrayList<>(
            (groupingKeyIndex != null ? groupingKeyIndex.length : 0) + tableAggregators.size());
    if (groupingKeyIndex != null) {
      for (int i = 0; i < groupingKeyIndex.length; i++) {
        resultDataTypes.add(TSDataType.TEXT);
      }
    }

    for (TableAggregator aggregator : tableAggregators) {
      resultDataTypes.add(aggregator.getType());
    }

    return resultDataTypes;
  }

  /** Append a row of aggregation results to the result tsBlock. */
  public void appendAggregationResult(
      TsBlockBuilder tsBlockBuilder, List<? extends TableAggregator> aggregators) {
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();

    if (groupingKeyIndex != null) {
      for (int i = 0; i < groupingKeyIndex.length; i++) {
        if (TsTableColumnCategory.ID == groupingKeySchemas.get(i).getColumnCategory()) {
          columnBuilders[i].writeBinary(
              (Binary)
                  deviceEntries.get(currentDeviceIndex).getNthSegment(groupingKeyIndex[i] + 1));
        } else {
          columnBuilders[i].writeBinary(
              new Binary(
                  deviceEntries
                      .get(currentDeviceIndex)
                      .getAttributeColumnValues()
                      .get(groupingKeyIndex[i])
                      .getBytes()));
        }
      }
    }

    int groupKeyLength = groupingKeyIndex == null ? 0 : groupingKeyIndex.length;

    for (int i = 0; i < aggregators.size(); i++) {
      aggregators.get(groupKeyLength + i).evaluate(columnBuilders[groupKeyLength + i]);
    }

    tsBlockBuilder.declarePosition();
  }
}
