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
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSeriesAggregationScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.window.IWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.TimeWindow;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.statistics.StringStatistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.satisfiedTimeRange;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.CURRENT_DEVICE_INDEX_STRING;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;
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
  private final Set<String> allSensors;

  private final List<IMeasurementSchema> measurementSchemas;

  private final List<TSDataType> measurementColumnTSDataTypes;

  // TODO calc maxTsBlockLineNum using date_bin
  private final int maxTsBlockLineNum;

  // for different aggregations aiming to same column, use this variable to point to same column
  private final List<Integer> aggArguments;

  private QueryDataSource queryDataSource;

  private int currentDeviceIndex;

  ITableTimeRangeIterator timeIterator;

  private boolean allAggregatorsHasFinalResult = false;

  public TableAggregationTableScanOperator(
      PlanNodeId sourceId,
      OperatorContext context,
      List<ColumnSchema> columnSchemas,
      int[] columnsIndexArray,
      List<DeviceEntry> deviceEntries,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      Set<String> allSensors,
      List<IMeasurementSchema> measurementSchemas,
      int maxTsBlockLineNum,
      int measurementCount,
      List<TableAggregator> tableAggregators,
      List<ColumnSchema> groupingKeySchemas,
      int[] groupingKeyIndex,
      ITableTimeRangeIterator tableTimeRangeIterator,
      boolean ascending,
      long maxReturnSize,
      boolean canUseStatistics,
      List<Integer> aggArguments) {

    super(
        sourceId,
        context,
        null,
        measurementCount,
        null,
        null,
        ascending,
        false,
        null,
        maxReturnSize,
        (1L + measurementCount) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte(),
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
    this.operatorContext.recordSpecifiedInfo(DEVICE_NUMBER, Integer.toString(this.deviceCount));
    this.scanOrder = scanOrder;
    this.seriesScanOptions = seriesScanOptions;
    this.measurementColumnNames = measurementColumnNames;
    this.allSensors = allSensors;
    this.measurementSchemas = measurementSchemas;
    this.measurementColumnTSDataTypes =
        measurementSchemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    this.currentDeviceIndex = 0;
    this.operatorContext.recordSpecifiedInfo(CURRENT_DEVICE_INDEX_STRING, Integer.toString(0));
    this.aggArguments = aggArguments;
    this.timeIterator = tableTimeRangeIterator;
    if (tableTimeRangeIterator.getType()
        == ITableTimeRangeIterator.TimeIteratorType.SINGLE_TIME_ITERATOR) {
      curTimeRange = new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    this.maxReturnSize = maxReturnSize;
    this.maxTsBlockLineNum = maxTsBlockLineNum;

    constructAlignedSeriesScanUtil();
  }

  @Override
  public boolean isFinished() throws Exception {
    if (!finished) {
      finished = !hasNextWithTimer();
    }
    return finished;

    //    return (retainedTsBlock == null)
    //        && (currentDeviceIndex >= deviceCount || seriesScanOptions.limitConsumedUp());
  }

  @Override
  public boolean hasNext() throws Exception {
    return timeIterator.hasCachedTimeRange()
        || timeIterator.hasNextTimeRange()
        || !resultTsBlockBuilder.isEmpty();
  }

  @Override
  public TsBlock next() throws Exception {

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
      if (calculateAggregationResultForCurrentTimeRange()) {
        timeIterator.resetCurTimeRange();
        // curTimeRange = null;
      }
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      return buildResultTsBlock();
    } else {
      return null;
    }
  }

  private TsBlock buildResultTsBlock() {
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

    TsBlock resultTsBlock =
        new TsBlock(
            resultTsBlockBuilder.getPositionCount(),
            new RunLengthEncodedColumn(
                TIME_COLUMN_TEMPLATE, resultTsBlockBuilder.getPositionCount()),
            valueColumns);
    resultTsBlockBuilder.reset();
    return resultTsBlock;
  }

  private void constructAlignedSeriesScanUtil() {
    DeviceEntry deviceEntry;

    if (this.deviceEntries.isEmpty() || this.deviceEntries.get(this.currentDeviceIndex) == null) {
      // for device which is not exist
      deviceEntry = new DeviceEntry(new StringArrayDeviceID(""), Collections.emptyList());
    } else {
      deviceEntry = this.deviceEntries.get(this.currentDeviceIndex);
    }

    AlignedFullPath alignedPath =
        constructAlignedPath(deviceEntry, measurementColumnNames, measurementSchemas, allSensors);

    this.seriesScanUtil =
        new AlignedSeriesScanUtil(
            alignedPath,
            scanOrder,
            seriesScanOptions,
            operatorContext.getInstanceContext(),
            true,
            measurementColumnTSDataTypes);
  }

  /** Return true if we have the result of this timeRange. */
  @Override
  protected boolean calculateAggregationResultForCurrentTimeRange() {
    try {
      if (calcFromCachedData()) {
        updateResultTsBlock();
        checkIfAllAggregatorHasFinalResult();
        return true;
      }

      if (readAndCalcFromPage()) {
        updateResultTsBlock();
        checkIfAllAggregatorHasFinalResult();
        return true;
      }

      // only when all the page data has been consumed, we need to read the chunk data
      if (!seriesScanUtil.hasNextPage() && readAndCalcFromChunk()) {
        updateResultTsBlock();
        checkIfAllAggregatorHasFinalResult();
        return true;
      }

      // only when all the page and chunk data has been consumed, we need to read the file data
      if (!seriesScanUtil.hasNextPage()
          && !seriesScanUtil.hasNextChunk()
          && readAndCalcFromFile()) {
        updateResultTsBlock();
        checkIfAllAggregatorHasFinalResult();
        return true;
      }

      // If the TimeRange is (Long.MIN_VALUE, Long.MAX_VALUE), for Aggregators like countAggregator,
      // we have to consume all the data before we finish the aggregation calculation.
      if (seriesScanUtil.hasNextPage()
          || seriesScanUtil.hasNextChunk()
          || seriesScanUtil.hasNextFile()) {
        return false;
      } else {
        // all data of current device has been consumed
        updateResultTsBlock();
        timeIterator.resetCurTimeRange();
        nextDevice();
      }

      if (currentDeviceIndex < deviceCount) {
        // construct AlignedSeriesScanUtil for next device
        constructAlignedSeriesScanUtil();
        queryDataSource.reset();
        this.seriesScanUtil.initQueryDataSource(queryDataSource);
      }

      if (currentDeviceIndex >= deviceCount) {
        // all devices have been consumed
        timeIterator.setFinished();
        return true;
      } else {
        return false;
      }
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult(resultTsBlockBuilder, tableAggregators);
    // after appendAggregationResult invoked, aggregators must be cleared
    resetTableAggregators();
  }

  @Override
  protected boolean calcFromCachedData() {
    return calcUsingRawData(inputTsBlock);
  }

  protected boolean calcUsingRawData(TsBlock tsBlock) {
    Pair<Boolean, TsBlock> calcResult = calculateAggregationFromRawData(tsBlock, ascending);
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
      TsBlock inputTsBlock, boolean ascending) {
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return new Pair<>(false, inputTsBlock);
    }

    updateCurTimeRange(inputTsBlock.getStartTime());

    TimeRange curTimeRange = timeIterator.getCurTimeRange();
    // check if the tsBlock does not contain points in current interval
    if (satisfiedTimeRange(inputTsBlock, curTimeRange, ascending)) {
      // skip points that cannot be calculated
      if ((ascending && inputTsBlock.getStartTime() < curTimeRange.getMin())
          || (!ascending && inputTsBlock.getStartTime() > curTimeRange.getMax())) {
        inputTsBlock = skipPointsOutOfTimeRange(inputTsBlock, curTimeRange, ascending);
      }

      inputTsBlock = process(inputTsBlock, curTimeRange);
    }

    // judge whether the calculation finished
    boolean isTsBlockOutOfBound =
        inputTsBlock != null
            && (ascending
                ? inputTsBlock.getEndTime() > curTimeRange.getMax()
                : inputTsBlock.getEndTime() < curTimeRange.getMin());
    return new Pair<>(
        isAllAggregatorsHasFinalResult(tableAggregators) || isTsBlockOutOfBound, inputTsBlock);
  }

  private TsBlock process(TsBlock inputTsBlock, TimeRange curTimeRange) {
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
    Column[] valueColumns = new Column[aggArguments.size()];
    for (int idx : aggArguments) {
      if (valueColumns[idx] != null) {
        continue;
      }
      valueColumns[idx] =
          buildValueColumn(columnSchemas.get(idx).getColumnCategory(), inputRegion, idx);
    }

    TsBlock tsBlock =
        new TsBlock(
            inputRegion.getPositionCount(),
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, inputRegion.getPositionCount()),
            valueColumns);

    for (TableAggregator aggregator : tableAggregators) {
      // current agg method has been calculated
      if (aggregator.hasFinalResult()) {
        continue;
      }

      aggregator.processBlock(tsBlock);
    }

    int lastReadRowIndex = lastIndexToProcess + 1;
    if (lastReadRowIndex >= inputTsBlock.getPositionCount()) {
      return null;
    } else {
      return inputTsBlock.subTsBlock(lastReadRowIndex);
    }
  }

  private Column buildValueColumn(
      TsTableColumnCategory columnSchemaCategory, TsBlock inputRegion, int columnIdx) {
    switch (columnSchemaCategory) {
      case TIME:
        return inputRegion.getTimeColumn();
      case ID:
        // TODO avoid create deviceStatics multi times; count, sum can use time statistics
        String id =
            (String)
                deviceEntries
                    .get(currentDeviceIndex)
                    .getNthSegment(columnsIndexArray[columnIdx] + 1);
        return getIdOrAttrColumn(
            inputRegion.getTimeColumn().getPositionCount(),
            id == null ? null : new Binary(id, TSFileConfig.STRING_CHARSET));
      case ATTRIBUTE:
        Binary attr =
            deviceEntries
                .get(currentDeviceIndex)
                .getAttributeColumnValues()
                .get(columnsIndexArray[columnIdx]);
        return getIdOrAttrColumn(inputRegion.getTimeColumn().getPositionCount(), attr);
      case MEASUREMENT:
        return inputRegion.getColumn(columnsIndexArray[columnIdx]);
      default:
        throw new IllegalStateException("Unsupported column type: " + columnSchemaCategory);
    }
  }

  private Column getIdOrAttrColumn(int positionCount, Binary columnName) {
    if (columnName == null) {
      return new RunLengthEncodedColumn(
          new BinaryColumn(1, Optional.of(new boolean[] {true}), new Binary[] {null}),
          positionCount);
    } else {
      return new RunLengthEncodedColumn(
          new BinaryColumn(1, Optional.of(new boolean[] {false}), new Binary[] {columnName}),
          positionCount);
    }
  }

  protected void calcFromStatistics(Statistics timeStatistics, Statistics[] valueStatistics) {
    int idx = -1;

    for (TableAggregator aggregator : tableAggregators) {
      if (aggregator.hasFinalResult()) {
        idx += aggregator.getChannelCount();
        continue;
      }

      Statistics[] statisticsArray = new Statistics[aggregator.getChannelCount()];
      for (int i = 0; i < aggregator.getChannelCount(); i++) {
        idx++;

        TsTableColumnCategory columnSchemaCategory =
            columnSchemas.get(aggArguments.get(idx)).getColumnCategory();
        statisticsArray[i] =
            buildStatistics(
                columnSchemaCategory, timeStatistics, valueStatistics, aggArguments.get(idx));
      }

      aggregator.processStatistics(statisticsArray);
    }
  }

  private Statistics buildStatistics(
      TsTableColumnCategory columnSchemaCategory,
      Statistics timeStatistics,
      Statistics[] valueStatistics,
      int columnIdx) {
    switch (columnSchemaCategory) {
      case TIME:
        return timeStatistics;
      case ID:
        // TODO avoid create deviceStatics multi times; count, sum can use time statistics
        String id =
            (String)
                deviceEntries
                    .get(currentDeviceIndex)
                    .getNthSegment(columnsIndexArray[columnIdx] + 1);
        return getStatistics(
            timeStatistics, id == null ? null : new Binary(id, TSFileConfig.STRING_CHARSET));
      case ATTRIBUTE:
        Binary attr =
            deviceEntries
                .get(currentDeviceIndex)
                .getAttributeColumnValues()
                .get(columnsIndexArray[columnIdx]);
        return getStatistics(timeStatistics, attr);
      case MEASUREMENT:
        return valueStatistics[columnsIndexArray[columnIdx]];
      default:
        throw new IllegalStateException("Unsupported column type: " + columnSchemaCategory);
    }
  }

  private Statistics getStatistics(Statistics timeStatistics, Binary columnName) {
    if (columnName == null) {
      return null;
    } else {
      StringStatistics stringStatics = new StringStatistics();
      stringStatics.setCount((int) timeStatistics.getCount());
      stringStatics.setStartTime(timeStatistics.getStartTime());
      stringStatics.setEndTime(timeStatistics.getEndTime());
      stringStatics.initializeStats(columnName, columnName, columnName, columnName);
      return stringStatics;
    }
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  @Override
  public boolean readAndCalcFromFile() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextFile()) {
      if (canUseStatistics && seriesScanUtil.canUseCurrentFileStatistics()) {
        Statistics fileTimeStatistics = seriesScanUtil.currentFileTimeStatistics();

        updateCurTimeRange(fileTimeStatistics.getStartTime());

        if (fileTimeStatistics.getStartTime() > timeIterator.getCurTimeRange().getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentFile();
            continue;
          }
        }

        // calc from fileMetaData
        if (timeIterator
            .getCurTimeRange()
            .contains(fileTimeStatistics.getStartTime(), fileTimeStatistics.getEndTime())) {
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentFileStatistics(i);
          }
          calcFromStatistics(fileTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentFile();
          if (isAllAggregatorsHasFinalResult(tableAggregators)) {
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

        updateCurTimeRange(chunkTimeStatistics.getStartTime());

        if (chunkTimeStatistics.getStartTime() > timeIterator.getCurTimeRange().getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentChunk();
            continue;
          }
        }

        // calc from chunkMetaData
        if (timeIterator
            .getCurTimeRange()
            .contains(chunkTimeStatistics.getStartTime(), chunkTimeStatistics.getEndTime())) {
          // calc from chunkMetaData
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentChunkStatistics(i);
          }
          calcFromStatistics(chunkTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentChunk();
          if (isAllAggregatorsHasFinalResult(tableAggregators)) {
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

          updateCurTimeRange(pageTimeStatistics.getStartTime());

          // There is no more eligible points in current time range
          // TODO(beyyes) will not appear in table model?
          if (pageTimeStatistics.getStartTime() > timeIterator.getCurTimeRange().getMax()) {
            if (ascending) {
              return true;
            } else {
              seriesScanUtil.skipCurrentPage();
              continue;
            }
          }

          // can use pageHeader
          if (timeIterator
              .getCurTimeRange()
              .contains(pageTimeStatistics.getStartTime(), pageTimeStatistics.getEndTime())) {
            Statistics[] statisticsList = new Statistics[subSensorSize];
            for (int i = 0; i < subSensorSize; i++) {
              statisticsList[i] = seriesScanUtil.currentPageStatistics(i);
            }
            calcFromStatistics(pageTimeStatistics, statisticsList);
            seriesScanUtil.skipCurrentPage();
            if (isAllAggregatorsHasFinalResult(tableAggregators)) {
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

        // calc from raw data
        if (calcUsingRawData(originalTsBlock)) {
          return true;
        }
      }

      return false;
    } finally {
      leftRuntimeOfOneNextCall -= (System.nanoTime() - start);
    }
  }

  private void updateCurTimeRange(long startTime) {
    if (timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.SINGLE_TIME_ITERATOR) {
      timeIterator.updateCurTimeRange(startTime);
      return;
    }

    if (!timeIterator.hasCachedTimeRange()) {
      timeIterator.updateCurTimeRange(startTime);
    } else if (timeIterator.canFinishCurrentTimeRange(startTime)) {
      updateResultTsBlock();
      timeIterator.resetCurTimeRange();
      timeIterator.updateCurTimeRange(startTime);
      resetTableAggregators();
    }
  }

  /** Append a row of aggregation results to the result tsBlock. */
  public void appendAggregationResult(
      TsBlockBuilder tsBlockBuilder, List<? extends TableAggregator> aggregators) {

    // no data in current time range, just output empty
    if (!timeIterator.hasCachedTimeRange()) {
      return;
    }

    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();

    int groupKeySize = groupingKeySchemas == null ? 0 : groupingKeySchemas.size();
    int dateBinSize =
        timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.DATE_BIN_TIME_ITERATOR
            ? 1
            : 0;

    if (groupingKeyIndex != null) {
      for (int i = 0; i < groupKeySize; i++) {
        if (TsTableColumnCategory.ID == groupingKeySchemas.get(i).getColumnCategory()) {
          String id =
              (String) deviceEntries.get(currentDeviceIndex).getNthSegment(groupingKeyIndex[i] + 1);
          if (id == null) {
            columnBuilders[i].appendNull();
          } else {
            columnBuilders[i].writeBinary(new Binary(id, TSFileConfig.STRING_CHARSET));
          }
        } else {
          Binary attribute =
              deviceEntries
                  .get(currentDeviceIndex)
                  .getAttributeColumnValues()
                  .get(groupingKeyIndex[i]);
          if (attribute == null) {
            columnBuilders[i].appendNull();
          } else {
            columnBuilders[i].writeBinary(attribute);
          }
        }
      }
    }

    if (dateBinSize > 0) {
      columnBuilders[groupKeySize].writeLong(timeIterator.getCurTimeRange().getMin());
    }

    for (int i = 0; i < aggregators.size(); i++) {
      aggregators.get(i).evaluate(columnBuilders[groupKeySize + dateBinSize + i]);
    }

    tsBlockBuilder.declarePosition();
  }

  public boolean isAllAggregatorsHasFinalResult(List<TableAggregator> aggregators) {
    // In groupByDateBin, we need read real data to calc next time range
    if (timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.DATE_BIN_TIME_ITERATOR) {
      return false;
    }

    // no aggregation function, just output ids or attributes
    if (aggregators.isEmpty()) {
      return false;
    }

    for (TableAggregator aggregator : aggregators) {
      if (!aggregator.hasFinalResult()) {
        return false;
      }
    }

    this.allAggregatorsHasFinalResult = true;
    return true;
  }

  private void checkIfAllAggregatorHasFinalResult() {
    if (allAggregatorsHasFinalResult
        && timeIterator.getType()
            == ITableTimeRangeIterator.TimeIteratorType.SINGLE_TIME_ITERATOR) {
      nextDevice();
      inputTsBlock = null;

      if (currentDeviceIndex < deviceCount) {
        // construct AlignedSeriesScanUtil for next device
        constructAlignedSeriesScanUtil();
        queryDataSource.reset();
        this.seriesScanUtil.initQueryDataSource(queryDataSource);
      }

      if (currentDeviceIndex >= deviceCount) {
        // all devices have been consumed
        timeIterator.setFinished();
      }

      allAggregatorsHasFinalResult = false;
    }
  }

  private void nextDevice() {
    currentDeviceIndex++;
    this.operatorContext.recordSpecifiedInfo(
        CURRENT_DEVICE_INDEX_STRING, Integer.toString(currentDeviceIndex));
  }

  private void resetTableAggregators() {
    tableAggregators.forEach(TableAggregator::reset);
  }

  @Override
  public List<TSDataType> getResultDataTypes() {
    int groupingKeySize = groupingKeySchemas != null ? groupingKeySchemas.size() : 0;
    int dateBinSize =
        timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.DATE_BIN_TIME_ITERATOR
            ? 1
            : 0;
    List<TSDataType> resultDataTypes =
        new ArrayList<>(groupingKeySize + dateBinSize + tableAggregators.size());

    if (groupingKeySchemas != null) {
      for (int i = 0; i < groupingKeySchemas.size(); i++) {
        resultDataTypes.add(TSDataType.STRING);
      }
    }
    if (dateBinSize > 0) {
      resultDataTypes.add(TSDataType.TIMESTAMP);
    }
    for (TableAggregator aggregator : tableAggregators) {
      resultDataTypes.add(aggregator.getType());
    }

    return resultDataTypes;
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSource) dataSource;
    this.seriesScanUtil.initQueryDataSource(queryDataSource);
    this.resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
    this.resultTsBlockBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
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
