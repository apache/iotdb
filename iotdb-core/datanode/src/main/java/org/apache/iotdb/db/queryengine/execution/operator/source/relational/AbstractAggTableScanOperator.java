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
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.window.IWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.TimeWindow;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
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
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.statistics.StringStatistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.satisfiedTimeRange;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.CURRENT_DEVICE_INDEX_STRING;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;
import static org.apache.tsfile.read.common.block.TsBlockUtil.skipPointsOutOfTimeRange;

public abstract class AbstractAggTableScanOperator extends AbstractDataSourceOperator {

  private boolean finished = false;
  private TsBlock inputTsBlock;

  protected List<TableAggregator> tableAggregators;
  protected final List<ColumnSchema> groupingKeySchemas;
  protected final int[] groupingKeyIndex;
  protected final int groupingKeySize;
  protected final int dateBinSize;

  protected final List<DeviceEntry> deviceEntries;
  protected final int deviceCount;
  protected int currentDeviceIndex;
  protected List<String> measurementColumnNames;
  protected Set<String> allSensors;
  protected List<IMeasurementSchema> measurementSchemas;
  protected List<TSDataType> measurementColumnTSDataTypes;
  protected int measurementCount;

  // distinct column schemas appeared in aggregation function
  protected List<ColumnSchema> aggColumnSchemas;
  // length of aggColumnsIndexArray equals the size of aggColumnSchemas
  protected int[] aggColumnsIndexArray;

  protected SeriesScanOptions seriesScanOptions;
  private final boolean ascending;
  private final Ordering scanOrder;
  // Some special data types(like BLOB) cannot use statistics
  protected final boolean canUseStatistics;
  private final long cachedRawDataSize;

  // stores all inputChannels of tableAggregators,
  // e.g. for aggregation `last(s1), count(s2), count(s1)`, the inputChannels should be [0, 1, 0]
  protected List<Integer> aggregatorInputChannels;

  private QueryDataSource queryDataSource;

  protected ITableTimeRangeIterator timeIterator;

  private boolean allAggregatorsHasFinalResult = false;

  protected AbstractAggTableScanOperator(AbstractAggTableScanOperatorParameter parameter) {

    this.sourceId = parameter.sourceId;
    this.operatorContext = parameter.context;
    this.canUseStatistics = parameter.canUseStatistics;
    this.tableAggregators = parameter.tableAggregators;
    this.groupingKeySchemas = parameter.groupingKeySchemas;
    this.groupingKeyIndex = parameter.groupingKeyIndex;
    this.groupingKeySize =
        parameter.groupingKeySchemas == null ? 0 : parameter.groupingKeySchemas.size();
    this.aggColumnSchemas = parameter.aggColumnSchemas;
    this.aggColumnsIndexArray = parameter.aggColumnsIndexArray;
    this.deviceEntries = parameter.deviceEntries;
    this.deviceCount = parameter.deviceCount;
    this.operatorContext.recordSpecifiedInfo(DEVICE_NUMBER, Integer.toString(this.deviceCount));
    this.ascending = parameter.ascending;
    this.scanOrder = parameter.ascending ? Ordering.ASC : Ordering.DESC;
    this.seriesScanOptions = parameter.seriesScanOptions;
    this.measurementColumnNames = parameter.measurementColumnNames;
    this.measurementCount = measurementColumnNames.size();
    this.cachedRawDataSize =
        (1L + this.measurementCount)
            * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.allSensors = parameter.allSensors;
    this.measurementSchemas = parameter.measurementSchemas;
    this.measurementColumnTSDataTypes =
        parameter.measurementSchemas.stream()
            .map(IMeasurementSchema::getType)
            .collect(Collectors.toList());
    this.currentDeviceIndex = 0;
    this.operatorContext.recordSpecifiedInfo(CURRENT_DEVICE_INDEX_STRING, Integer.toString(0));
    this.aggregatorInputChannels = parameter.aggregatorInputChannels;
    this.timeIterator = parameter.tableTimeRangeIterator;
    this.dateBinSize =
        timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.DATE_BIN_TIME_ITERATOR
            ? 1
            : 0;

    constructAlignedSeriesScanUtil();
  }

  @Override
  public boolean isFinished() throws Exception {
    if (!finished) {
      finished = !hasNextWithTimer();
    }
    return finished;
  }

  protected abstract void updateResultTsBlock();

  protected void buildResultTsBlock() {
    resultTsBlock =
        resultTsBlockBuilder.build(
            new RunLengthEncodedColumn(
                TIME_COLUMN_TEMPLATE, resultTsBlockBuilder.getPositionCount()));
    resultTsBlockBuilder.reset();
  }

  protected void constructAlignedSeriesScanUtil() {
    DeviceEntry deviceEntry;

    if (this.deviceEntries.isEmpty() || this.deviceEntries.get(this.currentDeviceIndex) == null) {
      // for device which is not exist
      deviceEntry = new AlignedDeviceEntry(SeriesScanUtil.EMPTY_DEVICE_ID, new Binary[0]);
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
  protected Optional<Boolean> calculateAggregationResultForCurrentTimeRange() {
    try {
      if (calcFromCachedData()) {
        updateResultTsBlock();
        checkIfAllAggregatorHasFinalResult();
        return Optional.of(true);
      }

      if (readAndCalcFromPage()) {
        updateResultTsBlock();
        checkIfAllAggregatorHasFinalResult();
        return Optional.of(true);
      }

      // only when all the page data has been consumed, we need to read the chunk data
      if (!seriesScanUtil.hasNextPage() && readAndCalcFromChunk()) {
        updateResultTsBlock();
        checkIfAllAggregatorHasFinalResult();
        return Optional.of(true);
      }

      // only when all the page and chunk data has been consumed, we need to read the file data
      if (!seriesScanUtil.hasNextPage()) {
        Optional<Boolean> b = seriesScanUtil.hasNextChunk();
        if (!b.isPresent()) {
          return b;
        }
        if (!b.get() && readAndCalcFromFile()) {
          updateResultTsBlock();
          checkIfAllAggregatorHasFinalResult();
          return Optional.of(true);
        }
      }

      // If the TimeRange is (Long.MIN_VALUE, Long.MAX_VALUE), for Aggregators like countAggregator,
      // we have to consume all the data before we finish the aggregation calculation.
      if (seriesScanUtil.hasNextPage()) {
        return Optional.of(false);
      }
      Optional<Boolean> b = seriesScanUtil.hasNextChunk();
      if (!b.isPresent()) {
        return b;
      }
      if (b.get()) {
        return Optional.of(false);
      }
      b = seriesScanUtil.hasNextFile();
      if (!b.isPresent()) {
        return b;
      }
      if (b.get()) {
        return Optional.of(false);
      }
      // all data of current device has been consumed
      updateResultTsBlock();
      timeIterator.resetCurTimeRange();
      nextDevice();

      if (currentDeviceIndex < deviceCount) {
        // construct AlignedSeriesScanUtil for next device
        constructAlignedSeriesScanUtil();
        queryDataSource.reset();
        this.seriesScanUtil.initQueryDataSource(queryDataSource);
      }

      if (currentDeviceIndex >= deviceCount) {
        // all devices have been consumed
        timeIterator.setFinished();
        return Optional.of(true);
      } else {
        return Optional.of(false);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

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
    Column[] valueColumns = new Column[aggregatorInputChannels.size()];
    for (int idx : aggregatorInputChannels) {
      if (valueColumns[idx] != null) {
        continue;
      }
      valueColumns[idx] =
          buildValueColumn(aggColumnSchemas.get(idx).getColumnCategory(), inputRegion, idx);
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
      case TAG:
        String id =
            getNthIdColumnValue(
                deviceEntries.get(currentDeviceIndex), aggColumnsIndexArray[columnIdx]);
        return getIdOrAttrColumn(
            inputRegion.getTimeColumn().getPositionCount(),
            id == null ? null : new Binary(id, TSFileConfig.STRING_CHARSET));
      case ATTRIBUTE:
        Binary attr =
            deviceEntries.get(currentDeviceIndex)
                .getAttributeColumnValues()[aggColumnsIndexArray[columnIdx]];
        return getIdOrAttrColumn(inputRegion.getTimeColumn().getPositionCount(), attr);
      case FIELD:
        return inputRegion.getColumn(aggColumnsIndexArray[columnIdx]);
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
            aggColumnSchemas.get(aggregatorInputChannels.get(idx)).getColumnCategory();
        statisticsArray[i] =
            buildStatistics(
                columnSchemaCategory,
                timeStatistics,
                valueStatistics,
                aggregatorInputChannels.get(idx));
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
      case TAG:
        String id =
            getNthIdColumnValue(
                deviceEntries.get(currentDeviceIndex), aggColumnsIndexArray[columnIdx]);
        return getStatistics(
            timeStatistics, id == null ? null : new Binary(id, TSFileConfig.STRING_CHARSET));
      case ATTRIBUTE:
        Binary attr =
            deviceEntries.get(currentDeviceIndex)
                .getAttributeColumnValues()[aggColumnsIndexArray[columnIdx]];
        return getStatistics(timeStatistics, attr);
      case FIELD:
        return valueStatistics[aggColumnsIndexArray[columnIdx]];
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
  public boolean readAndCalcFromFile() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall) {
      Optional<Boolean> b = seriesScanUtil.hasNextFile();
      if (!b.isPresent()) {
        continue;
      }
      if (!b.get()) {
        break;
      }
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
          Statistics[] statisticsList = new Statistics[measurementCount];
          for (int i = 0; i < measurementCount; i++) {
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
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall) {
      Optional<Boolean> b = seriesScanUtil.hasNextChunk();
      if (!b.isPresent()) {
        continue;
      }
      if (!b.get()) {
        break;
      }
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
          Statistics[] statisticsList = new Statistics[measurementCount];
          for (int i = 0; i < measurementCount; i++) {
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
            Statistics[] statisticsList = new Statistics[measurementCount];
            for (int i = 0; i < measurementCount; i++) {
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
  protected void appendAggregationResult() {
    // no data in current time range, just output empty
    if (!timeIterator.hasCachedTimeRange()) {
      return;
    }

    appendGroupKeysToResult(deviceEntries, currentDeviceIndex);

    if (dateBinSize > 0) {
      resultTsBlockBuilder.getValueColumnBuilders()[groupingKeySize].writeLong(
          timeIterator.getCurTimeRange().getMin());
    }

    for (int i = 0; i < tableAggregators.size(); i++) {
      tableAggregators
          .get(i)
          .evaluate(
              resultTsBlockBuilder.getValueColumnBuilders()[groupingKeySize + dateBinSize + i]);
    }

    resultTsBlockBuilder.declarePosition();
  }

  protected void appendGroupKeysToResult(List<DeviceEntry> deviceEntries, int deviceIndex) {
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    for (int i = 0; i < groupingKeySize; i++) {
      if (TsTableColumnCategory.TAG == groupingKeySchemas.get(i).getColumnCategory()) {
        String id = getNthIdColumnValue(deviceEntries.get(deviceIndex), groupingKeyIndex[i]);
        if (id == null) {
          columnBuilders[i].appendNull();
        } else {
          columnBuilders[i].writeBinary(new Binary(id, TSFileConfig.STRING_CHARSET));
        }
      } else {
        Binary attribute =
            deviceEntries.get(deviceIndex).getAttributeColumnValues()[groupingKeyIndex[i]];
        if (attribute == null) {
          columnBuilders[i].appendNull();
        } else {
          columnBuilders[i].writeBinary(attribute);
        }
      }
    }
  }

  public boolean isAllAggregatorsHasFinalResult(List<TableAggregator> aggregators) {
    // In groupByDateBin, we need read real data to calc next time range
    if (timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.DATE_BIN_TIME_ITERATOR) {
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
        && (timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.SINGLE_TIME_ITERATOR
            || tableAggregators.isEmpty())) {
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

  protected void resetTableAggregators() {
    tableAggregators.forEach(TableAggregator::reset);
  }

  @Override
  public List<TSDataType> getResultDataTypes() {
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
  }

  @Override
  public long calculateMaxPeekMemory() {
    return cachedRawDataSize + maxReturnSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return timeIterator.getType() == ITableTimeRangeIterator.TimeIteratorType.DATE_BIN_TIME_ITERATOR
        ? cachedRawDataSize
        : 0;
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableAggregators.forEach(TableAggregator::close);
  }

  abstract String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex);

  public static class AbstractAggTableScanOperatorParameter {
    private final String timeColumnName;
    protected final PlanNodeId sourceId;
    protected final OperatorContext context;
    protected final List<ColumnSchema> aggColumnSchemas;
    protected final int[] aggColumnsIndexArray;
    protected final SeriesScanOptions seriesScanOptions;
    protected final List<String> measurementColumnNames;
    protected final Set<String> allSensors;
    protected final List<IMeasurementSchema> measurementSchemas;
    protected final List<TableAggregator> tableAggregators;
    protected final List<ColumnSchema> groupingKeySchemas;
    protected final int[] groupingKeyIndex;
    protected final ITableTimeRangeIterator tableTimeRangeIterator;
    protected final boolean ascending;
    protected final boolean canUseStatistics;
    protected final List<Integer> aggregatorInputChannels;

    protected List<DeviceEntry> deviceEntries;
    protected int deviceCount;

    public AbstractAggTableScanOperatorParameter(
        PlanNodeId sourceId,
        OperatorContext context,
        List<ColumnSchema> aggColumnSchemas,
        int[] aggColumnsIndexArray,
        List<DeviceEntry> deviceEntries,
        int deviceCount,
        SeriesScanOptions seriesScanOptions,
        List<String> measurementColumnNames,
        Set<String> allSensors,
        List<IMeasurementSchema> measurementSchemas,
        List<TableAggregator> tableAggregators,
        List<ColumnSchema> groupingKeySchemas,
        int[] groupingKeyIndex,
        ITableTimeRangeIterator tableTimeRangeIterator,
        boolean ascending,
        boolean canUseStatistics,
        List<Integer> aggregatorInputChannels,
        String timeColumnName) {
      this.sourceId = sourceId;
      this.context = context;
      this.aggColumnSchemas = aggColumnSchemas;
      this.aggColumnsIndexArray = aggColumnsIndexArray;
      this.deviceEntries = deviceEntries;
      this.deviceCount = deviceCount;
      this.seriesScanOptions = seriesScanOptions;
      this.measurementColumnNames = measurementColumnNames;
      this.allSensors = allSensors;
      this.measurementSchemas = measurementSchemas;
      this.tableAggregators = tableAggregators;
      this.groupingKeySchemas = groupingKeySchemas;
      this.groupingKeyIndex = groupingKeyIndex;
      this.tableTimeRangeIterator = tableTimeRangeIterator;
      this.ascending = ascending;
      this.canUseStatistics = canUseStatistics;
      this.aggregatorInputChannels = aggregatorInputChannels;
      this.timeColumnName = timeColumnName;
    }

    public OperatorContext getOperatorContext() {
      return context;
    }

    public List<TableAggregator> getTableAggregators() {
      return tableAggregators;
    }

    public SeriesScanOptions getSeriesScanOptions() {
      return seriesScanOptions;
    }

    public Set<String> getAllSensors() {
      return allSensors;
    }

    public List<String> getMeasurementColumnNames() {
      return measurementColumnNames;
    }

    public List<IMeasurementSchema> getMeasurementSchemas() {
      return measurementSchemas;
    }

    public String getTimeColumnName() {
      return timeColumnName;
    }

    public void setDeviceEntries(List<DeviceEntry> deviceEntries) {
      this.deviceEntries = deviceEntries;
      this.deviceCount = deviceEntries.size();
    }
  }
}
