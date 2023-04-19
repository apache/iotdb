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

package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.SingleTimeWindowIterator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.TimeRangeIteratorFactory;
import org.apache.iotdb.db.mpp.execution.operator.window.IWindow;
import org.apache.iotdb.db.mpp.execution.operator.window.TimeWindow;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.statistics.StatisticsManager;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockUtil.skipPointsOutOfTimeRange;

public class AggregationUtil {

  private AggregationUtil() {
    // forbidding instantiation
  }

  /**
   * If groupByTimeParameter is null, which means it's an aggregation query without down sampling.
   * Aggregation query has only one time window and the result set of it does not contain a
   * timestamp, so it doesn't matter what the time range returns.
   */
  public static ITimeRangeIterator initTimeRangeIterator(
      GroupByTimeParameter groupByTimeParameter,
      boolean ascending,
      boolean outputPartialTimeWindow) {
    if (groupByTimeParameter == null) {
      return new SingleTimeWindowIterator(0, Long.MAX_VALUE);
    } else {
      return TimeRangeIteratorFactory.getTimeRangeIterator(
          groupByTimeParameter.getStartTime(),
          groupByTimeParameter.getEndTime(),
          groupByTimeParameter.getInterval(),
          groupByTimeParameter.getSlidingStep(),
          ascending,
          groupByTimeParameter.isIntervalByMonth(),
          groupByTimeParameter.isSlidingStepByMonth(),
          groupByTimeParameter.isLeftCRightO(),
          outputPartialTimeWindow);
    }
  }

  /**
   * Calculate aggregation value on the time range from the tsBlock containing raw data.
   *
   * @return left - whether the aggregation calculation of the current time range has done; right -
   *     remaining tsBlock
   */
  public static Pair<Boolean, TsBlock> calculateAggregationFromRawData(
      TsBlock inputTsBlock,
      List<Aggregator> aggregators,
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

      // Get the row which need to be processed by aggregator
      IWindow curWindow = new TimeWindow(curTimeRange);
      TimeColumn timeColumn = inputTsBlock.getTimeColumn();
      int lastIndexToProcess = 0;
      for (int i = 0; i < inputTsBlock.getPositionCount(); i++) {
        if (!curWindow.satisfy(timeColumn, i)) {
          break;
        }
        lastIndexToProcess = i;
      }

      for (Aggregator aggregator : aggregators) {
        // current agg method has been calculated
        if (aggregator.hasFinalResult()) {
          continue;
        }

        aggregator.processTsBlock(inputTsBlock, null, lastIndexToProcess);
      }
      int lastReadRowIndex = lastIndexToProcess + 1;
      if (lastReadRowIndex >= inputTsBlock.getPositionCount()) {
        inputTsBlock = null;
      } else {
        inputTsBlock = inputTsBlock.subTsBlock(lastReadRowIndex);
      }
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

  /** Append a row of aggregation results to the result tsBlock. */
  public static void appendAggregationResult(
      TsBlockBuilder tsBlockBuilder, List<? extends Aggregator> aggregators, long outputTime) {
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    // Use start time of current time range as time column
    timeColumnBuilder.writeLong(outputTime);
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    int columnIndex = 0;
    for (Aggregator aggregator : aggregators) {
      ColumnBuilder[] columnBuilder = new ColumnBuilder[aggregator.getOutputType().length];
      columnBuilder[0] = columnBuilders[columnIndex++];
      if (columnBuilder.length > 1) {
        columnBuilder[1] = columnBuilders[columnIndex++];
      }
      aggregator.outputResult(columnBuilder);
    }
    tsBlockBuilder.declarePosition();
  }

  /** @return whether the tsBlock contains the data of the current time window */
  public static boolean satisfiedTimeRange(
      TsBlock tsBlock, TimeRange curTimeRange, boolean ascending) {
    if (tsBlock == null || tsBlock.isEmpty()) {
      return false;
    }

    return ascending
        ? (tsBlock.getEndTime() >= curTimeRange.getMin()
            && tsBlock.getStartTime() <= curTimeRange.getMax())
        : (tsBlock.getEndTime() <= curTimeRange.getMax()
            && tsBlock.getStartTime() >= curTimeRange.getMin());
  }

  public static boolean isAllAggregatorsHasFinalResult(List<Aggregator> aggregators) {
    for (Aggregator aggregator : aggregators) {
      if (!aggregator.hasFinalResult()) {
        return false;
      }
    }
    return true;
  }

  public static long calculateMaxAggregationResultSize(
      List<? extends AggregationDescriptor> aggregationDescriptors,
      ITimeRangeIterator timeRangeIterator,
      TypeProvider typeProvider) {
    long timeValueColumnsSizePerLine = TimeColumn.SIZE_IN_BYTES_PER_POSITION;
    for (AggregationDescriptor descriptor : aggregationDescriptors) {
      List<TSDataType> outPutDataTypes =
          descriptor.getOutputColumnNames().stream()
              .map(typeProvider::getType)
              .collect(Collectors.toList());
      for (TSDataType tsDataType : outPutDataTypes) {
        // TODO modify after statistics finish
        PartialPath mockSeriesPath = new PartialPath();
        timeValueColumnsSizePerLine += getOutputColumnSizePerLine(tsDataType, mockSeriesPath);
      }
    }

    return Math.min(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        Math.min(
                TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber(),
                timeRangeIterator.getTotalIntervalNum())
            * timeValueColumnsSizePerLine);
  }

  public static long calculateMaxAggregationResultSizeForLastQuery(
      List<Aggregator> aggregators, PartialPath inputSeriesPath) {
    long timeValueColumnsSizePerLine = TimeColumn.SIZE_IN_BYTES_PER_POSITION;
    List<TSDataType> outPutDataTypes =
        aggregators.stream()
            .flatMap(aggregator -> Arrays.stream(aggregator.getOutputType()))
            .collect(Collectors.toList());
    for (TSDataType tsDataType : outPutDataTypes) {
      timeValueColumnsSizePerLine += getOutputColumnSizePerLine(tsDataType, inputSeriesPath);
    }
    return timeValueColumnsSizePerLine;
  }

  private static long getOutputColumnSizePerLine(
      TSDataType tsDataType, PartialPath inputSeriesPath) {
    switch (tsDataType) {
      case INT32:
        return IntColumn.SIZE_IN_BYTES_PER_POSITION;
      case INT64:
        return LongColumn.SIZE_IN_BYTES_PER_POSITION;
      case FLOAT:
        return FloatColumn.SIZE_IN_BYTES_PER_POSITION;
      case DOUBLE:
        return DoubleColumn.SIZE_IN_BYTES_PER_POSITION;
      case BOOLEAN:
        return BooleanColumn.SIZE_IN_BYTES_PER_POSITION;
      case TEXT:
        return StatisticsManager.getInstance().getMaxBinarySizeInBytes(inputSeriesPath);
      default:
        throw new UnsupportedOperationException("Unknown data type " + tsDataType);
    }
  }
}
