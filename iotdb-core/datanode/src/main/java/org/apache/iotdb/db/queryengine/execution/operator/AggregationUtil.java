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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.SingleTimeWindowIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.TimeRangeIteratorFactory;
import org.apache.iotdb.db.queryengine.execution.operator.window.IWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.TimeWindow;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.statistics.StatisticsManager;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.tsfile.read.common.block.TsBlockUtil.skipPointsOutOfTimeRange;

public class AggregationUtil {
  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private static final int INVALID_END_TIME = -1;

  private static final String PARTIAL_SUFFIX = "_partial";

  private AggregationUtil() {
    // Forbidding instantiation
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
      return new SingleTimeWindowIterator(Long.MIN_VALUE, Long.MAX_VALUE);
    } else {
      return TimeRangeIteratorFactory.getTimeRangeIterator(
          groupByTimeParameter.getStartTime(),
          groupByTimeParameter.getEndTime(),
          groupByTimeParameter.getInterval(),
          groupByTimeParameter.getSlidingStep(),
          ascending,
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
      TsBlock inputTsBlock, TimeRange curTimeRange, List<Aggregator> aggregators) {
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
    for (Aggregator aggregator : aggregators) {
      // current agg method has been calculated
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processTsBlock(inputRegion, null);
    }
    int lastReadRowIndex = lastIndexToProcess + 1;
    if (lastReadRowIndex >= inputTsBlock.getPositionCount()) {
      return null;
    } else {
      return inputTsBlock.subTsBlock(lastReadRowIndex);
    }
  }

  /** Append a row of aggregation results to the result tsBlock. */
  public static void appendAggregationResult(
      TsBlockBuilder tsBlockBuilder,
      List<? extends Aggregator> aggregators,
      long outputTime,
      long endTime) {
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    // Use start time of current time range as time column
    timeColumnBuilder.writeLong(outputTime);
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    int columnIndex = 0;
    if (endTime != INVALID_END_TIME) {
      columnBuilders[columnIndex].writeLong(endTime);
      columnIndex++;
    }
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

  public static void appendAggregationResult(
      TsBlockBuilder tsBlockBuilder, List<? extends Aggregator> aggregators, long outputTime) {
    appendAggregationResult(tsBlockBuilder, aggregators, outputTime, INVALID_END_TIME);
  }

  /** return whether the tsBlock contains the data of the current time window. */
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
              .map(typeProvider::getTreeModelType)
              .collect(Collectors.toList());
      for (TSDataType tsDataType : outPutDataTypes) {
        timeValueColumnsSizePerLine += getOutputColumnSizePerLine(tsDataType);
      }
    }

    return Math.min(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        Math.min(
                TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber(),
                timeRangeIterator.getTotalIntervalNum())
            * timeValueColumnsSizePerLine);
  }

  public static long calculateMaxAggregationResultSizeForLastQuery(List<Aggregator> aggregators) {
    long timeValueColumnsSizePerLine = TimeColumn.SIZE_IN_BYTES_PER_POSITION;
    List<TSDataType> outPutDataTypes =
        aggregators.stream()
            .flatMap(aggregator -> Arrays.stream(aggregator.getOutputType()))
            .collect(Collectors.toList());
    for (TSDataType tsDataType : outPutDataTypes) {
      timeValueColumnsSizePerLine += getOutputColumnSizePerLine(tsDataType);
    }
    return timeValueColumnsSizePerLine;
  }

  public static long getOutputColumnSizePerLine(TSDataType tsDataType) {
    switch (tsDataType) {
      case INT32:
      case DATE:
        return IntColumn.SIZE_IN_BYTES_PER_POSITION;
      case INT64:
      case TIMESTAMP:
        return LongColumn.SIZE_IN_BYTES_PER_POSITION;
      case FLOAT:
        return FloatColumn.SIZE_IN_BYTES_PER_POSITION;
      case DOUBLE:
        return DoubleColumn.SIZE_IN_BYTES_PER_POSITION;
      case BOOLEAN:
        return BooleanColumn.SIZE_IN_BYTES_PER_POSITION;
      case TEXT:
      case BLOB:
      case STRING:
        return StatisticsManager.getInstance().getMaxBinarySizeInBytes();
      default:
        throw new UnsupportedOperationException("Unknown data type " + tsDataType);
    }
  }

  public static String addPartialSuffix(String aggregationName) {
    return aggregationName + PARTIAL_SUFFIX;
  }

  public static boolean isBuiltinAggregationName(String functionName) {
    return BuiltinAggregationFunction.getNativeFunctionNames().contains(functionName);
  }
}
