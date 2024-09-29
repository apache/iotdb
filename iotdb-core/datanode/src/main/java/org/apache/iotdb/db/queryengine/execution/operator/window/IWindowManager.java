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

package org.apache.iotdb.db.queryengine.execution.operator.window;

import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Used to customize all the type of window managers, such as TimeWindowManager,
 * SessionWindowManager.
 */
public interface IWindowManager {

  /**
   * Judge whether the current window is initialized.
   *
   * @return whether the current window is initialized
   */
  boolean isCurWindowInit();

  /** Used to initialize the status of window. */
  void initCurWindow();

  /**
   * Used to determine whether there is a next window.
   *
   * @return whether there is a next window
   */
  boolean hasNext(boolean hasMoreData);

  /** Used to mark the current window has got last point. */
  void next();

  /**
   * Used to get current window.
   *
   * @return current window
   */
  IWindow getCurWindow();

  /**
   * Used to skip some points through the window attributes, such as timeRange and so on.
   *
   * @param inputTsBlock a TsBlock
   * @return a new TsBlock which skips some points
   */
  TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock);

  /**
   * Used to determine whether the current window overlaps with TsBlock.
   *
   * @param inputTsBlock a TsBlock
   * @return whether the current window overlaps with TsBlock
   */
  default boolean satisfiedCurWindow(TsBlock inputTsBlock) {
    return true;
  }

  /**
   * Used to determine whether there are extra points for the next window.
   *
   * @param inputTsBlock a TsBlock
   * @return whether there are extra points for the next window
   */
  default boolean isTsBlockOutOfBound(TsBlock inputTsBlock) {
    return false;
  }

  /**
   * According to the Aggregator list, we could obtain all the aggregation result column type list.
   *
   * @param aggregators the list of aggregators
   * @return Aggregation result column type list.
   */
  default List<TSDataType> getResultDataTypes(List<TreeAggregator> aggregators) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (TreeAggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    return dataTypes;
  }

  /**
   * Used to create the aggregation resultSet.
   *
   * <p>For the implementation, we should consider whether we need to add endTime column and event
   * column in the resultSet besides the aggregation columns.
   *
   * @param aggregators the list of aggregators
   * @return TsBlockBuilder of resultSet
   */
  TsBlockBuilder createResultTsBlockBuilder(List<TreeAggregator> aggregators);

  /**
   * Used to append a row of aggregation result into the resultSet.
   *
   * <p>For the implementation, similar to the method createResultTsBlockBuilder, we should consider
   * whether we need to add endTime column and event column in the resultSet besides the aggregation
   * columns.
   *
   * @param resultTsBlockBuilder tsBlockBuilder for resultSet
   * @param aggregators the list of aggregators
   */
  void appendAggregationResult(
      TsBlockBuilder resultTsBlockBuilder, List<TreeAggregator> aggregators);

  /**
   * Especially for TimeWindow, if there are no points belong to last TimeWindow, the last
   * TimeWindow will not initialize window and aggregators in the aggregation frame.
   *
   * @return whether the window is TimeWindow and the last TimeWindow has not been initialized
   */
  default boolean notInitializedLastTimeWindow() {
    return false;
  }

  /**
   * When endTime is required in resultSet, operator should skip the points in last window directly
   * instead of a default lazy way to get the endTime for constructing the result tsBlock.
   *
   * <p>For the windows like TimeWindow which has already cached endTime, this method always return
   * false.
   */
  boolean needSkipInAdvance();

  /**
   * When controlColumn is null, this method determined whether we should consider that row. if
   * ignoringNull is false, null will be considered as a normal value in window.
   */
  boolean isIgnoringNull();

  /**
   * output the result in aggregators to columnBuilders.
   *
   * @param endTime if the window doesn't need to output endTime, just assign -1 to endTime.
   */
  default void outputAggregators(
      List<TreeAggregator> aggregators,
      TsBlockBuilder resultTsBlockBuilder,
      long startTime,
      long endTime) {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(startTime);

    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    int columnIndex = 0;
    if (endTime != -1) {
      columnBuilders[0].writeLong(endTime);
      columnIndex = 1;
    }
    for (TreeAggregator aggregator : aggregators) {
      ColumnBuilder[] columnBuilder = new ColumnBuilder[aggregator.getOutputType().length];
      columnBuilder[0] = columnBuilders[columnIndex++];
      if (columnBuilder.length > 1) {
        columnBuilder[1] = columnBuilders[columnIndex++];
      }
      aggregator.outputResult(columnBuilder);
    }
    resultTsBlockBuilder.declarePosition();
  }
}
