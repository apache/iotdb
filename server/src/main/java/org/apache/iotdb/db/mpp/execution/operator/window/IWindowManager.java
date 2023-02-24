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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Used to customize all the type of window managers, such as TimeWindowManager,
 * SessionWindowManager
 */
public interface IWindowManager {

  /**
   * Judge whether the current window is initialized
   *
   * @return whether the current window is initialized
   */
  boolean isCurWindowInit();

  /** Used to initialize the status of window */
  void initCurWindow();

  /**
   * Used to determine whether there is a next window
   *
   * @return whether there is a next window
   */
  boolean hasNext(boolean hasMoreData);

  /** Used to mark the current window has got last point */
  void next();

  /**
   * Used to get current window
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
   * Used to determine whether the current window overlaps with TsBlock
   *
   * @param inputTsBlock a TsBlock
   * @return whether the current window overlaps with TsBlock
   */
  default boolean satisfiedCurWindow(TsBlock inputTsBlock) {
    return true;
  };

  /**
   * Used to determine whether there are extra points for the next window
   *
   * @param inputTsBlock a TsBlock
   * @return whether there are extra points for the next window
   */
  default boolean isTsBlockOutOfBound(TsBlock inputTsBlock) {
    return false;
  };

  /**
   * According to the Aggregator list, we could obtain all the aggregation result column type list.
   *
   * @param aggregators
   * @return Aggregation result column type list.
   */
  default List<TSDataType> getResultDataTypes(List<Aggregator> aggregators) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
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
   * @param aggregators
   * @return TsBlockBuilder of resultSet
   */
  TsBlockBuilder createResultTsBlockBuilder(List<Aggregator> aggregators);

  /**
   * Used to append a row of aggregation result into the resultSet.
   *
   * <p>For the implementation, similar to the method createResultTsBlockBuilder, we should consider
   * whether we need to add endTime column and event column in the resultSet besides the aggregation
   * columns.
   *
   * @param resultTsBlockBuilder
   * @param aggregators
   */
  void appendAggregationResult(TsBlockBuilder resultTsBlockBuilder, List<Aggregator> aggregators);

  /**
   * Especially for TimeWindow, if there are no points belong to last TimeWindow, the last
   * TimeWindow will not initialize window and aggregators in the aggregation frame.
   *
   * @return whether the window is TimeWindow and the last TimeWindow has not been initialized
   */
  default boolean notInitedLastTimeWindow() {
    return false;
  };

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

  // TODO: "group by series" used for keep value temporarily, it will be removed in the future.
  default void setKeep(long keep) {}

  // TODO: "group by session" used for keeping lastTsBlockTime, it will be removed in the future.
  default void setLastTsBlockTime() {}
}
