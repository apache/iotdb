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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.List;

public abstract class EventWindowManager implements IWindowManager {

  protected boolean initialized;

  protected boolean ascending;

  protected boolean needSkip;

  protected boolean hasAppendedResult;

  protected WindowParameter windowParameter;

  protected EventWindow eventWindow;

  protected EventWindowManager(WindowParameter windowParameter, boolean ascending) {
    this.windowParameter = windowParameter;
    this.initialized = false;
    this.ascending = ascending;
    // At beginning, we do not need to skip inputTsBlock
    this.needSkip = false;
    this.hasAppendedResult = false;
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow(TsBlock tsBlock) {
    this.initialized = true;
    this.hasAppendedResult = false;
    this.eventWindow.setInitializedEventValue(false);
  }

  @Override
  public boolean hasNext(boolean hasMoreData) {
    return hasMoreData;
  }

  @Override
  public void next() {
    // When we go into next window, we should pay attention to previous window whether all points
    // belong to previous window have been consumed. If not, we need skip these points.
    this.needSkip = true;
    this.initialized = false;
    this.eventWindow.updatePreviousEventValue();
  }

  @Override
  public IWindow getCurWindow() {
    return eventWindow;
  }

  @Override
  public boolean satisfiedCurWindow(TsBlock inputTsBlock) {
    return true;
  }

  @Override
  public boolean isTsBlockOutOfBound(TsBlock inputTsBlock) {
    return false;
  }

  @Override
  public TsBlockBuilder createResultTsBlockBuilder(List<Aggregator> aggregators) {
    List<TSDataType> dataTypes = getResultDataTypes(aggregators);
    // Judge whether we need output event column.
    if (windowParameter.isNeedOutputEvent()) {
      dataTypes.add(windowParameter.getDataType());
    }
    return new TsBlockBuilder(dataTypes);
  }

  protected ColumnBuilder[] appendOriginAggregationResult(
      TsBlockBuilder resultTsBlockBuilder, List<Aggregator> aggregators) {
    // Use the start time of eventWindow as default output time.
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(eventWindow.getStartTime());

    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    int columnIndex = 0;
    for (Aggregator aggregator : aggregators) {
      ColumnBuilder[] columnBuilder = new ColumnBuilder[aggregator.getOutputType().length];
      columnBuilder[0] = columnBuilders[columnIndex++];
      if (columnBuilder.length > 1) {
        columnBuilder[1] = columnBuilders[columnIndex++];
      }
      aggregator.outputResult(columnBuilder);
    }
    resultTsBlockBuilder.declarePosition();
    this.hasAppendedResult = true;
    return columnBuilders;
  }

  @Override
  public boolean notInitedLastTimeWindow() {
    return false;
  }
}
