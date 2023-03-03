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
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;

public abstract class VariationWindowManager implements IWindowManager {

  protected boolean initialized;

  protected boolean ascending;

  protected boolean needSkip;

  protected AbstractVariationWindow variationWindow;

  protected VariationWindowManager(boolean ascending) {
    this.initialized = false;
    this.ascending = ascending;
    // At beginning, we do not need to skip inputTsBlock
    this.needSkip = false;
  }

  public boolean isIgnoringNull() {
    return variationWindow.ignoreNull();
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow() {
    this.initialized = true;
    this.variationWindow.setInitializedHeadValue(false);
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
    this.variationWindow.updatePreviousValue();
  }

  @Override
  public IWindow getCurWindow() {
    return variationWindow;
  }

  @Override
  public TsBlockBuilder createResultTsBlockBuilder(List<Aggregator> aggregators) {
    List<TSDataType> dataTypes = getResultDataTypes(aggregators);
    // Judge whether we need output endTime column.
    if (variationWindow.isOutputEndTime()) {
      dataTypes.add(0, TSDataType.INT64);
    }
    return new TsBlockBuilder(dataTypes);
  }

  public void appendAggregationResult(
      TsBlockBuilder resultTsBlockBuilder, List<Aggregator> aggregators) {

    long endTime = variationWindow.isOutputEndTime() ? variationWindow.getEndTime() : -1;
    outputAggregators(aggregators, resultTsBlockBuilder, variationWindow.getStartTime(), endTime);
  }

  @Override
  public boolean needSkipInAdvance() {
    return variationWindow.isOutputEndTime();
  }
}
