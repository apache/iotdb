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

package org.apache.iotdb.db.queryengine.transformation.dag.intermediate;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.input.IUDFInputDataSet;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class MultiInputColumnIntermediateLayer extends IntermediateLayer
    implements IUDFInputDataSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MultiInputColumnIntermediateLayer.class);

  private final LayerReader[] layerReaders;
  private final TSDataType[] dataTypes;
  private final TimeSelector timeHeap;

  private TsBlock[] inputTVColumns;
  private int[] currentConsumedIndexes;
  private int[] nextConsumedIndexes;

  private TsBlockBuilder tsBlockBuilder;
  private TsBlock cachedTsBlock = null;

  public MultiInputColumnIntermediateLayer(
      Expression expression,
      String queryId,
      float memoryBudgetInMB,
      List<LayerReader> parentLayerReaders) {
    super(expression, queryId, memoryBudgetInMB);

    layerReaders = parentLayerReaders.toArray(new LayerReader[0]);
    currentConsumedIndexes = new int[layerReaders.length];
    nextConsumedIndexes = new int[layerReaders.length];
    inputTVColumns = new TsBlock[layerReaders.length];

    dataTypes = new TSDataType[layerReaders.length];
    for (int i = 0; i < layerReaders.length; ++i) {
      dataTypes[i] = layerReaders[i].getDataTypes()[0];
    }
    tsBlockBuilder = new TsBlockBuilder(Arrays.asList(dataTypes));

    timeHeap = new TimeSelector(layerReaders.length << 1, true);
  }

  @Override
  public List<TSDataType> getDataTypes() {
    return Arrays.asList(dataTypes);
  }

  @Override
  public YieldableState yield() throws Exception {
    tsBlockBuilder.reset();

    // Fill input columns
    YieldableState state = updateInputColumns();
    if (state != YieldableState.YIELDABLE) {
      return state;
    }

    // Choose minimum end time as this iteration's end time
    long endTime = Long.MAX_VALUE;
    for (TsBlock block : inputTVColumns) {
      long blockEndTime = block.getEndTime();
      endTime = Math.min(blockEndTime, endTime);
    }

    // Construct row for given time from time heap
    long currentTime = -1;
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    while (currentTime != endTime) {
      currentTime = timeHeap.pollFirst();

      timeBuilder.writeLong(currentTime); // Time
      appendRowInBuilder(currentTime); // Values
      tsBlockBuilder.declarePosition();

      updateTimeHeap();
    }
    timeHeap.clear();

    cachedTsBlock = tsBlockBuilder.build();
    return YieldableState.YIELDABLE;
  }

  private YieldableState updateInputColumns() throws Exception {
    for (int i = 0; i < layerReaders.length; i++) {
      // Skip TVColumns that still remains some data
      if (canSkipInputTVColumns(i)) {
        continue;
      }
      // Prepare data for TVColumns without data
      YieldableState state = layerReaders[i].yield();
      if (state == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
        return state;
      } else if (state == YieldableState.YIELDABLE) {
        Column[] columns = layerReaders[i].current();
        inputTVColumns[i] = new TsBlock((TimeColumn) columns[1], columns[0]);
        currentConsumedIndexes[i] = 0;
        layerReaders[i].consumedAll();

        timeHeap.add(((TimeColumn) columns[1]).getStartTime());
      } // Do nothing for YieldableState.NOT_YIELDABLE_NO_MORE_DATA
    }

    return timeHeap.isEmpty()
        ? YieldableState.NOT_YIELDABLE_NO_MORE_DATA
        : YieldableState.YIELDABLE;
  }

  private boolean canSkipInputTVColumns(int index) {
    return inputTVColumns[index] != null && !hasConsumedAll(index);
  }

  private boolean hasConsumedAll(int index) {
    return inputTVColumns[index].getPositionCount() == currentConsumedIndexes[index];
  }

  private void appendRowInBuilder(long time) {
    for (int i = 0; i < inputTVColumns.length; i++) {
      TsBlock block = inputTVColumns[i];
      ColumnBuilder builder = tsBlockBuilder.getColumnBuilder(i);

      int currentIndex = currentConsumedIndexes[i];
      long currentTime = block.getTimeByIndex(currentIndex);
      if (currentTime == time) {
        builder.write(block.getColumn(0), currentIndex);
        nextConsumedIndexes[i] = currentIndex + 1;
      } else {
        builder.appendNull();
      }
    }
  }

  private void updateTimeHeap() {
    for (int i = 0; i < inputTVColumns.length; i++) {
      if (currentConsumedIndexes[i] != nextConsumedIndexes[i]) {
        // Add new time to time heap
        currentConsumedIndexes[i] = nextConsumedIndexes[i];
        timeHeap.add(inputTVColumns[i].getTimeByIndex(currentConsumedIndexes[i]));
      }
    }
  }

  @Override
  public Column[] currentBlock() {
    Column[] ret = cachedTsBlock.getAllColumns();
    cachedTsBlock = null;
    return ret;
  }

  @Override
  public LayerReader constructReader() {
    return new LayerReader() {
      @Override
      public boolean isConstantPointReader() {
        return false;
      }

      @Override
      public void consumed(int count) {
        // Currently do nothing
      }

      @Override
      public void consumedAll() {
        // Currently do nothing
      }

      @Override
      public Column[] current() {
        return MultiInputColumnIntermediateLayer.this.currentBlock();
      }

      @Override
      public TSDataType[] getDataTypes() {
        return dataTypes;
      }

      @Override
      public YieldableState yield() throws Exception {
        return MultiInputColumnIntermediateLayer.this.yield();
      }
    };
  }
}
