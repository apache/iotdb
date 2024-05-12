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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.LayerRowWindowReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.adapter.ElasticSerializableRowRecordListBackedMultiColumnWindow;
import org.apache.iotdb.db.queryengine.transformation.dag.input.IUDFInputDataSet;
import org.apache.iotdb.db.queryengine.transformation.dag.util.LayerCacheUtils;
import org.apache.iotdb.db.queryengine.transformation.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.TVColumns;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.strategy.SessionTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MultiInputColumnIntermediateLayer extends IntermediateLayer
    implements IUDFInputDataSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MultiInputColumnIntermediateLayer.class);

  private final LayerReader[] layerReaders;
  private final TSDataType[] dataTypes;
  private final TimeSelector timeHeap;

  private TVColumns[] inputTVColumnsList;
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
    inputTVColumnsList = new TVColumns[layerReaders.length];

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
    for (TVColumns tvColumns : inputTVColumnsList) {
      if (!tvColumns.isConstant()) {
        long tvColumnsEndTime = tvColumns.getEndTime();
        endTime = Math.min(tvColumnsEndTime, endTime);
      }
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
        if (layerReaders[i].isConstantPointReader()) {
          inputTVColumnsList[i] = new TVColumns(columns[0]);
        } else {
          inputTVColumnsList[i] = new TVColumns((TimeColumn) columns[1], columns[0]);
          timeHeap.add(((TimeColumn) columns[1]).getStartTime());
        }

        currentConsumedIndexes[i] = 0;
        layerReaders[i].consumedAll();
      } // Do nothing for YieldableState.NOT_YIELDABLE_NO_MORE_DATA
    }

    return timeHeap.isEmpty()
        ? YieldableState.NOT_YIELDABLE_NO_MORE_DATA
        : YieldableState.YIELDABLE;
  }

  private boolean canSkipInputTVColumns(int index) {
    return inputTVColumnsList[index] != null && !hasConsumedAll(index);
  }

  private boolean hasConsumedAll(int index) {
    return inputTVColumnsList[index].getPositionCount() == currentConsumedIndexes[index];
  }

  private void appendRowInBuilder(long time) {
    for (int i = 0; i < inputTVColumnsList.length; i++) {
      ColumnBuilder builder = tsBlockBuilder.getColumnBuilder(i);
      if (hasConsumedAll(i)) {
        builder.appendNull();
        continue;
      }

      TVColumns tvColumns = inputTVColumnsList[i];
      if (tvColumns.isConstant()) {
        // TODO: maybe one constant is enough, notice this 0
        builder.write(tvColumns.getValueColumn(), 0);
        continue;
      }

      int currentIndex = currentConsumedIndexes[i];
      long currentTime = tvColumns.getTimeByIndex(currentIndex);
      if (currentTime == time) {
        if (tvColumns.getValueColumn().isNull(currentIndex)) {
          builder.appendNull();
        } else {
          builder.write(tvColumns.getValueColumn(), currentIndex);
        }
        nextConsumedIndexes[i] = currentIndex + 1;
      } else {
        builder.appendNull();
      }
    }
  }

  private void updateTimeHeap() {
    for (int i = 0; i < inputTVColumnsList.length; i++) {
      if (currentConsumedIndexes[i] != nextConsumedIndexes[i]) {
        currentConsumedIndexes[i] = nextConsumedIndexes[i];
        // Add remaining time to time heap
        if (!hasConsumedAll(i)) {
          timeHeap.add(inputTVColumnsList[i].getTimeByIndex(currentConsumedIndexes[i]));
        }
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

  @Override
  protected LayerRowWindowReader constructRowSlidingSizeWindowReader(
      SlidingSizeWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException {
    final IUDFInputDataSet udfInputDataSet = this;

    return new LayerRowWindowReader() {

      private final int windowSize = strategy.getWindowSize();
      private final int slidingStep = strategy.getSlidingStep();

      private final ElasticSerializableRowRecordList rowRecordList =
          new ElasticSerializableRowRecordList(
              dataTypes, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
      private final ElasticSerializableRowRecordListBackedMultiColumnWindow window =
          new ElasticSerializableRowRecordListBackedMultiColumnWindow(rowRecordList);

      private boolean hasCached = false;
      private int beginIndex = -slidingStep;

      @Override
      public YieldableState yield() throws Exception {
        if (hasCached) {
          return YieldableState.YIELDABLE;
        }

        beginIndex += slidingStep;
        int endIndex = beginIndex + windowSize;
        if (beginIndex < 0 || endIndex < 0) {
          LOGGER.warn(
              "LayerRowWindowReader index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.",
              beginIndex,
              endIndex,
              windowSize);
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        final int rowsToBeCollected = endIndex - rowRecordList.size();
        if (rowsToBeCollected > 0) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldRows(udfInputDataSet, rowRecordList, rowsToBeCollected);

          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            beginIndex -= slidingStep;
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }

          if (rowRecordList.size() <= beginIndex) {
            return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
          }

          // TVList's size may be less than endIndex
          // When parent layer reader has no more data
          endIndex = Math.min(endIndex, rowRecordList.size());
        }

        window.seek(
            beginIndex,
            endIndex,
            rowRecordList.getTime(beginIndex),
            rowRecordList.getTime(endIndex - 1));

        hasCached = true;
        return YieldableState.YIELDABLE;
      }

      @Override
      public void readyForNext() {
        hasCached = false;

        rowRecordList.setEvictionUpperBound(beginIndex + 1);
      }

      @Override
      public TSDataType[] getDataTypes() {
        return dataTypes;
      }

      @Override
      public RowWindow currentWindow() {
        return window;
      }
    };
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingTimeWindowReader(
      SlidingTimeWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException {

    final long timeInterval = strategy.getTimeInterval();
    final long slidingStep = strategy.getSlidingStep();
    final long displayWindowEnd = strategy.getDisplayWindowEnd();

    final IUDFInputDataSet udfInputDataSet = this;
    final ElasticSerializableRowRecordList rowRecordList =
        new ElasticSerializableRowRecordList(
            dataTypes, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableRowRecordListBackedMultiColumnWindow window =
        new ElasticSerializableRowRecordListBackedMultiColumnWindow(rowRecordList);

    return new LayerRowWindowReader() {

      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private boolean hasCached = false;
      private long nextWindowTimeBegin = strategy.getDisplayWindowBegin();
      private int nextIndexBegin = 0;

      @Override
      public YieldableState yield() throws Exception {
        if (isFirstIteration) {
          if (rowRecordList.size() == 0) {
            final YieldableState yieldableState =
                LayerCacheUtils.yieldRows(udfInputDataSet, rowRecordList);
            if (yieldableState != YieldableState.YIELDABLE) {
              return yieldableState;
            }
            if (nextWindowTimeBegin == Long.MIN_VALUE) {
              // display window begin should be set to the same as the min timestamp of the query
              // result set
              nextWindowTimeBegin = rowRecordList.getTime(0);
            }
          }
          hasAtLeastOneRow = rowRecordList.size() != 0;
          isFirstIteration = false;
        }

        if (hasCached) {
          return YieldableState.YIELDABLE;
        }

        if (!hasAtLeastOneRow || displayWindowEnd <= nextWindowTimeBegin) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        long nextWindowTimeEnd = Math.min(nextWindowTimeBegin + timeInterval, displayWindowEnd);
        while (rowRecordList.getTime(rowRecordList.size() - 1) < nextWindowTimeEnd) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldRows(udfInputDataSet, rowRecordList);
          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }
          if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
            break;
          }
        }

        for (int i = nextIndexBegin; i < rowRecordList.size(); ++i) {
          if (rowRecordList.getTime(i) >= nextWindowTimeBegin) {
            nextIndexBegin = i;
            break;
          }
          if (i == rowRecordList.size() - 1) {
            nextIndexBegin = rowRecordList.size();
          }
        }

        int nextIndexEnd = rowRecordList.size();
        for (int i = nextIndexBegin; i < rowRecordList.size(); ++i) {
          if (nextWindowTimeEnd <= rowRecordList.getTime(i)) {
            nextIndexEnd = i;
            break;
          }
        }
        if ((nextIndexEnd == nextIndexBegin)
            && nextWindowTimeEnd < rowRecordList.getTime(rowRecordList.size() - 1)) {
          window.setEmptyWindow(nextWindowTimeBegin, nextWindowTimeEnd);
          return YieldableState.YIELDABLE;
        }
        window.seek(
            nextIndexBegin,
            nextIndexEnd,
            nextWindowTimeBegin,
            nextWindowTimeBegin + timeInterval - 1);

        hasCached = !(nextIndexBegin == nextIndexEnd && nextIndexEnd == rowRecordList.size());
        return hasCached ? YieldableState.YIELDABLE : YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
      }

      @Override
      public void readyForNext() {
        hasCached = false;
        nextWindowTimeBegin += slidingStep;

        rowRecordList.setEvictionUpperBound(nextIndexBegin + 1);
      }

      @Override
      public TSDataType[] getDataTypes() {
        return dataTypes;
      }

      @Override
      public RowWindow currentWindow() {
        return window;
      }
    };
  }

  @Override
  protected LayerRowWindowReader constructRowSessionTimeWindowReader(
      SessionTimeWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException {
    final long displayWindowBegin = strategy.getDisplayWindowBegin();
    final long displayWindowEnd = strategy.getDisplayWindowEnd();
    final long sessionTimeGap = strategy.getSessionTimeGap();

    final IUDFInputDataSet udfInputDataSet = this;
    final ElasticSerializableRowRecordList rowRecordList =
        new ElasticSerializableRowRecordList(
            dataTypes, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableRowRecordListBackedMultiColumnWindow window =
        new ElasticSerializableRowRecordListBackedMultiColumnWindow(rowRecordList);

    return new LayerRowWindowReader() {
      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private long nextWindowTimeBegin = displayWindowBegin;
      private long nextWindowTimeEnd = 0;
      private int nextIndexBegin = 0;
      private int nextIndexEnd = 1;

      @Override
      public YieldableState yield() throws Exception {
        if (isFirstIteration) {
          if (rowRecordList.size() == 0) {
            final YieldableState yieldableState =
                LayerCacheUtils.yieldRows(udfInputDataSet, rowRecordList);
            if (yieldableState != YieldableState.YIELDABLE) {
              return yieldableState;
            }
          }
          nextWindowTimeBegin = Math.max(displayWindowBegin, rowRecordList.getTime(0));
          hasAtLeastOneRow = rowRecordList.size() != 0;
          isFirstIteration = false;
        }

        if (!hasAtLeastOneRow || displayWindowEnd <= nextWindowTimeBegin) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        // Set nextIndexEnd
        nextIndexEnd++;
        boolean findWindow = false;
        // Find target window or no more data to exit
        while (!findWindow) {
          while (nextIndexEnd < rowRecordList.size()) {
            long curTime = rowRecordList.getTime(nextIndexEnd - 1);
            long nextTime = rowRecordList.getTime(nextIndexEnd);

            if (curTime >= displayWindowEnd) {
              nextIndexEnd--;
              findWindow = true;
              break;
            }

            if (curTime >= displayWindowBegin && curTime - nextTime > sessionTimeGap) {
              findWindow = true;
              break;
            }
            nextIndexEnd++;
          }

          if (!findWindow) {
            if (rowRecordList.getTime(rowRecordList.size() - 1) < displayWindowEnd) {
              final YieldableState yieldableState =
                  LayerCacheUtils.yieldRows(udfInputDataSet, rowRecordList);
              if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
                return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
              } else if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
                break;
              }
            }
          }
        }

        nextWindowTimeEnd = rowRecordList.getTime(nextIndexEnd - 1);

        if (nextIndexBegin == nextIndexEnd) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        // Only if encounter user set the strategy's displayWindowBegin, which will go into the for
        // loop to find the true index of the first window begin.
        // For other situation, we will only go into if (nextWindowTimeBegin <= tvList.getTime(i))
        // once.
        for (int i = nextIndexBegin; i < rowRecordList.size(); ++i) {
          if (rowRecordList.getTime(i) >= nextWindowTimeBegin) {
            nextIndexBegin = i;
            break;
          }
          // The first window's beginning time is greater than all the timestamp of the query result
          // set
          if (i == rowRecordList.size() - 1) {
            return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
          }
        }

        window.seek(nextIndexBegin, nextIndexEnd, nextWindowTimeBegin, nextWindowTimeEnd);

        return YieldableState.YIELDABLE;
      }

      @Override
      public void readyForNext() throws IOException {
        if (nextIndexEnd < rowRecordList.size()) {
          nextWindowTimeBegin = rowRecordList.getTime(nextIndexEnd);
        }
        rowRecordList.setEvictionUpperBound(nextIndexBegin + 1);
        nextIndexBegin = nextIndexEnd;
      }

      @Override
      public TSDataType[] getDataTypes() {
        return dataTypes;
      }

      @Override
      public RowWindow currentWindow() {
        return window;
      }
    };
  }

  @Override
  protected LayerRowWindowReader constructRowStateWindowReader(
      StateWindowAccessStrategy strategy, float memoryBudgetInMB) {
    throw new UnsupportedOperationException(
        "StateWindowAccessStrategy only support one input series for now.");
  }
}
