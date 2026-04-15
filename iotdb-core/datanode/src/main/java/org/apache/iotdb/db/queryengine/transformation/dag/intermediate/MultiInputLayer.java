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
import org.apache.iotdb.db.queryengine.transformation.datastructure.TVColumns;
import org.apache.iotdb.db.queryengine.transformation.datastructure.iterator.RowListForwardIterator;
import org.apache.iotdb.db.queryengine.transformation.datastructure.row.ElasticSerializableRowList;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.strategy.SessionTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MultiInputLayer extends IntermediateLayer implements IUDFInputDataSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiInputLayer.class);

  private final LayerReader[] layerReaders;
  private final TSDataType[] dataTypes;
  private final TimeSelector timeHeap;

  private final TVColumns[] inputTVColumnsList;
  private final int[] currentConsumedIndexes;
  private final int[] nextConsumedIndexes;

  private final TsBlockBuilder tsBlockBuilder;
  private TsBlock cachedTsBlock = null;

  public MultiInputLayer(
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
          inputTVColumnsList[i] = new TVColumns(columns[1], columns[0]);
          timeHeap.add(columns[1].getLong(0));
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
      public void consumedAll() {
        // Currently do nothing
      }

      @Override
      public Column[] current() {
        return MultiInputLayer.this.currentBlock();
      }

      @Override
      public TSDataType[] getDataTypes() {
        return dataTypes;
      }

      @Override
      public YieldableState yield() throws Exception {
        return MultiInputLayer.this.yield();
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

      private final ElasticSerializableRowList rowRecordList =
          new ElasticSerializableRowList(dataTypes, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
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

        final int pointsToBeCollected = endIndex - rowRecordList.size();
        if (pointsToBeCollected > 0) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldRows(udfInputDataSet, rowRecordList, pointsToBeCollected);
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
    final ElasticSerializableRowList rowRecordList =
        new ElasticSerializableRowList(dataTypes, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableRowRecordListBackedMultiColumnWindow window =
        new ElasticSerializableRowRecordListBackedMultiColumnWindow(rowRecordList);

    return new LayerRowWindowReader() {

      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private boolean hasCached = false;
      private long nextWindowTimeBegin = strategy.getDisplayWindowBegin();
      private int nextIndexBegin = 0;
      private int nextIndexEnd = 0;
      private long currentEndTime = Long.MAX_VALUE;

      private final RowListForwardIterator beginIterator = rowRecordList.constructIterator();
      private Column cachedBeginTimeColumn;
      private int cachedBeginConsumed;

      private Column cachedEndTimeColumn;
      private int cachedEndConsumed;

      @Override
      public YieldableState yield() throws Exception {
        if (isFirstIteration) {
          if (rowRecordList.size() == 0) {
            final YieldableState state = udfInputDataSet.yield();
            if (state != YieldableState.YIELDABLE) {
              return state;
            }

            Column[] columns = udfInputDataSet.currentBlock();
            Column times = columns[columns.length - 1];

            rowRecordList.put(columns);

            cachedEndTimeColumn = times;
          }
          if (nextWindowTimeBegin == Long.MIN_VALUE) {
            // display window begin should be set to the same as the min timestamp of the query
            // result set
            nextWindowTimeBegin = cachedEndTimeColumn.getLong(0);
          }
          hasAtLeastOneRow = rowRecordList.size() != 0;
          if (hasAtLeastOneRow) {
            currentEndTime =
                cachedEndTimeColumn.getLong(cachedEndTimeColumn.getPositionCount() - 1);
          }
          isFirstIteration = false;
        }

        if (hasCached) {
          return YieldableState.YIELDABLE;
        }

        if (!hasAtLeastOneRow || displayWindowEnd <= nextWindowTimeBegin) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        long nextWindowTimeEnd = Math.min(nextWindowTimeBegin + timeInterval, displayWindowEnd);
        while (currentEndTime < nextWindowTimeEnd) {
          final YieldableState state = udfInputDataSet.yield();
          if (state == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }
          if (state == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
            break;
          }
          // Generate data
          Column[] columns = udfInputDataSet.currentBlock();
          Column times = columns[columns.length - 1];
          // Put data into container
          rowRecordList.put(columns);
          currentEndTime = times.getLong(times.getPositionCount() - 1);
          // Increase nextIndexEnd
          nextIndexEnd += cachedEndTimeColumn.getPositionCount() - cachedEndConsumed;
          // Update cache
          cachedEndTimeColumn = times;
          cachedEndConsumed = 0;
        }

        // Set nextIndexEnd field
        while (cachedEndConsumed < cachedEndTimeColumn.getPositionCount()) {
          if (cachedEndTimeColumn.getLong(cachedEndConsumed) >= nextWindowTimeEnd) {
            break;
          }
          cachedEndConsumed++;
          nextIndexEnd++;
        }

        // Set nextIndexBegin field
        boolean findNextIndexBegin = false;
        while (!findNextIndexBegin) {
          while (cachedBeginTimeColumn != null
              && cachedBeginConsumed < cachedBeginTimeColumn.getPositionCount()) {
            if (cachedBeginTimeColumn.getLong(cachedBeginConsumed) >= nextWindowTimeBegin) {
              findNextIndexBegin = true;
              break;
            }
            cachedBeginConsumed++;
            nextIndexBegin++;
          }

          if (!findNextIndexBegin) {
            if (beginIterator.hasNext()) {
              beginIterator.next();

              cachedBeginConsumed = 0;
              Column[] columns = beginIterator.currentBlock();
              cachedBeginTimeColumn = columns[columns.length - 1];
            } else {
              // No more data
              // Set nextIndexBegin to list's size
              findNextIndexBegin = true;
            }
          }
        }

        if ((nextIndexEnd == nextIndexBegin)
            && nextWindowTimeEnd
                < cachedEndTimeColumn.getLong(cachedEndTimeColumn.getPositionCount() - 1)) {
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
    final ElasticSerializableRowList rowRecordList =
        new ElasticSerializableRowList(dataTypes, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableRowRecordListBackedMultiColumnWindow window =
        new ElasticSerializableRowRecordListBackedMultiColumnWindow(rowRecordList);

    return new LayerRowWindowReader() {
      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private long nextWindowTimeBegin = displayWindowBegin;
      private long nextWindowTimeEnd = 0;
      private int nextIndexBegin = 0;
      private int nextIndexEnd = 0;

      private Column cachedTimes;
      private int cachedConsumed;

      @Override
      public YieldableState yield() throws Exception {
        if (isFirstIteration) {
          YieldableState state = yieldInFirstIteration();
          if (state != YieldableState.YIELDABLE) {
            return state;
          }
        }

        if (!hasAtLeastOneRow || nextWindowTimeBegin >= displayWindowEnd) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        // Set nextIndexEnd
        long curTime = cachedTimes.getLong(cachedConsumed);
        if (cachedConsumed < cachedTimes.getPositionCount()) {
          nextIndexEnd++;
          cachedConsumed++;
        }
        boolean findWindow = false;
        // Find target window or no more data to exit
        while (!findWindow && cachedConsumed < cachedTimes.getPositionCount()) {
          while (cachedConsumed < cachedTimes.getPositionCount()) {
            long nextTime = cachedTimes.getLong(cachedConsumed);

            if (nextTime >= displayWindowEnd) {
              findWindow = true;
              break;
            }

            if (nextTime - curTime > sessionTimeGap) {
              findWindow = true;
              break;
            }
            nextIndexEnd++;
            cachedConsumed++;
            curTime = nextTime;
          }

          if (!findWindow) {
            if (cachedTimes.getLong(cachedTimes.getPositionCount() - 1) < displayWindowEnd) {
              YieldableState state = yieldAndCache();
              if (state == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
                return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
              } else if (state == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
                break;
              }
            }
          }
        }
        // Set nextWindowTimeEnd
        nextWindowTimeEnd = rowRecordList.getTime(nextIndexEnd - 1);

        if (nextIndexBegin == nextIndexEnd) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }
        window.seek(nextIndexBegin, nextIndexEnd, nextWindowTimeBegin, nextWindowTimeEnd);
        return YieldableState.YIELDABLE;
      }

      private YieldableState yieldInFirstIteration() throws Exception {
        // Yield initial data in first iteration
        if (rowRecordList.size() == 0) {
          YieldableState state = yieldAndCache();
          if (state != YieldableState.YIELDABLE) {
            return state;
          }
        }
        // Initialize essential information
        nextWindowTimeBegin = Math.max(displayWindowBegin, cachedTimes.getLong(0));
        hasAtLeastOneRow = rowRecordList.size() != 0;
        isFirstIteration = false;

        // Set initial nextIndexBegin
        long currentEndTime = cachedTimes.getLong(cachedTimes.getPositionCount() - 1);
        // Find corresponding block
        while (currentEndTime < nextWindowTimeBegin) {
          // Consume all data
          cachedConsumed = cachedTimes.getPositionCount();
          nextIndexBegin += cachedConsumed;

          YieldableState state = yieldAndCache();
          if (state != YieldableState.YIELDABLE) {
            // Cannot find nextIndexBegin
            // Set nextIndexEnd to nextIndexBegin and exit
            nextIndexEnd = nextIndexBegin;
            return state;
          }
        }
        // Find nextIndexBegin in block
        while (cachedConsumed < cachedTimes.getPositionCount()) {
          if (cachedTimes.getLong(cachedConsumed) >= nextWindowTimeBegin) {
            break;
          }
          cachedConsumed++;
          nextIndexBegin++;
        }
        nextIndexEnd = nextIndexBegin;

        return YieldableState.YIELDABLE;
      }

      private YieldableState yieldAndCache() throws Exception {
        final YieldableState state = udfInputDataSet.yield();
        if (state != YieldableState.YIELDABLE) {
          return state;
        }
        Column[] columns = udfInputDataSet.currentBlock();
        Column times = columns[columns.length - 1];

        rowRecordList.put(columns);

        cachedTimes = times;
        cachedConsumed = 0;

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
