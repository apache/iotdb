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
import org.apache.iotdb.db.queryengine.transformation.api.LayerRowWindowReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.adapter.ElasticSerializableTVListBackedSingleColumnWindow;
import org.apache.iotdb.db.queryengine.transformation.dag.util.LayerCacheUtils;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TransformUtils;
import org.apache.iotdb.db.queryengine.transformation.datastructure.iterator.TVListForwardIterator;
import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.ValueRecorder;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.strategy.SessionTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleInputSingleReferenceLayer extends IntermediateLayer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SingleInputSingleReferenceLayer.class);

  private final LayerReader parentLayerReader;
  private final TSDataType dataType;

  public SingleInputSingleReferenceLayer(
      Expression expression,
      String queryId,
      float memoryBudgetInMB,
      LayerReader parentLayerReader) {
    super(expression, queryId, memoryBudgetInMB);
    this.parentLayerReader = parentLayerReader;
    dataType = parentLayerReader.getDataTypes()[0];
  }

  @Override
  public LayerReader constructReader() {
    return parentLayerReader;
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingSizeWindowReader(
      SlidingSizeWindowAccessStrategy strategy, float memoryBudgetInMB) {
    return new LayerRowWindowReader() {
      private final int windowSize = strategy.getWindowSize();
      private final int slidingStep = strategy.getSlidingStep();

      private final ElasticSerializableTVList tvList =
          ElasticSerializableTVList.construct(
              dataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
      private final ElasticSerializableTVListBackedSingleColumnWindow window =
          new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

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

        final int pointsToBeCollected = endIndex - tvList.size();
        if (pointsToBeCollected > 0) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldPoints(parentLayerReader, tvList, pointsToBeCollected);
          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            beginIndex -= slidingStep;
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }

          if (tvList.size() <= beginIndex) {
            return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
          }

          // TVList's size may be less than endIndex
          // When parent layer reader has no more data
          endIndex = Math.min(endIndex, tvList.size());
        }

        window.seek(beginIndex, endIndex, tvList.getTime(beginIndex), tvList.getTime(endIndex - 1));

        hasCached = true;
        return YieldableState.YIELDABLE;
      }

      @Override
      public void readyForNext() {
        hasCached = false;

        tvList.setEvictionUpperBound(beginIndex + 1);
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {dataType};
      }

      @Override
      public RowWindow currentWindow() {
        return window;
      }
    };
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingTimeWindowReader(
      SlidingTimeWindowAccessStrategy strategy, float memoryBudgetInMB) {
    final long timeInterval = strategy.getTimeInterval();
    final long slidingStep = strategy.getSlidingStep();
    final long displayWindowEnd = strategy.getDisplayWindowEnd();

    final ElasticSerializableTVList tvList =
        ElasticSerializableTVList.construct(dataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    return new LayerRowWindowReader() {
      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private boolean hasCached = false;
      private long nextWindowTimeBegin = strategy.getDisplayWindowBegin();
      private int nextIndexBegin = 0;
      private int nextIndexEnd = 0;
      private long currentEndTime = Long.MAX_VALUE;

      private final TVListForwardIterator beginIterator = tvList.constructIterator();
      private Column cachedBeginTimeColumn;
      private int cachedBeginConsumed;

      private Column cachedEndTimeColumn;
      private int cachedEndConsumed;

      @Override
      public YieldableState yield() throws Exception {
        if (isFirstIteration) {
          if (tvList.size() == 0) {
            final YieldableState state = parentLayerReader.yield();
            if (state != YieldableState.YIELDABLE) {
              return state;
            }

            Column[] columns = parentLayerReader.current();
            TimeColumn times = (TimeColumn) columns[1];
            Column values = columns[0];

            tvList.putColumn(times, values);
            parentLayerReader.consumedAll();

            cachedEndTimeColumn = times;
          }
          if (nextWindowTimeBegin == Long.MIN_VALUE) {
            // display window begin should be set to the same as the min timestamp of the query
            // result set
            nextWindowTimeBegin = cachedEndTimeColumn.getLong(0);
          }
          hasAtLeastOneRow = tvList.size() != 0;
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
          final YieldableState state = parentLayerReader.yield();
          if (state == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }
          if (state == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
            break;
          }
          // Generate data
          Column[] columns = parentLayerReader.current();
          TimeColumn times = (TimeColumn) columns[1];
          Column values = columns[0];
          // Put data into container
          tvList.putColumn(times, values);
          parentLayerReader.consumedAll();
          currentEndTime = times.getEndTime();
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
              cachedBeginTimeColumn = beginIterator.currentTimes();
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

        hasCached = !(nextIndexBegin == nextIndexEnd && nextIndexEnd == tvList.size());
        return hasCached ? YieldableState.YIELDABLE : YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
      }

      @Override
      public void readyForNext() {
        hasCached = false;
        nextWindowTimeBegin += slidingStep;

        tvList.setEvictionUpperBound(nextIndexBegin + 1);
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {dataType};
      }

      @Override
      public RowWindow currentWindow() {
        return window;
      }
    };
  }

  @Override
  protected LayerRowWindowReader constructRowSessionTimeWindowReader(
      SessionTimeWindowAccessStrategy strategy, float memoryBudgetInMB) {

    final long displayWindowBegin = strategy.getDisplayWindowBegin();
    final long displayWindowEnd = strategy.getDisplayWindowEnd();
    final long sessionTimeGap = strategy.getSessionTimeGap();

    final ElasticSerializableTVList tvList =
        ElasticSerializableTVList.construct(dataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    return new LayerRowWindowReader() {

      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private long nextWindowTimeBegin = displayWindowBegin;
      private long nextWindowTimeEnd = 0;
      private int nextIndexBegin = 0;
      private int nextIndexEnd = 0;

      private TimeColumn cachedTimes;
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
            if (cachedTimes.getEndTime() < displayWindowEnd) {
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
        nextWindowTimeEnd = tvList.getTime(nextIndexEnd - 1);

        if (nextIndexBegin == nextIndexEnd) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }
        window.seek(nextIndexBegin, nextIndexEnd, nextWindowTimeBegin, nextWindowTimeEnd);
        return YieldableState.YIELDABLE;
      }

      private YieldableState yieldInFirstIteration() throws Exception {
        // Yield initial data in first iteration
        if (tvList.size() == 0) {
          YieldableState state = yieldAndCache();
          if (state != YieldableState.YIELDABLE) {
            return state;
          }
        }
        // Initialize essential information
        nextWindowTimeBegin = Math.max(displayWindowBegin, cachedTimes.getStartTime());
        hasAtLeastOneRow = tvList.size() != 0;
        isFirstIteration = false;

        // Set initial nextIndexBegin
        long currentEndTime = cachedTimes.getEndTime();
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
        final YieldableState state = parentLayerReader.yield();
        if (state != YieldableState.YIELDABLE) {
          return state;
        }
        Column[] columns = parentLayerReader.current();
        TimeColumn times = (TimeColumn) columns[1];
        Column values = columns[0];

        tvList.putColumn(times, values);
        parentLayerReader.consumedAll();

        cachedTimes = times;
        cachedConsumed = 0;

        return YieldableState.YIELDABLE;
      }

      @Override
      public void readyForNext() throws IOException {
        if (nextIndexEnd < tvList.size()) {
          nextWindowTimeBegin = tvList.getTime(nextIndexEnd);
        }
        tvList.setEvictionUpperBound(nextIndexBegin + 1);
        nextIndexBegin = nextIndexEnd;
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {dataType};
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

    final long displayWindowBegin = strategy.getDisplayWindowBegin();
    final long displayWindowEnd = strategy.getDisplayWindowEnd();
    final double delta = strategy.getDelta();

    final ElasticSerializableTVList tvList =
        ElasticSerializableTVList.construct(dataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    return new LayerRowWindowReader() {

      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private long nextWindowTimeBegin = displayWindowBegin;
      private long nextWindowTimeEnd = 0;
      private int nextIndexBegin = 0;
      private int nextIndexEnd = 0;

      private TimeColumn cachedTimes;
      private Column cachedValues;
      private int cachedConsumed;

      private final ValueRecorder valueRecorder = new ValueRecorder();

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

            if (TransformUtils.splitWindowForStateWindow(
                dataType, valueRecorder, delta, cachedValues, cachedConsumed)) {
              findWindow = true;
              break;
            }
            nextIndexEnd++;
            cachedConsumed++;
          }

          if (!findWindow) {
            if (cachedTimes.getEndTime() < displayWindowEnd) {
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
        nextWindowTimeEnd = tvList.getTime(nextIndexEnd - 1);

        if (nextIndexBegin == nextIndexEnd) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }
        window.seek(nextIndexBegin, nextIndexEnd, nextWindowTimeBegin, nextWindowTimeEnd);
        return YieldableState.YIELDABLE;
      }

      private YieldableState yieldInFirstIteration() throws Exception {
        // Yield initial data in first iteration
        if (tvList.size() == 0) {
          YieldableState state = yieldAndCache();
          if (state != YieldableState.YIELDABLE) {
            return state;
          }
        }
        // Initialize essential information
        nextWindowTimeBegin = Math.max(displayWindowBegin, cachedTimes.getStartTime());
        hasAtLeastOneRow = tvList.size() != 0;
        isFirstIteration = false;

        // Set initial nextIndexBegin
        long currentEndTime = cachedTimes.getEndTime();
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
        final YieldableState state = parentLayerReader.yield();
        if (state != YieldableState.YIELDABLE) {
          return state;
        }
        Column[] columns = parentLayerReader.current();
        TimeColumn times = (TimeColumn) columns[1];
        Column values = columns[0];

        tvList.putColumn(times, values);
        parentLayerReader.consumedAll();

        cachedTimes = times;
        cachedValues = values;
        cachedConsumed = 0;

        return YieldableState.YIELDABLE;
      }

      @Override
      public void readyForNext() throws IOException {
        if (nextIndexEnd < tvList.size()) {
          nextWindowTimeBegin = tvList.getTime(nextIndexEnd);
        }
        tvList.setEvictionUpperBound(nextIndexBegin + 1);
        nextIndexBegin = nextIndexEnd;
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {dataType};
      }

      @Override
      public RowWindow currentWindow() {
        return window;
      }
    };
  }
}
