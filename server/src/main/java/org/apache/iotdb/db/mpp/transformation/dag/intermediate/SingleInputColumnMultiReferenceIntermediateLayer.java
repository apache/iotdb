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

package org.apache.iotdb.db.mpp.transformation.dag.intermediate;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.LayerRowReader;
import org.apache.iotdb.db.mpp.transformation.api.LayerRowWindowReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.adapter.ElasticSerializableTVListBackedSingleColumnRow;
import org.apache.iotdb.db.mpp.transformation.dag.adapter.ElasticSerializableTVListBackedSingleColumnWindow;
import org.apache.iotdb.db.mpp.transformation.dag.memory.SafetyLine;
import org.apache.iotdb.db.mpp.transformation.dag.memory.SafetyLine.SafetyPile;
import org.apache.iotdb.db.mpp.transformation.dag.util.LayerCacheUtils;
import org.apache.iotdb.db.mpp.transformation.dag.util.TransformUtils;
import org.apache.iotdb.db.mpp.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.mpp.transformation.datastructure.util.ValueRecorder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.strategy.SessionTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleInputColumnMultiReferenceIntermediateLayer extends IntermediateLayer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SingleInputColumnMultiReferenceIntermediateLayer.class);

  private final LayerPointReader parentLayerPointReader;
  private final TSDataType parentLayerPointReaderDataType;
  private final boolean isParentLayerPointReaderConstant;
  private final ElasticSerializableTVList tvList;
  private final SafetyLine safetyLine;

  public SingleInputColumnMultiReferenceIntermediateLayer(
      Expression expression,
      long queryId,
      float memoryBudgetInMB,
      LayerPointReader parentLayerPointReader) {
    super(expression, queryId, memoryBudgetInMB);
    this.parentLayerPointReader = parentLayerPointReader;

    parentLayerPointReaderDataType = parentLayerPointReader.getDataType();
    isParentLayerPointReaderConstant = parentLayerPointReader.isConstantPointReader();
    tvList =
        ElasticSerializableTVList.newElasticSerializableTVList(
            parentLayerPointReaderDataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    safetyLine = new SafetyLine();
  }

  @Override
  public LayerPointReader constructPointReader() {

    return new LayerPointReader() {

      private final SafetyPile safetyPile = safetyLine.addSafetyPile();

      private boolean hasCached = false;
      private int currentPointIndex = -1;

      @Override
      public boolean isConstantPointReader() {
        return isParentLayerPointReaderConstant;
      }

      @Override
      public YieldableState yield() throws IOException, QueryProcessException {
        if (hasCached) {
          return YieldableState.YIELDABLE;
        }

        if (currentPointIndex < tvList.size() - 1) {
          ++currentPointIndex;
          hasCached = true;
          return YieldableState.YIELDABLE;
        }

        final YieldableState yieldableState =
            LayerCacheUtils.yieldPoint(
                parentLayerPointReaderDataType, parentLayerPointReader, tvList);
        if (yieldableState == YieldableState.YIELDABLE) {
          ++currentPointIndex;
          hasCached = true;
        }
        return yieldableState;
      }

      @Override
      public boolean next() throws QueryProcessException, IOException {
        if (!hasCached
            && (currentPointIndex < tvList.size() - 1
                || LayerCacheUtils.cachePoint(
                    parentLayerPointReaderDataType, parentLayerPointReader, tvList))) {
          ++currentPointIndex;
          hasCached = true;
        }

        return hasCached;
      }

      @Override
      public void readyForNext() {
        hasCached = false;

        safetyPile.moveForwardTo(currentPointIndex + 1);
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());
      }

      @Override
      public TSDataType getDataType() {
        return parentLayerPointReaderDataType;
      }

      @Override
      public long currentTime() throws IOException {
        return tvList.getTime(currentPointIndex);
      }

      @Override
      public int currentInt() throws IOException {
        return tvList.getInt(currentPointIndex);
      }

      @Override
      public long currentLong() throws IOException {
        return tvList.getLong(currentPointIndex);
      }

      @Override
      public float currentFloat() throws IOException {
        return tvList.getFloat(currentPointIndex);
      }

      @Override
      public double currentDouble() throws IOException {
        return tvList.getDouble(currentPointIndex);
      }

      @Override
      public boolean currentBoolean() throws IOException {
        return tvList.getBoolean(currentPointIndex);
      }

      @Override
      public Binary currentBinary() throws IOException {
        return tvList.getBinary(currentPointIndex);
      }

      @Override
      public boolean isCurrentNull() throws IOException {
        return tvList.isNull(currentPointIndex);
      }
    };
  }

  @Override
  public LayerRowReader constructRowReader() {

    return new LayerRowReader() {

      private final SafetyPile safetyPile = safetyLine.addSafetyPile();
      private final ElasticSerializableTVListBackedSingleColumnRow row =
          new ElasticSerializableTVListBackedSingleColumnRow(tvList, -1);

      private boolean hasCached = false;
      private int currentRowIndex = -1;

      @Override
      public YieldableState yield() throws IOException, QueryProcessException {
        if (hasCached) {
          return YieldableState.YIELDABLE;
        }

        if (currentRowIndex < tvList.size() - 1) {
          row.seek(++currentRowIndex);
          hasCached = true;
          return YieldableState.YIELDABLE;
        }

        final YieldableState yieldableState =
            LayerCacheUtils.yieldPoint(
                parentLayerPointReaderDataType, parentLayerPointReader, tvList);
        if (yieldableState == YieldableState.YIELDABLE) {
          row.seek(++currentRowIndex);
          hasCached = true;
        }
        return yieldableState;
      }

      @Override
      public boolean next() throws QueryProcessException, IOException {
        if (!hasCached
            && ((currentRowIndex < tvList.size() - 1)
                || LayerCacheUtils.cachePoint(
                    parentLayerPointReaderDataType, parentLayerPointReader, tvList))) {
          row.seek(++currentRowIndex);
          hasCached = true;
        }

        return hasCached;
      }

      @Override
      public void readyForNext() {
        hasCached = false;

        safetyPile.moveForwardTo(currentRowIndex + 1);
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {parentLayerPointReaderDataType};
      }

      @Override
      public long currentTime() throws IOException {
        return row.getTime();
      }

      @Override
      public Row currentRow() {
        return row;
      }

      @Override
      public boolean isCurrentNull() throws IOException {
        return tvList.isNull(currentRowIndex);
      }
    };
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingSizeWindowReader(
      SlidingSizeWindowAccessStrategy strategy, float memoryBudgetInMB) {

    return new LayerRowWindowReader() {

      private final int windowSize = strategy.getWindowSize();
      private final int slidingStep = strategy.getSlidingStep();

      private final SafetyPile safetyPile = safetyLine.addSafetyPile();
      private final ElasticSerializableTVListBackedSingleColumnWindow window =
          new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

      private boolean hasCached = false;
      private int beginIndex = -slidingStep;

      @Override
      public YieldableState yield() throws IOException, QueryProcessException {
        if (hasCached) {
          return YieldableState.YIELDABLE;
        }

        beginIndex += slidingStep;
        final int endIndex = beginIndex + windowSize;
        if (beginIndex < 0 || endIndex < 0) {
          LOGGER.warn(
              "SingleInputColumnMultiReferenceIntermediateLayer$LayerRowWindowReader: index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.",
              beginIndex,
              endIndex,
              windowSize);
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        final int pointsToBeCollected = endIndex - tvList.size();
        if (0 < pointsToBeCollected) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldPoints(
                  parentLayerPointReaderDataType,
                  parentLayerPointReader,
                  tvList,
                  pointsToBeCollected);
          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            beginIndex -= slidingStep;
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }

          if (tvList.size() <= beginIndex) {
            return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
          }

          window.seek(
              beginIndex,
              tvList.size(),
              tvList.getTime(beginIndex),
              tvList.getTime(tvList.size() - 1));
        } else {
          window.seek(
              beginIndex, endIndex, tvList.getTime(beginIndex), tvList.getTime(endIndex - 1));
        }

        hasCached = true;
        return YieldableState.YIELDABLE;
      }

      @Override
      public boolean next() throws IOException, QueryProcessException {
        if (hasCached) {
          return true;
        }

        beginIndex += slidingStep;
        int endIndex = beginIndex + windowSize;
        if (beginIndex < 0 || endIndex < 0) {
          LOGGER.warn(
              "SingleInputColumnMultiReferenceIntermediateLayer$LayerRowWindowReader: index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.",
              beginIndex,
              endIndex,
              windowSize);
          return false;
        }

        int pointsToBeCollected = endIndex - tvList.size();
        if (0 < pointsToBeCollected) {
          LayerCacheUtils.cachePoints(
              parentLayerPointReaderDataType, parentLayerPointReader, tvList, pointsToBeCollected);
          if (tvList.size() <= beginIndex) {
            return false;
          }

          window.seek(
              beginIndex,
              tvList.size(),
              tvList.getTime(beginIndex),
              tvList.getTime(tvList.size() - 1));
        } else {
          window.seek(
              beginIndex, endIndex, tvList.getTime(beginIndex), tvList.getTime(endIndex - 1));
        }

        hasCached = true;
        return true;
      }

      @Override
      public void readyForNext() {
        hasCached = false;

        safetyPile.moveForwardTo(beginIndex + 1);
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {parentLayerPointReaderDataType};
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

    final SafetyPile safetyPile = safetyLine.addSafetyPile();
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    final long nextWindowTimeBeginGivenByStrategy = strategy.getDisplayWindowBegin();

    return new LayerRowWindowReader() {

      private boolean isFirstIteration = true;
      private boolean hasCached = false;
      private long nextWindowTimeBegin = nextWindowTimeBeginGivenByStrategy;
      private int nextIndexBegin = 0;
      private boolean hasAtLeastOneRow;

      @Override
      public YieldableState yield() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (tvList.size() == 0) {
            final YieldableState yieldableState =
                LayerCacheUtils.yieldPoint(
                    parentLayerPointReaderDataType, parentLayerPointReader, tvList);
            if (yieldableState != YieldableState.YIELDABLE) {
              return yieldableState;
            }
          }
          if (nextWindowTimeBeginGivenByStrategy == Long.MIN_VALUE) {
            // display window begin should be set to the same as the min timestamp of the query
            // result
            // set
            nextWindowTimeBegin = tvList.getTime(0);
          }
          hasAtLeastOneRow = tvList.size() != 0;
          isFirstIteration = false;
        }

        if (hasCached) {
          return YieldableState.YIELDABLE;
        }
        if (!hasAtLeastOneRow || displayWindowEnd <= nextWindowTimeBegin) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        long nextWindowTimeEnd = Math.min(nextWindowTimeBegin + timeInterval, displayWindowEnd);
        while (tvList.getTime(tvList.size() - 1) < nextWindowTimeEnd) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldPoint(
                  parentLayerPointReaderDataType, parentLayerPointReader, tvList);
          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }
          if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
            break;
          }
        }

        for (int i = nextIndexBegin; i < tvList.size(); ++i) {
          if (nextWindowTimeBegin <= tvList.getTime(i)) {
            nextIndexBegin = i;
            break;
          }
          if (i == tvList.size() - 1) {
            nextIndexBegin = tvList.size();
          }
        }

        int nextIndexEnd = tvList.size();
        for (int i = nextIndexBegin; i < tvList.size(); ++i) {
          if (nextWindowTimeEnd <= tvList.getTime(i)) {
            nextIndexEnd = i;
            break;
          }
        }

        if ((nextIndexEnd == nextIndexBegin)
            && nextWindowTimeEnd < tvList.getTime(tvList.size() - 1)) {
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
      public boolean next() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (tvList.size() == 0
              && LayerCacheUtils.cachePoint(
                  parentLayerPointReaderDataType, parentLayerPointReader, tvList)
              && nextWindowTimeBeginGivenByStrategy == Long.MIN_VALUE) {
            // display window begin should be set to the same as the min timestamp of the query
            // result
            // set
            nextWindowTimeBegin = tvList.getTime(0);
          }
          hasAtLeastOneRow = tvList.size() != 0;
          isFirstIteration = false;
        }
        if (hasCached) {
          return true;
        }
        if (!hasAtLeastOneRow || displayWindowEnd <= nextWindowTimeBegin) {
          return false;
        }

        long nextWindowTimeEnd = Math.min(nextWindowTimeBegin + timeInterval, displayWindowEnd);
        while (tvList.getTime(tvList.size() - 1) < nextWindowTimeEnd) {
          if (!LayerCacheUtils.cachePoint(
              parentLayerPointReaderDataType, parentLayerPointReader, tvList)) {
            break;
          }
        }

        for (int i = nextIndexBegin; i < tvList.size(); ++i) {
          if (nextWindowTimeBegin <= tvList.getTime(i)) {
            nextIndexBegin = i;
            break;
          }
          if (i == tvList.size() - 1) {
            nextIndexBegin = tvList.size();
          }
        }

        int nextIndexEnd = tvList.size();
        for (int i = nextIndexBegin; i < tvList.size(); ++i) {
          if (nextWindowTimeEnd <= tvList.getTime(i)) {
            nextIndexEnd = i;
            break;
          }
        }

        if ((nextIndexEnd == nextIndexBegin)
            && nextWindowTimeEnd < tvList.getTime(tvList.size() - 1)) {
          window.setEmptyWindow(nextWindowTimeBegin, nextWindowTimeEnd);
          return true;
        }

        window.seek(
            nextIndexBegin,
            nextIndexEnd,
            nextWindowTimeBegin,
            nextWindowTimeBegin + timeInterval - 1);

        hasCached = !(nextIndexBegin == nextIndexEnd && nextIndexEnd == tvList.size());
        return hasCached;
      }

      @Override
      public void readyForNext() {
        hasCached = false;
        nextWindowTimeBegin += slidingStep;

        safetyPile.moveForwardTo(nextIndexBegin + 1);
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {parentLayerPointReaderDataType};
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

    final SafetyPile safetyPile = safetyLine.addSafetyPile();
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    return new LayerRowWindowReader() {

      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private long nextWindowTimeBegin = displayWindowBegin;
      private long nextWindowTimeEnd = 0;
      private int nextIndexBegin = 0;
      private int nextIndexEnd = 1;

      @Override
      public YieldableState yield() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (tvList.size() == 0) {
            final YieldableState yieldableState =
                LayerCacheUtils.yieldPoint(
                    parentLayerPointReaderDataType, parentLayerPointReader, tvList);
            if (yieldableState != YieldableState.YIELDABLE) {
              return yieldableState;
            }
          }
          nextWindowTimeBegin = Math.max(displayWindowBegin, tvList.getTime(0));
          hasAtLeastOneRow = tvList.size() != 0;
          isFirstIteration = false;
        }

        if (!hasAtLeastOneRow || displayWindowEnd <= nextWindowTimeBegin) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        while (tvList.getTime(tvList.size() - 1) < displayWindowEnd) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldPoint(
                  parentLayerPointReaderDataType, parentLayerPointReader, tvList);
          if (yieldableState == YieldableState.YIELDABLE) {
            if (tvList.getTime(tvList.size() - 2) >= displayWindowBegin
                && tvList.getTime(tvList.size() - 1) - tvList.getTime(tvList.size() - 2)
                    > sessionTimeGap) {
              nextIndexEnd = tvList.size() - 1;
              break;
            } else {
              nextIndexEnd++;
            }
          } else if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          } else if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
            nextIndexEnd = tvList.size();
            break;
          }
        }

        nextWindowTimeEnd = tvList.getTime(nextIndexEnd - 1);

        if (nextIndexBegin == nextIndexEnd) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        // Only if encounter user set the strategy's displayWindowBegin, which will go into the for
        // loop to find the true index of the first window begin.
        // For other situation, we will only go into if (nextWindowTimeBegin <= tvList.getTime(i))
        // once.
        for (int i = nextIndexBegin; i < tvList.size(); ++i) {
          if (nextWindowTimeBegin <= tvList.getTime(i)) {
            nextIndexBegin = i;
            break;
          }
          // The first window's beginning time is greater than all the timestamp of the query result
          // set
          if (i == tvList.size() - 1) {
            return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
          }
        }

        window.seek(nextIndexBegin, nextIndexEnd, nextWindowTimeBegin, nextWindowTimeEnd);

        return YieldableState.YIELDABLE;
      }

      @Override
      public boolean next() throws IOException, QueryProcessException {
        return false;
      }

      @Override
      public void readyForNext() throws IOException, QueryProcessException {
        if (nextIndexEnd < tvList.size()) {
          nextWindowTimeBegin = tvList.getTime(nextIndexEnd);
        }
        safetyPile.moveForwardTo(nextIndexBegin + 1);
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());
        nextIndexBegin = nextIndexEnd;
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {parentLayerPointReaderDataType};
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

    final SafetyPile safetyPile = safetyLine.addSafetyPile();
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    return new LayerRowWindowReader() {

      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private long nextWindowTimeBegin = displayWindowBegin;
      private long nextWindowTimeEnd = 0;
      private int nextIndexBegin = 0;
      private int nextIndexEnd = 1;

      private ValueRecorder valueRecorder = new ValueRecorder();

      @Override
      public YieldableState yield() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (tvList.size() == 0) {
            final YieldableState yieldableState =
                LayerCacheUtils.yieldPoint(
                    parentLayerPointReaderDataType, parentLayerPointReader, tvList);
            if (yieldableState != YieldableState.YIELDABLE) {
              return yieldableState;
            }
          }
          nextWindowTimeBegin = Math.max(displayWindowBegin, tvList.getTime(0));
          hasAtLeastOneRow = tvList.size() != 0;
          isFirstIteration = false;
        }

        if (!hasAtLeastOneRow || displayWindowEnd <= nextWindowTimeBegin) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        while (tvList.getTime(tvList.size() - 1) < displayWindowEnd) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldPoint(
                  parentLayerPointReaderDataType, parentLayerPointReader, tvList);
          if (yieldableState == YieldableState.YIELDABLE) {
            if (tvList.getTime(tvList.size() - 2) >= displayWindowBegin
                && TransformUtils.splitWindowForStateWindow(
                    parentLayerPointReaderDataType, valueRecorder, delta, tvList)) {
              nextIndexEnd = tvList.size() - 1;
              break;
            } else {
              nextIndexEnd++;
            }
          } else if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          } else if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
            nextIndexEnd = tvList.size();
            break;
          }
        }

        nextWindowTimeEnd = tvList.getTime(nextIndexEnd - 1);

        if (nextIndexBegin == nextIndexEnd) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        // Only if encounter user set the strategy's displayWindowBegin, which will go into the for
        // loop to find the true index of the first window begin.
        // For other situation, we will only go into if (nextWindowTimeBegin <= tvList.getTime(i))
        // once.
        for (int i = nextIndexBegin; i < tvList.size(); ++i) {
          if (nextWindowTimeBegin <= tvList.getTime(i)) {
            nextIndexBegin = i;
            break;
          }
          // The first window's beginning time is greater than all the timestamp of the query result
          // set
          if (i == tvList.size() - 1) {
            return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
          }
        }

        window.seek(nextIndexBegin, nextIndexEnd, nextWindowTimeBegin, nextWindowTimeEnd);

        return YieldableState.YIELDABLE;
      }

      @Override
      public boolean next() throws IOException, QueryProcessException {
        return false;
      }

      @Override
      public void readyForNext() throws IOException, QueryProcessException {
        if (nextIndexEnd < tvList.size()) {
          nextWindowTimeBegin = tvList.getTime(nextIndexEnd);
        }
        safetyPile.moveForwardTo(nextIndexBegin + 1);
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());
        nextIndexBegin = nextIndexEnd;
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {parentLayerPointReaderDataType};
      }

      @Override
      public RowWindow currentWindow() {
        return window;
      }
    };
  }
}
