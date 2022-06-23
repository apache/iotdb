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
import org.apache.iotdb.db.mpp.transformation.dag.adapter.ElasticSerializableTVListBackedSingleColumnWindow;
import org.apache.iotdb.db.mpp.transformation.dag.adapter.LayerPointReaderBackedSingleColumnRow;
import org.apache.iotdb.db.mpp.transformation.dag.util.LayerCacheUtils;
import org.apache.iotdb.db.mpp.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleInputColumnSingleReferenceIntermediateLayer extends IntermediateLayer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SingleInputColumnSingleReferenceIntermediateLayer.class);

  private final LayerPointReader parentLayerPointReader;
  private final TSDataType dataType;

  public SingleInputColumnSingleReferenceIntermediateLayer(
      Expression expression,
      long queryId,
      float memoryBudgetInMB,
      LayerPointReader parentLayerPointReader) {
    super(expression, queryId, memoryBudgetInMB);
    this.parentLayerPointReader = parentLayerPointReader;
    dataType = parentLayerPointReader.getDataType();
  }

  @Override
  public LayerPointReader constructPointReader() {
    return parentLayerPointReader;
  }

  @Override
  public LayerRowReader constructRowReader() {

    return new LayerRowReader() {

      private final Row row = new LayerPointReaderBackedSingleColumnRow(parentLayerPointReader);

      private boolean hasCached = false;
      private boolean isCurrentNull = false;

      @Override
      public YieldableState yield() throws IOException, QueryProcessException {
        if (!hasCached) {
          final YieldableState yieldableState = parentLayerPointReader.yield();
          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }
          hasCached = yieldableState == YieldableState.YIELDABLE;
          if (hasCached) {
            isCurrentNull = parentLayerPointReader.isCurrentNull();
          }
        }
        return hasCached ? YieldableState.YIELDABLE : YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
      }

      @Override
      public boolean next() throws IOException, QueryProcessException {
        if (!hasCached) {
          hasCached = parentLayerPointReader.next();
          if (hasCached) {
            isCurrentNull = parentLayerPointReader.isCurrentNull();
          }
        }
        return hasCached;
      }

      @Override
      public void readyForNext() {
        hasCached = false;
        isCurrentNull = false;
        parentLayerPointReader.readyForNext();
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {parentLayerPointReader.getDataType()};
      }

      @Override
      public long currentTime() throws IOException {
        return parentLayerPointReader.currentTime();
      }

      @Override
      public Row currentRow() {
        return row;
      }

      @Override
      public boolean isCurrentNull() {
        return isCurrentNull;
      }
    };
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingSizeWindowReader(
      SlidingSizeWindowAccessStrategy strategy, float memoryBudgetInMB) {

    return new LayerRowWindowReader() {

      private final int windowSize = strategy.getWindowSize();
      private final int slidingStep = strategy.getSlidingStep();

      private final ElasticSerializableTVList tvList =
          ElasticSerializableTVList.newElasticSerializableTVList(
              dataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
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
        int endIndex = beginIndex + windowSize;
        if (beginIndex < 0 || endIndex < 0) {
          LOGGER.warn(
              "SingleInputColumnSingleReferenceIntermediateLayer$LayerRowWindowReader: index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.",
              beginIndex,
              endIndex,
              windowSize);
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        int pointsToBeCollected = endIndex - tvList.size();
        if (0 < pointsToBeCollected) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldPoints(
                  dataType, parentLayerPointReader, tvList, pointsToBeCollected);
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
              "SingleInputColumnSingleReferenceIntermediateLayer$LayerRowWindowReader: index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.",
              beginIndex,
              endIndex,
              windowSize);
          return false;
        }

        int pointsToBeCollected = endIndex - tvList.size();
        if (0 < pointsToBeCollected) {
          LayerCacheUtils.cachePoints(
              dataType, parentLayerPointReader, tvList, pointsToBeCollected);
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
        ElasticSerializableTVList.newElasticSerializableTVList(
            dataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    return new LayerRowWindowReader() {

      private boolean isFirstIteration = true;
      private boolean hasAtLeastOneRow = false;

      private boolean hasCached = false;
      private long nextWindowTimeBegin = strategy.getDisplayWindowBegin();
      private int nextIndexBegin = 0;

      @Override
      public YieldableState yield() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (tvList.size() == 0 && nextWindowTimeBegin == Long.MIN_VALUE) {
            final YieldableState yieldableState =
                LayerCacheUtils.yieldPoint(dataType, parentLayerPointReader, tvList);
            if (yieldableState != YieldableState.YIELDABLE) {
              return yieldableState;
            }
            // display window begin should be set to the same as the min timestamp of the query
            // result set
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
              LayerCacheUtils.yieldPoint(dataType, parentLayerPointReader, tvList);
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
        window.seek(
            nextIndexBegin,
            nextIndexEnd,
            nextWindowTimeBegin,
            nextWindowTimeBegin + timeInterval - 1);

        hasCached = nextIndexBegin != nextIndexEnd;
        return hasCached ? YieldableState.YIELDABLE : YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
      }

      @Override
      public boolean next() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (tvList.size() == 0
              && LayerCacheUtils.cachePoint(dataType, parentLayerPointReader, tvList)
              && nextWindowTimeBegin == Long.MIN_VALUE) {
            // display window begin should be set to the same as the min timestamp of the query
            // result set
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
          if (!LayerCacheUtils.cachePoint(dataType, parentLayerPointReader, tvList)) {
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
        window.seek(
            nextIndexBegin,
            nextIndexEnd,
            nextWindowTimeBegin,
            nextWindowTimeBegin + timeInterval - 1);

        hasCached = nextIndexBegin != nextIndexEnd;
        return hasCached;
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
}
