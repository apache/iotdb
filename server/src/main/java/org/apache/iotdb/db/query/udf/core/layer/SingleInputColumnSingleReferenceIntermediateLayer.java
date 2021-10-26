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

package org.apache.iotdb.db.query.udf.core.layer;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.core.access.ElasticSerializableTVListBackedSingleColumnWindow;
import org.apache.iotdb.db.query.udf.core.access.LayerPointReaderBackedSingleColumnRow;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowWindowReader;
import org.apache.iotdb.db.query.udf.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class SingleInputColumnSingleReferenceIntermediateLayer extends IntermediateLayer {

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

      @Override
      public boolean next() throws IOException, QueryProcessException {
        if (!hasCached) {
          hasCached = parentLayerPointReader.next();
        }
        return hasCached;
      }

      @Override
      public void readyForNext() {
        hasCached = false;

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
    };
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingSizeWindowReader(
      SlidingSizeWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException {

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
      public boolean next() throws IOException, QueryProcessException {
        if (hasCached) {
          return true;
        }

        beginIndex += slidingStep;
        int endIndex = beginIndex + windowSize;

        int pointsToBeCollected = endIndex - tvList.size();
        if (0 < pointsToBeCollected) {
          LayerCacheUtils.cachePoints(
              dataType, parentLayerPointReader, tvList, pointsToBeCollected);
          if (tvList.size() <= beginIndex) {
            return false;
          }

          window.seek(beginIndex, tvList.size());
        } else {
          window.seek(beginIndex, endIndex);
        }

        hasCached = true;
        return true;
      }

      @Override
      public void readyForNext() {
        hasCached = false;
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
      SlidingTimeWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException, IOException {

    final long timeInterval = strategy.getTimeInterval();
    final long slidingStep = strategy.getSlidingStep();
    final long displayWindowEnd = strategy.getDisplayWindowEnd();

    final ElasticSerializableTVList tvList =
        ElasticSerializableTVList.newElasticSerializableTVList(
            dataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    long nextWindowTimeBeginGivenByStrategy = strategy.getDisplayWindowBegin();
    if (tvList.size() == 0
        && LayerCacheUtils.cachePoint(dataType, parentLayerPointReader, tvList)
        && nextWindowTimeBeginGivenByStrategy == Long.MIN_VALUE) {
      // display window begin should be set to the same as the min timestamp of the query result
      // set
      nextWindowTimeBeginGivenByStrategy = tvList.getTime(0);
    }
    long finalNextWindowTimeBeginGivenByStrategy = nextWindowTimeBeginGivenByStrategy;

    final boolean hasAtLeastOneRow = tvList.size() != 0;

    return new LayerRowWindowReader() {

      private boolean hasCached = false;
      private long nextWindowTimeBegin = finalNextWindowTimeBeginGivenByStrategy;
      private int nextIndexBegin = 0;

      @Override
      public boolean next() throws IOException, QueryProcessException {
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
        window.seek(nextIndexBegin, nextIndexEnd);

        hasCached = nextIndexBegin != nextIndexEnd;
        return hasCached;
      }

      @Override
      public void readyForNext() {
        hasCached = false;
        nextWindowTimeBegin += slidingStep;
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
