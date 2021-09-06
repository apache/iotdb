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

public class SingleInputSingleOutputIntermediateLayer extends IntermediateLayer {

  private final LayerPointReader parentLayerPointReader;
  private final TSDataType dataType;

  public SingleInputSingleOutputIntermediateLayer(
      long queryId, float memoryBudgetInMB, LayerPointReader parentLayerPointReader) {
    super(queryId, memoryBudgetInMB);
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
      private final TSDataType[] dataTypes =
          new TSDataType[] {parentLayerPointReader.getDataType()};
      private boolean hasCached = false;

      @Override
      public boolean next() throws IOException, QueryProcessException {
        if (hasCached) {
          return true;
        }
        hasCached = parentLayerPointReader.next();
        return hasCached;
      }

      @Override
      public void readyForNext() {
        parentLayerPointReader.readyForNext();
        hasCached = false;
      }

      @Override
      public TSDataType[] getDataTypes() {
        return dataTypes;
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
              dataType, queryId, memoryBudgetInMB, 2);
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
          hasCached = collectPoints(pointsToBeCollected, tvList) != 0;
          window.seek(beginIndex, tvList.size());
        } else {
          hasCached = true;
          window.seek(beginIndex, endIndex);
        }

        return hasCached;
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
            dataType, queryId, memoryBudgetInMB, 2);
    final ElasticSerializableTVListBackedSingleColumnWindow window =
        new ElasticSerializableTVListBackedSingleColumnWindow(tvList);

    long nextWindowTimeBeginGivenByStrategy = strategy.getDisplayWindowBegin();
    if (tvList.size() == 0 && parentLayerPointReader.next()) {
      collectPoints(1, tvList);

      if (nextWindowTimeBeginGivenByStrategy == Long.MIN_VALUE) {
        // display window begin should be set to the same as the min timestamp of the query result
        // set
        nextWindowTimeBeginGivenByStrategy = tvList.getTime(0);
      }
    }
    long finalNextWindowTimeBeginGivenByStrategy = nextWindowTimeBeginGivenByStrategy;

    final boolean hasAtLeastOneRow = tvList.size() != 0;

    return new LayerRowWindowReader() {

      private long nextWindowTimeBegin = finalNextWindowTimeBeginGivenByStrategy;
      private int nextIndexBegin = 0;

      @Override
      public boolean next() throws IOException, QueryProcessException {
        if (displayWindowEnd <= nextWindowTimeBegin) {
          return false;
        }
        if (!hasAtLeastOneRow || 0 < tvList.size()) {
          return true;
        }

        long nextWindowTimeEnd = Math.min(nextWindowTimeBegin + timeInterval, displayWindowEnd);
        int oldTVListSize = tvList.size();
        while (tvList.getTime(tvList.size() - 1) < nextWindowTimeEnd) {
          if (parentLayerPointReader.next()) {
            collectPoints(1, tvList);
          } else if (displayWindowEnd == Long.MAX_VALUE
              // display window end == the max timestamp of the query result set
              && oldTVListSize == tvList.size()) {
            return false;
          } else {
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

        return true;
      }

      @Override
      public void readyForNext() {
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

  /** @return number of actually collected, which may be less than or equals to pointNumber */
  private int collectPoints(int pointNumber, ElasticSerializableTVList tvList)
      throws QueryProcessException, IOException {
    int count = 0;

    while (parentLayerPointReader.next() && count < pointNumber) {
      ++count;

      switch (dataType) {
        case INT32:
          tvList.putInt(parentLayerPointReader.currentTime(), parentLayerPointReader.currentInt());
          break;
        case INT64:
          tvList.putLong(
              parentLayerPointReader.currentTime(), parentLayerPointReader.currentLong());
          break;
        case FLOAT:
          tvList.putFloat(
              parentLayerPointReader.currentTime(), parentLayerPointReader.currentFloat());
          break;
        case DOUBLE:
          tvList.putDouble(
              parentLayerPointReader.currentTime(), parentLayerPointReader.currentDouble());
          break;
        case BOOLEAN:
          tvList.putBoolean(
              parentLayerPointReader.currentTime(), parentLayerPointReader.currentBoolean());
          break;
        case TEXT:
          tvList.putBinary(
              parentLayerPointReader.currentTime(), parentLayerPointReader.currentBinary());
          break;
        default:
      }

      parentLayerPointReader.readyForNext();
    }

    return count;
  }
}
