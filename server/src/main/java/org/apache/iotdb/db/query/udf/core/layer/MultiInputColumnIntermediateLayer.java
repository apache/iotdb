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
import org.apache.iotdb.db.query.dataset.UDFInputDataSet;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.core.access.ElasticSerializableRowRecordListBackedMultiColumnRow;
import org.apache.iotdb.db.query.udf.core.access.ElasticSerializableRowRecordListBackedMultiColumnWindow;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowWindowReader;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MultiInputColumnIntermediateLayer extends IntermediateLayer
    implements UDFInputDataSet {

  private final LayerPointReader[] layerPointReaders;
  private final TSDataType[] dataTypes;
  private final TimeSelector timeHeap;

  public MultiInputColumnIntermediateLayer(
      Expression expression,
      long queryId,
      float memoryBudgetInMB,
      List<LayerPointReader> parentLayerPointReaders)
      throws QueryProcessException, IOException {
    super(expression, queryId, memoryBudgetInMB);

    layerPointReaders = parentLayerPointReaders.toArray(new LayerPointReader[0]);

    dataTypes = new TSDataType[layerPointReaders.length];
    for (int i = 0; i < layerPointReaders.length; ++i) {
      dataTypes[i] = layerPointReaders[i].getDataType();
    }

    timeHeap = new TimeSelector(layerPointReaders.length << 1, true);
    for (LayerPointReader reader : layerPointReaders) {
      if (reader.next()) {
        timeHeap.add(reader.currentTime());
      }
    }
  }

  @Override
  public List<TSDataType> getDataTypes() {
    return Arrays.asList(dataTypes);
  }

  @Override
  public boolean hasNextRowInObjects() {
    return !timeHeap.isEmpty();
  }

  @Override
  public Object[] nextRowInObjects() throws IOException {
    long minTime = timeHeap.pollFirst();

    int rowLength = layerPointReaders.length;
    Object[] row = new Object[rowLength + 1];
    row[rowLength] = minTime;

    try {
      for (int i = 0; i < rowLength; ++i) {
        LayerPointReader reader = layerPointReaders[i];
        if (!reader.next() || reader.currentTime() != minTime) {
          continue;
        }

        switch (reader.getDataType()) {
          case INT32:
            row[i] = reader.currentInt();
            break;
          case INT64:
            row[i] = reader.currentLong();
            break;
          case FLOAT:
            row[i] = reader.currentFloat();
            break;
          case DOUBLE:
            row[i] = reader.currentDouble();
            break;
          case BOOLEAN:
            row[i] = reader.currentBoolean();
            break;
          case TEXT:
            row[i] = reader.currentBinary();
            break;
          default:
            throw new UnSupportedDataTypeException("Unsupported data type.");
        }
        reader.readyForNext();

        if (reader.next()) {
          timeHeap.add(reader.currentTime());
        }
      }
    } catch (QueryProcessException e) {
      throw new IOException(e.getMessage());
    }

    return row;
  }

  @Override
  public LayerPointReader constructPointReader() {
    throw new UnsupportedOperationException();
  }

  @Override
  public LayerRowReader constructRowReader() {

    return new LayerRowReader() {

      private final ElasticSerializableRowRecordListBackedMultiColumnRow row =
          new ElasticSerializableRowRecordListBackedMultiColumnRow(dataTypes);

      private boolean hasCached = false;

      @Override
      public boolean next() throws IOException {
        if (hasCached) {
          return true;
        }

        if (!hasNextRowInObjects()) {
          return false;
        }

        row.setRowRecord(nextRowInObjects());
        hasCached = true;
        return true;
      }

      @Override
      public void readyForNext() {
        hasCached = false;
      }

      @Override
      public TSDataType[] getDataTypes() {
        return dataTypes;
      }

      @Override
      public long currentTime() {
        return row.getTime();
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

    final UDFInputDataSet udfInputDataSet = this;

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
      public boolean next() throws IOException, QueryProcessException {
        if (hasCached) {
          return true;
        }

        beginIndex += slidingStep;
        int endIndex = beginIndex + windowSize;

        int rowsToBeCollected = endIndex - rowRecordList.size();
        if (0 < rowsToBeCollected) {
          LayerCacheUtils.cacheRows(udfInputDataSet, rowRecordList, rowsToBeCollected);
          if (rowRecordList.size() <= beginIndex) {
            return false;
          }

          window.seek(beginIndex, rowRecordList.size());
        } else {
          window.seek(beginIndex, endIndex);
        }

        hasCached = true;
        return true;
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
      throws QueryProcessException, IOException {

    final long timeInterval = strategy.getTimeInterval();
    final long slidingStep = strategy.getSlidingStep();
    final long displayWindowEnd = strategy.getDisplayWindowEnd();

    final UDFInputDataSet udfInputDataSet = this;
    final ElasticSerializableRowRecordList rowRecordList =
        new ElasticSerializableRowRecordList(
            dataTypes, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    final ElasticSerializableRowRecordListBackedMultiColumnWindow window =
        new ElasticSerializableRowRecordListBackedMultiColumnWindow(rowRecordList);

    long nextWindowTimeBeginGivenByStrategy = strategy.getDisplayWindowBegin();
    if (rowRecordList.size() == 0
        && LayerCacheUtils.cacheRow(udfInputDataSet, rowRecordList)
        && nextWindowTimeBeginGivenByStrategy == Long.MIN_VALUE) {
      // display window begin should be set to the same as the min timestamp of the query result
      // set
      nextWindowTimeBeginGivenByStrategy = rowRecordList.getTime(0);
    }
    long finalNextWindowTimeBeginGivenByStrategy = nextWindowTimeBeginGivenByStrategy;

    final boolean hasAtLeastOneRow = rowRecordList.size() != 0;

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
        while (rowRecordList.getTime(rowRecordList.size() - 1) < nextWindowTimeEnd) {
          if (!LayerCacheUtils.cacheRow(udfInputDataSet, rowRecordList)) {
            break;
          }
        }

        for (int i = nextIndexBegin; i < rowRecordList.size(); ++i) {
          if (nextWindowTimeBegin <= rowRecordList.getTime(i)) {
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
        window.seek(nextIndexBegin, nextIndexEnd);

        hasCached = nextIndexBegin != nextIndexEnd;
        return hasCached;
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
}
