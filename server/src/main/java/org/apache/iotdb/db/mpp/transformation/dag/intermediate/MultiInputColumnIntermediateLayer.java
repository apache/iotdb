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
import org.apache.iotdb.db.mpp.transformation.dag.adapter.ElasticSerializableRowRecordListBackedMultiColumnRow;
import org.apache.iotdb.db.mpp.transformation.dag.adapter.ElasticSerializableRowRecordListBackedMultiColumnWindow;
import org.apache.iotdb.db.mpp.transformation.dag.util.InputRowUtils;
import org.apache.iotdb.db.mpp.transformation.dag.util.LayerCacheUtils;
import org.apache.iotdb.db.mpp.transformation.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.query.dataset.IUDFInputDataSet;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MultiInputColumnIntermediateLayer extends IntermediateLayer
    implements IUDFInputDataSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MultiInputColumnIntermediateLayer.class);

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
      if (reader.isConstantPointReader()) {
        continue;
      }
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
        if (!reader.next()
            || (!reader.isConstantPointReader() && reader.currentTime() != minTime)) {
          continue;
        }

        if (!reader.isCurrentNull()) {
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
        }

        reader.readyForNext();

        if (!(reader.isConstantPointReader()) && reader.next()) {
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
      private boolean currentNull = false;

      @Override
      public boolean next() throws IOException {
        if (hasCached) {
          return true;
        }

        if (!hasNextRowInObjects()) {
          return false;
        }
        Object[] rowRecords = nextRowInObjects();
        currentNull = InputRowUtils.isAllNull(rowRecords);
        row.setRowRecord(rowRecords);
        hasCached = true;
        return true;
      }

      @Override
      public void readyForNext() {
        hasCached = false;
        currentNull = false;
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

      @Override
      public boolean isCurrentNull() {
        return currentNull;
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
      public boolean next() throws IOException, QueryProcessException {
        if (hasCached) {
          return true;
        }

        beginIndex += slidingStep;
        int endIndex = beginIndex + windowSize;
        if (beginIndex < 0 || endIndex < 0) {
          LOGGER.warn(
              "MultiInputColumnIntermediateLayer$LayerRowWindowReader: index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.",
              beginIndex,
              endIndex,
              windowSize);
          return false;
        }

        int rowsToBeCollected = endIndex - rowRecordList.size();
        if (0 < rowsToBeCollected) {
          LayerCacheUtils.cacheRows(udfInputDataSet, rowRecordList, rowsToBeCollected);
          if (rowRecordList.size() <= beginIndex) {
            return false;
          }

          window.seek(
              beginIndex,
              rowRecordList.size(),
              rowRecordList.getTime(beginIndex),
              rowRecordList.getTime(rowRecordList.size() - 1));
        } else {
          window.seek(
              beginIndex,
              endIndex,
              rowRecordList.getTime(beginIndex),
              rowRecordList.getTime(endIndex - 1));
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

    final IUDFInputDataSet udfInputDataSet = this;
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
