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
import org.apache.iotdb.db.mpp.transformation.dag.adapter.ElasticSerializableRowRecordListBackedMultiColumnRow;
import org.apache.iotdb.db.mpp.transformation.dag.adapter.ElasticSerializableRowRecordListBackedMultiColumnWindow;
import org.apache.iotdb.db.mpp.transformation.dag.input.IUDFInputDataSet;
import org.apache.iotdb.db.mpp.transformation.dag.util.InputRowUtils;
import org.apache.iotdb.db.mpp.transformation.dag.util.LayerCacheUtils;
import org.apache.iotdb.db.mpp.transformation.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
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

  private final LayerPointReader[] layerPointReaders;
  private final TSDataType[] dataTypes;
  private final TimeSelector timeHeap;
  private final boolean[] shouldMoveNext;

  private boolean isFirstIteration = true;
  private Object[] cachedRow = null;

  public MultiInputColumnIntermediateLayer(
      Expression expression,
      long queryId,
      float memoryBudgetInMB,
      List<LayerPointReader> parentLayerPointReaders) {
    super(expression, queryId, memoryBudgetInMB);

    layerPointReaders = parentLayerPointReaders.toArray(new LayerPointReader[0]);

    dataTypes = new TSDataType[layerPointReaders.length];
    for (int i = 0; i < layerPointReaders.length; ++i) {
      dataTypes[i] = layerPointReaders[i].getDataType();
    }

    timeHeap = new TimeSelector(layerPointReaders.length << 1, true);

    shouldMoveNext = new boolean[dataTypes.length];
  }

  @Override
  public List<TSDataType> getDataTypes() {
    return Arrays.asList(dataTypes);
  }

  @Override
  public boolean hasNextRowInObjects() throws IOException {
    if (cachedRow != null) {
      return true;
    }

    if (isFirstIteration) {
      for (LayerPointReader reader : layerPointReaders) {
        if (reader.isConstantPointReader()) {
          continue;
        }
        try {
          if (reader.next()) {
            timeHeap.add(reader.currentTime());
          }
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      isFirstIteration = false;
    }

    if (timeHeap.isEmpty()) {
      return false;
    }

    final long minTime = timeHeap.pollFirst();

    final int columnLength = layerPointReaders.length;
    cachedRow = new Object[columnLength + 1];
    cachedRow[columnLength] = minTime;

    try {
      for (int i = 0; i < columnLength; ++i) {
        final LayerPointReader reader = layerPointReaders[i];
        if (!reader.next()
            || (!reader.isConstantPointReader() && reader.currentTime() != minTime)) {
          continue;
        }

        if (!reader.isCurrentNull()) {
          switch (reader.getDataType()) {
            case INT32:
              cachedRow[i] = reader.currentInt();
              break;
            case INT64:
              cachedRow[i] = reader.currentLong();
              break;
            case FLOAT:
              cachedRow[i] = reader.currentFloat();
              break;
            case DOUBLE:
              cachedRow[i] = reader.currentDouble();
              break;
            case BOOLEAN:
              cachedRow[i] = reader.currentBoolean();
              break;
            case TEXT:
              cachedRow[i] = reader.currentBinary();
              break;
            default:
              throw new UnSupportedDataTypeException("Unsupported data type.");
          }
        }

        reader.readyForNext();

        if (!reader.isConstantPointReader() && reader.next()) {
          timeHeap.add(reader.currentTime());
        }
      }
    } catch (QueryProcessException e) {
      throw new IOException(e.getMessage());
    }

    return true;
  }

  @Override
  public YieldableState canYieldNextRowInObjects() throws IOException {
    if (cachedRow != null) {
      return YieldableState.YIELDABLE;
    }

    if (isFirstIteration) {
      for (LayerPointReader reader : layerPointReaders) {
        if (reader.isConstantPointReader()) {
          continue;
        }
        try {
          final YieldableState yieldableState = reader.yield();
          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }
          if (yieldableState == YieldableState.YIELDABLE) {
            timeHeap.add(reader.currentTime());
          }
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      isFirstIteration = false;
    } else {
      for (int i = 0, columnLength = layerPointReaders.length; i < columnLength; ++i) {
        if (shouldMoveNext[i]) {
          layerPointReaders[i].readyForNext();
          shouldMoveNext[i] = false;
        }
      }

      for (LayerPointReader layerPointReader : layerPointReaders) {
        try {
          if (!layerPointReader.isConstantPointReader()) {
            final YieldableState yieldableState = layerPointReader.yield();
            if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
              return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
            }
            if (yieldableState == YieldableState.YIELDABLE) {
              timeHeap.add(layerPointReader.currentTime());
            }
          }
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }

    if (timeHeap.isEmpty()) {
      return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
    }

    final long minTime = timeHeap.pollFirst();

    final int columnLength = layerPointReaders.length;
    final Object[] row = new Object[columnLength + 1];
    row[columnLength] = minTime;

    try {
      for (int i = 0; i < columnLength; ++i) {
        final LayerPointReader reader = layerPointReaders[i];

        final YieldableState yieldableState = reader.yield();
        if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
          for (int j = 0; j <= i; ++j) {
            shouldMoveNext[j] = false;
          }
          return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
        }
        if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
          continue;
        }
        if (!reader.isConstantPointReader() && reader.currentTime() != minTime) {
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

        shouldMoveNext[i] = true;
      }

      cachedRow = row;
    } catch (QueryProcessException e) {
      throw new IOException(e.getMessage());
    }

    return YieldableState.YIELDABLE;
  }

  @Override
  public Object[] nextRowInObjects() {
    final Object[] returnedRow = cachedRow;
    cachedRow = null;
    return returnedRow;
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
      public YieldableState yield() throws IOException {
        if (hasCached) {
          return YieldableState.YIELDABLE;
        }

        final YieldableState yieldableState = canYieldNextRowInObjects();
        if (yieldableState != YieldableState.YIELDABLE) {
          return yieldableState;
        }

        Object[] rowRecords = nextRowInObjects();
        currentNull = InputRowUtils.isAllNull(rowRecords);
        row.setRowRecord(rowRecords);
        hasCached = true;
        return YieldableState.YIELDABLE;
      }

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
      public YieldableState yield() throws IOException, QueryProcessException {
        if (hasCached) {
          return YieldableState.YIELDABLE;
        }

        beginIndex += slidingStep;
        int endIndex = beginIndex + windowSize;
        if (beginIndex < 0 || endIndex < 0) {
          LOGGER.warn(
              "MultiInputColumnIntermediateLayer$LayerRowWindowReader: index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.",
              beginIndex,
              endIndex,
              windowSize);
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        final int rowsToBeCollected = endIndex - rowRecordList.size();
        if (0 < rowsToBeCollected) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldRows(udfInputDataSet, rowRecordList, rowsToBeCollected);

          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            beginIndex -= slidingStep;
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }

          if (rowRecordList.size() <= beginIndex) {
            return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
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
      public YieldableState yield() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (rowRecordList.size() == 0 && nextWindowTimeBegin == Long.MIN_VALUE) {
            final YieldableState yieldableState =
                LayerCacheUtils.yieldRow(udfInputDataSet, rowRecordList);
            if (yieldableState != YieldableState.YIELDABLE) {
              return yieldableState;
            }
            // display window begin should be set to the same as the min timestamp of the query
            // result set
            nextWindowTimeBegin = rowRecordList.getTime(0);
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
              LayerCacheUtils.yieldRow(udfInputDataSet, rowRecordList);
          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }
          if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
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
      public boolean next() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (rowRecordList.size() == 0
              && LayerCacheUtils.cacheRow(udfInputDataSet, rowRecordList)
              && nextWindowTimeBegin == Long.MIN_VALUE) {
            // display window begin should be set to the same as the min timestamp of the query
            // result set
            nextWindowTimeBegin = rowRecordList.getTime(0);
          }
          hasAtLeastOneRow = rowRecordList.size() != 0;
          isFirstIteration = false;
        }

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

        if ((nextIndexEnd == nextIndexBegin)
            && nextWindowTimeEnd < rowRecordList.getTime(rowRecordList.size() - 1)) {
          window.setEmptyWindow(nextWindowTimeBegin, nextWindowTimeEnd);
          return true;
        }

        window.seek(
            nextIndexBegin,
            nextIndexEnd,
            nextWindowTimeBegin,
            nextWindowTimeBegin + timeInterval - 1);

        hasCached = !(nextIndexBegin == nextIndexEnd && nextIndexEnd == rowRecordList.size());
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
      public YieldableState yield() throws IOException, QueryProcessException {
        if (isFirstIteration) {
          if (rowRecordList.size() == 0) {
            final YieldableState yieldableState =
                LayerCacheUtils.yieldRow(udfInputDataSet, rowRecordList);
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

        while (rowRecordList.getTime(rowRecordList.size() - 1) < displayWindowEnd) {
          final YieldableState yieldableState =
              LayerCacheUtils.yieldRow(udfInputDataSet, rowRecordList);
          if (yieldableState == YieldableState.YIELDABLE) {
            if (rowRecordList.getTime(rowRecordList.size() - 2) >= displayWindowBegin
                && rowRecordList.getTime(rowRecordList.size() - 1)
                        - rowRecordList.getTime(rowRecordList.size() - 2)
                    > sessionTimeGap) {
              nextIndexEnd = rowRecordList.size() - 1;
              break;
            } else {
              nextIndexEnd++;
            }
          } else if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          } else if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
            nextIndexEnd = rowRecordList.size();
            break;
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
          if (nextWindowTimeBegin <= rowRecordList.getTime(i)) {
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
      public boolean next() throws IOException, QueryProcessException {
        return false;
      }

      @Override
      public void readyForNext() throws IOException, QueryProcessException {
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
