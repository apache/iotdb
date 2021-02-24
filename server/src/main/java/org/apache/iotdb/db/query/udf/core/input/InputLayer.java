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

package org.apache.iotdb.db.query.udf.core.input;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithValueFilter;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.dataset.UDFInputDataSet;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.core.access.RowImpl;
import org.apache.iotdb.db.query.udf.core.access.RowWindowImpl;
import org.apache.iotdb.db.query.udf.core.input.SafetyLine.SafetyPile;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowWindowReader;
import org.apache.iotdb.db.query.udf.datastructure.primitive.ElasticSerializableIntList;
import org.apache.iotdb.db.query.udf.datastructure.primitive.IntList;
import org.apache.iotdb.db.query.udf.datastructure.primitive.SerializableIntList;
import org.apache.iotdb.db.query.udf.datastructure.primitive.WrappedIntArray;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.List;

public class InputLayer {

  private long queryId;

  private UDFInputDataSet queryDataSet;
  private TSDataType[] dataTypes;
  private int timestampIndex;

  private ElasticSerializableRowRecordList rowRecordList;
  private SafetyLine safetyLine;

  /** InputLayerWithoutValueFilter */
  public InputLayer(
      long queryId,
      float memoryBudgetInMB,
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      List<ManagedSeriesReader> readers)
      throws QueryProcessException, IOException, InterruptedException {
    constructInputLayer(
        queryId,
        memoryBudgetInMB,
        new RawQueryDataSetWithoutValueFilter(queryId, paths, dataTypes, readers, true));
  }

  /** InputLayerWithValueFilter */
  public InputLayer(
      long queryId,
      float memoryBudgetInMB,
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      TimeGenerator timeGenerator,
      List<IReaderByTimestamp> readers,
      List<Boolean> cached)
      throws QueryProcessException {
    constructInputLayer(
        queryId,
        memoryBudgetInMB,
        new RawQueryDataSetWithValueFilter(paths, dataTypes, timeGenerator, readers, cached, true));
  }

  private void constructInputLayer(
      long queryId, float memoryBudgetInMB, UDFInputDataSet queryDataSet)
      throws QueryProcessException {
    this.queryId = queryId;
    this.queryDataSet = queryDataSet;
    dataTypes = queryDataSet.getDataTypes().toArray(new TSDataType[0]);
    timestampIndex = dataTypes.length;
    rowRecordList =
        new ElasticSerializableRowRecordList(
            dataTypes, queryId, memoryBudgetInMB, 1 + dataTypes.length / 2);
    safetyLine = new SafetyLine();
  }

  public void updateRowRecordListEvictionUpperBound() {
    rowRecordList.setEvictionUpperBound(safetyLine.getSafetyLine());
  }

  public LayerPointReader constructPointReader(int columnIndex) {
    return new InputLayerPointReader(columnIndex);
  }

  public LayerRowReader constructRowReader(int[] columnIndexes) {
    return new InputLayerRowReader(columnIndexes);
  }

  public LayerRowWindowReader constructRowWindowReader(
      int[] columnIndexes, AccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException, IOException {
    switch (strategy.getAccessStrategyType()) {
      case SLIDING_TIME_WINDOW:
        return new InputLayerRowSlidingTimeWindowReader(
            columnIndexes, (SlidingTimeWindowAccessStrategy) strategy, memoryBudgetInMB);
      case SLIDING_SIZE_WINDOW:
        return new InputLayerRowSlidingSizeWindowReader(
            columnIndexes, (SlidingSizeWindowAccessStrategy) strategy, memoryBudgetInMB);
      default:
        throw new IllegalStateException(
            "Unexpected access strategy: " + strategy.getAccessStrategyType());
    }
  }

  private class InputLayerPointReader implements LayerPointReader {

    private final SafetyPile safetyPile;

    private final int columnIndex;
    private int currentRowIndex;

    private boolean hasCachedRowRecord;
    private Object[] cachedRowRecord;

    InputLayerPointReader(int columnIndex) {
      safetyPile = safetyLine.addSafetyPile();

      this.columnIndex = columnIndex;
      currentRowIndex = -1;

      hasCachedRowRecord = false;
      cachedRowRecord = null;
    }

    @Override
    public boolean next() throws IOException, QueryProcessException {
      if (hasCachedRowRecord) {
        return true;
      }

      for (int i = currentRowIndex + 1; i < rowRecordList.size(); ++i) {
        Object[] rowRecordCandidate = rowRecordList.getRowRecord(i);
        if (rowRecordCandidate[columnIndex] != null) {
          hasCachedRowRecord = true;
          cachedRowRecord = rowRecordCandidate;
          currentRowIndex = i;
          break;
        }
      }

      if (!hasCachedRowRecord) {
        while (queryDataSet.hasNextRowInObjects()) {
          Object[] rowRecordCandidate = queryDataSet.nextRowInObjects();
          rowRecordList.put(rowRecordCandidate);
          if (rowRecordCandidate[columnIndex] != null) {
            hasCachedRowRecord = true;
            cachedRowRecord = rowRecordCandidate;
            currentRowIndex = rowRecordList.size() - 1;
            break;
          }
        }
      }

      return hasCachedRowRecord;
    }

    @Override
    public void readyForNext() {
      hasCachedRowRecord = false;
      cachedRowRecord = null;

      safetyPile.moveForwardTo(currentRowIndex + 1);
    }

    @Override
    public TSDataType getDataType() {
      return dataTypes[columnIndex];
    }

    @Override
    public long currentTime() {
      return (long) cachedRowRecord[timestampIndex];
    }

    @Override
    public int currentInt() {
      return (int) cachedRowRecord[columnIndex];
    }

    @Override
    public long currentLong() {
      return (long) cachedRowRecord[columnIndex];
    }

    @Override
    public float currentFloat() {
      return (float) cachedRowRecord[columnIndex];
    }

    @Override
    public double currentDouble() {
      return (double) cachedRowRecord[columnIndex];
    }

    @Override
    public boolean currentBoolean() {
      return (boolean) cachedRowRecord[columnIndex];
    }

    @Override
    public Binary currentBinary() {
      return (Binary) cachedRowRecord[columnIndex];
    }
  }

  private class InputLayerRowReader implements LayerRowReader {

    private final SafetyPile safetyPile;

    private final int[] columnIndexes;
    private int currentRowIndex;

    private boolean hasCachedRowRecord;
    private Object[] cachedRowRecord;

    private final RowImpl row;

    public InputLayerRowReader(int[] columnIndexes) {
      safetyPile = safetyLine.addSafetyPile();

      this.columnIndexes = columnIndexes;
      currentRowIndex = -1;

      hasCachedRowRecord = false;
      cachedRowRecord = null;

      row = new RowImpl(columnIndexes, dataTypes);
    }

    @Override
    public boolean next() throws IOException, QueryProcessException {
      if (hasCachedRowRecord) {
        return true;
      }

      for (int i = currentRowIndex + 1; i < rowRecordList.size(); ++i) {
        Object[] rowRecordCandidate = rowRecordList.getRowRecord(i);
        if (hasNotNullSelectedFields(rowRecordCandidate, columnIndexes)) {
          hasCachedRowRecord = true;
          cachedRowRecord = rowRecordCandidate;
          currentRowIndex = i;
          break;
        }
      }

      if (!hasCachedRowRecord) {
        while (queryDataSet.hasNextRowInObjects()) {
          Object[] rowRecordCandidate = queryDataSet.nextRowInObjects();
          rowRecordList.put(rowRecordCandidate);
          if (hasNotNullSelectedFields(rowRecordCandidate, columnIndexes)) {
            hasCachedRowRecord = true;
            cachedRowRecord = rowRecordCandidate;
            currentRowIndex = rowRecordList.size() - 1;
            break;
          }
        }
      }

      return hasCachedRowRecord;
    }

    @Override
    public void readyForNext() {
      hasCachedRowRecord = false;
      cachedRowRecord = null;

      safetyPile.moveForwardTo(currentRowIndex + 1);
    }

    @Override
    public TSDataType[] getDataTypes() {
      return dataTypes;
    }

    @Override
    public long currentTime() {
      return (long) cachedRowRecord[timestampIndex];
    }

    @Override
    public Row currentRow() {
      return row.setRowRecord(cachedRowRecord);
    }
  }

  private class InputLayerRowSlidingSizeWindowReader implements LayerRowWindowReader {

    private final SafetyPile safetyPile;

    private final int[] columnIndexes;
    private final TSDataType[] columnDataTypes;

    private final int windowSize;
    private final IntList rowIndexes;
    private final RowWindowImpl rowWindow;

    private final int slidingStep;

    private int maxIndexInLastWindow;

    private InputLayerRowSlidingSizeWindowReader(
        int[] columnIndexes, SlidingSizeWindowAccessStrategy accessStrategy, float memoryBudgetInMB)
        throws QueryProcessException {
      safetyPile = safetyLine.addSafetyPile();

      this.columnIndexes = columnIndexes;
      columnDataTypes = new TSDataType[columnIndexes.length];
      for (int i = 0; i < columnIndexes.length; ++i) {
        columnDataTypes[i] = dataTypes[columnIndexes[i]];
      }

      windowSize = accessStrategy.getWindowSize();
      rowIndexes =
          windowSize < SerializableIntList.calculateCapacity(memoryBudgetInMB)
              ? new WrappedIntArray(windowSize)
              : new ElasticSerializableIntList(queryId, memoryBudgetInMB, 2);
      rowWindow = new RowWindowImpl(rowRecordList, columnIndexes, dataTypes, rowIndexes);

      slidingStep = accessStrategy.getSlidingStep();

      maxIndexInLastWindow = -1;
    }

    @Override
    public boolean next() throws IOException, QueryProcessException {
      if (0 < rowIndexes.size()) {
        return true;
      }

      int count = 0;

      for (int i = maxIndexInLastWindow + 1; i < rowRecordList.size(); ++i) {
        if (hasNotNullSelectedFields(rowRecordList.getRowRecord(i), columnIndexes)) {
          rowIndexes.put(i);
          ++count;
          if (count == windowSize) {
            return true;
          }
        }
      }

      while (queryDataSet.hasNextRowInObjects()) {
        Object[] rowRecordCandidate = queryDataSet.nextRowInObjects();
        rowRecordList.put(rowRecordCandidate);
        if (hasNotNullSelectedFields(rowRecordCandidate, columnIndexes)) {
          rowIndexes.put(rowRecordList.size() - 1);
          ++count;
          if (count == windowSize) {
            return true;
          }
        }
      }

      return count != 0;
    }

    @Override
    public void readyForNext() throws IOException, QueryProcessException {
      updateMaxIndexForLastWindow();

      safetyPile.moveForwardTo(maxIndexInLastWindow + 1);

      rowIndexes.clear();
    }

    private void updateMaxIndexForLastWindow() throws IOException, QueryProcessException {
      if (rowIndexes.size() == 0) {
        return;
      }

      if (slidingStep <= rowIndexes.size()) {
        maxIndexInLastWindow = rowIndexes.get(slidingStep - 1);
        return;
      }

      int currentStep = rowIndexes.size() - 1;

      for (int i = rowIndexes.get(rowIndexes.size() - 1) + 1; i < rowRecordList.size(); ++i) {
        if (hasNotNullSelectedFields(rowRecordList.getRowRecord(i), columnIndexes)) {
          ++currentStep;
          if (currentStep == slidingStep) {
            maxIndexInLastWindow = i - 1;
            return;
          }
        }
      }

      while (queryDataSet.hasNextRowInObjects()) {
        Object[] rowRecordCandidate = queryDataSet.nextRowInObjects();
        rowRecordList.put(rowRecordCandidate);
        if (hasNotNullSelectedFields(rowRecordCandidate, columnIndexes)) {
          ++currentStep;
          if (currentStep == slidingStep) {
            maxIndexInLastWindow = rowRecordList.size() - 2;
            return;
          }
        }
      }

      maxIndexInLastWindow = rowRecordList.size() - 1;
    }

    @Override
    public TSDataType[] getDataTypes() {
      return columnDataTypes;
    }

    @Override
    public RowWindow currentWindow() {
      return rowWindow;
    }
  }

  private class InputLayerRowSlidingTimeWindowReader implements LayerRowWindowReader {

    private final SafetyPile safetyPile;

    private final int[] columnIndexes;
    private final TSDataType[] columnDataTypes;

    private final long timeInterval;
    private final long slidingStep;
    private final long displayWindowEnd;

    private final IntList rowIndexes;
    private final RowWindowImpl rowWindow;

    private long nextWindowTimeBegin;
    private int nextIndexBegin;

    private final boolean hasAtLeastOneRow;

    private InputLayerRowSlidingTimeWindowReader(
        int[] columnIndexes, SlidingTimeWindowAccessStrategy accessStrategy, float memoryBudgetInMB)
        throws QueryProcessException, IOException {
      safetyPile = safetyLine.addSafetyPile();

      this.columnIndexes = columnIndexes;
      columnDataTypes = new TSDataType[columnIndexes.length];
      for (int i = 0; i < columnIndexes.length; ++i) {
        columnDataTypes[i] = dataTypes[columnIndexes[i]];
      }

      timeInterval = accessStrategy.getTimeInterval();
      slidingStep = accessStrategy.getSlidingStep();
      displayWindowEnd = accessStrategy.getDisplayWindowEnd();

      rowIndexes = new ElasticSerializableIntList(queryId, memoryBudgetInMB, 2);
      rowWindow = new RowWindowImpl(rowRecordList, columnIndexes, dataTypes, rowIndexes);

      nextWindowTimeBegin = accessStrategy.getDisplayWindowBegin();
      nextIndexBegin = 0;

      if (rowRecordList.size() == 0 && queryDataSet.hasNextRowInObjects()) {
        rowRecordList.put(queryDataSet.nextRowInObjects());

        if (nextWindowTimeBegin == Long.MIN_VALUE) {
          // display window begin should be set to the same as the min timestamp of the query result
          // set
          nextWindowTimeBegin = rowRecordList.getTime(0);
        }
      }
      hasAtLeastOneRow = rowRecordList.size() != 0;
    }

    @Override
    public boolean next() throws IOException, QueryProcessException {
      if (displayWindowEnd <= nextWindowTimeBegin) {
        return false;
      }
      if (!hasAtLeastOneRow || 0 < rowIndexes.size()) {
        return true;
      }

      long nextWindowTimeEnd = Math.min(nextWindowTimeBegin + timeInterval, displayWindowEnd);
      int oldRowRecordListSize = rowRecordList.size();
      while ((Long) rowRecordList.getRowRecord(rowRecordList.size() - 1)[timestampIndex]
          < nextWindowTimeEnd) {
        if (queryDataSet.hasNextRowInObjects()) {
          rowRecordList.put(queryDataSet.nextRowInObjects());
        } else if (displayWindowEnd == Long.MAX_VALUE
            // display window end == the max timestamp of the query result set
            && oldRowRecordListSize == rowRecordList.size()) {
          return false;
        } else {
          break;
        }
      }

      for (int i = nextIndexBegin; i < rowRecordList.size(); ++i) {
        if (nextWindowTimeBegin <= (Long) rowRecordList.getRowRecord(i)[timestampIndex]) {
          nextIndexBegin = i;
          break;
        }
        if (i == rowRecordList.size() - 1) {
          nextIndexBegin = rowRecordList.size();
        }
      }

      for (int i = nextIndexBegin; i < rowRecordList.size(); ++i) {
        Object[] rowRecordCandidate = rowRecordList.getRowRecord(i);
        if (nextWindowTimeEnd <= (Long) rowRecordCandidate[timestampIndex]) {
          break;
        }
        if (hasNotNullSelectedFields(rowRecordCandidate, columnIndexes)) {
          rowIndexes.put(i);
        }
      }

      return true;
    }

    @Override
    public void readyForNext() {
      nextWindowTimeBegin += slidingStep;

      safetyPile.moveForwardTo(nextIndexBegin);

      rowIndexes.clear();
    }

    @Override
    public TSDataType[] getDataTypes() {
      return columnDataTypes;
    }

    @Override
    public RowWindow currentWindow() {
      return rowWindow;
    }
  }

  private static boolean hasNotNullSelectedFields(
      Object[] rowRecordCandidate, int[] columnIndexes) {
    for (int columnIndex : columnIndexes) {
      if (rowRecordCandidate[columnIndex] != null) {
        return true;
      }
    }
    return false;
  }
}
