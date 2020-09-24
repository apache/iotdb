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

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.TumblingWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.core.access.RowImpl;
import org.apache.iotdb.db.query.udf.core.access.RowWindowImpl;
import org.apache.iotdb.db.query.udf.core.input.SafetyLine.SafetyPile;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowWindowReader;
import org.apache.iotdb.db.query.udf.datastructure.primitive.ElasticSerializableIntList;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.query.udf.datastructure.primitive.IntList;
import org.apache.iotdb.db.query.udf.datastructure.primitive.SerializableIntList;
import org.apache.iotdb.db.query.udf.datastructure.primitive.WrappedIntArray;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

public class InputLayer {

  private static final String DATA_ID_PREFIX = "input_layer_";
  private int elasticSerializableIntListCount = 0;

  private final long queryId;

  private final QueryDataSet queryDataSet;
  private final TSDataType[] dataTypes;

  private final ElasticSerializableRowRecordList rowRecordList;
  private final SafetyLine safetyLine;

  public InputLayer(long queryId, QueryDataSet queryDataSet) throws QueryProcessException {
    this.queryId = queryId;
    this.queryDataSet = queryDataSet;
    dataTypes = queryDataSet.getDataTypes().toArray(new TSDataType[0]);
    rowRecordList = new ElasticSerializableRowRecordList(dataTypes, queryId,
        DATA_ID_PREFIX + elasticSerializableIntListCount++,
        ElasticSerializableRowRecordList.DEFAULT_MEMORY_USAGE_LIMIT,
        ElasticSerializableRowRecordList.DEFAULT_CACHE_SIZE);
    safetyLine = new SafetyLine();
  }

  public LayerPointReader constructPointReader(int columnIndex) {
    return new InputLayerPointReader(columnIndex);
  }

  public LayerRowReader constructRowReader(int[] columnIndexes) {
    return new InputLayerRowReader(columnIndexes);
  }

  public LayerRowWindowReader constructRowWindowReader(int[] columnIndexes, AccessStrategy strategy)
      throws QueryProcessException, IOException {
    switch (strategy.getAccessStrategyType()) {
      case SLIDING_TIME_WINDOW:
        return new InputLayerRowSlidingTimeWindowReader(columnIndexes,
            (SlidingTimeWindowAccessStrategy) strategy);
      case TUMBLING_WINDOW:
        return new InputLayerRowTumblingWindowReader(columnIndexes,
            (TumblingWindowAccessStrategy) strategy);
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
    private RowRecord cachedRowRecord;

    InputLayerPointReader(int columnIndex) {
      this.safetyPile = safetyLine.addSafetyPile();

      this.columnIndex = columnIndex;
      currentRowIndex = -1;

      hasCachedRowRecord = false;
      cachedRowRecord = null;
    }

    @Override
    public boolean next() throws IOException {
      if (hasCachedRowRecord) {
        return true;
      }

      for (int i = currentRowIndex + 1; i < rowRecordList.size(); ++i) {
        RowRecord rowRecordCandidate = rowRecordList.getRowRecord(i);
        if (rowRecordCandidate.getFields().get(columnIndex) != null) {
          hasCachedRowRecord = true;
          cachedRowRecord = rowRecordCandidate;
          currentRowIndex = i;
          break;
        }
      }

      if (!hasCachedRowRecord) {
        while (queryDataSet.hasNextWithoutConstraint()) {
          RowRecord rowRecordCandidate = queryDataSet.nextWithoutConstraint();
          rowRecordList.put(rowRecordCandidate);
          if (rowRecordCandidate.getFields().get(columnIndex) != null) {
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
      return cachedRowRecord.getTimestamp();
    }

    @Override
    public int currentInt() {
      return cachedRowRecord.getFields().get(columnIndex).getIntV();
    }

    @Override
    public long currentLong() {
      return cachedRowRecord.getFields().get(columnIndex).getLongV();
    }

    @Override
    public float currentFloat() {
      return cachedRowRecord.getFields().get(columnIndex).getFloatV();
    }

    @Override
    public double currentDouble() {
      return cachedRowRecord.getFields().get(columnIndex).getDoubleV();
    }

    @Override
    public boolean currentBoolean() {
      return cachedRowRecord.getFields().get(columnIndex).getBoolV();
    }

    @Override
    public Binary currentBinary() {
      return cachedRowRecord.getFields().get(columnIndex).getBinaryV();
    }
  }

  private class InputLayerRowReader implements LayerRowReader {

    private final int[] columnIndexes;
    private final SafetyPile[] safetyPiles;

    private int currentRowIndex;

    private boolean hasCachedRowRecord;
    private RowRecord cachedRowRecord;

    private final RowImpl row;

    public InputLayerRowReader(int[] columnIndexes) {
      this.columnIndexes = columnIndexes;
      safetyPiles = new SafetyPile[columnIndexes.length];
      for (int i = 0; i < safetyPiles.length; ++i) {
        safetyPiles[i] = safetyLine.addSafetyPile();
      }

      currentRowIndex = -1;

      hasCachedRowRecord = false;
      cachedRowRecord = null;

      row = new RowImpl(columnIndexes, dataTypes);
    }

    @Override
    public boolean next() throws IOException {
      if (hasCachedRowRecord) {
        return true;
      }

      for (int i = currentRowIndex + 1; i < rowRecordList.size(); ++i) {
        RowRecord rowRecordCandidate = rowRecordList.getRowRecord(i);
        if (hasNotNullSelectedFields(rowRecordCandidate, columnIndexes)) {
          hasCachedRowRecord = true;
          cachedRowRecord = rowRecordCandidate;
          currentRowIndex = i;
          break;
        }
      }

      if (!hasCachedRowRecord) {
        while (queryDataSet.hasNextWithoutConstraint()) {
          RowRecord rowRecordCandidate = queryDataSet.nextWithoutConstraint();
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

      for (SafetyPile safetyPile : safetyPiles) {
        safetyPile.moveForwardTo(currentRowIndex + 1);
      }
    }

    @Override
    public TSDataType[] getDataTypes() {
      return dataTypes;
    }

    @Override
    public long currentTime() {
      return cachedRowRecord.getTimestamp();
    }

    @Override
    public Row currentRow() {
      return row.setRowRecord(cachedRowRecord);
    }
  }

  private class InputLayerRowTumblingWindowReader implements LayerRowWindowReader {

    private final int[] columnIndexes;
    private final TSDataType[] columnDataTypes;
    private final SafetyPile[] safetyPiles;

    private final int windowSize;
    private final IntList rowIndexes;
    private final RowWindowImpl rowWindow;

    private int maxIndexInLastWindow;

    private InputLayerRowTumblingWindowReader(int[] columnIndexes,
        TumblingWindowAccessStrategy accessStrategy) throws QueryProcessException {
      this.columnIndexes = columnIndexes;
      columnDataTypes = new TSDataType[columnIndexes.length];
      safetyPiles = new SafetyPile[columnIndexes.length];
      for (int i = 0; i < columnIndexes.length; ++i) {
        columnDataTypes[i] = dataTypes[columnIndexes[i]];
        safetyPiles[i] = safetyLine.addSafetyPile();
      }

      windowSize = accessStrategy.getWindowSize();
      rowIndexes = windowSize < SerializableIntList
          .calculateCapacity(ElasticSerializableIntList.DEFAULT_MEMORY_USAGE_LIMIT)
          ? new WrappedIntArray(windowSize)
          : new ElasticSerializableIntList(queryId,
              DATA_ID_PREFIX + elasticSerializableIntListCount++,
              ElasticSerializableIntList.DEFAULT_MEMORY_USAGE_LIMIT,
              ElasticSerializableIntList.DEFAULT_CACHE_SIZE);
      rowWindow = new RowWindowImpl(rowRecordList, columnIndexes, dataTypes);

      maxIndexInLastWindow = -1;
    }

    @Override
    public boolean next() throws IOException {
      if (0 < rowRecordList.size()) {
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

      while (queryDataSet.hasNextWithoutConstraint()) {
        RowRecord rowRecordCandidate = queryDataSet.nextWithoutConstraint();
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
    public void readyForNext() throws IOException {
      maxIndexInLastWindow = rowIndexes.size() != 0
          ? rowIndexes.get(rowIndexes.size() - 1) : maxIndexInLastWindow;

      for (SafetyPile safetyPile : safetyPiles) {
        safetyPile.moveForwardTo(maxIndexInLastWindow + 1);
      }

      rowIndexes.clear();
    }

    @Override
    public TSDataType[] getDataTypes() {
      return columnDataTypes;
    }

    @Override
    public RowWindow currentWindow() {
      return rowWindow.set(rowIndexes);
    }
  }

  private class InputLayerRowSlidingTimeWindowReader implements LayerRowWindowReader {

    private final int[] columnIndexes;
    private final TSDataType[] columnDataTypes;
    private final SafetyPile[] safetyPiles;

    private final long timeInterval;
    private final long slidingStep;
    private final long displayWindowEnd;

    private final IntList rowIndexes;
    private final RowWindowImpl rowWindow;

    private long nextWindowTimeBegin;
    private int nextIndexBegin;

    private final boolean hasAtLeastOneRow;

    private InputLayerRowSlidingTimeWindowReader(int[] columnIndexes,
        SlidingTimeWindowAccessStrategy accessStrategy) throws QueryProcessException, IOException {
      this.columnIndexes = columnIndexes;
      columnDataTypes = new TSDataType[columnIndexes.length];
      safetyPiles = new SafetyPile[columnIndexes.length];
      for (int i = 0; i < columnIndexes.length; ++i) {
        columnDataTypes[i] = dataTypes[columnIndexes[i]];
        safetyPiles[i] = safetyLine.addSafetyPile();
      }

      timeInterval = accessStrategy.getTimeInterval();
      slidingStep = accessStrategy.getSlidingStep();
      displayWindowEnd = accessStrategy.getDisplayWindowEnd();

      rowIndexes = new ElasticSerializableIntList(queryId,
          DATA_ID_PREFIX + elasticSerializableIntListCount++,
          ElasticSerializableIntList.DEFAULT_MEMORY_USAGE_LIMIT,
          ElasticSerializableIntList.DEFAULT_CACHE_SIZE);
      rowWindow = new RowWindowImpl(rowRecordList, columnIndexes, dataTypes);

      nextWindowTimeBegin = accessStrategy.getDisplayWindowBegin();
      nextIndexBegin = 0;

      if (rowRecordList.size() == 0 && queryDataSet.hasNextWithoutConstraint()) {
        rowRecordList.put(queryDataSet.nextWithoutConstraint());
      }
      hasAtLeastOneRow = rowRecordList.size() != 0;
    }

    @Override
    public boolean next() throws IOException {
      if (displayWindowEnd <= nextWindowTimeBegin) {
        return false;
      }
      if (!hasAtLeastOneRow || 0 < rowRecordList.size()) {
        return true;
      }

      long nextWindowTimeEnd = nextWindowTimeBegin + timeInterval;
      while (rowRecordList.getRowRecord(rowRecordList.size() - 1).getTimestamp() < nextWindowTimeEnd
          && queryDataSet.hasNextWithoutConstraint()) {
        rowRecordList.put(queryDataSet.nextWithoutConstraint());
      }

      for (int i = nextIndexBegin; i < rowRecordList.size(); ++i) {
        if (nextWindowTimeBegin <= rowRecordList.getRowRecord(i).getTimestamp()) {
          nextIndexBegin = i;
          break;
        }
        if (i == rowRecordList.size() - 1) {
          nextIndexBegin = rowRecordList.size();
        }
      }

      for (int i = nextIndexBegin; i < rowRecordList.size(); ++i) {
        RowRecord rowRecordCandidate = rowRecordList.getRowRecord(i);
        if (nextWindowTimeEnd <= rowRecordCandidate.getTimestamp()) {
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

      for (SafetyPile safetyPile : safetyPiles) {
        safetyPile.moveForwardTo(nextIndexBegin);
      }

      rowIndexes.clear();
    }

    @Override
    public TSDataType[] getDataTypes() {
      return columnDataTypes;
    }

    @Override
    public RowWindow currentWindow() {
      return rowWindow.set(rowIndexes);
    }
  }

  private static boolean hasNotNullSelectedFields(RowRecord rowRecordCandidate,
      int[] columnIndexes) {
    List<Field> fields = rowRecordCandidate.getFields();
    for (int columnIndex : columnIndexes) {
      if (fields.get(columnIndex) != null) {
        return true;
      }
    }
    return false;
  }
}
