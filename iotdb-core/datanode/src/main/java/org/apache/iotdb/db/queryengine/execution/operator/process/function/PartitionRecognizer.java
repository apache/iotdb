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

package org.apache.iotdb.db.queryengine.execution.operator.process.function;

import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.PartitionState;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class PartitionRecognizer {

  private final List<Integer> partitionChannels;
  private final List<Object> partitionValues;
  private TsBlock currentTsBlock = null;
  private boolean noMoreData = false;
  private int currentIndex = 0;
  private PartitionState state = PartitionState.INIT_STATE;

  public PartitionRecognizer(List<Integer> partitionChannels) {
    this.partitionChannels = partitionChannels;
    this.partitionValues = new ArrayList<>(partitionChannels.size());
    for (int i = 0; i < partitionChannels.size(); i++) {
      partitionValues.add(null);
    }
  }

  public boolean needInput() {
    return PartitionState.NEED_MORE_DATA_STATE.equals(state)
        || PartitionState.INIT_STATE.equals(state);
  }

  // TsBlock is sorted by partition columns already
  public void addTsBlock(TsBlock tsBlock) {
    if (noMoreData) {
      throw new IllegalArgumentException(
          "The partition handler is finished, cannot add more data.");
    }
    currentTsBlock = tsBlock;
  }

  /** Marks the handler as finished. */
  public void noMoreData() {
    noMoreData = true;
  }

  public PartitionState getState() {
    updateState();
    return state;
  }

  private void updateState() {
    switch (state.getStateType()) {
      case INIT:
        state = handleInitState();
        break;
      case NEW_PARTITION:
        state = handleNewPartitionState();
        break;
      case ITERATING:
        state = handleIteratingState();
        break;
      case NEED_MORE_DATA:
        state = handleNeedMoreDataState();
        break;
      case FINISHED:
        // do nothing
        return;
    }
    if (PartitionState.NEED_MORE_DATA_STATE.equals(state)) {
      currentIndex = 0;
    }
  }

  private PartitionState handleInitState() {
    if (currentTsBlock == null || currentTsBlock.isEmpty()) {
      return PartitionState.INIT_STATE;
    }
    int endPartitionIndex = findEndPartitionIndex();
    Iterator<Record> recordIterator = getRecordIterator(currentIndex, endPartitionIndex);
    currentIndex = endPartitionIndex;
    return PartitionState.newPartitionState(recordIterator);
  }

  private PartitionState handleNewPartitionState() {
    if (currentIndex >= currentTsBlock.getPositionCount()) {
      return PartitionState.NEED_MORE_DATA_STATE;
    } else {
      int endPartitionIndex = findEndPartitionIndex();
      Iterator<Record> recordIterator = getRecordIterator(currentIndex, endPartitionIndex);
      currentIndex = endPartitionIndex;
      return PartitionState.newPartitionState(recordIterator);
    }
  }

  private PartitionState handleNeedMoreDataState() {
    if (noMoreData) {
      return PartitionState.FINISHED_STATE;
    } else if (currentTsBlock == null || currentTsBlock.isEmpty()) {
      return PartitionState.NEED_MORE_DATA_STATE;
    }
    int endPartitionIndex = findEndPartitionIndex();
    if (endPartitionIndex != 0) {
      Iterator<Record> recordIterator = getRecordIterator(currentIndex, endPartitionIndex);
      currentIndex = endPartitionIndex;
      return PartitionState.iteratingState(recordIterator);
    } else {
      currentIndex = endPartitionIndex;
      endPartitionIndex = findEndPartitionIndex();
      Iterator<Record> recordIterator = getRecordIterator(currentIndex, endPartitionIndex);
      currentIndex = endPartitionIndex;
      return PartitionState.newPartitionState(recordIterator);
    }
  }

  private PartitionState handleIteratingState() {
    if (currentIndex >= currentTsBlock.getPositionCount()) {
      return PartitionState.NEED_MORE_DATA_STATE;
    } else {
      int endPartitionIndex = findEndPartitionIndex();
      Iterator<Record> recordIterator = getRecordIterator(currentIndex, endPartitionIndex);
      currentIndex = endPartitionIndex;
      return PartitionState.newPartitionState(recordIterator);
    }
  }

  private int findEndPartitionIndex() {
    int i = currentIndex;
    while (i < currentTsBlock.getPositionCount()) {
      for (int j = 0; j < partitionChannels.size(); j++) {
        if (!Objects.equals(
            partitionValues.get(j),
            currentTsBlock.getColumn(partitionChannels.get(j)).getObject(i))) {
          // update partition values
          for (int k = 0; k < partitionChannels.size(); k++) {
            partitionValues.set(k, currentTsBlock.getColumn(partitionChannels.get(k)).getObject(i));
          }
          return i;
        }
      }
      i++;
    }
    return i;
  }

  private Iterator<Record> getRecordIterator(int startPartitionIndex, int endPartitionIndex) {
    return new Iterator<Record>() {
      private int curIndex = startPartitionIndex;
      private final int endIndex = endPartitionIndex;
      private final TsBlock tsBlock = currentTsBlock;

      @Override
      public boolean hasNext() {
        return curIndex < endIndex;
      }

      @Override
      public Record next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final int idx = curIndex++;
        return new Record() {
          @Override
          public int getInt(int columnIndex) {
            return tsBlock.getColumn(columnIndex).getInt(idx);
          }

          @Override
          public long getLong(int columnIndex) {
            return tsBlock.getColumn(columnIndex).getLong(idx);
          }

          @Override
          public float getFloat(int columnIndex) {
            return tsBlock.getColumn(columnIndex).getFloat(idx);
          }

          @Override
          public double getDouble(int columnIndex) {
            return tsBlock.getColumn(columnIndex).getDouble(idx);
          }

          @Override
          public boolean getBoolean(int columnIndex) {
            return tsBlock.getColumn(columnIndex).getBoolean(idx);
          }

          @Override
          public Binary getBinary(int columnIndex) {
            return tsBlock.getColumn(columnIndex).getBinary(idx);
          }

          @Override
          public String getString(int columnIndex) {
            return tsBlock
                .getColumn(columnIndex)
                .getBinary(idx)
                .getStringValue(TSFileConfig.STRING_CHARSET);
          }

          @Override
          public LocalDate getLocalDate(int columnIndex) {
            return DateUtils.parseIntToLocalDate(tsBlock.getColumn(columnIndex).getInt(idx));
          }

          @Override
          public Object getObject(int columnIndex) {
            return tsBlock.getColumn(columnIndex).getObject(idx);
          }

          @Override
          public Type getDataType(int columnIndex) {
            // TODO(UDF)
            return null;
          }

          @Override
          public boolean isNull(int columnIndex) {
            return tsBlock.getColumn(columnIndex).isNull(idx);
          }

          @Override
          public int size() {
            return tsBlock.getValueColumns().length;
          }
        };
      }
    };
  }
}
