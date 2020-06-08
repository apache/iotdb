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
package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMLongTVList extends NVMTVList {

  // TODO
  private long[][] sortedValues;
  private long[][] tempValuesForSort;

  NVMLongTVList(String sgId, String deviceId, String measurementId) {
    super(sgId, deviceId, measurementId);
    dataType = TSDataType.INT64;
  }

  @Override
  public void putLong(long timestamp, long value) {
    checkExpansion();
    int arrayIndex = size / ARRAY_SIZE;
    int elementIndex = size % ARRAY_SIZE;
    minTime = minTime <= timestamp ? minTime : timestamp;
    timestamps.get(arrayIndex).setData(elementIndex, timestamp);
    values.get(arrayIndex).setData(elementIndex, value);
    size++;
    if (sorted && size > 1 && timestamp < getTime(size - 2)) {
      sorted = false;
    }
  }

  @Override
  public long getLong(int index) {
    return (long) getValue(index);
  }

  @Override
  public LongTVList clone() {
    LongTVList cloneList = new LongTVList();
    cloneAs(cloneList);
    for (NVMDataSpace valueSpace : values) {
      cloneList.addBatchValue((long[]) cloneValue(valueSpace));
    }
    return cloneList;
  }

  @Override
  protected void initSortedAndTempArrays() {
    if (sortedTimestamps == null || sortedTimestamps.length < size) {
      sortedTimestamps = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(TSDataType.INT64, size);
      tempTimestampsForSort = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(TSDataType.INT64, size);
    }
    if (sortedValues == null || sortedValues.length < size) {
      sortedValues = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(dataType, size);
      tempValuesForSort = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(dataType, size);
    }
  }

  @Override
  protected void copyTVToTempArrays() {
    int arrayIndex = 0;
    int elementIndex = 0;
    for (int i = 0; i < size; i++) {
      long time = (long) timestamps.get(arrayIndex).getData(elementIndex);
      long value = (long) values.get(arrayIndex).getData(elementIndex);
      tempTimestampsForSort[arrayIndex][elementIndex] = time;
      tempValuesForSort[arrayIndex][elementIndex] = value;

      elementIndex++;
      if (elementIndex == ARRAY_SIZE) {
        elementIndex = 0;
        arrayIndex++;
      }
    }
  }

  @Override
  protected void copyTVFromTempArrays() {
    int arrayIndex = 0;
    int elementIndex = 0;
    for (int i = 0; i < size; i++) {
      long time = tempTimestampsForSort[arrayIndex][elementIndex];
      long value = tempValuesForSort[arrayIndex][elementIndex];

      timestamps.get(arrayIndex).setData(elementIndex, time);
      values.get(arrayIndex).setData(elementIndex, value);

      elementIndex++;
      if (elementIndex == ARRAY_SIZE) {
        elementIndex = 0;
        arrayIndex++;
      }
    }
  }

  @Override
  protected void clearSortedValue() {
    if (sortedValues != null) {
      for (long[] dataArray : sortedValues) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      sortedValues = null;
    }

    if (tempValuesForSort != null) {
      for (long[] dataArray : tempValuesForSort) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      tempValuesForSort = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    setForSort(dest, sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedValues[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    long srcV = getLong(src);
    set(dest, srcT, srcV);
  }

  @Override
  protected void setValueForSort(int arrayIndex, int elementIndex, Object value) {
    tempValuesForSort[arrayIndex][elementIndex] = (long) value;
  }

  @Override
  protected Object getValueForSort(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return tempValuesForSort[arrayIndex][elementIndex];
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest/ARRAY_SIZE][dest% ARRAY_SIZE] = getTimeForSort(src);
    sortedValues[dest/ARRAY_SIZE][dest%ARRAY_SIZE] = (long) getValueForSort(src);
  }

  @Override
  public void putLongs(long[] time, long[] value) {
    int length = time.length;

    for (int i = 0; i < length; i++) {
      putLong(time[i], value[i]);
    }
  }

  @Override
  public void putLongs(long[] time, long[] value, int start, int end) {
    for (int i = start; i < end; i++) {
      putLong(time[i], value[i]);
    }
  }
}

