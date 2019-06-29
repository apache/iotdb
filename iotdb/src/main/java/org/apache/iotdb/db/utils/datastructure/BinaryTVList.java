/**
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

import static org.apache.iotdb.db.rescon.PrimitiveDataListPool.ARRAY_SIZE;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.rescon.PrimitiveDataListPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public class BinaryTVList extends TVList {

  private List<Binary[]> values;

  private Binary[][] sortedValues;

  private Binary pivotValue;

  BinaryTVList() {
    super();
    values = new ArrayList<>();
  }

  @Override
  public void putBinary(long timestamp, Binary value) {
    checkExpansion();
    int arrayIndex = size / ARRAY_SIZE;
    int elementIndex = size % ARRAY_SIZE;
    minTime = minTime <= timestamp ? minTime : timestamp;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
    size++;
  }

  @Override
  public Binary getBinary(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    if (!sorted) {
      int arrayIndex = index / ARRAY_SIZE;
      int elementIndex = index % ARRAY_SIZE;
      return values.get(arrayIndex)[elementIndex];
    } else {
      return sortedValues[index/ARRAY_SIZE][index%ARRAY_SIZE];
    }
  }

  public void set(int index, long timestamp, Binary value) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
  }

  @Override
  public BinaryTVList clone() {
    BinaryTVList cloneList = new BinaryTVList();
    cloneAs(cloneList);
    if (!sorted) {
      for (Binary[] valueArray : values) {
        cloneList.values.add(cloneValue(valueArray));
      }
    } else {
      cloneList.sortedValues = new Binary[sortedValues.length][ARRAY_SIZE];
      for (int i = 0; i < sortedValues.length; i++) {
        System.arraycopy(sortedValues[i], 0, cloneList.sortedValues[i], 0, ARRAY_SIZE);
      }
    }
    return cloneList;
  }

  private Binary[] cloneValue(Binary[] array) {
    Binary[] cloneArray = new Binary[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  public void sort() {
    if (sortedTimestamps == null || sortedTimestamps.length < size) {
      sortedTimestamps = (long[][]) PrimitiveDataListPool.getInstance().getDataListsByType(TSDataType.INT64, size);
    }
    if (sortedValues == null || sortedValues.length < size) {
      sortedValues = (Binary[][]) PrimitiveDataListPool.getInstance().getDataListsByType(TSDataType.TEXT, size);
    }
    sort(0, size);
    clearTime();
    clearValue();
    sorted = true;
  }

  @Override
  void clearValue() {
    if (values != null) {
      for (Binary[] dataArray : values) {
        PrimitiveDataListPool.getInstance().release(dataArray);
      }
      values.clear();
    }
  }

  @Override
  void clearSortedValue() {
    if (sortedValues != null) {
      for (Binary[] dataArray : sortedValues) {
        PrimitiveDataListPool.getInstance().release(dataArray);
      }
      sortedValues = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    set(dest, sortedTimestamps[src/ARRAY_SIZE][src%ARRAY_SIZE], sortedValues[src/ARRAY_SIZE][src%ARRAY_SIZE]);
  }

  protected void set(int src, int dest) {
    long srcT = getTime(src);
    Binary srcV = getBinary(src);
    set(dest, srcT, srcV);
  }

  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest/ARRAY_SIZE][dest% ARRAY_SIZE] = getTime(src);
    sortedValues[dest/ARRAY_SIZE][dest%ARRAY_SIZE] = getBinary(src);
  }

  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      Binary loV = getBinary(lo);
      long hiT = getTime(hi);
      Binary hiV = getBinary(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  protected void expandValues() {
    values.add((Binary[]) PrimitiveDataListPool
        .getInstance().getPrimitiveDataListByType(TSDataType.TEXT));
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotValue = getBinary(pos);
  }

  @Override
  protected void setPivotTo(int pos) {
    set(pos, pivotTime, pivotValue);
  }

}
