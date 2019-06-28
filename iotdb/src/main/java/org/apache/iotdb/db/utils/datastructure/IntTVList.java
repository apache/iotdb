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

import java.util.ArrayList;
import java.util.List;

public class IntTVList extends TVList {

  private List<int[]> values;

  private int[] sortedValues;

  private int pivotValue;

  public IntTVList() {
    super();
    values = new ArrayList<>();
  }

  @Override
  public void putInt(long timestamp, int value) {
    checkExpansion();
    int arrayIndex = size / SINGLE_ARRAY_SIZE;
    int elementIndex = size % SINGLE_ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
    size++;
  }

  @Override
  public int getInt(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    if (!sorted) {
      int arrayIndex = index / SINGLE_ARRAY_SIZE;
      int elementIndex = index % SINGLE_ARRAY_SIZE;
      return values.get(arrayIndex)[elementIndex];
    } else {
      return sortedValues[index];
    }
  }

  public void set(int index, long timestamp, int value) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / SINGLE_ARRAY_SIZE;
    int elementIndex = index % SINGLE_ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
  }

  @Override
  public IntTVList clone() {
    IntTVList cloneList = new IntTVList();
    cloneAs(cloneList);
    if (!sorted) {
      for (int[] valueArray : values) {
        cloneList.values.add(cloneValue(valueArray));
      }
    } else {
      cloneList.sortedValues = new int[size];
      System.arraycopy(sortedValues, 0, cloneList.sortedValues, 0, size);
    }
    return cloneList;
  }

  private int[] cloneValue(int[] array) {
    int[] cloneArray = new int[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  public void sort() {
    if (sortedTimestamps == null || sortedTimestamps.length < size) {
      sortedTimestamps = new long[size];
    }
    if (sortedValues == null || sortedValues.length < size) {
      sortedValues = new int[size];
    }
    sort(0, size);
    sorted = true;
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    set(dest, sortedTimestamps[src], sortedValues[src]);
  }

  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getInt(src);
    set(dest, srcT, srcV);
  }

  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest] = getTime(src);
    sortedValues[dest] = getInt(src);
  }

  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      int loV = getInt(lo);
      long hiT = getTime(hi);
      int hiV = getInt(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  protected void expandValues() {
    values.add(new int[SINGLE_ARRAY_SIZE]);
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotValue = getInt(pos);
  }

  @Override
  protected void setPivotTo(int pos) {
    set(pos, pivotTime, pivotValue);
  }

}
