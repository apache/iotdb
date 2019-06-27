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

public class LongTVList extends TVList {

  private List<long[]> values;

  private long[] sortedValues;

  public LongTVList() {
    super();
    values = new ArrayList<>();

  }

  @Override
  public void putLong(long timestamp, long value) {
    if ((size % SINGLE_ARRAY_SIZE) == 0) {
      values.add(new long[SINGLE_ARRAY_SIZE]);
      timestamps.add(new long[SINGLE_ARRAY_SIZE]);
    }
    int arrayIndex = size / SINGLE_ARRAY_SIZE;
    int elementIndex = size % SINGLE_ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
    size++;
  }

  @Override
  public long getLong(int index) {
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

  public void set(int index, long timestamp, long value) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / SINGLE_ARRAY_SIZE;
    int elementIndex = index % SINGLE_ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
  }

  @Override
  public LongTVList clone() {
    LongTVList cloneList = new LongTVList();
    if (!sorted) {
      for (long[] valueArray : values) {
        cloneList.values.add(cloneValue(valueArray));
      }
      for (long[] timestampArray : timestamps) {
        cloneList.timestamps.add(cloneTime(timestampArray));
      }
    } else {
      cloneList.sortedTimestamps = new long[size];
      cloneList.sortedValues = new long[size];
      System.arraycopy(sortedTimestamps, 0, cloneList.sortedTimestamps, 0, size);
      System.arraycopy(sortedValues, 0, cloneList.sortedValues, 0, size);
    }
    cloneList.size = size;
    cloneList.sorted = sorted;

    return cloneList;
  }

  private long[] cloneValue(long[] array) {
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  public void reset() {
    size = 0;
  }

  public void sort() {
    sortedTimestamps = new long[size];
    sortedValues = new long[size];
    sort(0, size);
    sorted = true;
    values = null;
    timestamps = null;
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    set(dest, sortedTimestamps[src], sortedValues[src]);
  }

  protected void set(int src, int dest) {
    long srcT = getTime(src);
    long srcV = getLong(src);
    set(dest, srcT, srcV);
  }

  protected void setSorted(int src, int dest) {
    sortedTimestamps[dest] = getTime(src);
    sortedValues[dest] = getLong(src);
  }

  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      long loV = getLong(lo);
      long hiT = getTime(hi);
      long hiV = getLong(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }
}
