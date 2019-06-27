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

public class LongTVList {

  private static final int SMALL_ARRAY_LENGTH = 32;
  private static final int SINGLE_ARRAY_SIZE = 512;

  private Class clazz;
  private List<long[]> values;
  private List<long[]> timestamps;

  private int size; // Total data number of all objects of current ArrayList

  private long[] sortedTimestamps;
  private long[] sortedValues;
  private boolean sorted = false;

  public LongTVList(Class clazz) {
    this.clazz = clazz;
    values = new ArrayList<>();
    timestamps = new ArrayList<>();
    size = 0;

  }

  public void append(long timestamp, long value) {
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

  public long getTimestamp(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    if (!sorted) {
      int arrayIndex = index / SINGLE_ARRAY_SIZE;
      int elementIndex = index % SINGLE_ARRAY_SIZE;
      return timestamps.get(arrayIndex)[elementIndex];
    } else {
      return sortedTimestamps[index];
    }
  }

  public long getValue(int index) {
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

  public int size() {
    return size;
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
    LongTVList cloneList = new LongTVList(clazz);
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

  private long[] cloneTime(long[] array) {
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
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
  }

  private void sort(int lo, int hi) {
    if (hi - lo <= SMALL_ARRAY_LENGTH) {
      int initRunLen = countRunAndMakeAscending(lo, hi);
      binarySort(lo, hi, lo + initRunLen);
      return;
    }
    int mid = (lo + hi) >>> 1;
    sort(lo, mid);
    sort(mid, hi);
    merge(lo, mid, hi);
    sorted = true;
    values = null;
    timestamps = null;
  }

  private void merge(int lo, int mid, int hi) {
    int tmpIdx = 0;

    int leftIdx = lo;
    int rightIdx = mid;

    long leftFirstT = getTimestamp(leftIdx);
    long leftFirstV = getValue(leftIdx);
    long rightFirstT = getTimestamp(rightIdx);
    long rightFirstV = getValue(rightIdx);

    int endSide = 0;
    while (endSide == 0) {
      if (leftFirstT <= rightFirstT) {
        sortedTimestamps[lo + tmpIdx] = leftFirstT;
        sortedValues[lo + tmpIdx] = leftFirstV;
        tmpIdx ++;
        leftIdx ++;
        if (leftIdx == mid) {
          endSide = 1;
        } else {
          leftFirstT = getTimestamp(leftIdx);
          leftFirstV = getValue(leftIdx);
        }
      } else {
        sortedTimestamps[lo + tmpIdx] = rightFirstT;
        sortedValues[lo + tmpIdx] = rightFirstV;
        tmpIdx ++;
        rightIdx ++;
        if (rightIdx == hi) {
          endSide = 2;
        } else {
          rightFirstT = getTimestamp(leftIdx);
          rightFirstV = getValue(leftIdx);
        }
      }
    }
    if (endSide == 1) {
      for (; rightIdx < hi; rightIdx++) {
        rightFirstT = getTimestamp(leftIdx);
        rightFirstV = getValue(leftIdx);
        sortedTimestamps[lo + tmpIdx] = rightFirstT;
        sortedValues[lo + tmpIdx] = rightFirstV;
        tmpIdx ++;
      }
    } else {
      for(; leftIdx < mid; leftIdx++) {
        leftFirstT = getTimestamp(leftIdx);
        leftFirstV = getValue(leftIdx);
        sortedTimestamps[lo + tmpIdx] = leftFirstT;
        sortedValues[lo + tmpIdx] = leftFirstV;
        tmpIdx ++;
      }
    }
    for (int i = lo; i < hi; i++) {
      set(i, sortedTimestamps[i], sortedValues[i]);
    }
  }

  private void set(int src, int dest) {
    long srcT = getTimestamp(src);
    long srcV = getTimestamp(src);
    set(dest, srcT, srcV);
  }

  /**
   * From TimSort.java
   */
  private void binarySort(int lo, int hi, int start) {
    assert lo <= start && start <= hi;
    if (start == lo)
      start++;
    for ( ; start < hi; start++) {
      long pivotT = getTimestamp(start);
      long pivotV = getValue(start);

      // Set left (and right) to the index where a[start] (pivot) belongs
      int left = lo;
      int right = start;
      assert left <= right;
      /*
       * Invariants:
       *   pivot >= all in [lo, left).
       *   pivot <  all in [right, start).
       */
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (pivotT < getTimestamp(mid))
          right = mid;
        else
          left = mid + 1;
      }
      assert left == right;

      /*
       * The invariants still hold: pivot >= all in [lo, left) and
       * pivot < all in [left, start), so pivot belongs at left.  Note
       * that if there are elements equal to pivot, left points to the
       * first slot after them -- that's why this sort is stable.
       * Slide elements over to make room for pivot.
       */
      int n = start - left;  // The number of elements to move
      for (int i = n; i >= 1; i--) {
        set(left + i - 1, left + i);
      }
      set(left, pivotT, pivotV);
    }
  }

  private int countRunAndMakeAscending(int lo, int hi) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi) {
      return 1;
    }

    // Find end of run, and reverse range if descending
    if (getTimestamp(runHi++) < getTimestamp(lo)) { // Descending
      while (runHi < hi && getTimestamp(runHi) < getTimestamp(runHi - 1)) {
        runHi++;
      }
      reverseRange(lo, runHi);
    } else {                              // Ascending
      while (runHi < hi &&getTimestamp(runHi) >= getTimestamp(runHi - 1))
        runHi++;
    }

    return runHi - lo;
  }

  private void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTimestamp(lo);
      long loV = getValue(lo);
      long hiT = getTimestamp(hi);
      long hiV = getValue(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
      lo++;
    }
  }
}
