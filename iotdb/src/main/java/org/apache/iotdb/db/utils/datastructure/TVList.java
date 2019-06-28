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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class TVList {

  protected static final int SMALL_ARRAY_LENGTH = 32;
  protected static final int SINGLE_ARRAY_SIZE = 512;

  protected List<long[]> timestamps;
  protected int size;
  protected int limit;

  protected long[] sortedTimestamps;
  protected boolean sorted = false;

  private long timeOffset = -1;

  protected long pivotTime;

  public TVList() {
    timestamps = new ArrayList<>();
    size = 0;
    limit = 0;
  }

  public int size() {
    return size;
  }

  public long getTime(int index) {
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

  public void putLong(long time, long value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putInt(long time, int value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putFloat(long time, float value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putDouble(long time, double value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putBinary(long time, Binary value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putBoolean(long time, boolean value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public int getInt(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public float getFloat(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public double getDouble(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public Binary getBinary(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public boolean getBoolean(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public abstract void sort();

  protected abstract void set(int src, int dest);

  protected abstract void setFromSorted(int src, int dest);

  protected abstract void setSorted(int src, int dest);

  protected abstract void reverseRange(int lo, int hi);

  protected abstract void expandValues();

  public abstract TVList clone();

  public void reset() {
    size = 0;
    limit = 0;
  }

  protected void checkExpansion() {
    if ((size == limit) && (size % SINGLE_ARRAY_SIZE) == 0) {
      expandValues();
      timestamps.add(new long[SINGLE_ARRAY_SIZE]);
      limit += SINGLE_ARRAY_SIZE;
    }
  }

  protected long[] cloneTime(long[] array) {
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  protected void sort(int lo, int hi) {
    if (hi - lo <= SMALL_ARRAY_LENGTH) {
      int initRunLen = countRunAndMakeAscending(lo, hi);
      binarySort(lo, hi, lo + initRunLen);
      return;
    }
    int mid = (lo + hi) >>> 1;
    sort(lo, mid);
    sort(mid, hi);
    merge(lo, mid, hi);
  }

  protected int countRunAndMakeAscending(int lo, int hi) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi) {
      return 1;
    }

    // Find end of run, and reverse range if descending
    if (getTime(runHi++) < getTime(lo)) { // Descending
      while (runHi < hi && getTime(runHi) < getTime(runHi - 1)) {
        runHi++;
      }
      reverseRange(lo, runHi);
    } else {                              // Ascending
      while (runHi < hi &&getTime(runHi) >= getTime(runHi - 1))
        runHi++;
    }

    return runHi - lo;
  }

  public static TVList newList(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
      case FLOAT:
      case INT32:
      case INT64:
        return new LongTVList();
      case DOUBLE:
        return new DoubleTVList();
      case BOOLEAN:
    }
    return null;
  }

  public long getTimeOffset() {
    return timeOffset;
  }

  public void setTimeOffset(long timeOffset) {
    this.timeOffset = timeOffset;
  }

  protected int compare(int idx1, int idx2) {
    long t1 = getTime(idx1);
    long t2 = getTime(idx2);
    return Long.compare(t1, t2);
  }

  protected abstract void saveAsPivot(int pos);

  protected abstract void setPivotTo(int pos);

  /**
   * From TimSort.java
   */
  protected void binarySort(int lo, int hi, int start) {
    assert lo <= start && start <= hi;
    if (start == lo)
      start++;
    for ( ; start < hi; start++) {

      saveAsPivot(start);
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
        if (compare(start, mid) < 0)
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
      setPivotTo(left);
    }
    for (int i = lo; i < hi; i++) {
      setSorted(i, i);
    }
  }

  protected void merge(int lo, int mid, int hi) {
    int tmpIdx = 0;

    int leftIdx = lo;
    int rightIdx = mid;

    int endSide = 0;
    while (endSide == 0) {
      if (compare(leftIdx, rightIdx) <= 0) {
        setSorted(leftIdx, lo + tmpIdx);
        tmpIdx ++;
        leftIdx ++;
        if (leftIdx == mid) {
          endSide = 1;
        }
      } else {
        setSorted(rightIdx, lo + tmpIdx);
        tmpIdx ++;
        rightIdx ++;
        if (rightIdx == hi) {
          endSide = 2;
        }
      }
    }
    if (endSide == 1) {
      for (; rightIdx < hi; rightIdx++) {
        setSorted(rightIdx, lo + tmpIdx);
        tmpIdx ++;
      }
    } else {
      for(; leftIdx < mid; leftIdx++) {
        setSorted(leftIdx, lo + tmpIdx);
        tmpIdx ++;
      }
    }
    for (int i = lo; i < hi; i++) {
      setFromSorted(i, i);
    }
  }
}
