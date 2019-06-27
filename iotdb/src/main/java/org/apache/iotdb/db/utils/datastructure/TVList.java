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

public abstract class TVList {

  protected static final int SMALL_ARRAY_LENGTH = 32;
  protected static final int SINGLE_ARRAY_SIZE = 512;

  protected List<long[]> timestamps;
  protected int size;

  protected long[] sortedTimestamps;
  protected boolean sorted = false;

  public TVList() {
    timestamps = new ArrayList<>();
    size = 0;
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

  public void putString(long time, String value) {
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

  public String getString(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public boolean getBoolean(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public abstract void sort();

  protected abstract void binarySort(int lo, int hi, int start);

  protected abstract void merge(int lo, int mid, int hi);

  protected abstract void cleanAfterSort();

  protected abstract void reverseRange(int lo, int hi);

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
    sorted = true;
    cleanAfterSort();
    timestamps = null;
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
}
