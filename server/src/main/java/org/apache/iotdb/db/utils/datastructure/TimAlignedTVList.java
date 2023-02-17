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

import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class TimAlignedTVList extends AlignedTVList implements TimSort {

  private long[][] sortedTimestamps;
  private long pivotTime;

  private int[][] sortedIndices;
  private int pivotIndex;

  TimAlignedTVList(List<TSDataType> types) {
    super(types);
  }

  @Override
  public void sort() {
    if (sortedTimestamps == null
        || sortedTimestamps.length < PrimitiveArrayManager.getArrayRowCount(rowCount)) {
      sortedTimestamps =
          (long[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, rowCount);
    }
    if (sortedIndices == null
        || sortedIndices.length < PrimitiveArrayManager.getArrayRowCount(rowCount)) {
      sortedIndices =
          (int[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT32, rowCount);
    }
    if (!sorted) {
      sort(0, rowCount);
    }
    clearSortedValue();
    clearSortedTime();
    sorted = true;
  }

  @Override
  public void tim_set(int src, int dest) {
    set(src, dest);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getValueIndex(src);
    set(dest, srcT, srcV);
  }

  @Override
  public void setFromSorted(int src, int dest) {
    set(
        dest,
        sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedIndices[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  @Override
  public void setToSorted(int src, int dest) {
    sortedTimestamps[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getTime(src);
    sortedIndices[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getValueIndex(src);
  }

  @Override
  public void setPivotTo(int pos) {
    set(pos, pivotTime, pivotIndex);
  }

  @Override
  public void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotIndex = getValueIndex(pos);
  }

  @Override
  public void clearSortedTime() {
    if (sortedTimestamps != null) {
      sortedTimestamps = null;
    }
  }

  @Override
  public void clearSortedValue() {
    if (sortedIndices != null) {
      sortedIndices = null;
    }
  }

  @Override
  public int compare(int idx1, int idx2) {
    long t1 = getTime(idx1);
    long t2 = getTime(idx2);
    return Long.compare(t1, t2);
  }

  @Override
  public void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      int loV = getValueIndex(lo);
      long hiT = getTime(hi);
      int hiV = getValueIndex(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  public void clear() {
    super.clear();
    clearSortedTime();
    clearSortedValue();
  }
}
