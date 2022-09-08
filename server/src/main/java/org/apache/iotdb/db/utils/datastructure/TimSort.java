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

/**
 * The interface refers to TimSort.java, and is used for sort the TVList Functions for tim_sort like
 * merge, sort, binary_sort is implemented here as default, reuse code whenever possible.
 */
public interface TimSort {
  /** when array size <= 32, it's better to use binarysort. */
  int SMALL_ARRAY_LENGTH = 32;

  /** the same as the 'set' function in TVList, the reason is to avoid two equal functions. */
  void tim_set(int src, int dest);

  void setFromSorted(int src, int dest);

  void setToSorted(int src, int dest);

  void setPivotTo(int pos);

  void saveAsPivot(int pos);

  /**
   * The arrays for sorting are not including in write memory now, the memory usage is considered as
   * temporary memory.
   */
  void clearSortedTime();

  void clearSortedValue();

  /** compare the timestamps in idx1 and idx2 */
  int compare(int idx1, int idx2);

  /** From TimSort.java */
  void reverseRange(int lo, int hi);

  /**
   * the entrance of tim_sort; 1. array_size <= 32, use binary sort. 2. recursively invoke merge
   * sort.
   */
  default void sort(int lo, int hi) {
    if (lo == hi) {
      return;
    }
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

  default int countRunAndMakeAscending(int lo, int hi) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi) {
      return 1;
    }
    // Find end of run, and reverse range if descending
    if (compare(runHi++, lo) == -1) { // Descending
      while (runHi < hi && compare(runHi, runHi - 1) == -1) {
        runHi++;
      }
      reverseRange(lo, runHi);
    } else { // Ascending
      while (runHi < hi && compare(runHi, runHi - 1) >= 0) {
        runHi++;
      }
    }

    return runHi - lo;
  }

  default void binarySort(int lo, int hi, int start) {
    assert lo <= start && start <= hi;
    if (start == lo) {
      start++;
    }
    for (; start < hi; start++) {

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
        if (compare(start, mid) < 0) {
          right = mid;
        } else {
          left = mid + 1;
        }
      }
      assert left == right;

      /*
       * The invariants still hold: pivot >= all in [lo, left) and
       * pivot < all in [left, start), so pivot belongs at left.  Note
       * that if there are elements equal to pivot, left points to the
       * first slot after them -- that's why this sort is stable.
       * Slide elements over to make room for pivot.
       */
      int n = start - left; // The number of elements to move
      for (int i = n; i >= 1; i--) {
        tim_set(left + i - 1, left + i);
      }
      setPivotTo(left);
    }
    for (int i = lo; i < hi; i++) {
      setToSorted(i, i);
    }
  }

  /** merge arrays [lo, mid) [mid, hi] */
  default void merge(int lo, int mid, int hi) {
    // end of sorting buffer
    int tmpIdx = 0;

    // start of unmerged parts of each sequence
    int leftIdx = lo;
    int rightIdx = mid;

    // copy the minimum elements to sorting buffer until one sequence is exhausted
    int endSide = 0;
    while (endSide == 0) {
      if (compare(leftIdx, rightIdx) <= 0) {
        setToSorted(leftIdx, lo + tmpIdx);
        tmpIdx++;
        leftIdx++;
        if (leftIdx == mid) {
          endSide = 1;
        }
      } else {
        setToSorted(rightIdx, lo + tmpIdx);
        tmpIdx++;
        rightIdx++;
        if (rightIdx == hi) {
          endSide = 2;
        }
      }
    }

    // copy the remaining elements of another sequence
    int start;
    int end;
    if (endSide == 1) {
      start = rightIdx;
      end = hi;
    } else {
      start = leftIdx;
      end = mid;
    }
    for (; start < end; start++) {
      setToSorted(start, lo + tmpIdx);
      tmpIdx++;
    }

    // copy from sorting buffer to the original arrays so that they can be further sorted
    // potential speed up: change the place of sorting buffer and origin data between merge
    // iterations
    for (int i = lo; i < hi; i++) {
      setFromSorted(i, i);
    }
  }
}
