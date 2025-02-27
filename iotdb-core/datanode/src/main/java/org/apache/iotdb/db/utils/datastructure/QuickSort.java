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

public interface QuickSort {
  /** compare the timestamps in idx1 and idx2 */
  int compare(int idx1, int idx2);

  void swap(int p, int q);

  default int partition(int lo, int hi) {
    // Choose the middle of the array as pivot.
    // In time series, usually the middle element is of middle range
    int pIndex = (lo + hi) / 2;
    // long pivot = getTime(pIndex);
    int gIndex = lo;
    // Find the greatest index not smaller than pivot.
    // while (getTime(gIndex) < pivot) {
    while (compare(gIndex, pIndex) == -1) {
      gIndex++;
    }
    for (int i = gIndex; i <= hi; i++) {
      if (compare(i, pIndex) == -1) {
        // swap the element < pivot to gIndex, and gIndex++
        swap(gIndex, i);
        // maintain the pIndex
        if (pIndex == gIndex) {
          pIndex = i;
        }
        gIndex++;
      }
    }
    if (gIndex != pIndex) {
      swap(gIndex, pIndex);
      pIndex = gIndex;
    }
    return pIndex;
  }

  //    default void insertion_sort(int lo, int hi){
  //        for(int i = lo + 1; i <= hi; i++) {
  //        }
  //    }

  default void qsort(int lo, int hi) {
    if (lo < hi) {
      // TODO: use insertion sort in smaller array
      // if(hi - lo <= 32) {
      //    insertion_sort(lo, hi);
      // }
      // partition
      int pivotIndex = partition(lo, hi);
      qsort(lo, pivotIndex - 1);
      qsort(pivotIndex + 1, hi);
    }
  }
}
