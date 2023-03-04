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

import java.util.Arrays;

public class TimeSelector {

  private static final int MIN_DEFAULT_CAPACITY = 8;

  private final boolean ascending;

  private long[] timeHeap;
  private int heapSize;
  private long lastTime;

  public TimeSelector(int defaultCapacity, boolean isAscending) {
    this.ascending = isAscending;
    timeHeap = new long[Math.max(defaultCapacity, MIN_DEFAULT_CAPACITY)];
    heapSize = 0;
    lastTime = Long.MIN_VALUE;
  }

  public boolean isEmpty() {
    while (heapSize != 0 && timeHeap[0] == lastTime) {
      timeHeap[0] = timeHeap[heapSize - 1];
      percolateDown(0, timeHeap[0]);
      --heapSize;
    }
    return heapSize == 0;
  }

  public void add(long time) {
    if (heapSize == 0) {
      timeHeap[0] = time;
    }
    if (percolateUp(heapSize, time)) {
      ++heapSize;
      checkExpansion();
    }
  }

  public long pollFirst() {
    long minTime = lastTime;

    while (minTime == lastTime) {
      minTime = timeHeap[0];

      timeHeap[0] = timeHeap[heapSize - 1];
      percolateDown(0, timeHeap[0]);
      --heapSize;
    }

    lastTime = minTime;
    return minTime;
  }

  private void checkExpansion() {
    if (heapSize == timeHeap.length) {
      timeHeap = Arrays.copyOf(timeHeap, timeHeap.length << 1);
    }
  }

  private boolean percolateUp(int index, long element) {
    if (index == 0) {
      return true;
    }

    int parentIndex = (index - 1) >>> 1;
    long parent = timeHeap[parentIndex];

    if (parent == element) {
      return false;
    } else if (ascending ? element < parent : parent < element) {
      timeHeap[index] = parent;
      timeHeap[parentIndex] = element;
      boolean isSuccessful = percolateUp(parentIndex, element);
      if (!isSuccessful) {
        timeHeap[index] = element;
        timeHeap[parentIndex] = parent;
      }
      return isSuccessful;
    } else { // ascending ? parent < element : element < parent
      timeHeap[index] = element;
      return true;
    }
  }

  private void percolateDown(int index, long element) {
    if (index == heapSize - 1) {
      return;
    }

    int childIndex = getSmallerChildIndex(index);

    if (childIndex != -1) {
      long child = timeHeap[childIndex];
      if (ascending ? child < element : element < child) {
        timeHeap[childIndex] = element;
        timeHeap[index] = child;
        percolateDown(childIndex, element);
      }
    }
  }

  /**
   * Calculates the children indexes for a given index and checks to see which one is smaller and
   * returns the index.
   *
   * @param index the given index
   * @return index of a smaller child or -1 if no children
   */
  private int getSmallerChildIndex(int index) {
    final int leftChildIndex = (index << 1) + 1;
    final int rightChildIndex = (index << 1) + 2;

    int smallerChildIndex;
    if (heapSize <= leftChildIndex) {
      smallerChildIndex = -1;
    } else if (heapSize <= rightChildIndex) {
      smallerChildIndex = leftChildIndex;
    } else {
      if (ascending) {
        smallerChildIndex =
            timeHeap[leftChildIndex] < timeHeap[rightChildIndex] ? leftChildIndex : rightChildIndex;
      } else {
        smallerChildIndex =
            timeHeap[leftChildIndex] < timeHeap[rightChildIndex] ? rightChildIndex : leftChildIndex;
      }
    }
    return smallerChildIndex;
  }

  @Override
  public String toString() {
    return Arrays.toString(this.timeHeap);
  }
}
