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
import java.util.Comparator;

public class MergeSortHeap {
  private final MergeSortKey[] heap;
  private int heapSize;

  private final Comparator<MergeSortKey> comparator;

  public MergeSortHeap(int childNum, Comparator<MergeSortKey> comparator) {
    this.heap = new MergeSortKey[childNum];
    this.heapSize = 0;
    this.comparator = comparator;
  }

  public boolean isEmpty() {
    return heapSize == 0;
  }

  public void push(MergeSortKey mergeSortKey) {
    if (heapSize == 0) {
      heap[0] = mergeSortKey;
    }
    shiftUp(heapSize, mergeSortKey);
    ++heapSize;
  }

  public MergeSortKey poll() {
    MergeSortKey res = heap[0];
    heap[0] = heap[heapSize - 1];
    shiftDown(0, heap[0]);
    heapSize--;
    return res;
  }

  public MergeSortKey peek() {
    return heap[0];
  }

  @Override
  public String toString() {
    return Arrays.toString(heap);
  }

  private int getSmallerChildIndex(int index) {
    final int leftChildIndex = (index << 1) + 1;
    final int rightChildIndex = (index << 1) + 2;

    int smallerChildIndex;
    if (heapSize <= leftChildIndex) {
      smallerChildIndex = -1;
    } else if (heapSize <= rightChildIndex) {
      smallerChildIndex = leftChildIndex;
    } else {
      smallerChildIndex =
          comparator.compare(heap[leftChildIndex], heap[rightChildIndex]) > 0
              ? rightChildIndex
              : leftChildIndex;
    }
    return smallerChildIndex;
  }

  private void shiftDown(int parentIndex, MergeSortKey parent) {
    if (parentIndex == heapSize - 1) return;

    int childIndex = getSmallerChildIndex(parentIndex);

    if (childIndex != -1) {
      MergeSortKey child = heap[childIndex];
      if (comparator.compare(parent, child) > 0) {
        heap[parentIndex] = child;
        heap[childIndex] = parent;
        shiftDown(childIndex, parent);
      }
    }
  }

  private void shiftUp(int childIndex, MergeSortKey child) {
    if (childIndex == 0) return;

    int parentIndex = (childIndex - 1) >>> 1;
    MergeSortKey parent = heap[parentIndex];

    if (comparator.compare(parent, child) > 0) {
      heap[parentIndex] = child;
      heap[childIndex] = parent;
      shiftUp(parentIndex, child);
    } else {
      heap[childIndex] = child;
    }
  }
}
