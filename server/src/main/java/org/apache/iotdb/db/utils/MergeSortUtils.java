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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class MergeSortUtils {
  protected boolean[] tsBlocksExist;
  protected int targetKeyIndex;
  protected Ordering timeOrdering;
  protected Ordering deviceOrdering;
  protected TsBlock[] tsBlocks;
  protected int tsBlockCount;
  public KeyValueSelector keyValueSelector;

  /** add TsBlock for specific child it usually called when last one was run out */
  public abstract void addTsBlock(TsBlock tsBlock, int index);

  /** update consumed result */
  public abstract void updateTsBlock(int index, TsBlock tsBlock);
  /**
   * get the index of TsBlock whose startValue<=targetValue when ordering is asc,and
   * startValue>=targetValue when ordering is desc. targetValue is the smallest endKey when ordering
   * is asc and the biggest endKey when ordering is desc.
   */
  public abstract List<Integer> getTargetTsBlockIndex();

  // +-----------------------+
  // |  keyValue comparator  |
  // +-----------------------+

  /** check if the keyValue in tsBlockIterator is less than current targetValue */
  public abstract boolean satisfyCurrentEndValue(TsBlockSingleColumnIterator tsBlockIterator);

  /** check if t is greater than s greater means the one with bigger rowIndex in result set */
  public abstract boolean greater(TsBlockSingleColumnIterator t, TsBlockSingleColumnIterator s);

  // find the targetValue in TsBlocks
  // it is:
  // (1) the smallest endKey when ordering is asc
  // (2) the biggest endKey when ordering is desc
  // which is controlled by greater method
  @FunctionalInterface
  public interface Comparator<T> {
    boolean greater(T a, T b);
  }

  public <T> List<Integer> getTargetIndex(T[] startKey, T[] endKey, Comparator<T> comparator) {
    T minEndKey = null;
    int index = Integer.MAX_VALUE;
    int tsBlockCount = startKey.length;
    for (int i = 0; i < tsBlockCount; i++) {
      if (tsBlocksExist[i]) {
        minEndKey = endKey[i];
        index = i;
        break;
      }
    }
    targetKeyIndex = index;
    for (int i = index + 1; i < tsBlockCount; i++) {
      if (tsBlocksExist[i] && comparator.greater(minEndKey, endKey[i])) {
        minEndKey = endKey[i];
        targetKeyIndex = i;
      }
    }
    List<Integer> targetTsBlockIndex = new ArrayList<>();
    for (int i = 0; i < tsBlockCount; i++) {
      if (tsBlocksExist[i]
          && (comparator.greater(minEndKey, startKey[i]) || minEndKey.equals(startKey[i]))) {
        targetTsBlockIndex.add(i);
      }
    }
    return targetTsBlockIndex;
  }

  // the heap for KeyValue
  public class KeyValueSelector {
    private final TsBlockSingleColumnIterator[] tsBlockIterators;
    private final int[] heap;
    private int heapSize;
    private int lastIndex;

    public KeyValueSelector(int childNum) {
      heap = new int[childNum];
      tsBlockIterators = new TsBlockSingleColumnIterator[childNum];
      heapSize = 0;
    }

    public void updateTsBlock(int index, TsBlockSingleColumnIterator tsBlockSingleColumnIterator) {
      tsBlockIterators[index] = tsBlockSingleColumnIterator;
      push(index);
    }

    public void update() {
      push(lastIndex);
    }

    public void push(int value) {
      if (heapSize == 0) {
        heap[0] = value;
      }
      shiftUp(heapSize, value);
      ++heapSize;
    }

    public void clear() {
      heapSize = 0;
    }

    public int poll() {
      int minIndex = heap[0];
      if (!satisfyCurrentEndValue(tsBlockIterators[minIndex])) return -1;
      heap[0] = heap[heapSize - 1];
      shiftDown(0, heap[0]);
      heapSize--;
      lastIndex = minIndex;
      return minIndex;
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
            greater(tsBlockIterators[heap[leftChildIndex]], tsBlockIterators[heap[rightChildIndex]])
                ? rightChildIndex
                : leftChildIndex;
      }
      return smallerChildIndex;
    }

    private void shiftDown(int parentIndex, int parent) {
      if (parentIndex == heapSize - 1) return;

      int childIndex = getSmallerChildIndex(parentIndex);

      if (childIndex != -1) {
        int child = heap[childIndex];
        if (greater(tsBlockIterators[parent], tsBlockIterators[child])) {
          heap[parentIndex] = child;
          heap[childIndex] = parent;
          shiftDown(childIndex, parent);
        }
      }
    }

    private void shiftUp(int childIndex, int child) {
      if (childIndex == 0) return;

      int parentIndex = (childIndex - 1) >>> 1;
      int parent = heap[parentIndex];

      if (greater(tsBlockIterators[parent], tsBlockIterators[child])) {
        heap[parentIndex] = child;
        heap[childIndex] = parent;
        shiftUp(parentIndex, child);
      } else {
        heap[childIndex] = child;
      }
    }
  }
}
