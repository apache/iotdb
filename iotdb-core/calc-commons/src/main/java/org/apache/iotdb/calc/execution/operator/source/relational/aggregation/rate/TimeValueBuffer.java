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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class TimeValueBuffer {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TimeValueBuffer.class);
  private static final int INITIAL_CAPACITY = 16;
  private static final int INSERTION_SORT_THRESHOLD = 16;

  private long[] times = new long[INITIAL_CAPACITY];
  private double[] values = new double[INITIAL_CAPACITY];
  private int size;
  private boolean sortedAndValidated;

  public void add(long time, double value) {
    ensureCapacity(Math.addExact(size, 1));
    times[size] = time;
    values[size] = value;
    size++;
    sortedAndValidated = false;
  }

  public void merge(TimeValueBuffer other) {
    if (other == null || other.size == 0) {
      return;
    }
    int newSize = Math.addExact(size, other.size);
    ensureCapacity(newSize);
    System.arraycopy(other.times, 0, times, size, other.size);
    System.arraycopy(other.values, 0, values, size, other.size);
    size = newSize;
    sortedAndValidated = false;
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public long getTime(int index) {
    return times[index];
  }

  public double getValue(int index) {
    return values[index];
  }

  public long getEstimatedSize() {
    return estimatedSizeForCapacity(times.length);
  }

  static long estimatedSizeForSampleCount(int sampleCount) {
    int capacity = INITIAL_CAPACITY;
    while (capacity < sampleCount) {
      capacity = Math.multiplyExact(capacity, 2);
    }
    return estimatedSizeForCapacity(capacity);
  }

  public void reset() {
    size = 0;
    sortedAndValidated = false;
  }

  public void writePayload(ByteBuffer target) {
    for (int index = 0; index < size; index++) {
      target.putLong(times[index]);
      target.putDouble(values[index]);
    }
  }

  public void sortAndValidate(String functionName) {
    if (sortedAndValidated) {
      return;
    }
    if (size > 1) {
      introSort(0, size, 2 * floorLog2(size));
      for (int index = 1; index < size; index++) {
        if (times[index - 1] == times[index]) {
          throw new SemanticException(
              String.format(
                  CalcMessages
                      .EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_DUPLICATE_TIME_COL_VALUES_IN_THE_SAME_AGGREGATION_GROUP_ARG_087A91BC,
                  functionName,
                  times[index]));
        }
      }
    }
    sortedAndValidated = true;
  }

  private void ensureCapacity(int requiredCapacity) {
    if (requiredCapacity <= times.length) {
      return;
    }
    int newCapacity = Math.max(requiredCapacity, Math.multiplyExact(times.length, 2));
    times = Arrays.copyOf(times, newCapacity);
    values = Arrays.copyOf(values, newCapacity);
  }

  private void introSort(int from, int to, int depthLimit) {
    int length = to - from;
    if (length <= INSERTION_SORT_THRESHOLD) {
      insertionSort(from, to);
      return;
    }
    if (depthLimit == 0) {
      heapSort(from, to);
      return;
    }

    long pivot = median(times[from], times[from + length / 2], times[to - 1]);
    int left = from - 1;
    int right = to;
    while (true) {
      do {
        left++;
      } while (times[left] < pivot);
      do {
        right--;
      } while (times[right] > pivot);
      if (left >= right) {
        break;
      }
      swap(left, right);
    }

    introSort(from, right + 1, depthLimit - 1);
    introSort(right + 1, to, depthLimit - 1);
  }

  private void insertionSort(int from, int to) {
    for (int index = from + 1; index < to; index++) {
      long time = times[index];
      double value = values[index];
      int position = index - 1;
      while (position >= from && times[position] > time) {
        times[position + 1] = times[position];
        values[position + 1] = values[position];
        position--;
      }
      times[position + 1] = time;
      values[position + 1] = value;
    }
  }

  private void heapSort(int from, int to) {
    int length = to - from;
    for (int root = length / 2 - 1; root >= 0; root--) {
      siftDown(from, root, length);
    }
    for (int end = length - 1; end > 0; end--) {
      swap(from, from + end);
      siftDown(from, 0, end);
    }
  }

  private void siftDown(int base, int root, int length) {
    while (root * 2 + 1 < length) {
      int child = root * 2 + 1;
      if (child + 1 < length && times[base + child] < times[base + child + 1]) {
        child++;
      }
      if (times[base + root] >= times[base + child]) {
        return;
      }
      swap(base + root, base + child);
      root = child;
    }
  }

  private void swap(int left, int right) {
    long time = times[left];
    times[left] = times[right];
    times[right] = time;

    double value = values[left];
    values[left] = values[right];
    values[right] = value;
  }

  private static long median(long first, long second, long third) {
    if (first < second) {
      return second < third ? second : Math.max(first, third);
    }
    return first < third ? first : Math.max(second, third);
  }

  private static int floorLog2(int value) {
    return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(value);
  }

  private static long estimatedSizeForCapacity(int capacity) {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOfLongArray(capacity)
        + RamUsageEstimator.sizeOfDoubleArray(capacity);
  }
}
