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

package org.apache.iotdb.db.queryengine.execution.operator.process.function.partition;

import org.apache.tsfile.block.column.Column;

import java.util.ArrayList;
import java.util.List;

/** Used to manage the slices of the partition. It is all in memory now. */
public class PartitionCache {

  private final List<Slice> slices = new ArrayList<>();
  private final List<Long> startOffsets = new ArrayList<>();

  private long estimatedSize = 0;

  public List<Column[]> getPassThroughResult(Column passThroughIndexes) {
    List<Column[]> result = new ArrayList<>();
    int sliceIndex = findSliceIndex(passThroughIndexes.getLong(0));
    int indexStart = 0;
    for (int i = 1; i < passThroughIndexes.getPositionCount(); i++) {
      int tmp = findSliceIndex(passThroughIndexes.getLong(i));
      if (tmp != sliceIndex) {
        int[] indexArray = new int[i - indexStart];
        for (int j = indexStart; j < i; j++) {
          indexArray[j - indexStart] =
              (int) (passThroughIndexes.getLong(j) - getSliceOffset(sliceIndex));
        }

        result.add(slices.get(sliceIndex).getPassThroughResult(indexArray));
        indexStart = i;
        sliceIndex = tmp;
      }
    }
    int[] indexArray = new int[passThroughIndexes.getPositionCount() - indexStart];
    for (int j = indexStart; j < passThroughIndexes.getPositionCount(); j++) {
      indexArray[j - indexStart] =
          (int) (passThroughIndexes.getLong(j) - getSliceOffset(sliceIndex));
    }
    result.add(slices.get(sliceIndex).getPassThroughResult(indexArray));
    return result;
  }

  public void addSlice(Slice slice) {
    slices.add(slice);
    if (startOffsets.isEmpty()) {
      startOffsets.add(0L);
    } else {
      startOffsets.add(
          startOffsets.get(startOffsets.size() - 1)
              + slices.get(startOffsets.size() - 1).getSize());
    }
    this.estimatedSize += slice.getEstimatedSize();
  }

  private long getSliceOffset(int slideIndex) {
    return startOffsets.get(slideIndex);
  }

  private int findSliceIndex(long passThroughIndex) {
    int left = 0;
    int right = startOffsets.size() - 1;
    int result = -1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      if (startOffsets.get(mid) <= passThroughIndex) {
        result = mid;
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
    return result;
  }

  public long getEstimatedSize() {
    return estimatedSize;
  }

  public void clear() {
    slices.clear();
  }

  public void close() {
    // do nothing
  }
}
