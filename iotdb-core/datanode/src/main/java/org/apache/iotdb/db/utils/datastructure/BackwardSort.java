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

import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;

import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

public class BackwardSort extends QuickSort {
  public static double INVERSION_RATIOS_THRESHOLD = 0.004;

  private final List<long[]> tmpTimestamps = new ArrayList<>();
  private final List<int[]> tmpIndices = new ArrayList<>();
  private int tmpLength = 0;

  public BackwardSort(TVList tvList) {
    super(tvList);
  }

  public void setFromTmp(int src, int dest) {
    tvList.set(
        dest,
        tmpTimestamps.get(src / ARRAY_SIZE)[src % ARRAY_SIZE],
        tmpIndices.get(src / ARRAY_SIZE)[src % ARRAY_SIZE]);
  }

  public void setToTmp(int src, int dest) {
    tmpTimestamps.get(dest / ARRAY_SIZE)[dest % ARRAY_SIZE] = tvList.getTime(src);
    tmpIndices.get(dest / ARRAY_SIZE)[dest % ARRAY_SIZE] = tvList.getValueIndex(src);
  }

  public void backward_set(int src, int dest) {
    tvList.set(src, dest);
  }

  public int compareTmp(int idx, int tmpIdx) {
    long t1 = tvList.getTime(idx);
    long t2 = tmpTimestamps.get(tmpIdx / ARRAY_SIZE)[tmpIdx % ARRAY_SIZE];
    return Long.compare(t1, t2);
  }

  public void checkTmpLength(int len) {
    while (len > tmpLength) {
      tmpTimestamps.add((long[]) tvList.getPrimitiveArraysByType(TSDataType.INT64));
      tmpIndices.add((int[]) tvList.getPrimitiveArraysByType(TSDataType.INT32));
      tmpLength += ARRAY_SIZE;
    }
  }

  public void clearTmp() {
    for (long[] dataArray : tmpTimestamps) {
      PrimitiveArrayManager.release(dataArray);
    }
    tmpTimestamps.clear();
    for (int[] dataArray : tmpIndices) {
      PrimitiveArrayManager.release(dataArray);
    }
    tmpIndices.clear();
    tmpLength = 0;
  }

  public void backwardSort(List<long[]> timestamps, int rowCount) {
    int block_size = setBlockLength(timestamps, 1);
    // System.out.printf("rowCount=%d, block_size=%d\n",rowCount, block_size);
    int B = rowCount / block_size + 1;
    sortBlock((B - 1) * block_size, rowCount - 1);
    for (int i = B - 2; i >= 0; i--) {
      int lo = i * block_size, hi = lo + block_size - 1;
      sortBlock(lo, hi);
      backwardMergeBlocks(lo, hi, rowCount);
    }
  }

  /**
   * check block-inversions to find the proper block_size, which is a multiple of array_size. For
   * totally ordered, the block_size will equals to array_size For totally reverse ordered, the
   * block_size will equals to the rowCount. INVERSION_RATIOS_THRESHOLD=0.005 is a empiric value.
   *
   * @param timestamps
   * @param step
   * @return
   */
  public int setBlockLength(List<long[]> timestamps, int step) {
    double overlap = 0;
    long last_time = timestamps.get(0)[0];
    int i = step, blocks = 0;
    while (i < timestamps.size()) {
      long cur_time = timestamps.get(i)[0];
      if (last_time > cur_time) {
        overlap += 1;
      }
      last_time = cur_time;
      i += step;
      blocks += 1;
    }
    if (blocks == 0.0) {
      throw new IllegalStateException("blocks should never be zero.");
    }
    double ratio = overlap / blocks;
    int mul = (int) Math.ceil(ratio / INVERSION_RATIOS_THRESHOLD);
    // System.out.printf("Overlap ratio=%.4f mul=%d, step=%d\n", ratio, mul, step);
    // ensure inversion ratio < INVERSION_RATIOS_THRESHOLD
    if (mul <= 1) {
      return step * ARRAY_SIZE;
    }
    return setBlockLength(timestamps, mul * step);
  }

  /**
   * Backward merge the blocks to reduce repetitive moves.
   *
   * @param lo
   * @param hi
   * @param rowCount
   */
  public void backwardMergeBlocks(int lo, int hi, int rowCount) {
    int overlapIdx = hi + 1;
    while (overlapIdx < rowCount && compare(hi, overlapIdx) == 1) {
      overlapIdx++;
    }
    if (overlapIdx == hi + 1) {
      return;
    }

    int tmpIdx = 0;
    int len = overlapIdx - hi;
    checkTmpLength(len);
    for (int i = hi + 1; i < overlapIdx; i++) {
      setToTmp(i, tmpIdx);
      tmpIdx++;
    }

    int a = hi, b = tmpIdx - 1, idx = overlapIdx - 1;
    while (a >= lo && b >= 0) {
      if (compareTmp(a, b) == 1) {
        backward_set(a, idx);
        a--;
      } else {
        setFromTmp(b, idx);
        b--;
      }
      idx--;
    }
    while (b >= 0) {
      setFromTmp(b, idx);
      b--;
      idx--;
    }
  }

  /**
   * TODO: optional sort algorithms by inversion rates and block_size
   *
   * @param lo
   * @param hi
   */
  public void sortBlock(int lo, int hi) {
    qsort(lo, hi);
  }
}
