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

import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public interface BackwardSort extends QuickSort {

  double INVERSION_RATIOS_THRESHOLD = 0.004;

  void setFromTmp(int src, int dest);

  void setToTmp(int src, int dest);

  void backward_set(int src, int dest);

  int compareTmp(int idx, int tmpIdx);

  void checkTmpLength(int len);

  void clearTmp();

  default void backwardSort(List<long[]> timestamps, int rowCount) {
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
  default int setBlockLength(List<long[]> timestamps, int step) {
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
  default void backwardMergeBlocks(int lo, int hi, int rowCount) {
    int overlapIdx = hi + 1;
    while (overlapIdx < rowCount && compare(hi, overlapIdx) == 1) {
      overlapIdx++;
    }
    if (overlapIdx == hi + 1) return;

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
  default void sortBlock(int lo, int hi) {
    qsort(lo, hi);
  }
}
