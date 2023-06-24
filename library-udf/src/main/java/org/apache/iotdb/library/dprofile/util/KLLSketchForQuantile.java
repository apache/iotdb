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

package org.apache.iotdb.library.dprofile.util;

import java.util.Arrays;
import java.util.Random;

// based on KLL Sketch in DataSketch. See
// https://github.com/apache/datasketches-java/tree/master/src/main/java/org/apache/datasketches/kll
public abstract class KLLSketchForQuantile {
  long n;
  int maxMemoryNum;
  long[] num;
  boolean level0Sorted;
  int cntLevel;
  int[] levelPos;
  int[] levelMaxSize;
  long XORSHIFT;

  protected KLLSketchForQuantile() {
    XORSHIFT = new Random().nextInt();
  }

  protected abstract int calcMaxMemoryNum(int maxMemoryByte);

  protected abstract void calcLevelMaxSize(int setLevel);

  public int getLevelSize(int level) {
    return levelPos[level + 1] - levelPos[level];
  }

  public void update(long x) { // signed long
    if (levelPos[0] == 0) {
      compact();
    }
    num[--levelPos[0]] = x;
    n++;
    level0Sorted = false;
  }

  protected abstract void compact();

  protected int getNextRand01() { // xor shift *
    XORSHIFT ^= XORSHIFT >>> 12;
    XORSHIFT ^= XORSHIFT << 25;
    XORSHIFT ^= XORSHIFT >>> 27;
    return (int) ((XORSHIFT * 0x2545F4914F6CDD1DL) & 1);
  }

  protected void randomlyHalveDownToLeft(int l, int r) {
    int delta = getNextRand01();
    int mid = (l + r) >>> 1;
    for (int i = l, j = l; i < mid; i++, j += 2) {
      num[i] = num[j + delta];
    }
  }

  protected void mergeSortWithoutSpace(int l1, int mid, int l2, int r2) {
    int p1 = l1;
    int p2 = l2;
    int cntPos = mid;
    while (p1 < mid || p2 < r2) {
      if (p1 < mid && (p2 == r2 || num[p1] < num[p2])) {
        num[cntPos++] = num[p1++];
      } else {
        num[cntPos++] = num[p2++];
      }
    }
  }

  protected int findRankInLevel(int level, long v) {
    int l = levelPos[level];
    int r = levelPos[level + 1];
    if (level == 0 && !level0Sorted) {
      Arrays.sort(num, l, r);
      level0Sorted = true;
    }
    r--;
    if (l > r || num[l] >= v) {
      return 0;
    }
    while (l < r) {
      int mid = (l + r + 1) >> 1;
      if (num[mid] < v) {
        l = mid;
      } else {
        r = mid - 1;
      }
    }
    return (l - levelPos[level] + 1) * (1 << level);
  }

  public int getApproxRank(long v) {
    int approxRank = 0;
    for (int i = 0; i < cntLevel; i++) {
      approxRank += findRankInLevel(i, v);
    }
    return approxRank;
  }

  public long findMaxValueWithRank(long k) {
    long l = Long.MIN_VALUE;
    long r = Long.MAX_VALUE;
    long mid;
    while (l < r) {
      mid = l + ((r - l) >>> 1);
      if (mid == l) {
        mid++;
      }
      if (getApproxRank(mid) <= k) {
        l = mid;
      } else {
        r = mid - 1;
      }
    }
    return l;
  }

  public long findMinValueWithRank(long k) {
    long l = Long.MIN_VALUE;
    long r = Long.MAX_VALUE;
    long mid;
    while (l < r) {
      mid = l + ((r - l) >>> 1);
      if (mid == r) {
        mid--;
      }
      if (getApproxRank(mid) >= k) {
        r = mid;
      } else {
        l = mid + 1;
      }
    }
    return l;
  }

  public long getN() {
    return n;
  }

  public int getMaxMemoryNum() {
    return maxMemoryNum;
  }

  public int getNumLen() {
    return levelPos[cntLevel] - levelPos[0];
  }

  public boolean exactResult() {
    return this.n == this.getNumLen();
  }

  public long getExactResult(int k) {
    int l = levelPos[0];
    int r = levelPos[1];
    if (!level0Sorted) {
      Arrays.sort(num, l, r);
      level0Sorted = true;
    }
    return num[l + k];
  }
}
