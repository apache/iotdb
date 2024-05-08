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
  long N;
  int maxMemoryNum;
  long[] num;
  boolean level0Sorted;
  int cntLevel;
  int[] levelPos, levelMaxSize;
  long XORSHIFT = new Random().nextInt(); // 0x2333333319260817L;
  //  Random test_random = new Random();

  public KLLSketchForQuantile() {}

  protected abstract int calcMaxMemoryNum(int maxMemoryByte);

  protected abstract void calcLevelMaxSize(int setLevel);

  public int getLevelSize(int level) {
    return levelPos[level + 1] - levelPos[level];
  }

  public void show() {
    for (int i = 0; i < cntLevel; i++) {
      System.out.print("\t");
      System.out.print("[" + (levelPos[i + 1] - levelPos[i]) + "]");
      System.out.print("\t");
    }
    System.out.println();
  }

  public void showNum() {
    for (int i = 0; i < cntLevel; i++) {
      System.out.print("\t|");
      for (int j = levelPos[i]; j < levelPos[i + 1]; j++) System.out.print(num[j] + ",");
      System.out.print("|\t");
    }
    System.out.println();
  }

  public void update(long x) { // signed long
    if (levelPos[0] == 0) compact();
    num[--levelPos[0]] = x;
    N++;
    level0Sorted = false;

    //    boolean flag=false;
    //    for(int i=0;i<cntLevel;i++)if(levelPos[i+1]-levelPos[i]>levelMaxSize[i])flag=true;
    //    if(flag)compact();
    //    System.out.println("\t\t\t"+x);
  }

  protected abstract void compact();

  protected int getNextRand01() { // xor shift *
    XORSHIFT ^= XORSHIFT >>> 12;
    XORSHIFT ^= XORSHIFT << 25;
    XORSHIFT ^= XORSHIFT >>> 27;
    return (int) ((XORSHIFT * 0x2545F4914F6CDD1DL) & 1);
    //    return test_random.nextInt()&1;
  }

  protected void randomlyHalveDownToLeft(int L, int R) {
    int delta = getNextRand01();
    int mid = (L + R) >>> 1;
    for (int i = L, j = L; i < mid; i++, j += 2) num[i] = num[j + delta];
  }

  protected void mergeSortWithoutSpace(int L1, int mid, int L2, int R2) {
    int p1 = L1, p2 = L2, cntPos = mid;
    while (p1 < mid || p2 < R2) {
      if (p1 < mid && (p2 == R2 || num[p1] < num[p2])) num[cntPos++] = num[p1++];
      else num[cntPos++] = num[p2++];
    }
  }

  protected int findRankInLevel(int level, long v) {
    int L = levelPos[level], R = levelPos[level + 1];
    if (level == 0 && !level0Sorted) {
      Arrays.sort(num, L, R);
      level0Sorted = true;
    }
    R--;
    if (L > R || num[L] >= v) return 0;
    while (L < R) {
      int mid = (L + R + 1) >> 1;
      if (num[mid] < v) L = mid;
      else R = mid - 1;
    }
    return (L - levelPos[level] + 1) * (1 << level);
  }

  public int getApproxRank(long v) {
    int approxRank = 0;
    for (int i = 0; i < cntLevel; i++) {
      approxRank += findRankInLevel(i, v);
      //      for (int j = levelPos[i]; j < levelPos[i + 1]; j++)
      //        if (num[j] < v) approxRank += 1 << i;
    }
    return approxRank;
  }

  public long findMaxValueWithRank(long K) {
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE, mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      if (mid == L) mid++;
      if (getApproxRank(mid) <= K) L = mid;
      else R = mid - 1;
    }
    return L;
  }

  public long findMinValueWithRank(long K) {
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE, mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      if (mid == R) mid--;
      if (getApproxRank(mid) >= K) R = mid;
      else L = mid + 1;
    }
    return L;
  }

  public long getN() {
    return N;
  }

  public int getMaxMemoryNum() {
    return maxMemoryNum;
  }

  public int getNumLen() {
    return levelPos[cntLevel] - levelPos[0];
  }

  public boolean exactResult() {
    return this.N == this.getNumLen();
  }

  public long getExactResult(int K) {
    int L = levelPos[0], R = levelPos[1];
    if (!level0Sorted) {
      Arrays.sort(num, L, R);
      level0Sorted = true;
    }
    return num[L + K];
  }
}
