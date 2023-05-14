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

package org.apache.iotdb.db.utils.quantiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is a treap with a maximum size. values in treap are treated as unsigned when size meets
 * limit, it will right shift the values and rebuild when bits of values too small, it will become a
 * bucket
 */
public class FixedTreap {
  private static final int maxSize = (1 << 16) + 1;
  private static final int bucketBits = 16;
  //  private static final int SHR = 1;
  private long deltaForUnsignedCompare;
  int[] tmpQueue;
  int tmpQueueLength;
  int[] trashStack;
  int trashTop;
  int[] lc, rc, fa, rnd;
  long[] value, count;
  int root;
  long randSeed;
  int bitsOfValue, remainingBits, minBits; // tuan to bucket(16bit) when remainingBits <= minBits
  boolean isBucket;
  long[] bucket;
  long DEBUG = 0;

  public FixedTreap(int bits, int minBits) { // minBits<bits
    this.minBits = minBits;
    isBucket = false;
    tmpQueue = new int[maxSize + 1];
    trashStack = new int[maxSize + 1];
    trashTop = maxSize;
    for (int i = 1; i <= maxSize; i++) trashStack[i] = maxSize - i + 1;
    lc = new int[maxSize + 1];
    rc = new int[maxSize + 1];
    fa = new int[maxSize + 1];
    rnd = new int[maxSize + 1];
    value = new long[maxSize + 1];
    count = new long[maxSize + 1];
    root = 0;
    randSeed = 233L; // System.currentTimeMillis();
    bitsOfValue = remainingBits = bits;
    if (bits == 64) deltaForUnsignedCompare = 1L << 63; // unsigned long
    else deltaForUnsignedCompare = 0;
  }

  private int getNewNode() {
    int x = trashStack[trashTop--];
    lc[x] = rc[x] = fa[x] = 0;
    count[x] = 0;
    return x;
  }

  private void freeNode(int x) {
    trashStack[++trashTop] = x;
  }

  private int nextRandInt() { // xorShift
    randSeed ^= (randSeed << 21);
    randSeed ^= (randSeed >>> 35);
    randSeed ^= (randSeed << 4);
    return (int) randSeed;
  }

  private void rotate(int x) {
    int f = fa[x];
    if (fa[f] != 0) {
      if (lc[fa[f]] == f) lc[fa[f]] = x;
      else rc[fa[f]] = x;
    }
    fa[x] = fa[f];
    if ((value[x] ^ deltaForUnsignedCompare) < (value[f] ^ deltaForUnsignedCompare)) {
      lc[f] = rc[x];
      fa[rc[x]] = f;
      rc[x] = f;
      fa[f] = x;
    } else {
      rc[f] = lc[x];
      fa[lc[x]] = f;
      lc[x] = f;
      fa[f] = x;
    }
  }

  private void insertNonRecursive(long num, long freq) {
    int x = root, last = 0;
    while (x > 0) {
      last = x;
      if ((num ^ deltaForUnsignedCompare) < (value[x] ^ deltaForUnsignedCompare)) x = lc[x];
      else if (num != value[x]) x = rc[x];
      else {
        count[x] += freq; // 19934325
        return;
      }
    }
    DEBUG++;
    x = getNewNode();
    rnd[x] = nextRandInt();
    value[x] = num;
    count[x] = freq;
    if (last == 0) {
      root = x;
      fa[root] = 0;
    } else {
      if ((num ^ deltaForUnsignedCompare) < (value[last] ^ deltaForUnsignedCompare)) lc[last] = x;
      else rc[last] = x;
      fa[x] = last;
      while (fa[x] != 0 && rnd[x] < rnd[fa[x]]) rotate(x);
      if (fa[x] == 0) root = x;
    }

    if (trashTop == 0) { // when size >= 2^16+1
      rebuild();
      if (remainingBits <= minBits) turnToBucket();
    }
    //    System.out.println("\t\t\t+  root:" + root + "   " + lc[root] + "  " + rc[root]);
  }

  private int findSHR() { // find min SHR to make some values become the same after >>>=SHR
    long x, y, xor;
    int SHR = remainingBits - 16, tmpLen;
    for (int i = 2; i <= tmpQueueLength; i++) {
      x = value[tmpQueue[i - 1]];
      y = value[tmpQueue[i]];
      xor = x ^ y;
      if ((xor >>> SHR) > 0) continue;
      tmpLen = 0;
      //            System.out.println("[DEBUG treap] findSHR  xor:"+xor);
      for (int j = 32; j > 0; j >>= 1)
        if ((xor >>> j) > 0) {
          tmpLen += j;
          xor >>>= j;
        }
      tmpLen += xor;
      SHR = Math.min(SHR, tmpLen);
    }
    //        System.out.println("[DEBUG treap] findSHR  :"+SHR);
    return SHR;
  }

  private int dfsForRebuild(int L, int R) {
    int mid = (L + R) >> 1, x = tmpQueue[mid];
    if (L < mid) {
      fa[lc[x] = dfsForRebuild(L, mid - 1)] = x;
      if (rnd[lc[x]] < rnd[x]) {
        int tmp = rnd[x];
        rnd[x] = rnd[lc[x]];
        rnd[lc[x]] = tmp;
      }
    } else lc[x] = 0;
    if (mid < R) {
      fa[rc[x] = dfsForRebuild(mid + 1, R)] = x;
      if (rnd[rc[x]] < rnd[x]) {
        int tmp = rnd[x];
        rnd[x] = rnd[rc[x]];
        rnd[rc[x]] = tmp;
      }
    } else rc[x] = 0;
    return x;
  }

  private void rebuild() {
    tmpQueueLength = 0;
    dfs(root);
    int SHR = findSHR();
    remainingBits -= SHR;
    deltaForUnsignedCompare = 0;
    int last = tmpQueue[1], now, newSize = 1;
    value[last] >>>= SHR;
    for (int i = 2; i <= tmpQueueLength; i++) {
      now = tmpQueue[i];
      value[now] >>>= SHR;
      if (value[now] == value[last]) {
        count[last] += count[now];
        freeNode(now);
      } else last = tmpQueue[++newSize] = now;
    } // old size == maxSize; 2^16 >= newSize >= (maxSize+1)/2 = 2^15+1
    fa[root = dfsForRebuild(1, newSize)] = 0;
  }

  private void dfs(int x) {
    if (lc[x] != 0) dfs(lc[x]);
    tmpQueue[++tmpQueueLength] = x;
    if (rc[x] != 0) dfs(rc[x]);
  }

  private void resetBucket() {
    if (bucket == null) bucket = new long[1 << bucketBits];
    else Arrays.fill(bucket, 0);
  }

  private void turnToBucket() {
    int x, num;
    isBucket = true;
    resetBucket();
    tmpQueueLength = 0;
    dfs(root);
    //    long DEBUG = 0;
    for (int i = 1; i <= tmpQueueLength; i++) {
      x = tmpQueue[i];
      num = (int) (value[x] >>> (remainingBits - bucketBits));
      bucket[num] += count[x];
      //      DEBUG+=count[x];
    }
    //    System.out.println("[treap DEBUG] turnToBucket at Size "+DEBUG);
    remainingBits = bucketBits;
  }

  public void insert(long num, long freq) {
    num >>>= bitsOfValue - remainingBits;
    if (isBucket) {
      bucket[(int) num] += freq;
    } else {
      insertNonRecursive(num, freq);
    }
  }

  public List<Long> findResultIndex(long K1, long K2) {
    List<Long> result = new ArrayList<>(8);
    long sum = 0;

    if (isBucket) {
      for (int i = 0; i < (1 << bucketBits); i++) {
        sum += bucket[i];
        if (sum >= K1 && result.size() == 0) {
          result.add((long) i);
          result.add(sum - bucket[i]);
        }
        if (sum >= K2 && result.size() == 2) {
          result.add((long) i);
          result.add(sum - bucket[i]);
          break;
        }
      }
    } else {
      tmpQueueLength = 0;
      dfs(root);
      for (int i = 1; i <= tmpQueueLength; i++) {
        //      System.out.println(count[tmpQueue[i]] + "  " + value[tmpQueue[i]]);
        sum += count[tmpQueue[i]];
        if (sum >= K1 && result.size() == 0) {
          result.add(value[tmpQueue[i]]);
          result.add(sum - count[tmpQueue[i]]);
        }
        if (sum >= K2 && result.size() == 2) {
          result.add(value[tmpQueue[i]]);
          result.add(sum - count[tmpQueue[i]]);
          break;
        }
      }
    }
    return result;
  }

  public int getRemainingBits() {
    return remainingBits;
  }

  public void reset(int bits, int minBits) {
    bitsOfValue = remainingBits = bits;
    this.minBits = minBits;
    if (bits <= minBits) {
      isBucket = true;
      resetBucket();
    } else {
      isBucket = false;
      trashTop = maxSize;
      for (int i = 1; i <= maxSize; i++) trashStack[i] = maxSize - i + 1;
      root = 0;
      Arrays.fill(lc, 0);
      Arrays.fill(rc, 0);
    }
  }
}
