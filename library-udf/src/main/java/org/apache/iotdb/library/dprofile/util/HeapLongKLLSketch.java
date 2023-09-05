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
import java.util.List;

// based on KLL Sketch in DataSketch. See
// https://github.com/apache/datasketches-java/tree/master/src/main/java/org/apache/datasketches/kll
// This is an implementation for long data type.
// works only in heap memory. don't need to serialize or deserialize
public class HeapLongKLLSketch extends org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile {

  public HeapLongKLLSketch(int maxMemoryByte) { // maxN=7000 for PAGE, 1.6e6 for CHUNK
    n = 0;
    calcParameters(maxMemoryByte);
    calcLevelMaxSize(1);
  }

  private void calcParameters(int maxMemoryByte) {
    maxMemoryNum = calcMaxMemoryNum(maxMemoryByte);
    num = new long[maxMemoryNum];
    level0Sorted = false;
    cntLevel = 0;
  }

  @Override
  protected int calcMaxMemoryNum(int maxMemoryByte) {
    return Math.min(1 << 20, maxMemoryByte / 8);
  }

  @Override
  protected void calcLevelMaxSize(int setLevel) { // set cntLevel.  cntLevel won't decrease
    int[] tmpArr = new int[setLevel + 1];
    int maxPos = cntLevel > 0 ? Math.max(maxMemoryNum, levelPos[cntLevel]) : maxMemoryNum;
    for (int i = 0; i < setLevel + 1; i++) {
      tmpArr[i] = i < cntLevel ? levelPos[i] : maxPos;
    }
    levelPos = tmpArr;
    cntLevel = setLevel;
    levelMaxSize = new int[cntLevel];
    int newK = 0;
    for (int addK = 1 << 28; addK > 0; addK >>>= 1) { // find a new K to fit the memory limit.
      int need = 0;
      for (int i = 0; i < cntLevel; i++) {
        need +=
            Math.max(8, (int) Math.round(((newK + addK) * Math.pow(2.0 / 3, cntLevel - i - 1.0d))));
      }
      if (need <= maxMemoryNum) {
        newK += addK;
      }
    }
    for (int i = 0; i < cntLevel; i++) {
      levelMaxSize[i] =
          Math.max(8, (int) Math.round((newK * Math.pow(2.0 / 3, cntLevel - i - 1.0d))));
    }
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(n);
    sb.append(levelMaxSize[cntLevel - 1]);
    sb.append(cntLevel);
    sb.append((levelPos[cntLevel] - levelPos[0]));
    for (int i = 0; i < cntLevel; i++) {
      sb.append(levelPos[i]);
    }
    return sb.toString();
  }

  private static int mergeSort(
      long[] a1, int l1, int r1, long[] a2, int l2, int r2, long[] a3, int pos) {
    if (l1 == r1) {
      System.arraycopy(a2, l2, a3, pos, r2 - l2);
    } else if (l2 == r2) {
      System.arraycopy(a1, l1, a3, pos, r1 - l1);
    } else {
      int p1 = l1;
      int p2 = l2;
      while (p1 < r1 || p2 < r2) {
        if (p1 < r1 && (p2 == r2 || a1[p1] < a2[p2])) {
          a3[pos++] = a1[p1++];
        } else {
          a3[pos++] = a2[p2++];
        }
      }
    }
    return r1 - l1 + r2 - l2;
  }

  private void compactOneLevel(int level) { // compact half of data when numToReduce is small
    if (level == cntLevel - 1) {
      calcLevelMaxSize(cntLevel + 1);
    }
    int l1 = levelPos[level];
    int r1 = levelPos[level + 1]; // [L,R)
    if (level == 0 && !level0Sorted) {
      Arrays.sort(num, l1, r1);
      level0Sorted = true;
    }
    l1 += (r1 - l1) & 1;
    if (l1 == r1) {
      return;
    }

    randomlyHalveDownToLeft(l1, r1);

    int mid = (l1 + r1) >>> 1;
    mergeSortWithoutSpace(l1, mid, levelPos[level + 1], levelPos[level + 2]);
    levelPos[level + 1] = mid;
    int newP = levelPos[level + 1] - 1;
    int oldP = l1 - 1;
    for (int i = oldP; i >= levelPos[0]; i--) {
      num[newP--] = num[oldP--];
    }

    levelPos[level] = levelPos[level + 1] - (l1 - levelPos[level]);
    int numReduced = (r1 - l1) >>> 1;
    for (int i = level - 1; i >= 0; i--) {
      levelPos[i] += numReduced;
    }
  }

  @Override
  public void compact() {
    int compactLevel = cntLevel - 1;
    for (int i = 0; i < cntLevel; i++) {
      if (levelPos[i + 1] - levelPos[i] > levelMaxSize[i]) {
        compactLevel = i;
      }
    }

    compactOneLevel(compactLevel);
  }

  public void merge(org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another) {
    if (another.cntLevel > cntLevel) {
      calcLevelMaxSize(another.cntLevel);
    }
    for (int i = 0; i < another.cntLevel; i++) {
      int numToMerge = another.levelPos[i + 1] - another.levelPos[i];
      if (numToMerge == 0) {
        continue;
      }
      int mergingL = another.levelPos[i];
      while (numToMerge > 0) {
        if (levelPos[0] == 0) {
          compact();
        }
        int delta = Math.min(numToMerge, levelPos[0]);
        if (i > 0) { // move to give space for level i
          for (int j = 0; j < i; j++) {
            levelPos[j] -= delta;
          }
          System.arraycopy(num, delta, num, 0, levelPos[i] - delta);
        }
        System.arraycopy(another.num, mergingL, num, levelPos[i] - delta, delta);
        levelPos[i] -= delta;
        numToMerge -= delta;
        mergingL += delta;
      }
    }
    this.n += another.n;
  }

  public void mergeWithTempSpace(
      org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another) {
    int[] oldLevelPos = levelPos;
    int oldCntLevel = cntLevel;
    calcLevelMaxSize(Math.max(cntLevel, another.cntLevel));
    if (getNumLen() + another.getNumLen() <= maxMemoryNum) {
      int cntPos = oldLevelPos[0] - another.getNumLen();
      for (int i = 0; i < cntLevel; i++) {
        int tmpL = cntPos;
        if (i < oldCntLevel && i < another.cntLevel) {
          cntPos +=
              mergeSort(
                  num,
                  oldLevelPos[i],
                  oldLevelPos[i + 1],
                  another.num,
                  another.levelPos[i],
                  another.levelPos[i + 1],
                  num,
                  cntPos);
        } else if (i < oldCntLevel) {
          cntPos +=
              mergeSort(num, oldLevelPos[i], oldLevelPos[i + 1], another.num, 0, 0, num, cntPos);
        } else if (i < another.cntLevel) {
          cntPos +=
              mergeSort(
                  num,
                  0,
                  0,
                  another.num,
                  another.levelPos[i],
                  another.levelPos[i + 1],
                  num,
                  cntPos);
        }
        levelPos[i] = tmpL;
      }
      levelPos[cntLevel] = cntPos;
      this.n += another.n;
    } else {
      long[] oldNum = num;
      num = new long[getNumLen() + another.getNumLen()];
      int numLen = 0;
      for (int i = 0; i < cntLevel; i++) {
        int tmpL = numLen;
        if (i < oldCntLevel && i < another.cntLevel) {
          numLen +=
              mergeSort(
                  oldNum,
                  oldLevelPos[i],
                  oldLevelPos[i + 1],
                  another.num,
                  another.levelPos[i],
                  another.levelPos[i + 1],
                  num,
                  numLen);
        } else if (i < oldCntLevel) {
          numLen +=
              mergeSort(oldNum, oldLevelPos[i], oldLevelPos[i + 1], another.num, 0, 0, num, numLen);
        } else if (i < another.cntLevel) {
          numLen +=
              mergeSort(
                  oldNum,
                  0,
                  0,
                  another.num,
                  another.levelPos[i],
                  another.levelPos[i + 1],
                  num,
                  numLen);
        }
        levelPos[i] = tmpL;
      }
      levelPos[cntLevel] = numLen;
      while (getNumLen() > maxMemoryNum) {
        compact();
      }
      int newPos0 = maxMemoryNum - getNumLen();
      System.arraycopy(num, levelPos[0], oldNum, newPos0, getNumLen());
      for (int i = cntLevel; i >= 0; i--) {
        levelPos[i] = levelPos[i] - levelPos[0] + newPos0;
      }
      num = oldNum;
      this.n += another.n;
    }
  }

  public void mergeWithTempSpace(
      List<org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile> otherList) {
    int[] oldLevelPos = Arrays.copyOf(levelPos, cntLevel + 1);
    int oldCntLevel = cntLevel;
    int otherNumLen = 0;
    long otherN = 0;
    for (org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another : otherList) {
      if (another != null) {
        if (another.cntLevel > cntLevel) {
          calcLevelMaxSize(another.cntLevel);
        }
        otherNumLen += another.getNumLen();
        otherN += another.getN();
      }
    }
    if (getNumLen() + otherNumLen <= maxMemoryNum) {
      int cntPos = oldLevelPos[0] - otherNumLen;
      for (int i = 0; i < cntLevel; i++) {
        levelPos[i] = cntPos;
        if (i < oldCntLevel) {
          System.arraycopy(num, oldLevelPos[i], num, cntPos, oldLevelPos[i + 1] - oldLevelPos[i]);
          cntPos += oldLevelPos[i + 1] - oldLevelPos[i];
        }
        for (org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another : otherList) {
          if (another != null && i < another.cntLevel) {
            System.arraycopy(
                another.num, another.levelPos[i], num, cntPos, another.getLevelSize(i));
            cntPos += another.getLevelSize(i);
          }
        }
        Arrays.sort(num, levelPos[i], cntPos);
      }
      levelPos[cntLevel] = cntPos;
      this.n += otherN;
    } else {
      long[] oldNum = num;
      num = new long[getNumLen() + otherNumLen];
      int numLen = 0;
      for (int i = 0; i < cntLevel; i++) {
        levelPos[i] = numLen;
        if (i < oldCntLevel) {
          int length = oldLevelPos[i + 1] - oldLevelPos[i];
          System.arraycopy(oldNum, oldLevelPos[i], num, numLen, length);
          numLen += length;
        }
        for (org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another : otherList) {
          if (another != null && i < another.cntLevel) {
            System.arraycopy(
                another.num, another.levelPos[i], num, numLen, another.getLevelSize(i));
            numLen += another.getLevelSize(i);
          }
        }
        Arrays.sort(num, levelPos[i], numLen);
      }
      levelPos[cntLevel] = numLen;
      this.n += otherN;
      while (getNumLen() > maxMemoryNum) {
        compact();
      }
      int newPos0 = maxMemoryNum - getNumLen();
      System.arraycopy(num, levelPos[0], oldNum, newPos0, getNumLen());
      for (int i = cntLevel; i >= 0; i--) {
        levelPos[i] += newPos0 - levelPos[0];
      }
      num = oldNum;
    }
  }
}
