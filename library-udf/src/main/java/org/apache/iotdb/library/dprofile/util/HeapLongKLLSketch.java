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
    N = 0;
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
    for (int i = 0; i < setLevel + 1; i++) tmpArr[i] = i < cntLevel ? levelPos[i] : maxPos;
    levelPos = tmpArr;
    cntLevel = setLevel;
    levelMaxSize = new int[cntLevel];
    int newK = 0;
    for (int addK = 1 << 28; addK > 0; addK >>>= 1) { // find a new K to fit the memory limit.
      int need = 0;
      for (int i = 0; i < cntLevel; i++)
        need +=
            Math.max(8, (int) Math.round(((newK + addK) * Math.pow(2.0 / 3, cntLevel - i - 1))));
      if (need <= maxMemoryNum) newK += addK;
    }
    for (int i = 0; i < cntLevel; i++)
      levelMaxSize[i] = Math.max(8, (int) Math.round((newK * Math.pow(2.0 / 3, cntLevel - i - 1))));
    //    show();
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(N);
    sb.append(levelMaxSize[cntLevel - 1]);
    sb.append(cntLevel);
    sb.append((levelPos[cntLevel] - levelPos[0]));
    for (int i = 0; i < cntLevel; i++) sb.append(levelPos[i]);
    return sb.toString();
  }

  public void showLevelMaxSize() {
    int numLEN = levelPos[cntLevel] - levelPos[0];
    System.out.println("\t\t//maxMemNum:" + maxMemoryNum + "\t//N:" + N);
    for (int i = 0; i < cntLevel; i++) System.out.print("\t\t" + levelMaxSize[i] + "\t");
    System.out.println();
    System.out.println("-------------------------------------------------------");
  }

  private static int mergeSort(
      long[] a1, int L1, int R1, long[] a2, int L2, int R2, long[] a3, int pos) {
    if (L1 == R1) System.arraycopy(a2, L2, a3, pos, R2 - L2);
    else if (L2 == R2) System.arraycopy(a1, L1, a3, pos, R1 - L1);
    else {
      int p1 = L1, p2 = L2;
      while (p1 < R1 || p2 < R2)
        if (p1 < R1 && (p2 == R2 || a1[p1] < a2[p2])) a3[pos++] = a1[p1++];
        else a3[pos++] = a2[p2++];
    }
    return R1 - L1 + R2 - L2;
  }

  private void compactOneLevel(int level) { // compact half of data when numToReduce is small
    if (level == cntLevel - 1) calcLevelMaxSize(cntLevel + 1);
    int L1 = levelPos[level], R1 = levelPos[level + 1]; // [L,R)
    //    System.out.println("T_T\t"+(R1-L1));
    if (level == 0 && !level0Sorted) {
      Arrays.sort(num, L1, R1);
      level0Sorted = true;
    }
    L1 += (R1 - L1) & 1;
    if (L1 == R1) return;

    randomlyHalveDownToLeft(L1, R1);

    int mid = (L1 + R1) >>> 1;
    mergeSortWithoutSpace(L1, mid, levelPos[level + 1], levelPos[level + 2]);
    levelPos[level + 1] = mid;
    int newP = levelPos[level + 1] - 1, oldP = L1 - 1;
    for (int i = oldP; i >= levelPos[0]; i--) num[newP--] = num[oldP--];

    levelPos[level] = levelPos[level + 1] - (L1 - levelPos[level]);
    int numReduced = (R1 - L1) >>> 1;
    for (int i = level - 1; i >= 0; i--) levelPos[i] += numReduced;
  }

  @Override
  public void compact() {
    int compactLevel = cntLevel - 1;
    double mxRate = 0;
    for (int i = 0; i < cntLevel; i++)
      if (levelPos[i + 1] - levelPos[i] > levelMaxSize[i]) {
        //      double rate = 1.0*(levelPos[i+1]-levelPos[i])/Math.pow(2,i);
        //      if(rate>mxRate&&(i<cntLevel-1 || mxRate==0)) {
        compactLevel = i;
        //        mxRate=rate;
        //      }
        //      break;
      }

    compactOneLevel(compactLevel);
  }

  public void merge(org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another) {
    //    System.out.println("[MERGE]");
    //    show();
    //    another.show();
    if (another.cntLevel > cntLevel) calcLevelMaxSize(another.cntLevel);
    for (int i = 0; i < another.cntLevel; i++) {
      int numToMerge = another.levelPos[i + 1] - another.levelPos[i];
      if (numToMerge == 0) continue;
      int mergingL = another.levelPos[i];
      while (numToMerge > 0) {
        //        System.out.println("\t\t"+levelPos[0]);show();showLevelMaxSize();
        if (levelPos[0] == 0) compact();
        //        if(levelPos[0]==0){
        //          show();
        //          showLevelMaxSize();
        //        }
        int delta = Math.min(numToMerge, levelPos[0]);
        if (i > 0) { // move to give space for level i
          for (int j = 0; j < i; j++) levelPos[j] -= delta;
          System.arraycopy(num, delta, num, 0, levelPos[i] - delta);
        }
        System.arraycopy(another.num, mergingL, num, levelPos[i] - delta, delta);
        levelPos[i] -= delta;
        numToMerge -= delta;
        mergingL += delta;
      }
    }
    this.N += another.N;
    //    System.out.println("[MERGE result]");
    //    show();
    //    System.out.println();
  }

  public void mergeWithTempSpace(
      org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another) {
    //    System.out.println("[MERGE]");
    //    show();
    //    another.show();
    int[] oldLevelPos = levelPos;
    int oldCntLevel = cntLevel;
    calcLevelMaxSize(Math.max(cntLevel, another.cntLevel));
    if (getNumLen() + another.getNumLen() <= maxMemoryNum) {
      int cntPos = oldLevelPos[0] - another.getNumLen();
      for (int i = 0; i < cntLevel; i++) {
        int tmpL = cntPos;
        //        if(i<oldCntLevel)
        //
        // System.out.println("\t\t\t\t"+tmpL+"\t\t"+levelPos[i]+"\t"+oldLevelPos[i]+"---"+oldLevelPos[i+1]);
        //        levelPos[i] = tmpL;
        if (i < oldCntLevel && i < another.cntLevel)
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
        else if (i < oldCntLevel)
          cntPos +=
              mergeSort(num, oldLevelPos[i], oldLevelPos[i + 1], another.num, 0, 0, num, cntPos);
        else if (i < another.cntLevel)
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
        levelPos[i] = tmpL;
      }
      levelPos[cntLevel] = cntPos;
      this.N += another.N;
    } else {
      long[] oldNum = num;
      num = new long[getNumLen() + another.getNumLen()];
      int numLen = 0;
      for (int i = 0; i < cntLevel; i++) {
        int tmpL = numLen;
        if (i < oldCntLevel && i < another.cntLevel)
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
        else if (i < oldCntLevel)
          numLen +=
              mergeSort(oldNum, oldLevelPos[i], oldLevelPos[i + 1], another.num, 0, 0, num, numLen);
        else if (i < another.cntLevel)
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
        levelPos[i] = tmpL;
      }
      levelPos[cntLevel] = numLen;
      //    System.out.println("-------------------------------.............---------");
      //    show();
      while (getNumLen() > maxMemoryNum) compact();
      //    show();
      //    System.out.println("\t\t??\t\t"+Arrays.toString(num));
      int newPos0 = maxMemoryNum - getNumLen();
      System.arraycopy(num, levelPos[0], oldNum, newPos0, getNumLen());
      for (int i = cntLevel; i >= 0; i--) levelPos[i] = levelPos[i] - levelPos[0] + newPos0;
      num = oldNum;
      this.N += another.N;
    }
    //    System.out.println("\t\t??\t\t"+Arrays.toString(num));
    //    System.out.println("\t\t??\t\t"+Arrays.toString(levelPos));
    //    System.out.println("-------------------------------.............---------");
    //    System.out.println("[MERGE result]");
    //    show();
    //    System.out.println();
  }

  public void mergeWithTempSpace(
      List<org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile> otherList) {
    //    System.out.println("[MERGE]");
    //    show();
    //
    // System.out.println("[mergeWithTempSpace]\t???\t"+num.length+"\t??\t"+cntLevel+"\t??\toldPos0:"+levelPos[0]);
    //    System.out.println("[mergeWithTempSpace]\t???\tmaxMemNum:"+maxMemoryNum);
    //    another.show();
    int[] oldLevelPos = Arrays.copyOf(levelPos, cntLevel + 1);
    int oldCntLevel = cntLevel;
    int otherNumLen = 0;
    long otherN = 0;
    //    System.out.print("\t\t\t\t[mergeWithTempSpace] others:");
    for (org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another : otherList)
      if (another != null) {
        //      System.out.print("\t"+another.getN());
        if (another.cntLevel > cntLevel) calcLevelMaxSize(another.cntLevel);
        otherNumLen += another.getNumLen();
        otherN += another.getN();
      }
    //    System.out.println();
    //    System.out.println("[mergeWithTempSpace]\totherNumLen:"+otherNumLen);
    if (getNumLen() + otherNumLen <= maxMemoryNum) {
      int cntPos = oldLevelPos[0] - otherNumLen;
      for (int i = 0; i < cntLevel; i++) {
        levelPos[i] = cntPos;
        if (i < oldCntLevel) {
          System.arraycopy(num, oldLevelPos[i], num, cntPos, oldLevelPos[i + 1] - oldLevelPos[i]);
          cntPos += oldLevelPos[i + 1] - oldLevelPos[i];
        }
        for (org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another : otherList)
          if (another != null && i < another.cntLevel) {
            System.arraycopy(
                another.num, another.levelPos[i], num, cntPos, another.getLevelSize(i));
            cntPos += another.getLevelSize(i);
          }
        Arrays.sort(num, levelPos[i], cntPos);
        //        System.out.println("\t\t!!\t"+cntPos);
      }
      levelPos[cntLevel] = cntPos;
      this.N += otherN;
    } else {
      long[] oldNum = num;
      num = new long[getNumLen() + otherNumLen];
      //      System.out.println("\t\t\t\ttmp_num:"+num.length+"
      // old_num:"+levelPos[0]+"..."+levelPos[oldCntLevel]);
      int numLen = 0;
      for (int i = 0; i < cntLevel; i++) {
        levelPos[i] = numLen;
        if (i < oldCntLevel) {
          //          System.out.println("\t\t\tlv"+i+"\toldPos:"+oldLevelPos[i]+"\t"+numLen+"
          // this_level_old_len:"+(oldLevelPos[i + 1] - oldLevelPos[i]));
          //          System.out.println("\t\t\t"+oldNum[oldLevelPos[i + 1]-1]);
          System.arraycopy(
              oldNum, oldLevelPos[i], num, numLen, oldLevelPos[i + 1] - oldLevelPos[i]);
          numLen += oldLevelPos[i + 1] - oldLevelPos[i];
        }
        for (org.apache.iotdb.library.dprofile.util.KLLSketchForQuantile another : otherList)
          if (another != null && i < another.cntLevel) {
            System.arraycopy(
                another.num, another.levelPos[i], num, numLen, another.getLevelSize(i));
            numLen += another.getLevelSize(i);
          }
        Arrays.sort(num, levelPos[i], numLen);
      }
      levelPos[cntLevel] = numLen;
      this.N += otherN;
      //    System.out.println("-------------------------------.............---------");
      //      show();System.out.println("\t?\t"+levelPos[0]);
      while (getNumLen() > maxMemoryNum) compact();
      //      show();System.out.println("\t?\t"+levelPos[0]);
      //    System.out.println("\t\t??\t\t"+Arrays.toString(num));
      int newPos0 = maxMemoryNum - getNumLen();
      System.arraycopy(num, levelPos[0], oldNum, newPos0, getNumLen());
      for (int i = cntLevel; i >= 0; i--) levelPos[i] += newPos0 - levelPos[0];
      num = oldNum;
    }
    //    System.out.println("\t\t??\t\t"+Arrays.toString(num));
    //    System.out.println("\t\t??\t\t"+Arrays.toString(levelPos));
    //    System.out.println("-------------------------------.............---------");
    //    System.out.println("[MERGE result]");
    //    show();
    //    System.out.println();
  }
}
