package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Arrays;

public class KLLDupliPairFaster extends KLLSketchForQuantile { // TODO: too large count
  IntArrayList compactionNumInLevel;
  public XoRoShiRo128PlusRandom randomForReserve = new XoRoShiRo128PlusRandom();
  long MIN_V = Long.MAX_VALUE, MAX_V = Long.MIN_VALUE;
  int[] levelPosForPairs, levelActualSize;
  int bufferPosForLV0;
  int PairIndexBit, PairCountBit;
  static final long mask32From64_0 = 0xFFFFFFFF00000000L, mask32From64_1 = 0xFFFFFFFFL;
  static int maskIndexInPair, maskCountInPair;
  // bufferPosForLV0 ... levelPos[0]-1: unordered arrivals
  // levelPos[i] ... levelPosForPair[i]-1 : ordered & paired.
  // levelPosForPair[i] ... levelPos[i+1] : pair info for level i.
  int AmortizedRatioForBuffer = 2;

  public KLLDupliPairFaster(int maxMemoryByte) {
    N = 0;
    calcParameters(maxMemoryByte);
    calcLevelMaxSize(1);
    bufferPosForLV0 = levelPos[0];
  }

  @Override
  public void showNum() {
    for (int i = 0; i < cntLevel; i++) {
      System.out.print("\t|");
      if (i == 0 && bufferPosForLV0 < levelPos[0]) {
        System.out.print("\t{{BUFFER ");
        for (int j = bufferPosForLV0; j < levelPos[0]; j++)
          System.out.print(longToResult(num[j]) + ", ");
        System.out.print("}}\t");
      }
      for (int j = levelPos[i]; j < levelPosForPairs[i]; j++)
        //        System.out.print(num[j]+",");
        System.out.print(longToResult(num[j]) + ", ");
      for (int j = 0; j < getPairsInLevel(i); j++) {
        int IC = getIndexCountInNum(num, levelPosForPairs[i], j);
        System.out.print("<" + (calcIndex(IC) - 1) + " " + calcCount(IC) + ">, ");
      }

      System.out.print("|\t");
    }
    System.out.println();
  }

  @Override
  public void update(long x) { // signed long
    if (bufferPosForLV0 <= 1) { // TODO: 像pairFt那样写临时空间，现在是因为compact占更多一个空间而
      boolean needCompaction = processLV0();
      int targetSketchSize =
          levelPos[cntLevel]
              - Math.max(MinimumLevelMaxSize, getLevelSize(0) / AmortizedRatioForBuffer)
              - 1;
      if (needCompaction) {
        compact();
        //        showNum();
        while (getNumLen() >= targetSketchSize) {
          compact();
          //          showNum();
        }
      }
    }
    num[--bufferPosForLV0] = x;
    N++;
    levelActualSize[0]++;
  }

  private void calcParameters(int maxMemoryByte) {
    maxMemoryNum = calcMaxMemoryNum(maxMemoryByte);
    num = new long[maxMemoryNum];
    level0Sorted = false;
    cntLevel = 0;
    compactionNumInLevel = new IntArrayList();

    PairIndexBit = 1;
    while ((1 << PairIndexBit) < maxMemoryNum) PairIndexBit++;
    PairCountBit = 32 - PairIndexBit;
    //    System.out.println("\t\tmaxMemNum:"+maxMemoryNum+"\t\tPairIndexBit:"+PairIndexBit);

    maskIndexInPair = ((1 << PairIndexBit) - 1) << PairCountBit;
    maskCountInPair = (1 << PairCountBit) - 1;
  }

  private int calcIndexCount(int index, int count) {
    if (count > maskCountInPair)
      System.out.println(
          "\t[ERR KLLPairFast] too large count." + "\t" + count + ">" + maskCountInPair);
    return (index << PairCountBit) | count;
  }

  private int calcIndex(int indexCount) {
    return (indexCount & maskIndexInPair) >>> PairCountBit;
  }

  private int calcCount(int indexCount) {
    return (indexCount & maskCountInPair);
  }

  /** pairID:>=0 */
  private int getIndexCountInNum(long[] num, int posForPairsInNum, int pairID) {
    //    System.out.println("\n[getting][posForPairsInNum]:"+posForPairsInNum+"\tpairID:"+pairID);
    int indexCount =
        (pairID & 1) == 0
            ? (int) ((num[posForPairsInNum + (pairID >> 1)] & mask32From64_0) >>> 32)
            : (int) (num[posForPairsInNum + (pairID >> 1)] & mask32From64_1);
    //    System.out.println("\t\t\t\t\t\t\t\t\t\tic:"+indexCount);
    return indexCount;
  }
  /** SomeIndex:>=0 */
  private boolean checkSomeIndexIsNextPair(int level, int nextPairID, int SomeIndex) {
    int locatedPairPosInNum = levelPosForPairs[level] + nextPairID / 2;
    return locatedPairPosInNum < levelPos[level + 1]
        && calcIndex(getIndexCountInNum(num, levelPosForPairs[level], nextPairID)) - 1 == SomeIndex;
    // empty in num_pairIndex will be 0-1=-1, always != cntIndex.
  }
  /** SomeIndex:>=0 */
  private boolean checkSomeIndexIsNextPair(
      long[] num, int posForPairsInNum, int nextPairID, int SomeIndex) {
    int locatedPairPosInNum = posForPairsInNum + nextPairID / 2;
    return locatedPairPosInNum < num.length
        && calcIndex(getIndexCountInNum(num, posForPairsInNum, nextPairID)) - 1 == SomeIndex;
  }

  @Override
  protected int calcMaxMemoryNum(int maxMemoryByte) {
    return Math.min(1 << 30, maxMemoryByte / 8);
  }

  @Override
  protected void calcLevelMaxSize(
      int setLevel) { // set cntLevel.  make sure cntLevel won't decrease
    int[] tmpArr = new int[setLevel + 1];
    int[] tmpArrPair = new int[setLevel + 1];
    int[] tmpArrActualSize = new int[setLevel + 1];
    int maxPos = cntLevel > 0 ? Math.max(maxMemoryNum, levelPos[cntLevel]) : maxMemoryNum;
    for (int i = 0; i < setLevel + 1; i++) {
      tmpArr[i] = i < cntLevel ? levelPos[i] : maxPos;
      tmpArrPair[i] = i < cntLevel ? levelPosForPairs[i] : maxPos;
      tmpArrActualSize[i] = i < cntLevel ? levelActualSize[i] : 0;
    }
    levelPos = tmpArr;
    levelPosForPairs = tmpArrPair;
    levelActualSize = tmpArrActualSize;
    for (int i = cntLevel; i < setLevel; i++) {
      compactionNumInLevel.add(0);
    }
    cntLevel = setLevel;
    levelMaxSize = calcLevelMaxSizeByLevel(maxMemoryNum, cntLevel);
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

  void setIndexCountInNum(long[] num, int levelPosForPair, int pairID, int IndexCount) {
    //    System.out.println("\t\tsetting IndexCount:
    // \t"+"pairID:"+pair+"\t\t"+calcIndex(IndexCount)+","+calcCount(IndexCount));
    int pairPosInNum = levelPosForPair + pairID / 2;
    //    System.out.println("[pairPosInNum]"+pairPosInNum);
    if ((pairID & 1) == 0)
      num[pairPosInNum] = (num[pairPosInNum] & mask32From64_1) | ((long) IndexCount << 32);
    else num[pairPosInNum] = (num[pairPosInNum] & mask32From64_0) | (IndexCount & mask32From64_1);
  }

  private long mergeTwoIndexCountIntoLong(int IC1, int IC2) {
    return ((long) IC1 << 32L) | (IC2 & mask32From64_1);
  }

  private void processLVNoExistingPair(int LV) { // lvPos[LV]...lvPos[LV]-1 are ordered num
    int cntIndex = 1, cntLVPos = levelPos[LV], nextLVPos = levelPos[LV + 1];
    long cntV, lastV = Long.MAX_VALUE;
    int cntCount;
    IntArrayList IndexCountList = new IntArrayList();
    for (int i = cntLVPos + 1; i < nextLVPos; i++) {
      cntV = num[i];
      if (cntV == num[i - 1]) {
        cntCount = 2;
        while (i + 1 < nextLVPos && num[i + 1] == cntV) {
          i++;
          cntCount++;
        }
        IndexCountList.add(calcIndexCount(cntIndex - 1 + 1, cntCount));
      } else {
        num[cntLVPos + cntIndex] = cntV;
        cntIndex += 1;
      }
    }
    if (IndexCountList.size() > 0) {
      int posForPairs = ((IndexCountList.size() + 1) >>> 1);
      int delta = nextLVPos - cntLVPos - cntIndex - posForPairs;
      if (delta > 0) {
        for (int i = cntLVPos + cntIndex - 1 + delta; i >= bufferPosForLV0 + delta; i--)
          num[i] = num[i - delta];
        bufferPosForLV0 += delta;
        for (int i = 0; i < LV; i++) {
          levelPos[i] += delta;
          levelPosForPairs[i] += delta;
        }
        levelPos[LV] += delta;
      }
      IndexCountList.add(0);
      for (int i = nextLVPos - posForPairs, pairsID = 0; i < nextLVPos; i++, pairsID += 2)
        num[i] =
            mergeTwoIndexCountIntoLong(
                IndexCountList.getInt(pairsID), IndexCountList.getInt(pairsID + 1));
      levelPosForPairs[LV] = nextLVPos - posForPairs;
    }
  }

  private void getExistingPairsInLevel(
      int nowLVPos,
      int nowLVPairPos,
      int nextLVPos,
      LongArrayList existingPairValues,
      IntArrayList existingPairCounts) {
    int tmpIC, tmpIndex, tmpCount;
    for (int existingPairID = 0, i = nowLVPairPos;
        i < nextLVPos;
        i += existingPairID & 1, existingPairID++) {
      tmpIC =
          (existingPairID & 1) == 0
              ? (int) ((num[i] & mask32From64_0) >>> 32)
              : (int) (num[i] & mask32From64_1);
      tmpIndex = (tmpIC & maskIndexInPair) >>> PairCountBit;
      if (tmpIndex == 0) break;
      tmpCount = tmpIC & maskCountInPair;
      //
      // System.out.println("\t\t\texistingPairID:\t"+existingPairID+"\t\ttmpIC:\t"+tmpIC+"\t\tI,C:\t"+tmpIndex+","+tmpCount+"\t\tV:\t"+longToResult(num[nowLVPos+(tmpIndex-1)]));
      existingPairValues.add(num[nowLVPos + (tmpIndex - 1)]);
      existingPairCounts.add(tmpCount);
    }
  }

  /**
   * numbers num[nowLVPos,...,nowLVPairPos-1] are ordered but may duplicate; merge with existing
   * pairs (value,count)
   *
   * @return (1) int value: index of last num after deduplicate; (2) numICList: IndexCount list of
   *     merged result.
   */
  private int processPairsInNumWithExistingPairs(
      int nowLVPos,
      int nowLVPairPos,
      LongArrayList existingPairValues,
      IntArrayList existingPairCounts,
      IntArrayList numICList) {
    int cntNumIndex = 0;
    long existingPairCntValue;
    int existingPairCntCount;
    int cntNumPos = nowLVPos, cntNumCount;
    long cntNumValue = num[cntNumPos];
    existingPairValues.add(Long.MAX_VALUE);
    existingPairCounts.add(-233);
    for (int i = 0; i < existingPairValues.size(); i++) {
      existingPairCntValue = existingPairValues.getLong(i);
      existingPairCntCount = existingPairCounts.getInt(i);
      while (cntNumPos < nowLVPairPos && (cntNumValue = num[cntNumPos]) < existingPairCntValue) {
        if (cntNumPos + 1 < nowLVPairPos && num[cntNumPos + 1] == cntNumValue) {
          cntNumCount = 1;
          while (cntNumPos + 1 < nowLVPairPos && num[cntNumPos + 1] == cntNumValue) {
            cntNumPos++;
            cntNumCount++;
          }
          numICList.add(calcIndexCount(cntNumIndex + 1, cntNumCount));
        }
        num[nowLVPos + cntNumIndex] = cntNumValue;
        cntNumIndex++;
        cntNumPos++;
      }
      if (existingPairCntCount == -233) break;
      cntNumCount = existingPairCntCount;
      if (cntNumValue != existingPairCntValue)
        System.out.println(
            "\t\t[ERR PairFast].\t\tcntV:"
                + longToResult(cntNumValue)
                + "\t\tcntPairV:\t"
                + longToResult(existingPairCntValue));
      while (cntNumPos + 1 < nowLVPairPos && num[cntNumPos + 1] == existingPairCntValue) {
        cntNumPos++;
        cntNumCount++;
      }
      numICList.add(calcIndexCount(cntNumIndex + 1, cntNumCount));
      num[nowLVPos + cntNumIndex] = cntNumValue;
      cntNumIndex++;
      cntNumPos++;
    }
    return cntNumIndex;
  }

  /**
   * input: num[nowLVPos,nowLVPos+cntNumIndex) are unique num.
   *
   * @return delta. should move numbers below level LV. may be > 0 (move to right) or < 0 (move to
   *     left)
   */
  private int setIndexCountsIntoNum(
      int LV, int nowLVPos, int cntNumIndex, int nextLVPos, IntArrayList numICList) {
    int posForPairs = ((numICList.size() + 1) >>> 1);
    int delta = nextLVPos - nowLVPos - cntNumIndex - posForPairs;
    if (delta > 0) { // move to right
      for (int i = nowLVPos + cntNumIndex - 1; i >= nowLVPos; i--) num[i + delta] = num[i];
    }
    if (delta < 0) { // move to left
      for (int i = nowLVPos; i < nowLVPos + cntNumIndex; i++) num[i + delta] = num[i];
    }
    levelPos[LV] += delta;
    if (LV == 0) bufferPosForLV0 = levelPos[LV];
    levelPosForPairs[LV] = nextLVPos - posForPairs;
    numICList.add(0);
    for (int i = levelPosForPairs[LV], pairsID = 0; i < nextLVPos; i++, pairsID += 2)
      num[i] = mergeTwoIndexCountIntoLong(numICList.getInt(pairsID), numICList.getInt(pairsID + 1));
    return delta;
  }
  /** return: need a compaction or not */
  private boolean processLV0() {
    if (bufferPosForLV0 == levelPos[0]) return true;

    if (levelPosForPairs[0] == levelPos[1]) {
      Arrays.sort(num, bufferPosForLV0, levelPos[1]);
      levelPos[0] = bufferPosForLV0;
      processLVNoExistingPair(0);
      //
      // System.out.println("\t\tprocessLV0NoExistingPair.\tposForPairs:\t"+(levelPos[1]-levelPosForPairs[0]));
      //      showNum();
      return bufferPosForLV0
          <= Math.max(MinimumLevelMaxSize, getLevelSize(0) / AmortizedRatioForBuffer);
    }

    LongArrayList existingPairValues = new LongArrayList();
    IntArrayList existingPairCounts = new IntArrayList();
    int nowLVPos = levelPos[0], nowLVPairPos = levelPosForPairs[0], nextLVPos = levelPos[1];
    getExistingPairsInLevel(
        nowLVPos, nowLVPairPos, nextLVPos, existingPairValues, existingPairCounts);

    Arrays.sort(num, bufferPosForLV0, nowLVPairPos);
    nowLVPos = levelPos[0] = bufferPosForLV0;

    IntArrayList numICList = new IntArrayList();
    int cntNumIndex =
        processPairsInNumWithExistingPairs(
            nowLVPos, nowLVPairPos, existingPairValues, existingPairCounts, numICList);
    setIndexCountsIntoNum(0, nowLVPos, cntNumIndex, nextLVPos, numICList);
    //
    // System.out.println("\t\tprocessLV0WithExistingPair.\tposForPairs:\t"+(levelPos[1]-levelPosForPairs[0]));
    //    showNum();
    return bufferPosForLV0
        <= Math.max(MinimumLevelMaxSize, getLevelSize(0) / AmortizedRatioForBuffer);
  }

  static int reservedAnItem = 0;
  static long reservedNum;
  /** @return int value: propagated numbers are in num[ lvPosForPair[LV], return_value ) */
  private int propagateNumInLevel(
      int LV,
      int nowLVPos,
      int nowLVPairPos,
      int nextLVPos,
      LongArrayList propagatedPairValues,
      IntArrayList propagatedPairCounts) {
    if ((levelActualSize[LV] & 1) == 1) { // reserve a minimal item
      reservedAnItem = 1;
      reservedNum = num[nowLVPos];
    } else reservedAnItem = 0;
    levelPosForPairs[LV] = nowLVPos;
    //    System.out.println("[propagateNumInLevel]\treservedAnItem:"+reservedAnItem);
    int rand01 = getNextRand01();
    int tmpIC, existingPairCntIndex, existingPairCntCount;
    long existingPairCntValue;
    int cntNumPos = nowLVPos, cntNumIndex = 0, propagatedCount;
    //    for(int
    // j=nowLVPos;j<nowLVPairPos;j++)System.out.print("\t\t\t"+longToResult(num[j]));System.out.println();
    for (int existingPairID = 0, i = nowLVPairPos;
        i < nextLVPos;
        i += existingPairID & 1, existingPairID++) {
      //      for(int
      // j=nowLVPos;j<nowLVPairPos;j++)System.out.print("\t\t\t"+longToResult(num[j]));System.out.println();
      tmpIC =
          (existingPairID & 1) == 0
              ? (int) ((num[i] & mask32From64_0) >>> 32)
              : (int) (num[i] & mask32From64_1);
      existingPairCntIndex = ((tmpIC & maskIndexInPair) >>> PairCountBit) - 1;
      if (existingPairCntIndex < 0) break;
      existingPairCntCount = tmpIC & maskCountInPair;
      existingPairCntValue = num[nowLVPos + existingPairCntIndex];
      //      System.out.println("\t\t\t\t\texistingPair
      // I,V,C:\t"+existingPairCntIndex+","+longToResult(existingPairCntValue)+","+existingPairCntCount+"\trand01:"+rand01);
      if (reservedAnItem == 1 && existingPairID == 0) {
        if (existingPairCntIndex == 0) {
          existingPairCntCount -= 1;
          if (existingPairCntCount
              <= rand01) { // first paired_number does not exist in the propagated num.
            cntNumPos++;
            cntNumIndex++;
            reservedAnItem = 0; // reserved.
            levelPosForPairs[LV] = nowLVPos + 1;
            rand01 = 0;
            continue;
          } // otherwise, must occur in the propagated num
        } else { // first num is single num.
          cntNumPos++;
          cntNumIndex++;
          reservedAnItem = 0; // reserved.
          levelPosForPairs[LV] = nowLVPos + 1;
          //          System.out.println("\t\t\t\t\tReserved. first num is single num.");
        }
      }
      // propagate rand01 of every 2 nums, whose index from (cntNumPos-nowLVPos) to
      // existingPairCntIndex-1.
      while ((cntNumPos - nowLVPos) + 1 < existingPairCntIndex) {
        num[nowLVPos + cntNumIndex] = num[cntNumPos + rand01];
        cntNumIndex++;
        cntNumPos += 2;
      }
      if ((cntNumPos - nowLVPos) + 1 == existingPairCntIndex) {
        if (rand01 == 0) {
          num[nowLVPos + cntNumIndex] = num[cntNumPos];
          cntNumIndex++;
          cntNumPos++;
          propagatedCount = (existingPairCntCount) >>> 1;
          num[nowLVPos + cntNumIndex] = existingPairCntValue;
          cntNumIndex++;
          cntNumPos++;
          rand01 ^= ((existingPairCntCount) & 1) ^ 1;
        } else {
          propagatedCount = (existingPairCntCount + 1) >>> 1;
          num[nowLVPos + cntNumIndex] = existingPairCntValue;
          cntNumIndex++;
          cntNumPos += 2;
          rand01 ^= ((existingPairCntCount) & 1) ^ 1;
        }
      } else {
        propagatedCount = (existingPairCntCount - rand01 + 1) >>> 1;
        num[nowLVPos + cntNumIndex] = existingPairCntValue;
        cntNumIndex++;
        cntNumPos++;
        rand01 ^= ((existingPairCntCount) & 1);
      }
      if (propagatedCount > 1) {
        propagatedPairValues.add(existingPairCntValue);
        propagatedPairCounts.add(propagatedCount);
      }
    }
    if (reservedAnItem == 1 && cntNumIndex == 0) {
      cntNumPos++;
      cntNumIndex++;
      reservedAnItem = 0; // reserved
      levelPosForPairs[LV] = nowLVPos + 1;
    }
    while (cntNumPos + rand01 < nowLVPairPos) {
      //
      // System.out.println("\t\t\t\t\t\tcntNumPos:"+cntNumPos+"\t\tnum:"+longToResult(num[cntNumPos]));
      num[nowLVPos + cntNumIndex] = num[cntNumPos + rand01];
      cntNumIndex++;
      cntNumPos += 2;
    }
    return nowLVPos + cntNumIndex;
  }

  private void unionPairs(
      LongArrayList values1,
      LongArrayList values2,
      IntArrayList counts1,
      IntArrayList counts2,
      LongArrayList values,
      IntArrayList counts) {
    //    System.out.println("\t\t\tunionPairs"+values1+values2);
    if (values1.size() == 0) {
      values.addAll(values2);
      counts.addAll(counts2);
      return;
    }
    if (values2.size() == 0) {
      values.addAll(values1);
      counts.addAll(counts1);
      return;
    }
    values1.add(Long.MAX_VALUE);
    int sz1 = values1.size(), sz2 = values2.size(), p1 = 0, p2 = 0;
    for (; p1 < sz1; p1++) {
      while (p2 < sz2 && values2.getLong(p2) < values1.getLong(p1)) {
        values.add(values2.getLong(p2));
        counts.add(counts2.getInt(p2));
        p2++;
      }
      if (p1 == sz1 - 1) break;
      values.add(values1.getLong(p1));
      if (p2 < sz2 && values2.getLong(p2) == values1.getLong(p1))
        counts.add(counts1.getInt(p1) + counts2.getInt(p2++) - 1);
      else counts.add(counts1.getInt(p1));
    }
  }

  /** shift numbers in level 0...belowLV-1 */
  private void shiftNum(int belowLV, int belowNumPos, int delta) {
    if (delta == 0 || belowLV == 0) return;
    if (delta > 0) for (int i = belowNumPos - 1; i >= bufferPosForLV0; i--) num[i + delta] = num[i];
    else for (int i = bufferPosForLV0; i < belowNumPos; i++) num[i + delta] = num[i];
    for (int i = 0; i < belowLV; i++) {
      levelPos[i] += delta;
      levelPosForPairs[i] += delta;
    }
    bufferPosForLV0 += delta;
  }

  private void compactOneLevelFaster(int LV) {
    //    System.out.println("\t\t[compactOneLevelFaster]\tLV="+LV);
    if (LV == cntLevel - 1) calcLevelMaxSize(cntLevel + 1);
    int nowLVPos = levelPos[LV], nowLVPairPos = levelPosForPairs[LV];
    addRecord(false, Long.MIN_VALUE, Long.MAX_VALUE, LV);
    LongArrayList propagatedPairValues = new LongArrayList(),
        nextLVPairValues = new LongArrayList(),
        unionPairValues = new LongArrayList();
    IntArrayList propagatedPairCounts = new IntArrayList(),
        nextLVPairCounts = new IntArrayList(),
        unionPairCounts = new IntArrayList();
    int L2 = levelPos[LV + 1];
    int R1 =
        propagateNumInLevel(
            LV, nowLVPos, nowLVPairPos, L2, propagatedPairValues, propagatedPairCounts);
    int L1 = levelPosForPairs[LV]; // propagated numbers are in num[ lvPairPos[LV], ... )
    int R2 = levelPosForPairs[LV + 1];

    getExistingPairsInLevel(
        levelPos[LV + 1],
        levelPosForPairs[LV + 1],
        levelPos[LV + 2],
        nextLVPairValues,
        nextLVPairCounts);
    unionPairs(
        propagatedPairValues,
        nextLVPairValues,
        propagatedPairCounts,
        nextLVPairCounts,
        unionPairValues,
        unionPairCounts);

    int freeSpaceInMid = L2 - R1,
        propagatedNumLen = R1 - L1,
        freeSpaceInNextLevel = levelPos[LV + 2] - R2;
    //
    // System.out.println("\t\t\tpropagatedNumLen:"+propagatedNumLen+"\t\t|unionPairs|:"+unionPairCounts.size());
    //    for(int i=L1;i<R1;i++)System.out.print("\t"+longToResult(num[i]));System.out.println();
    if (freeSpaceInMid
        >= propagatedNumLen) { // merge sort without temp space. freeSpace: [R1+1,L2),
      // [R2,lvPos[LV+2])
      //      System.out.println("\t\t\tfreeSpaceInMid is enough");
      int p1 = L1, p2 = L2, cntNumPos = L2 - propagatedNumLen;
      while (p1 < R1 || p2 < R2) {
        if (p1 < R1 && (p2 == R2 || num[p1] < num[p2])) num[cntNumPos++] = num[p1++];
        else num[cntNumPos++] = num[p2++];
      }
      levelPos[LV + 1] = L2 - propagatedNumLen;
    } else {
      //      System.out.println("\t\tfreeSpaceInMid is not enough");
      if (freeSpaceInMid + freeSpaceInNextLevel >= propagatedNumLen) {
        //        System.out.println("\t\t\tfreeSpaceInMid + freeSpaceInNextLevel is enough");
        for (int i = R2 - 1; i >= L2; i--) num[i + freeSpaceInNextLevel] = num[i];
        L2 += freeSpaceInNextLevel;
        R2 += freeSpaceInNextLevel;
        int p1 = L1, p2 = L2, cntNumPos = R1;
        while (p1 < R1 || p2 < R2) {
          if (p1 < R1 && (p2 == R2 || num[p1] < num[p2])) num[cntNumPos++] = num[p1++];
          else num[cntNumPos++] = num[p2++];
        }
        levelPos[LV + 1] = R1;
        levelPosForPairs[LV + 1] = R1 + (R1 - L1) + (R2 - L2);
      } else {
        //        System.out.println("\t\t\ttemp space.");
        long[] tmpNum = Arrays.copyOfRange(num, L1, R1);
        int p1 = L1, p2 = L2, cntNumPos = L2 - propagatedNumLen;
        while (p1 < R1 || p2 < R2) {
          if (p1 < R1 && (p2 == R2 || tmpNum[p1 - L1] < num[p2]))
            num[cntNumPos++] = tmpNum[p1++ - L1];
          else num[cntNumPos++] = num[p2++];
        }
        levelPos[LV + 1] = L2 - propagatedNumLen;
        //        if(cntNumPos!=R2)System.out.println("?????ERR cntNumPos!=R2");
      }
    }

    IntArrayList mergedICList = new IntArrayList();
    int cntNumIndex =
        processPairsInNumWithExistingPairs(
            levelPos[LV + 1],
            levelPosForPairs[LV + 1],
            unionPairValues,
            unionPairCounts,
            mergedICList);
    int deltaForOtherLV =
        setIndexCountsIntoNum(
            LV + 1, levelPos[LV + 1], cntNumIndex, levelPos[LV + 2], mergedICList);
    deltaForOtherLV = levelPos[LV + 1] - levelPosForPairs[LV];
    if (reservedAnItem == 1) { // level LV is empty now but should reserve an item.
      deltaForOtherLV -= 1;
      shiftNum(LV + 1, levelPosForPairs[LV], deltaForOtherLV);
      levelPos[LV] = levelPos[LV + 1] - 1;
      levelPosForPairs[LV] = levelPos[LV] + 1;
      num[levelPos[LV]] = reservedNum;
      reservedAnItem = 0;
    } else { // level LV may be empty or with a reserved item.
      shiftNum(LV + 1, levelPosForPairs[LV], deltaForOtherLV);
      if (levelPosForPairs[LV] != levelPos[LV + 1]) System.out.println("??????");
    }

    levelActualSize[LV + 1] += levelActualSize[LV] / 2;
    levelActualSize[LV] &= 1; // may reservedAnItem
  }

  private int getPairsInLevel(int level) {
    int pairs = 2 * (levelPos[level + 1] - levelPosForPairs[level]);
    if (pairs > 0 && calcIndex(getIndexCountInNum(num, levelPosForPairs[level], pairs - 1)) == 0)
      pairs--;
    return pairs;
  }

  /**
   * tmpNum[0...tmpPosForPairs) : values tmpNum[tmpPosForPairs...tmpNumLength) : Index-Count pairs.
   */
  private void mergePairsToLevel(
      long[] tmpNum, int tmpNumLength, int tmpPosForPairs, int LV, int startFreePos) {
    // merge a segment of pairs into current level LV.
    // num[startFreePos,levelPos[LV]) is free space
    if (getLevelSize(LV) == 0) {
      levelPos[LV] = levelPos[LV + 1] - tmpNumLength;
      levelPosForPairs[LV] = levelPos[LV] + tmpPosForPairs;
      //      System.out.println("\t\t[mergePairSegments] simply
      // COPY.\t|tmpNum|:"+tmpNumLength+"\t\tresultLevelPos:"+levelPos[LV]+"\tresultlevelPosForPairs:"+levelPosForPairs[LV]);
      System.arraycopy(tmpNum, 0, num, levelPos[LV], tmpNumLength);
      if (LV >= 1 && levelPos[LV] != startFreePos) {
        int delta = levelPos[LV] - startFreePos; // level 0...LV-1 was [?,startFreePos)
        for (int i = levelPos[LV] - 1; i >= bufferPosForLV0 + delta; i--) num[i] = num[i - delta];
        bufferPosForLV0 += delta;
        for (int i = 0; i < LV; i++) {
          levelPos[i] += delta;
          levelPosForPairs[i] += delta;
        }
      }
      return;
    }

    long[] mergedPairNum =
        new long
            [(tmpNumLength - tmpPosForPairs) * +(levelPos[LV + 1] - levelPosForPairs[LV])
                + Math.min(tmpPosForPairs, levelPosForPairs[LV] - levelPos[LV])];
    int distinct = 1, pairs = 0, mergedNumPos = startFreePos, mergedPairs = 0;
    long lastV = 0, cntV = 0;
    int cntCount = 0, lastCount = 0;
    int numIndexTMP = 0,
        numIndexLV = 0,
        pairIDTMP = 0,
        pairIDLV = 0,
        maxPairIDTMP = (tmpNum.length - tmpPosForPairs) * 2,
        maxPairIDLV = (levelPos[LV + 1] - levelPosForPairs[LV]) * 2;
    while (numIndexTMP < tmpPosForPairs || numIndexLV < levelPosForPairs[LV] - levelPos[LV]) {
      if (numIndexTMP == tmpPosForPairs
          || (numIndexLV < levelPosForPairs[LV] - levelPos[LV]
              && num[levelPos[LV] + numIndexLV] < tmpNum[numIndexTMP])) {
        cntV = num[levelPos[LV] + numIndexLV];
        if (pairIDLV < maxPairIDLV && checkSomeIndexIsNextPair(LV, pairIDLV, numIndexLV)) {
          cntCount = calcCount(getIndexCountInNum(num, levelPosForPairs[LV], pairIDLV));
          pairIDLV++;
        } else cntCount = 1;
        numIndexLV++;
        //        System.out.println("\t\t\t\tmerge a num from LV.
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount);
      } else {
        cntV = tmpNum[numIndexTMP];
        if (pairIDTMP < maxPairIDTMP
            && checkSomeIndexIsNextPair(tmpNum, tmpPosForPairs, pairIDTMP, numIndexTMP)) {
          cntCount = calcCount(getIndexCountInNum(tmpNum, tmpPosForPairs, pairIDTMP));
          pairIDTMP++;
        } else cntCount = 1;
        numIndexTMP++;
        //        System.out.println("\t\t\t\tmerge a num from TMP.
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount);
      }

      if (cntV != lastV) {
        if (lastCount != 0) {
          num[mergedNumPos] = lastV;
          if (lastCount >= 2) {
            setIndexCountInNum(
                mergedPairNum,
                0,
                mergedPairs++,
                calcIndexCount(1 + mergedNumPos - startFreePos, lastCount));
          }
          mergedNumPos++;
        }
        lastV = cntV;
        lastCount = cntCount;
      } else lastCount += cntCount;
    }
    if (lastCount != 0) {
      num[mergedNumPos] = lastV;
      if (lastCount >= 2) {
        setIndexCountInNum(
            mergedPairNum,
            0,
            mergedPairs++,
            calcIndexCount(1 + mergedNumPos - startFreePos, lastCount));
      }
      mergedNumPos++;
    }
    int spaceForMergedPairs = (mergedPairs + 1) / 2;
    levelPos[LV] = levelPos[LV + 1] - spaceForMergedPairs - (mergedNumPos - startFreePos);
    levelPosForPairs[LV] = levelPos[LV + 1] - spaceForMergedPairs;
    for (int i = 0; i < spaceForMergedPairs; i++) num[levelPosForPairs[LV] + i] = mergedPairNum[i];
    for (int i = 0; i < mergedNumPos - startFreePos; i++)
      num[levelPosForPairs[LV] - i - 1] = num[mergedNumPos - i - 1];

    int delta = levelPos[LV] - startFreePos; // level 0...LV-1 was [?,startFreePos)
    for (int i = levelPos[LV] - 1; i >= bufferPosForLV0 + delta; i--) num[i] = num[i - delta];
    bufferPosForLV0 += delta;
    for (int i = 0; i < LV; i++) {
      levelPos[i] += delta;
      levelPosForPairs[i] += delta;
    }
  }

  private void compactOneLevel(int level) { // 由于reserve，可能compact后占用更多的一个单位空间
    //    System.out.println("\t\t[KLLPair]compactOneLevel LV:\t"+level);
    if (level == cntLevel - 1) calcLevelMaxSize(cntLevel + 1);
    int L1 = levelPos[level], R1 = levelPosForPairs[level];
    addRecord(false, num[L1], num[R1 - 1], level);
    int actualSize = levelActualSize[level];
    levelActualSize[level + 1] += actualSize / 2;
    levelActualSize[level] &= 1; // may reservedAnItem

    int reservedAnItem = 0;
    long reservedNum = -233;
    if ((actualSize & 1) == 1) { // reserve a minimal item
      reservedAnItem = 1;
      reservedNum = num[L1];
    }
    long cntV = 0;
    int cntCount = 0, nextPairID = 0;
    int propagatedIndex = 0, propagatedPairs = 0, propagatedCount;
    int rand01 = getNextRand01();
    //    System.out.println("\t\t\t\t\t\trand01:\t"+rand01);
    for (int i = levelPos[level]; i < levelPosForPairs[level]; i++) {
      if (checkSomeIndexIsNextPair(level, nextPairID, i - levelPos[level])) {
        cntCount = calcCount(getIndexCountInNum(num, levelPosForPairs[level], nextPairID));
        nextPairID++;
      } else cntCount = 1;
      if (i == levelPos[level]) {
        cntCount -= reservedAnItem;
      }
      if (cntCount <= 0) continue;
      else if (cntCount <= rand01) {
        rand01 ^= (cntCount & 1);
        continue;
      } else {
        propagatedCount = (cntCount - rand01 + 1) / 2;
        rand01 ^= (cntCount & 1);
        num[levelPos[level] + propagatedIndex] = num[i];
        if (propagatedCount >= 2) {
          setIndexCountInNum(
              num,
              levelPosForPairs[level],
              propagatedPairs,
              calcIndexCount(1 + propagatedIndex, propagatedCount));
          propagatedPairs++;
        }
        propagatedIndex++;
      }
    }
    if ((propagatedPairs & 1) == 1)
      setIndexCountInNum(
          num,
          levelPosForPairs[level],
          propagatedPairs,
          0); // danger:reuse num_Pair, mind odd pairs.
    long[] proNum = new long[propagatedIndex + (propagatedPairs + 1) / 2];
    for (int i = 0; i < propagatedIndex; i++) proNum[i] = num[levelPos[level] + i];
    for (int i = 0; i < (propagatedPairs + 1) / 2; i++)
      proNum[propagatedIndex + i] = num[levelPosForPairs[level] + i];
    levelPosForPairs[level] = levelPos[level]; // clear. add reservedNum later.

    //    System.out.print("compactOneLevel LV="+level);
    //    System.out.print("\tproNums:");
    //    for(int i=0;i<propagatedIndex;i++)System.out.print(" "+longToResult(proNum[i])+",");
    //    System.out.print("\tproPairs:");
    //    for(int i=0;i<propagatedPairs;i++) {
    //      int IC = getIndexCountInNum(proNum,propagatedIndex,i);
    //      System.out.print("\t(" +(calcIndex(IC)-1)+","+calcCount(IC)+")");
    //    }System.out.println();

    mergePairsToLevel(proNum, proNum.length, propagatedIndex, level + 1, levelPos[level]);
    if (reservedAnItem == 1) {
      if (bufferPosForLV0 == 0)
        System.out.println("!![ERR] no space after compaction. due to reserved item.");
      for (int i = bufferPosForLV0 - 1; i < levelPos[level] - 1; i++) num[i] = num[i + 1];
      bufferPosForLV0--;
      for (int i = 0; i < level; i++) {
        levelPos[i] -= 1;
        levelPosForPairs[i] -= 1;
      }
      levelPos[level]--;
      num[levelPos[level]] = reservedNum;
      levelPosForPairs[level] = levelPos[level] + 1;
    }
    //    showNum();

    //    int mid = (L1 + R1) >>> 1;
    //    mergeSortWithoutSpace(L1, mid, levelPos[level + 1], levelPos[level + 2]);
    //    levelPos[level + 1] = mid;
    //    int newP = levelPos[level + 1] - 1, oldP = L1 - 1;
    //    for (int i = oldP; i >= levelPos[0]; i--)
    //      num[newP--] = num[oldP--];
    //
    //    levelPos[level] = levelPos[level + 1] - (L1 - levelPos[level]);
    //    int numReduced = (R1 - L1) >>> 1;
    //    for (int i = level - 1; i >= 0; i--) levelPos[i] += numReduced;
    //    if(levelPos[level+1]-levelPos[level]>levelMaxSize[level+1]){
    //      compactOneLevel(level+1);
    //    }
  }

  @Override
  public void compact() {
    //    System.out.println("[[COMPACT]]");
    long totActualSize = 0;
    double cntW = 1, totW = 0, cntActualSizeLimit;
    for (int i = cntLevel - 1; i >= 0; i--) {
      totActualSize += levelActualSize[i];
      totW += cntW;
      cntW /= 3.0 / 2; // exponential actual capacity
    }
    for (int i = 0; i < cntLevel; i++) {
      cntW *= 3.0 / 2;
      cntActualSizeLimit = Math.max(MinimumLevelMaxSize, totActualSize * cntW / totW);
      if (levelActualSize[i] > cntActualSizeLimit || i == cntLevel - 1) {
        compactOneLevelFaster(i);
        break;
      }
    }
    //    this.showNum();
  }

  public int getMaxErr() {
    int totERR = 0;
    for (int i = 0; i < cntLevel; i++) totERR += compactionNumInLevel.getInt(i) << i;
    return totERR;
  }

  public double getSig2() {
    double sig2 = 0;
    for (int i = 0; i < cntLevel; i++)
      sig2 += 0.5 * compactionNumInLevel.getInt(i) * Math.pow(2, i * 2);
    return sig2;
  }

  public void showCompact() {
    int totERR = 0;
    long totSIGMA = 0;
    for (int i = 0; i < cntLevel; i++) {
      System.out.print("\t");
      System.out.print("[" + compactionNumInLevel.getInt(i) + "]");
      System.out.print("\t");
      totERR += compactionNumInLevel.getInt(i) << i;
      totSIGMA += (long) compactionNumInLevel.getInt(i) << (i * 2L);
    }
    System.out.println(
        "\tmaxLV=" + (cntLevel - 1) + "\ttotERR=" + totERR + "\ttotSIGMA=" + totSIGMA);
  }

  private void addRecordInLevel(long minV, long maxV, int level) {
    int hisNum = compactionNumInLevel.getInt(level);
    hisNum++;
    compactionNumInLevel.set(level, hisNum);
  }

  public void addRecord(boolean isSum, long minV, long maxV, int level) {
    MIN_V = Math.min(MIN_V, minV);
    MAX_V = Math.max(MAX_V, maxV);
    if (!isSum) {
      addRecordInLevel(minV, maxV, level);
    } else {
      for (int i = 0; i <= level; i++) addRecordInLevel(minV, maxV, i);
    }
  }

  static long lastBound;
  static double lastSig2, lastPr = -1;
  static NormalDistribution lastNormalDis;

  public int[] getRelatedCompactNum() {
    int[] relatedCompactNum = new int[cntLevel - 1];
    for (int i = 0; i < cntLevel - 1; i++) relatedCompactNum[i] = compactionNumInLevel.getInt(i);
    return relatedCompactNum;
  }

  public void sortLV0() {
    if (bufferPosForLV0 != levelPos[0]) processLV0();
  }

  public long getMin() {
    long mn = MIN_V;
    if (levelPos[0] < levelPos[1]) {
      if (!level0Sorted) sortLV0();
      mn = Math.min(mn, num[levelPos[0]]);
    }
    return mn;
  }

  public long getMax() {
    long mx = MAX_V;
    if (levelPos[0] < levelPos[1]) {
      if (!level0Sorted) sortLV0();
      mx = Math.max(mx, num[levelPos[1] - 1]);
    }
    return mx;
  }

  public static long queryRankErrBoundGivenParameter(double sig2, long maxERR, double Pr) {
    if (maxERR == 0) return 0;
    NormalDistribution dis =
        sig2 == lastSig2 && lastNormalDis != null
            ? lastNormalDis
            : new NormalDistribution(0, Math.sqrt(sig2));
    if (sig2 == lastSig2 && Pr == lastPr) return lastBound;
    double tmpBound = dis.inverseCumulativeProbability(0.5 * (1 - Pr));
    tmpBound = Math.min(-Math.floor(tmpBound), maxERR);
    //
    // System.out.println("\t\t\t\t..\t\ttmp\t\t"+sig2+"\t"+maxERR+"\t"+Pr+"\t\t\ttmpBound:\t"+tmpBound);
    lastBound = (long) tmpBound;
    lastPr = Pr;
    lastSig2 = sig2;
    lastNormalDis = dis;
    //    System.out.println("\tPr:"+Pr+"\t"+"sig2:\t"+sig2+"\t\tbyInvCum:\t"+(long)tmpBound);
    return (long) tmpBound;
  }

  public static long queryRankErrBound(int[] relatedCompactNum, double Pr) {
    double sig2 = 0;
    long maxERR = 0;
    for (int i = 0; i < relatedCompactNum.length; i++)
      sig2 += 0.5 * relatedCompactNum[i] * Math.pow(2, i * 2);
    for (int i = 0; i < relatedCompactNum.length; i++) maxERR += (long) relatedCompactNum[i] << i;
    //    System.out.println("\t\t\t\t\t\tqueryRankErrBound\tmaxERR:\t"+maxERR+"\t\tsig2:\t"+
    // sig2+"\t\t\n"+ Arrays.toString(relatedCompactNum));
    return queryRankErrBoundGivenParameter(sig2, maxERR, Pr);
    //    NormalDistribution dis = sig2==lastSig2&&lastNormalDis!=null?lastNormalDis:new
    // NormalDistribution(0, Math.sqrt(sig2));
    //    if(sig2==lastSig2&&Pr==lastPr)return lastBound;
    //    double tmpBound = dis.inverseCumulativeProbability(0.5*(1-Pr));
    //    tmpBound=Math.min(-Math.floor(tmpBound),maxERR);
    //    lastBound=(int)tmpBound;
    //    lastPr=Pr;
    //    lastSig2=sig2;
    //    lastNormalDis=dis;
    ////    System.out.println("\tPr:"+Pr+"\t"+"sig2:\t"+sig2+"\t\tbyInvCum:\t"+(long)tmpBound);
    //    return (int)tmpBound;
  }

  public long queryRankErrBound(double Pr) {
    return queryRankErrBound(compactionNumInLevel.toIntArray(), Pr);
  }

  // 返回误差err，该数值在sketch里估计排名的偏差绝对值有Pr的概率<err
  public long queryRankErrBound(long result, double Pr) {
    int[] relatedCompactNum = getRelatedCompactNum();
    return queryRankErrBound(relatedCompactNum, Pr);
  }

  protected int findRankInLevel(int level, long v) {
    if (levelPos[level] >= levelPos[level + 1]) return 0;
    if (level == 0) sortLV0();
    int index = 0, pair = 0;
    long cntCount, rank = 0;
    for (int i = levelPos[level]; i < levelPosForPairs[level]; i++) {
      if (checkSomeIndexIsNextPair(level, pair, i - levelPos[level])) {
        cntCount = calcCount(getIndexCountInNum(num, levelPosForPairs[level], pair));
        pair++;
      } else cntCount = 1;
      if (num[i] <= v) rank += cntCount * (1 << level);
      else return (int) rank;
    }
    return (int) (rank);
  }

  @Override
  public int getApproxRank(long v) {
    int approxRank = 0;
    for (int i = 0; i < cntLevel; i++)
      if (levelPos[i] < levelPos[i + 1]) {
        approxRank += findRankInLevel(i, v);
      }
    return approxRank;
  }

  public long findMinValueWithRank(long K) {
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE, mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      //
      // System.out.println("2fen\t\t"+L+"..."+R+"\t\tmid="+mid+"\t\t"+(getApproxRank(mid)>=K));
      if (getApproxRank(mid) > K) R = mid;
      else L = mid + 1;
    }
    //    System.out.println("FT K:"+K+"\tN:"+getN()+" rank(L):"+getApproxRank(L));
    return L;
  }

  public LongArrayList findMinValuesWithRanks(LongArrayList Ks) {
    sortLV0();
    LongArrayList Vs = new LongArrayList();
    int[] pairs = new int[cntLevel], indexes = new int[cntLevel];
    long cntRank = 0, cntV = -233, lastAns = -233;
    for (long K : Ks) {
      while (cntRank < K) {
        int minVLV = -1;
        cntV = Long.MAX_VALUE;
        for (int i = 0; i < cntLevel; i++) {
          //
          // if(levelPos[i]+indexes[i]<levelPosForPairs[i])System.out.println("\t\t"+num[levelPos[i]
          // + indexes[i]]+" ???? "+cntV+"\t<?"+(num[levelPos[i] + indexes[i]] < cntV));
          if (levelPos[i] + indexes[i] < levelPosForPairs[i]
              && num[levelPos[i] + indexes[i]] < cntV) {
            minVLV = i;
            cntV = num[levelPos[i] + indexes[i]];
          }
        }
        if (minVLV == -1) break;
        int cntCount = 1;
        if (checkSomeIndexIsNextPair(minVLV, pairs[minVLV], indexes[minVLV])) {
          cntCount = calcCount(getIndexCountInNum(num, levelPosForPairs[minVLV], pairs[minVLV]));
          pairs[minVLV]++;
        }
        indexes[minVLV]++;
        cntRank += cntCount * (1 << minVLV);
      }
      if (cntV == Long.MAX_VALUE) Vs.add(lastAns);
      else {
        Vs.add(cntV);
        lastAns = cntV;
      }
    }
    //    System.out.println("\tKs:"+Ks);
    //    System.out.println("\tVs:"+Vs);
    //    System.out.println("\tDoubleVs:");for(long
    // v:Vs)System.out.print("\t"+longToResult(v));System.out.println();
    return Vs;
  }

  public LongArrayList getApproxRanks(DoubleArrayList doubleDataList) {
    sortLV0();
    LongArrayList rankList = new LongArrayList();
    int[] pairs = new int[cntLevel], indexes = new int[cntLevel];
    long cntV = Long.MIN_VALUE, cntRank = 0;
    for (double doubleData : doubleDataList) {
      long longData = dataToLong(doubleData);
      while (cntV <= longData) {
        int minVLV = -1;
        long nextMinV = Long.MAX_VALUE;
        for (int i = 0; i < cntLevel; i++) {
          //
          // if(levelPos[i]+indexes[i]<levelPosForPairs[i])System.out.println("\t\t"+num[levelPos[i]
          // + indexes[i]]+" ???? "+cntV+"\t<?"+(num[levelPos[i] + indexes[i]] < cntV));
          if (levelPos[i] + indexes[i] < levelPosForPairs[i]
              && num[levelPos[i] + indexes[i]] < nextMinV) {
            minVLV = i;
            nextMinV = num[levelPos[i] + indexes[i]];
          }
        }
        if (minVLV == -1 || nextMinV > longData) break;
        int cntCount = 1;
        if (checkSomeIndexIsNextPair(minVLV, pairs[minVLV], indexes[minVLV])) {
          cntCount = calcCount(getIndexCountInNum(num, levelPosForPairs[minVLV], pairs[minVLV]));
          pairs[minVLV]++;
        }
        indexes[minVLV]++;
        cntRank += cntCount * (1 << minVLV);
        cntV = nextMinV;
      }
      rankList.add(cntRank);
    }
    return rankList;
  }
}
