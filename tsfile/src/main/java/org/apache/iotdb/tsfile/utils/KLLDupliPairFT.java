package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class KLLDupliPairFT extends KLLSketchForQuantile { // TODO: too large count
  boolean DEBUG = false;
  IntArrayList compactionNumInLevel;
  public XoRoShiRo128PlusRandom randomForReserve = new XoRoShiRo128PlusRandom();
  long MIN_V = Long.MAX_VALUE, MAX_V = Long.MIN_VALUE;
  int[] lvPosT;
  // range in level L:
  // [ levelPos[L] , lvPosPairsF[L] ), // False Distinct values
  // [ lvPosPairsF[L] , levelPosT[L] ), // False Index-Count pairs
  // [ levelPosT[L] , lvPosPairsT[L] ), // True Distinct values
  // [ lvPosPairsF[L] , levelPos[L+1] ) // True Index-Count pairs
  int[] lvPosPairsF, lvPosPairsT, lvActualSize;
  int bufferPosForLV0, LV0PairsT = 0;
  int PairIndexBit, PairCountBit;
  static final long mask32From64_0 = 0xFFFFFFFF00000000L, mask32From64_1 = 0xFFFFFFFFL;
  static int maskIndexInPair, maskCountInPair;
  int maxPairsTCount = 0;

  // bufferPosForLV0 ... levelPos[0]-1: unordered arrivals
  // levelPos[i] ... levelPosForPair[i]-1 : ordered & paired.
  // levelPosForPair[i] ... levelPos[i+1] : pair info for level i.
  class TmpLevel {
    public long[] num;
    public int posPairsF, posT, posPairsT, actualSize = 0;

    public TmpLevel(long[] num, int posPairsF, int posT, int posPairsT, int actualSize) {
      this.num = num;
      this.posPairsF = posPairsF;
      this.posT = posT;
      this.posPairsT = posPairsT;
      this.actualSize = actualSize;
    }
  }

  Int2ObjectOpenHashMap<TmpLevel> tmpSketch;
  int tmpSketchActualSize = 0, tmpSketchNumLen = 0;

  public Long2LongOpenHashMap freqValueCount = null;
  boolean freqValueCountFrozen = false, frozenCheckingHashMapForNonFreqValue = false;
  long[] freqValue, freqCountSum;
  int maxMemoryByte, maxMemoryNumForSketch, lastUpdateFreqLevel = 0;
  long lastCheckFreqValueN = 0,
      newSketchNAfterLastCheck = 0,
      lastThreshold = 0,
      maxPairTAfterLastCheck = 0,
      checkFreqValueCount = 0;
  final int BitsPerItem = 64;
  boolean ENABLE_FREQ_HASHMAP = true;
  boolean COMPACT_ACTUAL_SIZE = true;
  int AmortizedRatioForBuffer = 50, MaxFreqThresholdRatio = 1;

  public KLLDupliPairFT(int maxMemoryByte) {
    N = 0;
    this.maxMemoryByte = maxMemoryByte;
    calcParameters(maxMemoryByte);
    calcLevelMaxSize(1);
    bufferPosForLV0 = levelPos[0];
  }

  public KLLDupliPairFT(int maxMemoryByte, boolean enableFreqHashMap) {
    this(maxMemoryByte);
    this.ENABLE_FREQ_HASHMAP = enableFreqHashMap;
  }

  public KLLDupliPairFT(int maxMemoryByte, boolean enableFreqHashMap, boolean compactActualSize) {
    this(maxMemoryByte, enableFreqHashMap);
    this.COMPACT_ACTUAL_SIZE = compactActualSize;
  }

  private void calcParameters(int maxMemoryByte) {
    maxMemoryNumForSketch = calcMaxMemoryNum(maxMemoryByte);
    num = new long[maxMemoryNumForSketch];
    level0Sorted = false;
    cntLevel = 0;
    compactionNumInLevel = new IntArrayList();

    PairIndexBit = 1;
    while ((1 << PairIndexBit) <= maxMemoryNumForSketch) PairIndexBit++;
    PairCountBit = 32 - PairIndexBit;
    // System.out.println("\t\tmaxMemNum:"+maxMemoryNumForSketch+"\t\tPairIndexBit:"+PairIndexBit);

    maskIndexInPair = ((1 << PairIndexBit) - 1) << PairCountBit;
    maskCountInPair = (1 << PairCountBit) - 1;
  }

  @Override
  protected int calcMaxMemoryNum(int maxMemoryByte) {
    return Math.min(1 << 20, maxMemoryByte / 8);
  }

  @Override
  protected void calcLevelMaxSize(int setLevel) { // set cntLevel. make sure cntLevel won't decrease
    int[] tmpArr = new int[setLevel + 1], tmpArrT = new int[setLevel + 1];
    int[] tmpArrPairF = new int[setLevel + 1], tmpArrPairT = new int[setLevel + 1];
    int[] tmpArrActualSize = new int[setLevel + 1];
    int maxPos =
        cntLevel > 0 ? Math.max(maxMemoryNumForSketch, levelPos[cntLevel]) : maxMemoryNumForSketch;
    for (int i = 0; i < setLevel + 1; i++) {
      tmpArr[i] = i < cntLevel ? levelPos[i] : maxPos;
      tmpArrT[i] = i < cntLevel ? lvPosT[i] : maxPos;
      tmpArrPairF[i] = i < cntLevel ? lvPosPairsF[i] : maxPos;
      tmpArrPairT[i] = i < cntLevel ? lvPosPairsT[i] : maxPos;
      tmpArrActualSize[i] = i < cntLevel ? lvActualSize[i] : 0;
    }
    levelPos = tmpArr;
    lvPosT = tmpArrT;
    lvPosPairsF = tmpArrPairF;
    lvPosPairsT = tmpArrPairT;
    lvActualSize = tmpArrActualSize;
    for (int i = cntLevel; i < setLevel; i++) {
      compactionNumInLevel.add(0);
    }
    cntLevel = setLevel;
    levelMaxSize = calcLevelMaxSizeByLevel(maxMemoryNumForSketch, cntLevel);
  }

  @Override
  public void showNum() {
    if (ENABLE_FREQ_HASHMAP && freqValueCount != null) {
      System.out.print("\t|\tHashMap: ");
      long totFreqCount = 0;
      for (Long2LongMap.Entry entry : freqValueCount.long2LongEntrySet()) {
        if (DEBUG)
          System.out.print(
              "{" + longToResult(entry.getLongKey()) + " " + entry.getLongValue() + "}, ");
        totFreqCount += entry.getLongValue();
      }
      System.out.print("\ttotFreqCount:\t" + totFreqCount + "\t|\t");
    }
    for (int i = 0; i < cntLevel; i++) {
      System.out.print("\tLV" + i + "{|");
      if (i == 0 && bufferPosForLV0 < levelPos[0]) {
        System.out.print("\t{{BUFFER ");
        for (int j = bufferPosForLV0; j < levelPos[0]; j++)
          System.out.print(longToResult(num[j]) + ", ");
        System.out.print("}}\t");
      }
      for (int j = levelPos[i]; j < lvPosPairsF[i]; j++)
        System.out.print(longToResult(num[j]) + "(F)" + ", ");
      for (int j = 0; j < getActualLvPairsF(i); j++) {
        int IC = getIndexCountInNum(num, lvPosPairsF[i], j);
        System.out.print("<" + (calcIndex(IC) - 1) + " " + calcCount(IC) + ">, ");
      }
      for (int j = lvPosT[i]; j < lvPosPairsT[i]; j++)
        System.out.print(longToResult(num[j]) + "(T)" + ", ");
      for (int j = 0; j < getActualLvPairsT(i); j++) {
        int IC = getIndexCountInNum(num, lvPosPairsT[i], j);
        System.out.print("<" + (calcIndex(IC) - 1) + " " + calcCount(IC) + ">, ");
      }

      System.out.print("|}\t");
    }
    System.out.println();
  }

  public void showBottomNum(int bottomLV) {
    if (ENABLE_FREQ_HASHMAP && freqValueCount != null) {
      System.out.print("\t|\tHashMap: ");
      for (Long2LongMap.Entry entry : freqValueCount.long2LongEntrySet())
        System.out.print(
            "{" + longToResult(entry.getLongKey()) + " " + entry.getLongValue() + "}, ");
      System.out.print("\t|\t");
    }
    for (int i = 0; i < bottomLV; i++) {
      System.out.print("\tLV" + i + "{|");
      if (i == 0 && bufferPosForLV0 < levelPos[0]) {
        System.out.print("\t{{BUFFER ");
        for (int j = bufferPosForLV0; j < levelPos[0]; j++)
          System.out.print(longToResult(num[j]) + ", ");
        System.out.print("}}\t");
      }
      for (int j = levelPos[i]; j < lvPosPairsF[i]; j++)
        System.out.print(longToResult(num[j]) + "(F)" + ", ");
      for (int j = 0; j < getActualLvPairsF(i); j++) {
        int IC = getIndexCountInNum(num, lvPosPairsF[i], j);
        System.out.print("<" + (calcIndex(IC) - 1) + " " + calcCount(IC) + ">, ");
      }
      for (int j = lvPosT[i]; j < lvPosPairsT[i]; j++)
        System.out.print(longToResult(num[j]) + "(T)" + ", ");
      for (int j = 0; j < getActualLvPairsT(i); j++) {
        int IC = getIndexCountInNum(num, lvPosPairsT[i], j);
        System.out.print("<" + (calcIndex(IC) - 1) + " " + calcCount(IC) + ">, ");
      }

      System.out.print("|}\t");
    }
    System.out.println();
  }

  public void showHashMap() {
    System.out.print("HashMap of FreqValueCount:\t");
    if (ENABLE_FREQ_HASHMAP && freqValueCount != null) {
      System.out.print("\t|\tHashMap: ");
      long totFreqCount = 0;
      for (Long2LongMap.Entry entry : freqValueCount.long2LongEntrySet()) {
        if (DEBUG)
          System.out.print(
              "{" + longToResult(entry.getLongKey()) + " " + entry.getLongValue() + "}, ");
        totFreqCount += entry.getLongValue();
      }
      System.out.print(
          "\ttotFreqCount:\t"
              + totFreqCount
              + "\tcheckFreqValue() "
              + checkFreqValueCount
              + " times"
              + "\t|\t");
    }
    System.out.println();
  }

  private int getLevelSizeT(int level) {
    return lvPosPairsT[level] - lvPosT[level];
  }

  private boolean mayNeedCompaction() {
    int freeSpace = bufferPosForLV0;
    for (int lv = 0; lv < cntLevel; lv++)
      if (freeSpace <= 2 * (levelPos[lv + 1] - lvPosPairsT[lv])) return true;
    return false;
  }

  // @Override
  // public void update(long x) { // signed long
  //
  // if(ENABLE_FREQ_HASHMAP&&freqValueCount!=null&&freqValueCount.containsKey(x))
  // {
  // freqValueCountFrozen=false;
  // freqValueCount.addTo(x, 1);
  // N++;
  // }
  // else {
  // if (bufferPosForLV0 == 0 || LV0PairsT>bufferPosForLV0) {
  // boolean needCompaction = processLV0();
  // needCompaction|=mayNeedCompaction();
  // if (needCompaction) {
  // compact();
  //// System.out.println("After1Compaction.
  // freeSize:"+1.0*bufferPosForLV0/maxMemoryNumForSketch+","+bufferPosForLV0+"\t\tLV0PairsT:"+getActualLvPairsT(0));
  //// System.out.print("After1Compaction. lvPos[0]="+levelPos[0]+" Sketch:
  // ");showNum();checkN();
  // int MMP=0;
  // while (bufferPosForLV0 == 0 || LV0PairsT>bufferPosForLV0 ||
  // mayNeedCompaction()) {
  //// MMP++;
  //// if(MMP>10)break;
  // compact();
  //// System.out.println("AfterXCompaction.
  // freeSize:"+1.0*bufferPosForLV0/maxMemoryNumForSketch+","+bufferPosForLV0+"\t\tLV0PairsT:"+getActualLvPairsT(0));
  //// System.out.print("afterXCompaction. lvPos[0]="+levelPos[0]+" Sketch:
  // ");showNum();checkN();
  // }
  // }
  //// else System.out.println("Do not need compaction after processLV0().
  // freeSize:"+1.0*bufferPosForLV0/maxMemoryNumForSketch+","+bufferPosForLV0+"\t\tLV0PairsT:"+getActualLvPairsT(0));
  // }
  //
  // if(ENABLE_FREQ_HASHMAP&&freqValueCount!=null&&freqValueCount.containsKey(x))
  // {
  // freqValueCountFrozen=false;
  // freqValueCount.addTo(x, 1);
  // N++;
  // }else {
  // num[--bufferPosForLV0] = x;
  // lvActualSize[0]++;
  // N++;
  // newSketchNAfterLastCheck++;
  // }
  // }
  // }

  @Override
  public void update(long x) { // signed long
    if (ENABLE_FREQ_HASHMAP && freqValueCount != null && freqValueCount.containsKey(x)) {
      freqValueCountFrozen = false;
      freqValueCount.addTo(x, 1);
      N++;
    } else {
      while (bufferPosForLV0 <= 0) {
        boolean needCompaction = processLV0();
        if (needCompaction) {
          // int
          // targetSketchSize=levelPos[cntLevel]-levelPos[0]-Math.max(MinimumLevelMaxSize,getLevelSize(0)/AmortizedRatioForBuffer);
          int targetSketchSize =
              // getNumLen()-1
              levelPos[cntLevel]
                  - Math.max(MinimumLevelMaxSize, getLevelSize(0) / AmortizedRatioForBuffer)
                  - 1;
          if (levelPos[cntLevel] != maxMemoryNumForSketch)
            System.out.println("!![err]\tlevelPos[cntLevel]!=maxMemoryNumForSketch");
          continuousCompact(targetSketchSize);
          // if(getNumLen()>targetSketchSize||bufferPosForLV0<=0)System.out.println("!![err]\tgetNumLen():"+(getNumLen())+"
          // targetSketchSize:"+targetSketchSize+" bufferPosForLV0:"+bufferPosForLV0);
        }
        // else System.out.println("Do not need compaction after processLV0().
        // freeSize:"+1.0*bufferPosForLV0/maxMemoryNumForSketch+","+bufferPosForLV0+"\t\tLV0PairsT:"+getActualLvPairsT(0));
      }

      if (ENABLE_FREQ_HASHMAP && freqValueCount != null && freqValueCount.containsKey(x)) {
        freqValueCountFrozen = false;
        freqValueCount.addTo(x, 1);
        N++;
      } else {
        num[--bufferPosForLV0] = x;
        lvActualSize[0]++;
        N++;
        newSketchNAfterLastCheck++;
      }
    }
  }

  int getTotalActualSize() {
    int size = tmpSketchActualSize;
    for (int i = 0; i < cntLevel; i++) {
      size += lvActualSize[i];
    }
    return size;
  }

  int getTotalNumLen() {
    return tmpSketchNumLen + levelPos[cntLevel] - levelPos[0];
  }

  public void continuousCompact(int targetNumLen) {
    tmpSketch = new Int2ObjectOpenHashMap<>(cntLevel);
    tmpSketchActualSize = tmpSketchNumLen = 0;
    if (DEBUG)
      System.out.println(
          "[DEBUG]continuousCompact\tcntTotNumLen="
              + getTotalNumLen()
              + "\tTarget:"
              + targetNumLen);
    while (getTotalNumLen() > targetNumLen) {
      if (DEBUG) System.out.println("  [DEBUG]continuousCompact\tcntTotNumLen=" + getTotalNumLen());
      double cntW = 1, totW = 0, cntActualSizeLimit, cntCompressSizeLimit;
      for (int i = cntLevel - 1; i >= 0; i--) {
        totW += cntW;
        cntW /= 3.0 / 2; // exponential actual capacity
      }
      int levelToCompact = cntLevel - 1, totalActualSize = getTotalActualSize();
      for (int i = 0; i < cntLevel; i++) {
        cntW *= 3.0 / 2;
        cntActualSizeLimit = Math.max(MinimumLevelMaxSize, totalActualSize * cntW / totW);
        cntCompressSizeLimit = Math.max(MinimumLevelMaxSize, maxMemoryNumForSketch * cntW / totW);
        if (COMPACT_ACTUAL_SIZE) {
          if (
          /* (i == 0 && LV0PairsT > bufferPosForLV0) || */ (tmpSketch.containsKey(i)
                      ? tmpSketch.get(i).actualSize
                      : lvActualSize[i])
                  >= cntActualSizeLimit
              || i == cntLevel - 1) {
            levelToCompact = i;
            break;
          }
        } else {
          if (
          /* (i == 0 && LV0PairsT > bufferPosForLV0) || */ (tmpSketch.containsKey(i)
                      ? tmpSketch.get(i).num.length
                      : getLevelSize(i))
                  >= cntCompressSizeLimit
              || i == cntLevel - 1) {
            levelToCompact = i;
            break;
          }
        }
      }
      if (tmpSketch.containsKey(levelToCompact)) compactOneTmpLevel(levelToCompact);
      else compactOneLevelToTmpSketch(levelToCompact);
      if (DEBUG) checkTotalN();
    }
    mergeTmpSketchIntoNum();
    // if(DEBUG)showNum();
    if (DEBUG) checkN();

    if (ENABLE_FREQ_HASHMAP && needToCheckFreqValue()) checkFreqValue(); // 可能导致sketch大小增大
    // checkOrdered();
    // checkN();
  }

  private int calcIndexCount(int index, int count) {
    if (count > maskCountInPair)
      System.out.println("\t[ERR] too large count." + "\t" + count + ">" + maskCountInPair);
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
    // System.out.println("\n[getting][posForPairsInNum]:"+posForPairsInNum+"\tpairID:"+pairID);
    int indexCount =
        (pairID & 1) == 0
            ? (int) ((num[posForPairsInNum + pairID / 2] & mask32From64_0) >>> 32)
            : (int) (num[posForPairsInNum + pairID / 2] & mask32From64_1);
    // System.out.println("\t\t\t\t\t\t\t\t\t\tic:"+indexCount);
    return indexCount;
  }

  /** SomeIndex:>=0 */
  private boolean checkSomeIndexIsNextPairF(int level, int nextPairID, int SomeIndex) {
    int locatedPairPosInNum = lvPosPairsF[level] + nextPairID / 2;
    return locatedPairPosInNum < lvPosT[level]
        && calcIndex(getIndexCountInNum(num, lvPosPairsF[level], nextPairID)) - 1 == SomeIndex;
    // empty in num_pairIndex will be 0-1=-1, always != cntIndex.
  }

  private boolean checkSomeIndexIsNextPairT(int level, int nextPairID, int SomeIndex) {
    int locatedPairPosInNum = lvPosPairsT[level] + nextPairID / 2;
    return locatedPairPosInNum < levelPos[level + 1]
        && calcIndex(getIndexCountInNum(num, lvPosPairsT[level], nextPairID)) - 1 == SomeIndex;
    // empty in num_pairIndex will be 0-1=-1, always != cntIndex.
  }

  private boolean checkSomeIndexIsNextPair(
      long[] num,
      int posForPairsInNumStart,
      int posForPairsInNumEnd,
      int nextPairID,
      int SomeIndex) {
    int locatedPairPosInNum = posForPairsInNumStart + nextPairID / 2;
    return locatedPairPosInNum < posForPairsInNumEnd
        && calcIndex(getIndexCountInNum(num, posForPairsInNumStart, nextPairID)) - 1 == SomeIndex;
    // empty in num_pairIndex will be 0-1=-1, always != cntIndex.
  }

  /** SomeIndex:>=0 */
  private boolean checkSomeIndexIsNextPair(
      long[] num, int posForPairsInNum, int nextPairID, int SomeIndex) {
    int locatedPairPosInNum = posForPairsInNum + nextPairID / 2;
    return locatedPairPosInNum < num.length
        && calcIndex(getIndexCountInNum(num, posForPairsInNum, nextPairID)) - 1 == SomeIndex;
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
    // System.out.println("\t\tsetting IndexCount:
    // \t"+"pairID:"+pair+"\t\t"+calcIndex(IndexCount)+","+calcCount(IndexCount));
    int pairPosInNum = levelPosForPair + pairID / 2;
    // System.out.println("[pairPosInNum]"+pairPosInNum);
    if ((pairID & 1) == 0)
      num[pairPosInNum] = (num[pairPosInNum] & mask32From64_1) | ((long) IndexCount << 32);
    else num[pairPosInNum] = (num[pairPosInNum] & mask32From64_0) | (IndexCount & mask32From64_1);
  }

  /** return: need a compaction or not */
  private boolean processLV0() {
    if (bufferPosForLV0 == levelPos[0]) return true;
    Arrays.sort(num, bufferPosForLV0, levelPos[0]);
    int distinctInBuffer = 1, pairInBuffer = 0;
    for (int i = bufferPosForLV0 + 1; i < levelPos[0]; i++) {
      if (num[i] != num[i - 1]) distinctInBuffer++;
      else if (i == bufferPosForLV0 + 1 || num[i - 1] != num[i - 2]) pairInBuffer++;
    }
    int spaceForBufferIndexCount = (pairInBuffer + 1) / 2;
    long[] tmpNumBuf = new long[distinctInBuffer + spaceForBufferIndexCount];
    long lastV;
    int cntIndex = 0, cntPair = 0, cntCount = 0;
    for (int i = bufferPosForLV0 + 1; i <= levelPos[0]; i++) {
      lastV = num[i - 1];
      cntCount++;
      if (i == levelPos[0] || num[i] != lastV) {
        tmpNumBuf[cntIndex] = lastV;
        if (cntCount >= 2) {
          setIndexCountInNum(
              tmpNumBuf, distinctInBuffer, cntPair, calcIndexCount(1 + cntIndex, cntCount));
          cntPair++;
        }
        cntIndex++;
        cntCount = 0;
      }
    }

    // {
    // System.out.print("\t\t[Process LV0 Buffer] [Distinct values:\t");
    // for (int i = 0; i < distinctInBuffer; i++) System.out.print("\t" +
    // longToResult(tmpNumBuf[i]));
    // System.out.print("]\t\t\t[Dupli_Index,Count:\t");
    // for (int i = 0; i < pairInBuffer; i++) {
    // int indexCount = getIndexCountInNum(tmpNumBuf, distinctInBuffer, i);
    // System.out.print("\t(" + (calcIndex(indexCount) - 1) + "," +
    // calcCount(indexCount) + ")");
    // }
    // System.out.println("\t]");
    // }

    mergePairsToLevel(
        tmpNumBuf,
        distinctInBuffer + (cntPair + 1) / 2,
        0,
        0,
        distinctInBuffer,
        0,
        bufferPosForLV0); // merge
    // two
    // seg
    bufferPosForLV0 = levelPos[0];

    // System.out.print("After Process LV0. Sketch: ");showNum();
    // System.out.println("After process LV0. lvPos[0]="+levelPos[0]);
    // System.out.print("After Process LV0. CheckN: ");checkN();

    LV0PairsT = getActualLvPairsT(0);
    // return
    // bufferPosForLV0<=(lvPosPairsF[0]-levelPos[0]+lvPosPairsT[0]-lvPosT[0])/2 ||
    // bufferPosForLV0<=MinimumLevelMaxSize || bufferPosForLV0<LV0PairsT;// ||
    // bufferPosForLV0<=2*(levelPos[1]-lvPosPairsT[0]);
    // 均摊：处理缓冲区开销为整个LV0大小，假如收益足够大可以不compact
    return bufferPosForLV0
        <= Math.max(MinimumLevelMaxSize, getLevelSize(0) / AmortizedRatioForBuffer);
  }

  private int getActualLvPairsF(int level) {
    int pairs = 2 * (lvPosT[level] - lvPosPairsF[level]);
    if (pairs > 0 && calcIndex(getIndexCountInNum(num, lvPosPairsF[level], pairs - 1)) == 0)
      pairs--;
    return pairs;
  }

  private int getActualLvPairsT(int level) {
    int pairs = 2 * (levelPos[level + 1] - lvPosPairsT[level]);
    if (pairs > 0 && calcIndex(getIndexCountInNum(num, lvPosPairsT[level], pairs - 1)) == 0)
      pairs--;
    return pairs;
  }

  /**
   * tmpNum[0...tmpPosPairsF) : valuesF tmpNum[tmpPosPairsF...tmpPosT) : Index-Count pairs.
   * tmpNum[tmpPosT...tmpPosPairsT) : valuesT tmpNum[tmpPosPairsT...tmpNumLength) : Index-Count
   * pairs.
   */
  // todo
  // 先检查是否能在num里完成，否则合并为新的tmpNum[]，清空LV，再做compact直到numLen可以把tmpNum[]合并进去(可能因为LV-1的compact导致LV又有东西？是否需要把compact的二选一结果先做判断，LV->(cntN,tmpNum[],tmp...))
  private void mergePairsToLevel(
      long[] tmpNum,
      int tmpNumLength,
      int tmpPosPairsF,
      int tmpPosT,
      int tmpPosPairsT,
      int LV,
      int startFreePos) {
    // merge a segment of pairs into current level LV.
    // num[startFreePos,levelPos[LV]) is free space
    if (getLevelSize(LV) == 0) {
      levelPos[LV] = levelPos[LV + 1] - tmpNumLength;
      lvPosPairsF[LV] = levelPos[LV] + tmpPosPairsF;
      lvPosT[LV] = levelPos[LV] + tmpPosT;
      lvPosPairsT[LV] = levelPos[LV] + tmpPosPairsT;
      // if(levelPos[LV]<0)
      // System.out.println("\t\t[mergePairSegments] simply COPY." + "\tLV:" + LV +
      // "\t|tmpNum|:" + tmpNumLength + "\t\tlevelPos:" + levelPos[LV] +
      // "\tlvPosPairsF:" + lvPosPairsF[LV] + "\tlvPosT:" + lvPosT[LV] +
      // "\tlvPosPairsT:" + lvPosPairsT[LV]);
      System.arraycopy(tmpNum, 0, num, levelPos[LV], tmpNumLength);

      if (LV >= 1 && levelPos[LV] != startFreePos) {
        // System.out.println(">???????????????>>>>>??????");
        int delta = levelPos[LV] - startFreePos; // level 0...LV-1 was [?,startFreePos)
        if (delta > 0)
          for (int i = levelPos[LV] - 1; i >= bufferPosForLV0 + delta; i--) num[i] = num[i - delta];
        else
          for (int i = bufferPosForLV0 + delta; i <= levelPos[LV] - 1; i++) num[i] = num[i - delta];
        bufferPosForLV0 += delta;
        for (int i = 0; i < LV; i++) {
          levelPos[i] += delta;
          lvPosPairsF[i] += delta;
          lvPosT[i] += delta;
          lvPosPairsT[i] += delta;
        }
        // System.out.print("InCompaction Before Merge. consolidate_delta="+delta+"
        // Bottom Sketch [0...LV):");showBottomNum(LV);
      }
      return;
    }

    if (bufferPosForLV0 > 0) {
      for (int i = 0; i < levelPos[LV] - bufferPosForLV0; i++) num[i] = num[i + bufferPosForLV0];
      for (int i = 0; i < LV; i++) {
        levelPos[i] -= bufferPosForLV0;
        lvPosPairsF[i] -= bufferPosForLV0;
        lvPosT[i] -= bufferPosForLV0;
        lvPosPairsT[i] -= bufferPosForLV0;
      }
      startFreePos -= bufferPosForLV0;
      bufferPosForLV0 = 0;
    }

    long[] mergedPairF =
        new long
            [(tmpPosT - tmpPosPairsF) // pairF in tmpNum
                + (lvPosT[LV] - lvPosPairsF[LV]) // pairF in num
                + (Math.min(tmpPosPairsF, lvPosPairsF[LV] - levelPos[LV]) + 1) / 2];
    int mergedNumPos = startFreePos, mergedPairsF = 0;
    long lastV = 0, cntV = 0;
    int lastCount = 0, cntCount = 0;
    int numIndexTMP = 0, numIndexLV = 0, pairIDTMP = 0, pairIDLV = 0; // System.out.println();
    while (numIndexTMP < tmpPosPairsF || numIndexLV < lvPosPairsF[LV] - levelPos[LV]) {
      if (numIndexTMP == tmpPosPairsF
          || (numIndexLV < lvPosPairsF[LV] - levelPos[LV]
              && num[levelPos[LV] + numIndexLV] < tmpNum[numIndexTMP])) {
        cntV = num[levelPos[LV] + numIndexLV];
        if (checkSomeIndexIsNextPairF(LV, pairIDLV, numIndexLV)) {
          cntCount = calcCount(getIndexCountInNum(num, lvPosPairsF[LV], pairIDLV));
          // if(pairIDLV>getActualLvPairsF(LV))System.out.println("\t[ERR] pairIDLV larger
          // than getActualLvPairsF!");
          pairIDLV++;
        } else cntCount = 1;
        numIndexLV++;
        // if(cntCount>=50000)
        // System.out.println("\t\t\t\tmerge a numF from LV.
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount+"\tLV:"+LV);
      } else {
        cntV = tmpNum[numIndexTMP];
        if (checkSomeIndexIsNextPair(tmpNum, tmpPosPairsF, pairIDTMP, numIndexTMP)) {
          cntCount = calcCount(getIndexCountInNum(tmpNum, tmpPosPairsF, pairIDTMP));
          // if(cntCount>=50000)
          // System.out.println("\t\t??"+"\tcntV,C:"+longToResult(cntV)+","+cntCount+"\tpairID:"+pairIDTMP+"\tI,C/:"+calcIndex(getIndexCountInNum(tmpNum,tmpPosPairsF,pairIDTMP))+","+calcCount(getIndexCountInNum(tmpNum,tmpPosPairsF,pairIDTMP)));
          pairIDTMP++;
        } else cntCount = 1;
        numIndexTMP++;
        // System.out.println("\t\t\t\tmerge a numF from TMP.
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount+"\tLV:"+LV);
      }

      if (cntV != lastV) {
        if (lastCount != 0) {
          num[mergedNumPos] = lastV;
          if (lastCount >= 2) {
            if (mergedPairsF >= mergedPairF.length * 2) {
              System.out.println(
                  "\t\t[ERR]\tMerging Pairs To Level"
                      + LV
                      + ".  Too many PairF."
                      + "\tpairs:"
                      + mergedPairsF
                      + "<=\t"
                      + numIndexTMP
                      + "+"
                      + numIndexLV
                      + " + "
                      + (tmpPosT - tmpPosPairsF)
                      + " + "
                      + (lvPosT[LV] - lvPosPairsF[LV]));
              for (int i = 1; i < tmpPosPairsF; i++)
                if (tmpNum[i] == tmpNum[i - 1])
                  System.out.println("\t\t?????SAME TMPNUM VALUE" + "\t" + longToResult(tmpNum[i]));
              // tmpPosPairsF
            }
            setIndexCountInNum(
                mergedPairF,
                0,
                mergedPairsF++,
                calcIndexCount(1 + mergedNumPos - startFreePos, lastCount));
            if (lastCount >= maskCountInPair) System.out.println("???????????????????????????");
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
        // System.out.println("\t\tlastV,C:"+longToResult(lastV)+","+lastCount);
        setIndexCountInNum(
            mergedPairF,
            0,
            mergedPairsF++,
            calcIndexCount(1 + mergedNumPos - startFreePos, lastCount));
      }
      mergedNumPos++;
    }

    int spaceForMergedPairsF = (mergedPairsF + 1) / 2;
    for (int i = 0; i < spaceForMergedPairsF; i++) num[mergedNumPos + i] = mergedPairF[i];
    levelPos[LV] = startFreePos;
    lvPosPairsF[LV] = mergedNumPos;
    mergedNumPos += spaceForMergedPairsF;

    // todo 不能直接放到num？ 似乎没发生
    if (mergedNumPos >= lvPosT[LV])
      System.out.println("[ERR!!] mergedNumPos>=lvPosT[LV]. LV:" + LV);
    //
    // int spaceForMergedPairs=(mergedPairsF+1)/2;
    // levelPos[LV] = levelPos[LV+1] - spaceForMergedPairs -
    // (mergedNumPos-startFreePos);
    // lvPosPairsF[LV] = levelPos[LV+1]-spaceForMergedPairs;

    // for(int i=0;i<mergedNumPos-startFreePos;i++)
    // num[lvPosPairsF[LV]-i-1] = num[mergedNumPos-i-1];

    //
    int startFreePosT = mergedNumPos;
    long[] mergedPairT =
        new long
            [(tmpNumLength - tmpPosPairsT)
                + (levelPos[LV + 1] - lvPosPairsT[LV])
                + Math.min(tmpPosPairsT - tmpPosT, lvPosPairsT[LV] - lvPosT[LV])];
    int mergedPairsT = 0;
    lastV = 0;
    cntV = 0;
    lastCount = 0;
    cntCount = 0;
    numIndexTMP = 0;
    numIndexLV = 0;
    pairIDTMP = 0;
    pairIDLV = 0; // System.out.println();
    while (tmpPosT + numIndexTMP < tmpPosPairsT || numIndexLV < lvPosPairsT[LV] - lvPosT[LV]) {
      if (tmpPosT + numIndexTMP == tmpPosPairsT
          || (numIndexLV < lvPosPairsT[LV] - lvPosT[LV]
              && num[lvPosT[LV] + numIndexLV] < tmpNum[tmpPosT + numIndexTMP])) {
        cntV = num[lvPosT[LV] + numIndexLV];
        if (checkSomeIndexIsNextPairT(LV, pairIDLV, numIndexLV)) {
          cntCount = calcCount(getIndexCountInNum(num, lvPosPairsT[LV], pairIDLV));
          pairIDLV++;
        } else cntCount = 1;
        numIndexLV++;
        // if(cntCount>=20000)
        // System.out.println(" \t\t\t\tmerge a numT from LV.
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount+"\tLV:"+LV);
        // todo: debug 有数据已经被压上去之后，在LV0依然有他
      } else {
        cntV = tmpNum[tmpPosT + numIndexTMP];
        if (checkSomeIndexIsNextPair(tmpNum, tmpPosPairsT, pairIDTMP, numIndexTMP)) {
          cntCount = calcCount(getIndexCountInNum(tmpNum, tmpPosPairsT, pairIDTMP));
          pairIDTMP++;
        } else cntCount = 1;
        numIndexTMP++;
        // if(cntCount>=20000)
        // System.out.println("\t\t\t\tmerge a numT from TMP.
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount+"\tLV:"+LV);
      }

      if (cntV != lastV) {
        if (lastCount != 0) {
          num[mergedNumPos] = lastV;
          if (lastCount >= 2) {
            setIndexCountInNum(
                mergedPairT,
                0,
                mergedPairsT++,
                calcIndexCount(1 + mergedNumPos - startFreePosT, lastCount));
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
            mergedPairT,
            0,
            mergedPairsT++,
            calcIndexCount(1 + mergedNumPos - startFreePosT, lastCount));
      }
      mergedNumPos++;
    }
    int spaceForMergedPairsT = (mergedPairsT + 1) / 2;
    for (int i = 0; i < spaceForMergedPairsT; i++) num[mergedNumPos + i] = mergedPairT[i];
    lvPosT[LV] = startFreePosT;
    lvPosPairsT[LV] = mergedNumPos;
    mergedNumPos += spaceForMergedPairsT;

    int delta = levelPos[LV + 1] - mergedNumPos; // level 0...LV-1 was [?,startFreePos)
    if (delta < 0) {
      System.out.println(
          "\t[ERR] Merging Pairs to LV"
              + LV
              + "\tToo many num after merged."
              + "\t\tEXCEEDS SPACE:"
              + (-delta));
    }
    if (delta > 0) {
      for (int i = levelPos[LV + 1] - 1; i >= bufferPosForLV0 + delta; i--) num[i] = num[i - delta];
      bufferPosForLV0 += delta;
      for (int i = 0; i <= LV; i++) {
        levelPos[i] += delta;
        lvPosPairsF[i] += delta;
        lvPosT[i] += delta;
        lvPosPairsT[i] += delta;
      }
    }
  }

  private void mergeNumToTmpSketch(
      long[] arriveNum,
      int arriveNumLen,
      int arrivePosPairsF,
      int arrivePosT,
      int arrivePosPairsT,
      int arriveActualSize,
      int LV) {
    if (LV >= cntLevel) calcLevelMaxSize(LV + 1);
    if (DEBUG)
      System.out.println(
          "\t[DEBUG]\tmergeNumToTmpSketch\tLV="
              + LV
              + "\tarriveNumLen:"
              + arriveNumLen
              + "\tarriveActualSize:"
              + arriveActualSize);
    //// if(arriveActualSize==arriveNumLen+1){
    ////
    // System.out.println("\t\t\t\tarrivePosPairsF="+arrivePosPairsF+"\tarrivePosT="+arrivePosT+"\tarrivePosPairsT="+arrivePosPairsT+"\tarriveNumLen="+arriveNumLen);
    //// }
    if (tmpSketch.get(LV) == null) {
      TmpLevel tmpLV =
          new TmpLevel(
              arriveNumLen == arriveNum.length
                  ? arriveNum
                  : Arrays.copyOfRange(arriveNum, 0, arriveNumLen),
              arrivePosPairsF,
              arrivePosT,
              arrivePosPairsT,
              arriveActualSize);
      tmpSketch.put(LV, tmpLV);
      tmpSketchActualSize += arriveActualSize;
      tmpSketchNumLen += arriveNumLen;
      if (DEBUG) checkTmpSketchLevel(LV);
      return;
    }
    TmpLevel tmpLV = tmpSketch.get(LV);
    long[] tmpNum = tmpLV.num;
    int tmpPosPairsF = tmpLV.posPairsF,
        tmpPosT = tmpLV.posT,
        tmpPosPairsT = tmpLV.posPairsT,
        tmpNumLen = tmpNum.length;
    int checkArriveActualSize = 0, checkTmpActualSize = 0;

    LongArrayList mergedNum = new LongArrayList();
    long[] mergedPairF =
        new long
            [(arrivePosT - arrivePosPairsF) // pairF in arriveNum
                + (tmpPosT - tmpPosPairsF) // pairF in num
                + (Math.min(arrivePosPairsF, tmpPosPairsF) + 1) / 2];
    int mergedPairsF = 0;
    long lastV = 0, cntV = 0;
    int lastCount = 0, cntCount = 0;
    int numIndexArrive = 0,
        numIndexTmp = 0,
        pairIDArrive = 0,
        pairIDTmp = 0; // System.out.println();
    while (numIndexArrive < arrivePosPairsF || numIndexTmp < tmpPosPairsF) {
      if (numIndexArrive == arrivePosPairsF
          || (numIndexTmp < tmpPosPairsF && tmpNum[numIndexTmp] < arriveNum[numIndexArrive])) {
        cntV = tmpNum[numIndexTmp];
        if (checkSomeIndexIsNextPair(tmpNum, tmpPosPairsF, tmpPosT, pairIDTmp, numIndexTmp)) {
          cntCount = calcCount(getIndexCountInNum(tmpNum, tmpPosPairsF, pairIDTmp));
          pairIDTmp++;
        } else cntCount = 1;
        numIndexTmp++;
        checkTmpActualSize += cntCount;
      } else {
        cntV = arriveNum[numIndexArrive];
        if (checkSomeIndexIsNextPair(
            arriveNum, arrivePosPairsF, arrivePosT, pairIDArrive, numIndexArrive)) {
          cntCount = calcCount(getIndexCountInNum(arriveNum, arrivePosPairsF, pairIDArrive));
          pairIDArrive++;
        } else cntCount = 1;
        numIndexArrive++;
        checkArriveActualSize += cntCount;
        // System.out.println("\t\t\t\tmerge a numF from arriveNum.
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount+"\tLV:"+LV);
        // if(arriveActualSize==arriveNumLen+1)
        // System.out.println("\t\t\t\tmerge a numF from arriveNum.
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount+"\tLV:"+LV);
      }

      if (cntV != lastV) {
        if (lastCount != 0) {
          mergedNum.add(lastV);
          if (lastCount >= 2) {
            if (mergedPairsF >= mergedPairF.length * 2) {
              System.out.println(
                  "\t\t[ERR]\tMerging Pairs To Level"
                      + LV
                      + ".  Too many PairF."
                      + "\tpairs:"
                      + mergedPairsF
                      + "<=\t"
                      + numIndexArrive
                      + "+"
                      + numIndexTmp
                      + " + "
                      + (arrivePosT - arrivePosPairsF)
                      + " + "
                      + (tmpPosT - tmpPosPairsF));
              for (int i = 1; i < arrivePosPairsF; i++)
                if (arriveNum[i] == arriveNum[i - 1])
                  System.out.println(
                      "\t\t?????SAME TMPNUM VALUE" + "\t" + longToResult(arriveNum[i]));
            }
            setIndexCountInNum(
                mergedPairF,
                0,
                mergedPairsF++,
                calcIndexCount(1 + mergedNum.size() - 1, lastCount));
            if (lastCount >= maskCountInPair)
              System.out.println("[ERR]\tlastCount>=maskCountInPair");
          }
        }
        lastV = cntV;
        lastCount = cntCount;
      } else lastCount += cntCount;
    }
    if (lastCount != 0) {
      mergedNum.add(lastV);
      if (lastCount >= 2) {
        // System.out.println("\t\tlastV,C:"+longToResult(lastV)+","+lastCount);
        setIndexCountInNum(
            mergedPairF, 0, mergedPairsF++, calcIndexCount(1 + mergedNum.size() - 1, lastCount));
      }
    }

    int spaceForMergedPairsF = (mergedPairsF + 1) / 2;
    int mergedPosPairsF = mergedNum.size();
    for (int i = 0; i < spaceForMergedPairsF; i++) mergedNum.add(mergedPairF[i]);
    int mergedPosT = mergedNum.size();

    long[] mergedPairT =
        new long
            [(arriveNumLen - arrivePosPairsT)
                + (tmpNumLen - tmpPosPairsT)
                + Math.min(arrivePosPairsT - arrivePosT, tmpPosPairsT - tmpPosT)];
    int mergedPairsT = 0;
    lastV = 0;
    cntV = 0;
    lastCount = 0;
    cntCount = 0;
    numIndexArrive = 0;
    numIndexTmp = 0;
    pairIDArrive = 0;
    pairIDTmp = 0; // System.out.println();
    while (arrivePosT + numIndexArrive < arrivePosPairsT || numIndexTmp < tmpPosPairsT - tmpPosT) {
      if (arrivePosT + numIndexArrive == arrivePosPairsT
          || (numIndexTmp < tmpPosPairsT - tmpPosT
              && tmpNum[tmpPosT + numIndexTmp] < arriveNum[arrivePosT + numIndexArrive])) {
        cntV = tmpNum[tmpPosT + numIndexTmp];
        if (checkSomeIndexIsNextPair(tmpNum, tmpPosPairsT, tmpNumLen, pairIDTmp, numIndexTmp)) {
          cntCount = calcCount(getIndexCountInNum(tmpNum, tmpPosPairsT, pairIDTmp));
          pairIDTmp++;
        } else cntCount = 1;
        numIndexTmp++;
        checkTmpActualSize += cntCount;
      } else {
        cntV = arriveNum[arrivePosT + numIndexArrive];
        if (checkSomeIndexIsNextPair(
            arriveNum, arrivePosPairsT, arriveNumLen, pairIDArrive, numIndexArrive)) {
          cntCount = calcCount(getIndexCountInNum(arriveNum, arrivePosPairsT, pairIDArrive));
          pairIDArrive++;
        } else cntCount = 1;
        numIndexArrive++;
        checkArriveActualSize += cntCount;
      }

      if (cntV != lastV) {
        if (lastCount != 0) {
          mergedNum.add(lastV);
          maxPairTAfterLastCheck =
              Math.max(maxPairTAfterLastCheck, (long) lastCount << ((LV - 1) + 1));
          if (lastCount >= 2)
            setIndexCountInNum(
                mergedPairT,
                0,
                mergedPairsT++,
                calcIndexCount(1 + mergedNum.size() - mergedPosT - 1, lastCount));
        }
        lastV = cntV;
        lastCount = cntCount;
      } else lastCount += cntCount;
    }
    if (lastCount != 0) {
      mergedNum.add(lastV);
      maxPairTAfterLastCheck = Math.max(maxPairTAfterLastCheck, (long) lastCount << (LV - 1));
      if (lastCount >= 2)
        setIndexCountInNum(
            mergedPairT,
            0,
            mergedPairsT++,
            calcIndexCount(1 + mergedNum.size() - mergedPosT - 1, lastCount));
    }

    if (checkArriveActualSize != arriveActualSize)
      System.out.println(
          "\t\t!![ERR]\tcheckArriveActualSize:"
              + checkArriveActualSize
              + " != "
              + arriveActualSize);
    if (checkTmpActualSize != tmpLV.actualSize)
      System.out.println(
          "\t\t!![ERR]\tcheckTmpActualSize:" + checkTmpActualSize + " != " + tmpLV.actualSize);
    if (DEBUG)
      System.out.println(
          "\t\t\t\tCorrectActualSize. arrive:"
              + checkArriveActualSize
              + "\ttmp:"
              + checkTmpActualSize);

    int spaceForMergedPairsT = (mergedPairsT + 1) / 2;
    int mergedPosPairsT = mergedNum.size();
    for (int i = 0; i < spaceForMergedPairsT; i++) mergedNum.add(mergedPairT[i]);

    TmpLevel mergedLV =
        new TmpLevel(
            mergedNum.toLongArray(),
            mergedPosPairsF,
            mergedPosT,
            mergedPosPairsT,
            tmpLV.actualSize + arriveActualSize);

    tmpSketch.put(LV, mergedLV);
    tmpSketchActualSize += arriveActualSize;
    tmpSketchNumLen += mergedLV.num.length - tmpNumLen;
    if (DEBUG) checkTmpSketchLevel(LV);
  }

  void mergeLVtoTmpSketch(int LV) {
    if (LV >= cntLevel || getLevelSize(LV) == 0) return;
    if (DEBUG) System.out.println("\t[DEBUG]\tmergeLVtoTmpSketch\tLV=" + LV);
    // System.out.println("\t\tbefore mergeLVtoTmpSketch
    // totSize:\t"+getTotalActualSize()+"\t\ttmpLVNumLen:"+(tmpSketch.containsKey(LV)?tmpSketch.get(LV).num.length:0));
    long[] tmpNum = Arrays.copyOfRange(num, levelPos[LV], levelPos[LV + 1]);
    int posPairsF = lvPosPairsF[LV] - levelPos[LV],
        posT = lvPosT[LV] - levelPos[LV],
        posPairsT = lvPosPairsT[LV] - levelPos[LV];
    mergeNumToTmpSketch(tmpNum, tmpNum.length, posPairsF, posT, posPairsT, lvActualSize[LV], LV);
    int delta = getLevelSize(LV);
    for (int i = levelPos[LV + 1] - 1; i >= bufferPosForLV0 + delta; i--) num[i] = num[i - delta];
    bufferPosForLV0 += delta;
    for (int i = 0; i < LV; i++) {
      levelPos[i] += delta;
      lvPosPairsF[i] += delta;
      lvPosT[i] += delta;
      lvPosPairsT[i] += delta;
    }
    levelPos[LV] = lvPosPairsF[LV] = lvPosT[LV] = lvPosPairsT[LV] = levelPos[LV + 1];
    lvActualSize[LV] = 0;
    // System.out.println("\t\tafter mergeLVtoTmpSketch
    // totSize:\t"+getTotalActualSize()+"\t\ttmpLVNumLen:"+(tmpSketch.containsKey(LV)?tmpSketch.get(LV).num.length:0));
    if (DEBUG) checkTotalN();
    if (DEBUG) showTmpSketch();
    // if(DEBUG)showNum();
  }

  void checkTmpSketchLevel(int LV) {
    int pair = 0, lvSize = 0;
    TmpLevel tmpLV = tmpSketch.get(LV);
    long[] tmpNum = tmpLV.num;
    for (int j = 0; j < tmpLV.posPairsF; j++) {
      int count = 1;
      if (checkSomeIndexIsNextPair(tmpNum, tmpLV.posPairsF, tmpLV.posT, pair, j))
        count = calcCount(getIndexCountInNum(tmpNum, tmpLV.posPairsF, pair++));
      lvSize += count;
    }
    pair = 0;
    for (int j = tmpLV.posT; j < tmpLV.posPairsT; j++) {
      int count = 1;
      if (checkSomeIndexIsNextPair(tmpNum, tmpLV.posPairsT, tmpNum.length, pair, j - tmpLV.posT))
        count = calcCount(getIndexCountInNum(tmpNum, tmpLV.posPairsT, pair++));
      lvSize += count;
    }
    if (lvSize != tmpLV.actualSize)
      System.out.println(
          "!![ERR]tmpSketch lvActualSize is wrong. LV:"
              + LV
              + "\tcount="
              + lvSize
              + "\ttmpLV.actualSize="
              + tmpLV.actualSize);
    // else System.out.println("\t\t\t\t[DEBUG] CorrectTmpSketchLevel
    // LV="+LV+"\tactualSize="+lvSize);
  }

  void mergeTmpSketchIntoNum() {
    // int maxLV= Math.max(cntLevel-1,Collections.max(tmpSketch.keySet()))+1;
    // if(maxLV>cntLevel)calcLevelMaxSize(maxLV);
    // System.out.println(" mergeTmpSketchIntoNum maxLV:"+(cntLevel-1));
    if (DEBUG) checkTotalN();
    int[] newLevelPos = Arrays.copyOf(levelPos, cntLevel + 1);
    int cntPos =
        newLevelPos[0] = bufferPosForLV0 = maxMemoryNumForSketch - getNumLen() - tmpSketchNumLen;
    int maxLevelToMerge = Collections.max(tmpSketch.keySet()) + 1;
    for (int LV = 0; LV < maxLevelToMerge /* cntLevel */; LV++) {
      if (tmpSketch.containsKey(LV)) {
        // if(getLevelSize(LV)>0||lvActualSize[LV]>0)System.out.println("!![ERR]mergeTmpSketchIntoNum
        // LV="+LV+" in BOTH tmpSketch AND num."+"\tnumLevelSize:"+getLevelSize(LV));
        TmpLevel tmpLV = tmpSketch.get(LV);
        lvPosPairsF[LV] = cntPos + tmpLV.posPairsF;
        lvPosT[LV] = cntPos + tmpLV.posT;
        lvPosPairsT[LV] = cntPos + tmpLV.posPairsT;
        lvActualSize[LV] = tmpLV.actualSize;
        System.arraycopy(tmpLV.num, 0, num, cntPos, tmpLV.num.length);
        cntPos += tmpLV.num.length;
      } else {
        lvPosPairsF[LV] = cntPos + lvPosPairsF[LV] - levelPos[LV];
        lvPosT[LV] = cntPos + lvPosT[LV] - levelPos[LV];
        lvPosPairsT[LV] = cntPos + lvPosPairsT[LV] - levelPos[LV];
        System.arraycopy(num, levelPos[LV], num, cntPos, getLevelSize(LV));
        cntPos += getLevelSize(LV);
      }
      newLevelPos[LV + 1] = cntPos;
    }
    levelPos = newLevelPos;
  }

  private void compactOneTmpLevel(int LV) {
    if (LV + 1 >= cntLevel) calcLevelMaxSize(LV + 2);
    if (DEBUG) System.out.println("\t[DEBUG]\tcompactOneTmpLevel\tLV=" + LV);
    addRecordInLevel(LV);
    TmpLevel tmpLV = tmpSketch.get(LV);
    long[] tmpNum = tmpLV.num;
    int tmpPosPairsF = tmpLV.posPairsF,
        tmpPosT = tmpLV.posT,
        tmpPosPairsT = tmpLV.posPairsT,
        tmpNumLen = tmpNum.length;

    int reservedAnItem = 0;
    long reservedNum = -233;
    boolean reservedF = false;
    if ((tmpLV.actualSize & 1) == 1) { // reserve a minimal item
      reservedAnItem = 1;
      if (tmpPosT == tmpNumLen || (0 < tmpPosT && tmpNum[0] < tmpNum[tmpPosT])) {
        reservedF = true;
        reservedNum = tmpNum[0];
      } else {
        reservedF = false;
        reservedNum = tmpNum[tmpPosT];
        maxPairTAfterLastCheck = Math.max(maxPairTAfterLastCheck, 1L << (LV - 1));
      }
      // System.out.println("\t\t[DEBUG]
      // tmpSketch_LV:"+LV+"\treservedNum:"+longToResult(reservedNum));
    }

    long cntV = 0;
    int cntCount = 0, pairIDF = 0, pairIDT = 0, posF = 0, posT = tmpPosT;
    int rand01 = getNextRand01();
    boolean cntIsF = false;
    boolean hasLastDropped = false, lastDroppedIsF = false;
    long lastDroppedNum = 0;
    int totCntCount = 0;
    // System.out.println("\t\t\t\t\t\trand01:\t"+rand01);

    LongArrayList proNumF = new LongArrayList();
    IntArrayList proPairF = new IntArrayList(); // System.out.println("");
    LongArrayList proNumT = new LongArrayList();
    IntArrayList proPairT = new IntArrayList(); // System.out.println("");
    int proActualSize = tmpLV.actualSize / 2;
    while (posF < tmpPosPairsF || posT < tmpPosPairsT) {
      if (posF == tmpPosPairsF || (posT < tmpPosPairsT && tmpNum[posT] < tmpNum[posF])) {
        cntV = tmpNum[posT];
        if (checkSomeIndexIsNextPair(tmpNum, tmpPosPairsT, tmpNumLen, pairIDT, posT - tmpPosT)) {
          cntCount = calcCount(getIndexCountInNum(tmpNum, tmpPosPairsT, pairIDT));
          pairIDT++;
        } else cntCount = 1;
        cntIsF = false;
        posT++;
        // System.out.println("\t\t\t\tin compaction a num from tmpNum[posT].
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount +"\tLV:"+LV);
      } else {
        cntV = tmpNum[posF];
        if (checkSomeIndexIsNextPair(tmpNum, tmpPosPairsF, tmpPosT, pairIDF, posF)) {
          cntCount = calcCount(getIndexCountInNum(tmpNum, tmpPosPairsF, pairIDF));
          pairIDF++;
        } else cntCount = 1;
        cntIsF = true;
        posF++;
        // System.out.println("\t\t\t\tin compaction a num from tmpNum[posF].
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount +"\tLV:"+LV);
      }
      if (reservedAnItem > 0 && cntIsF == reservedF && cntV == reservedNum) cntCount--;
      if (cntCount == 0) continue;
      int proCount = (cntCount - (rand01 ^ (totCntCount & 1)) + 1) / 2;
      // System.out.println("\t\tcompacting tmpLV. V:"+longToResult(cntV)+"
      // cntCount:"+cntCount+" totCntCount:"+totCntCount+"\tproCount:"+proCount);
      // case1: 0F 0T 0T 0T, rand01=1 ==> 0F 0T; When process 0T, there is lastDropped
      // 0F.
      // case2: 0F 0T 0T 1T, rand01=0 ==> 0F 0F
      if (proCount > 0) {
        if (cntIsF) {
          proNumF.add(cntV);
          if (proNumF.size() >= 2 && proNumF.getLong(proNumF.size() - 2) == cntV)
            System.out.println(
                "\t\t[ERR] LV:"
                    + LV
                    + " ?????SAME proNumF VALUE when cntIsF=TRUE"
                    + "\t\tcheck num:"
                    + longToResult(tmpNum[posF - 2])
                    + " "
                    + longToResult(tmpNum[posF - 1])
                    + "  cntV:"
                    + longToResult(cntV));
          if (proCount >= 2) proPairF.add(calcIndexCount(1 + proNumF.size() - 1, proCount));
        } else { // rand01==1 case2_condition <==> proCount*2>cntCount <==> hasLastDropped
          if (hasLastDropped
              || proCount * 2 > cntCount - (totCntCount & 1 & (rand01 ^ 1))) { // first or last
            // propagated num should
            // be F instead of T,
            // i.e., case 1 or case 2.
            // They don't happen at
            // the same time.
            if (proPairF.size() > 0
                && cntV == proNumF.getLong(proNumF.size() - 1)
                && calcIndex(proPairF.getInt(proPairF.size() - 1)) - 1 == proNumF.size() - 1) {
              int IC = proPairF.getInt(proPairF.size() - 1);
              proPairF.set(proPairF.size() - 1, IC + 1);
            } else if (proNumF.size() > 0 && cntV == proNumF.getLong(proNumF.size() - 1)) {
              proPairF.add(calcIndexCount(1 + proNumF.size() - 1, 2));
            } else {
              proNumF.add(cntV);
              if (proNumF.size() >= 2 && proNumF.getLong(proNumF.size() - 2) == cntV)
                System.out.println("\t\t[ERR]?????SAME proNumF VALUE when cntIsF=TRUE");
            }
            proCount--;
          }
          if (proCount > 0) {
            proNumT.add(cntV);
            if (proNumT.size() >= 2 && proNumT.getLong(proNumT.size() - 2) == cntV)
              System.out.println(
                  "\t\t[ERR] LV:"
                      + LV
                      + " ?????SAME proNumF VALUE when cntIsF=FALSE"
                      + "\t\tcheck num:"
                      + longToResult(tmpNum[posF - 2])
                      + " "
                      + longToResult(tmpNum[posF - 1])
                      + "  cntV:"
                      + longToResult(cntV));
            if (proCount >= 2) proPairT.add(calcIndexCount(1 + proNumT.size() - 1, proCount));
            maxPairTAfterLastCheck =
                Math.max(maxPairTAfterLastCheck, (long) proCount << ((LV - 1) + 1));
          }
        }
      }
      totCntCount += cntCount;
      if ((totCntCount & 1) == 1 && rand01 == 1) {
        hasLastDropped = true;
        lastDroppedNum = cntV;
        lastDroppedIsF = cntIsF;
      } // record the 0F in case 1
      else hasLastDropped = false;
    }
    if (totCntCount + reservedAnItem != tmpLV.actualSize)
      System.out.println(
          "!![ERR] tmpLV.actualSize is wrong. mergedTMPLV="
              + LV
              + " tmpLV.actualSize="
              + tmpLV.actualSize
              + " actualSize="
              + (totCntCount + reservedAnItem));
    int spaceForProPairF = (proPairF.size() + 1) / 2;
    int spaceForProPairT = (proPairT.size() + 1) / 2;
    // System.out.println("\t\t\t\tspaceForProPairF,T:"+spaceForProPairF+","+spaceForProPairT+"\ttotCntCount(exclude
    // reserved):"+totCntCount);
    LongArrayList proNum =
        new LongArrayList(proNumF.size() + spaceForProPairF + proNumT.size() + spaceForProPairT);

    proNum.addAll(proNumF);
    for (int i = 0; i < spaceForProPairF; i++) {
      int IC0 = proPairF.getInt(i * 2);
      int IC1 = (i * 2 + 1) < proPairF.size() ? proPairF.getInt(i * 2 + 1) : 0;
      proNum.add((((long) IC0 << 32) & mask32From64_0) | (IC1 & mask32From64_1));
    }
    proNum.addAll(proNumT);
    for (int i = 0; i < spaceForProPairT; i++) {
      int IC0 = proPairT.getInt(i * 2);
      int IC1 = (i * 2 + 1) < proPairT.size() ? proPairT.getInt(i * 2 + 1) : 0;
      proNum.add((((long) IC0 << 32) & mask32From64_0) | (IC1 & mask32From64_1));
      // System.out.println("\t\t??\t\t"+(calcIndex()-1));
    }
    if (DEBUG) {
      int actualSizeInProNum = proNumF.size() + proNumT.size();
      long[] proNumArray = proNum.toLongArray();
      for (int i = 0; i < proPairF.size(); i++) {
        System.out.print(
            "\tF("
                + (calcIndex(getIndexCountInNum(proNumArray, proNumF.size(), i)) - 1)
                + ","
                + calcCount(getIndexCountInNum(proNumArray, proNumF.size(), i))
                + ")");
        actualSizeInProNum += calcCount(getIndexCountInNum(proNumArray, proNumF.size(), i)) - 1;
      }
      for (int i = 0; i < proPairT.size(); i++) {
        System.out.print(
            "\tT("
                + (calcIndex(
                        getIndexCountInNum(
                            proNumArray, proNumF.size() + spaceForProPairF + proNumT.size(), i))
                    - 1)
                + ","
                + calcCount(
                    getIndexCountInNum(
                        proNumArray, proNumF.size() + spaceForProPairF + proNumT.size(), i))
                + ")");
        actualSizeInProNum +=
            calcCount(
                    getIndexCountInNum(
                        proNumArray, proNumF.size() + spaceForProPairF + proNumT.size(), i))
                - 1;
      }
      System.out.println("");
      System.out.println("\tactualSizeInProNum:" + actualSizeInProNum);
    }

    if (reservedAnItem == 1) {
      // System.out.println("?????!!!!reservedAnItem! "+longToResult(reservedNum));
      long[] resNum = new long[] {reservedNum};
      int resPosPairsF, resPosT, resPosPairsT;
      if (reservedF) resPosPairsF = resPosT = resPosPairsT = 1;
      else {
        resPosPairsF = resPosT = 0;
        resPosPairsT = 1;
      }
      tmpSketch.put(LV, new TmpLevel(resNum, resPosPairsF, resPosT, resPosPairsT, 1));
      tmpSketchActualSize -= tmpLV.actualSize - 1;
      tmpSketchNumLen -= tmpNumLen - 1;
    } else {
      tmpSketch.remove(LV);
      tmpSketchActualSize -= tmpLV.actualSize;
      tmpSketchNumLen -= tmpNumLen;
    }

    mergeNumToTmpSketch(
        proNum.toLongArray(),
        proNum.size(),
        proNumF.size(),
        proNumF.size() + spaceForProPairF,
        proNumF.size() + spaceForProPairF + proNumT.size(),
        proActualSize,
        LV + 1);
    if (LV + 1 < cntLevel && getLevelSize(LV + 1) > 0) {
      mergeLVtoTmpSketch(LV + 1);
    }
    if (DEBUG) checkTmpSketchLevel(LV + 1);
    if (DEBUG) showTmpSketch();
  }

  void compactOneLevelToTmpSketch(int LV) {
    mergeLVtoTmpSketch(LV);
    compactOneTmpLevel(LV);
  }

  void showTmpSketch() {
    System.out.print("\tshowTmpSketch:\t");
    for (int i = 0; i < cntLevel; i++)
      if (tmpSketch.containsKey(i))
        System.out.print(
            "\t[\ttmpLV:"
                + i
                + "\ttmpLevelNumLen:"
                + tmpSketch.get(i).num.length
                + "\ttmpLevelActualSize:"
                + tmpSketch.get(i).actualSize
                + "\t]\t");
    System.out.println();
  }

  private void compactOneLevel(int level) {
    // System.out.println("\t\t[KLLPair]compactOneLevel LV:\t"+level);
    if (level == cntLevel - 1) calcLevelMaxSize(cntLevel + 1);
    int L1 = levelPos[level], R1 = lvPosPairsF[level];
    // long lvMinV=Math.max(levelPos[level]<lvPosPairsF[level]?);
    addRecord(false, Long.MIN_VALUE, Long.MAX_VALUE, level);
    int actualSize =
        lvPosPairsF[level]
            - levelPos[level]
            + lvPosPairsT[level]
            - lvPosT[level]; // System.out.println("\t\t\t\t..\tactualSize:"+actualSize);
    for (int i = lvPosPairsF[level]; i < lvPosT[level]; i++) {
      actualSize += Math.max(0, ((num[i] >>> 32) & maskCountInPair) - 1);
      actualSize += Math.max(0, ((num[i] & mask32From64_1) & maskCountInPair) - 1);
      // System.out.println("\t\t\t\t.?\tpairF :"+((num[i]>>>32)&maskCountInPair)+"
      // "+((num[i]&mask32From64_1)&maskCountInPair));
    }
    for (int i = lvPosPairsT[level]; i < levelPos[level + 1]; i++) {
      actualSize += Math.max(0, ((num[i] >>> 32) & maskCountInPair) - 1);
      actualSize += Math.max(0, ((num[i] & mask32From64_1) & maskCountInPair) - 1);
      // System.out.println("\t\t\t\t.?\tpairT :"+((num[i]>>>32)&maskCountInPair)+"
      // "+((num[i]&mask32From64_1)&maskCountInPair));
    }
    // System.out.println("\t\t\tLV"+level+"\t??\tactualSize:"+actualSize);
    // for(int
    // i=0;i<getPairsInLevel(level);i++)actualSize+=calcCount(getIndexCountInNum(num,levelPosForPairs[level],i))-1;
    lvActualSize[level + 1] += actualSize / 2;
    lvActualSize[level] &= 1; // may reservedAnItem

    int reservedAnItem = 0;
    long reservedNum = -233;
    boolean reservedF = false;
    if ((actualSize & 1) == 1) { // reserve a minimal item
      reservedAnItem = 1;
      if (lvPosT[level] == levelPos[level + 1]
          || (levelPos[level] < lvPosT[level] && num[levelPos[level]] < num[lvPosT[level]])) {
        reservedF = true;
        reservedNum = num[levelPos[level]];
      } else {
        reservedF = false;
        reservedNum = num[lvPosT[level]];
      }
      // System.out.println("\t\t[DEBUG]
      // LV:"+level+"\treservedNum:"+longToResult(reservedNum));
    }

    long cntV = 0;
    int cntCount = 0, pairIDF = 0, pairIDT = 0, posF = levelPos[level], posT = lvPosT[level];
    int proIndexT = 0, proPairsT = 0;
    int rand01 = getNextRand01();
    boolean cntIsF = false;
    boolean hasLastDropped = false, lastDroppedIsF = false;
    long lastDroppedNum = 0;
    int totCntCount = 0;
    // System.out.println("\t\t\t\t\t\trand01:\t"+rand01);

    LongArrayList proNumF = new LongArrayList();
    IntArrayList proPairF = new IntArrayList(); // System.out.println("");
    while (posF < lvPosPairsF[level] || posT < lvPosPairsT[level]) {
      if (posF == lvPosPairsF[level] || (posT < lvPosPairsT[level] && num[posT] < num[posF])) {
        cntV = num[posT];
        if (checkSomeIndexIsNextPairT(level, pairIDT, posT - lvPosT[level])) {
          cntCount = calcCount(getIndexCountInNum(num, lvPosPairsT[level], pairIDT));
          pairIDT++;
        } else cntCount = 1;
        cntIsF = false;
        posT++;
        // System.out.println("\t\t\t\tin compaction a num from num[posT].
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount +"\tLV:"+level);
      } else {
        cntV = num[posF];
        if (checkSomeIndexIsNextPairF(level, pairIDF, posF - levelPos[level])) {
          cntCount = calcCount(getIndexCountInNum(num, lvPosPairsF[level], pairIDF));
          pairIDF++;
        } else cntCount = 1;
        cntIsF = true;
        posF++;
        // System.out.println("\t\t\t\tin compaction a num from num[posF].
        // cntV,Count:\t"+longToResult(cntV)+","+cntCount +"\tLV:"+level);
      }
      if (reservedAnItem > 0 && cntIsF == reservedF && cntV == reservedNum) cntCount--;
      if (cntCount == 0) continue;
      int proCount = (cntCount - (rand01 ^ (totCntCount & 1)) + 1) / 2;
      // case1: 0F 0T 0T 0T, rand01=1 ==> 0F 0T; When process 0T, there is lastDropped
      // 0F.
      // case2: 0F 0T 0T 1T, rand01=0 ==> 0F 0F
      if (proCount > 0) {
        if (cntIsF) {
          proNumF.add(cntV);
          if (proNumF.size() >= 2 && proNumF.getLong(proNumF.size() - 2) == cntV)
            System.out.println(
                "\t\t[ERR] LV:"
                    + level
                    + " ?????SAME proNumF VALUE when cntIsF=TRUE"
                    + "\t\tcheck num:"
                    + longToResult(num[posF - 2])
                    + " "
                    + longToResult(num[posF - 1])
                    + "  cntV:"
                    + longToResult(cntV));
          if (proCount >= 2) proPairF.add(calcIndexCount(1 + proNumF.size() - 1, proCount));
        } else { // rand01==1 case2_condition <==> proCount*2>cntCount <==> hasLastDropped
          if (hasLastDropped
              || proCount * 2 > cntCount - (totCntCount & 1 & (rand01 ^ 1))) { // first or last
            // propagated num should
            // not be T, i.e., case 1
            // or case 2. They don't
            // happen at the same
            // time.
            if (proPairF.size() > 0
                && calcIndex(proPairF.getInt(proPairF.size() - 1)) == proNumF.size()) {
              int IC = proPairF.getInt(proPairF.size() - 1);
              proPairF.set(proPairF.size() - 1, IC + 1);
            } else if (proNumF.size() > 0 && cntV == proNumF.getLong(proNumF.size() - 1)) {
              proPairF.add(calcIndexCount(1 + proNumF.size() - 1, 2));
            } else {
              proNumF.add(cntV);
              if (proNumF.size() >= 2 && proNumF.getLong(proNumF.size() - 2) == cntV)
                System.out.println("\t\t[ERR]?????SAME proNumF VALUE when cntIsF=TRUE");
            }
            proCount--;
          }
          if (proCount > 0) {
            num[lvPosT[level] + proIndexT] = cntV;
            if (proCount >= 2) {
              setIndexCountInNum(
                  num, lvPosPairsT[level], proPairsT, calcIndexCount(1 + proIndexT, proCount));
              proPairsT++;
            }

            proIndexT++;
          }
        }
      }
      totCntCount += cntCount;
      if ((totCntCount & 1) == 1 && rand01 == 1) {
        hasLastDropped = true;
        lastDroppedNum = cntV;
        lastDroppedIsF = cntIsF;
      } // record the 0F in case 1
      else hasLastDropped = false;
      // System.out.print("\t\t\t\tcntV,C:"+longToResult(cntV)+","+cntCount);
      // System.out.print("\tproNumF:");for(long x:proNumF)System.out.print("
      // "+longToResult(x));
      // System.out.print("\tpairF:");for(int x:proPairF)System.out.print("
      // <"+(calcIndex(x)-1)+","+calcCount(x)+">");
      // System.out.print("\tproNumT:");for(int i=0;i<proIndexT;i++)System.out.print("
      // "+longToResult(num[lvPosT[level]+i]));
      // System.out.print("\tpairT:");for(int i=0;i<proPairsT;i++){int IC =
      // getIndexCountInNum(num,lvPosPairsT[level],i);System.out.print("
      // <"+(calcIndex(IC)-1)+","+calcCount(IC)+">");}
      // System.out.println();
    }

    int spaceForProPairF = (proPairF.size() + 1) / 2;
    int spaceForProPairT = (proPairsT + 1) / 2;
    LongArrayList proNum =
        new LongArrayList(proNumF.size() + spaceForProPairF + proIndexT + spaceForProPairT);

    proNum.addAll(proNumF);
    for (int i = 0; i < spaceForProPairF; i++) {
      int IC0 = proPairF.getInt(i * 2);
      int IC1 = (i * 2 + 1) < proPairF.size() ? proPairF.getInt(i * 2 + 1) : 0;
      proNum.add((long) IC0 << 32 | IC1);
    }
    for (int i = 0; i < proIndexT; i++) proNum.add(num[lvPosT[level] + i]);
    for (int i = 0; i < spaceForProPairT; i++) {
      long tmp = num[lvPosPairsT[level] + i];
      if (i * 2 + 1 >= proPairsT) {
        tmp &= mask32From64_0;
      }
      proNum.add(tmp);
    }

    if (reservedAnItem == 1) {
      // System.out.println("?????!!!!reservedAnItem! "+longToResult(reservedNum));
      num[levelPos[level]] = reservedNum;
      if (reservedF) {
        lvPosPairsF[level] = lvPosT[level] = lvPosPairsT[level] = levelPos[level] + 1;
      } else {
        lvPosPairsF[level] = lvPosT[level] = levelPos[level];
        lvPosPairsT[level] = levelPos[level] + 1;
      }
    } else lvPosPairsF[level] = lvPosT[level] = lvPosPairsT[level] = levelPos[level];
    // System.out.println("\t[compactOneLevel]LV:"+level+"\tafter propagation.
    // numF:"+proNumF.size()+"\tpairsF:"+proPairF.size()+"\t"+"\tnumT:"+proIndexT+"\tpairsT:"+proPairsT+"\t|ProNum|:"+proNum.size());
    // System.out.println("\t\tN:"+N);
    // for(int
    // i=0;i<proNumF.size()-1;i++)if(proNumF.getLong(i)==proNumF.getLong(i+1))System.out.println("\t\t[ERR]
    // SAME VALUE in proNumF");
    // for(int
    // i=proNumF.size()+spaceForProPairF+0;i<proNumF.size()+spaceForProPairF+proIndexT;i++)if(proNum.getLong(i)==proNum.getLong(i+1))System.out.println("\t\t[ERR]
    // SAME VALUE in proNumT");
    // System.out.print("\tInCompaction Before Merge. Bottom Sketch
    // [0...LV]:");showBottomNum(level+1);
    // // todo: check if proNum is too large to store
    // if(bufferPosForLV0+levelPos[level+1]-(levelPos[level]+reservedAnItem)<proNum.size()){
    // // might be too large
    // // why too large: some numT with odd count are propagated to be numF
    // IntArrayList TCounts = new IntArrayList();
    //// for(int )
    // }

    boolean mergeMayExceedCurrentLevel =
        proNum.size() > (levelPos[level + 1] - (levelPos[level] + reservedAnItem));
    // System.out.println("\t\t\t\t???\t\tbefore merge to next LV"+(level+1)+"..
    // cntLVPos:
    // "+levelPos[level]+"\tnextLVPos:"+levelPos[level+1]+"\t\t\tlvPos[0]:"+levelPos[0]+"\t\tmergeMayExceedCurrentLevel:"+mergeMayExceedCurrentLevel);
    // if(!mergeMayExceedCurrentLevel)
    mergePairsToLevel(
        proNum.toLongArray(),
        proNum.size(),
        proNumF.size(),
        proNumF.size() + spaceForProPairF,
        proNumF.size() + spaceForProPairF + proIndexT,
        level + 1,
        levelPos[level] + reservedAnItem);
    // else{
    // int delta = bufferPosForLV0;
    // System.out.println("\t\t\t\tTryToShiftForSpace.\tdelta:"+delta+"\t\tOLDStartFreePos:"+(levelPos[level]+reservedAnItem));
    // for (int i = 0; i <levelPos[level]+reservedAnItem-delta; i++) num[i] = num[i
    // + delta];
    // bufferPosForLV0 = 0;
    // for (int i = 0; i <= level; i++) {
    // levelPos[i] -= delta;
    // lvPosPairsF[i] -= delta;
    // lvPosT[i] -= delta;
    // lvPosPairsT[i] -= delta;
    // }
    // System.out.println("\t\t\t\tTryToShiftForSpace.NEWStartFreePos:"+(levelPos[level]+reservedAnItem));
    // mergePairsToLevel(
    // proNum.toLongArray(),proNum.size(),
    // proNumF.size(),proNumF.size()+spaceForProPairF,
    // proNumF.size()+spaceForProPairF+proIndexT,
    // level+1,levelPos[level]+reservedAnItem);
    // }
    // System.out.println("\t\t\t\t???\t\tafter merge to next LV.. cntLVPos:
    // "+levelPos[level]+"\tnextLVPos:"+levelPos[level+1]);
  }

  private long getFreqMinimalThreshold() {
    // return N+1;
    // return (4L<<(cntLevel-1));
    return (1L << (cntLevel - 1)) + 1;
  } // todo 根据数据的具体内存占用来决定，比如有的数据可能次数多但是只有一个pair，那挪出去不省空间

  private boolean needToCheckFreqValue() {
    return (lastUpdateFreqLevel < cntLevel)
        || (lastUpdateFreqLevel <= cntLevel
            && newSketchNAfterLastCheck >= getFreqMinimalThreshold()
            && !frozenCheckingHashMapForNonFreqValue
            && maxPairTAfterLastCheck >= getFreqMinimalThreshold());
    //    return lastUpdateFreqLevel <= cntLevel && newSketchNAfterLastCheck >=
    // getFreqMinimalThreshold() / 2
    //            && !frozenCheckingHashMapForNonFreqValue;
  }

  private void checkFreqValue() {
    checkFreqValueCount++;
    lastUpdateFreqLevel = cntLevel;
    lastCheckFreqValueN = N;
    newSketchNAfterLastCheck = 0;
    maxPairTAfterLastCheck = 0;
    Long2LongOpenHashMap fullFreqValueCount =
        freqValueCount != null
            ? new Long2LongOpenHashMap(freqValueCount)
            : new Long2LongOpenHashMap();
    long cntValue, cntCount;
    for (int lv = 0; lv < cntLevel; lv++)
      for (int i = lvPosT[lv], pair = 0; i < lvPosPairsT[lv]; i++) {
        cntValue = num[i];
        if (checkSomeIndexIsNextPairT(lv, pair, i - lvPosT[lv]))
          cntCount = calcCount(getIndexCountInNum(num, lvPosPairsT[lv], pair++));
        else cntCount = 1;
        fullFreqValueCount.addTo(cntValue, cntCount << lv);
        // System.out.println("\t\t\t\tV:"+longToResult(cntValue)+"\tC:"+(cntCount <<
        // lv));
      }

    int freqThresholdRatio = 1;
    Long2LongOpenHashMap newFreqValueCount = null;
    for (; freqThresholdRatio <= MaxFreqThresholdRatio; freqThresholdRatio++) {
      if (freqThresholdRatio * getFreqMinimalThreshold() < lastThreshold) continue;
      Long2LongOpenHashMap tmpFreqValueCount = new Long2LongOpenHashMap();
      for (Long2LongMap.Entry entry : fullFreqValueCount.long2LongEntrySet())
        if (entry.getLongValue() >= freqThresholdRatio * getFreqMinimalThreshold())
          tmpFreqValueCount.put(entry.getLongKey(), entry.getLongValue());
      if (tmpFreqValueCount.isEmpty()) {
        // compact();
        // System.out.println("\t[checkFreqValue] Empty NewFreq. no-op. exit.");
        return;
        // TODO: if old values do not meet the threshold, should clear freqValueCount.
      }
      int tmpMaxMemoryNumForSketch =
          (maxMemoryByte
                  - 2
                      * 8
                      * HashCommon.arraySize(
                          tmpFreqValueCount.size(), Long2LongOpenHashMap.DEFAULT_LOAD_FACTOR))
              * 8
              / BitsPerItem;
      int tmpSketchNumLen = 0;
      for (int lv = 0; lv < cntLevel; lv++) {
        tmpSketchNumLen += lvPosT[lv] - levelPos[lv];
        int tmpLevelPairT = 0, tmpIsPairT;
        for (int i = lvPosT[lv], pair = 0; i < lvPosPairsT[lv]; i++) {
          cntValue = num[i];
          tmpIsPairT = 0;
          if (checkSomeIndexIsNextPairT(lv, pair, i - lvPosT[lv])) {
            tmpIsPairT = 1;
            pair++;
          }
          if (!tmpFreqValueCount.containsKey(cntValue)) {
            tmpSketchNumLen++;
            tmpLevelPairT += tmpIsPairT;
          }
        }
        tmpSketchNumLen += (tmpLevelPairT + 1) / 2;
      }
      // todo: add up tmpSketchNumLen for some non-freq in hashmap (will be moved to
      // lv0 pairs).
      // System.out.println("\t\ttry
      // threshold:"+freqThresholdRatio*getFreqMinimalThreshold()+"\tnewNumLen:"+tmpSketchNumLen+"\tshould
      // not exceed "+tmpMaxMemoryNumForSketch);
      if (tmpSketchNumLen < tmpMaxMemoryNumForSketch) {
        newFreqValueCount = tmpFreqValueCount;
        break;
      }
    }
    if (freqThresholdRatio > MaxFreqThresholdRatio) {
      // System.out.println("\t[checkFreqValue] NewFreq makes newNum too large. no-op.
      // exit.");
      // System.out.print("\t[checkFreqValue] Sketch:");showNum();
      return;
    }
    maxMemoryNumForSketch =
        (maxMemoryByte
                - 2
                    * 8
                    * HashCommon.arraySize(
                        newFreqValueCount.size(), Long2LongOpenHashMap.DEFAULT_LOAD_FACTOR))
            * 8
            / BitsPerItem;
    levelMaxSize = calcLevelMaxSizeByLevel(maxMemoryNumForSketch, cntLevel);
    lastThreshold = freqThresholdRatio * getFreqMinimalThreshold();
    // System.out.println("\t[checkFreqValue]\tthreshold:\t"+freqThresholdRatio*getFreqMinimalThreshold()+"\tmaxMemoryNumForSketch:"+maxMemoryNumForSketch+"\tN:"+N);
    // System.out.print("\t[checkFreqValue] Sketch:\t");showNum();
    long[] newNum = new long[maxMemoryNumForSketch];
    int[] newLevelPos = new int[cntLevel + 1],
        newLvPosPairsF = new int[cntLevel + 1],
        newLvPosT = new int[cntLevel + 1],
        newLvPosPairsT = new int[cntLevel + 1],
        newLvActualSize = new int[cntLevel + 1];

    int cntPos = bufferPosForLV0 = newLevelPos[0] = 0;
    for (int lv = 0; lv < cntLevel; lv++) {
      newLvActualSize[lv] = lvActualSize[lv];
      for (int i = levelPos[lv]; i < lvPosPairsF[lv]; i++) newNum[cntPos++] = num[i];
      newLvPosPairsF[lv] = cntPos;
      for (int i = lvPosPairsF[lv]; i < lvPosT[lv]; i++) newNum[cntPos++] = num[i];
      newLvPosT[lv] = cntPos;
      long[] tmpNewPairT = new long[levelPos[lv + 1] - lvPosPairsT[lv] + 1];
      int tmpNewPair = 0;
      for (int i = lvPosT[lv], pair = 0; i < lvPosPairsT[lv]; i++) {
        cntValue = num[i];
        cntCount = 1;
        if (checkSomeIndexIsNextPairT(lv, pair, i - lvPosT[lv]))
          cntCount = calcCount(getIndexCountInNum(num, lvPosPairsT[lv], pair++));
        // System.out.println("\t\tcntV,C:"+longToResult(cntValue)+","+cntCount);
        if (!newFreqValueCount.containsKey(cntValue)) {
          // System.out.println("\t\t\t!nonFreq
          // cntV,C:"+longToResult(cntValue)+","+cntCount);
          newNum[cntPos] = cntValue;
          if (cntCount > 1) {
            setIndexCountInNum(
                tmpNewPairT,
                0,
                tmpNewPair,
                calcIndexCount(1 + cntPos - newLvPosT[lv], (int) cntCount));
            // System.out.println("\t\t\t!!nonFreqPair
            // cntV,C:"+longToResult(cntValue)+","+cntCount+"\tINDEX:"+(cntPos -
            // newLvPosT[lv]));
            tmpNewPair++;
          }
          cntPos++;
        } else newLvActualSize[lv] -= cntCount;
      }
      newLvPosPairsT[lv] = cntPos;
      for (int i = 0; i < (tmpNewPair + 1) / 2; i++) newNum[cntPos++] = tmpNewPairT[i];
      // System.out.println("\t\t\t\t???space for newPairT:"+((tmpNewPair + 1) / 2));
      newLevelPos[lv + 1] = cntPos;
    }
    int delta = maxMemoryNumForSketch - cntPos;
    for (int i = maxMemoryNumForSketch - 1; i >= delta; i--) newNum[i] = newNum[i - delta];
    bufferPosForLV0 += delta;
    for (int i = 0; i <= cntLevel; i++) {
      newLevelPos[i] += delta;
      newLvPosPairsF[i] += delta;
      newLvPosT[i] += delta;
      newLvPosPairsT[i] += delta;
    }
    num = newNum;
    levelPos = newLevelPos;
    lvPosPairsF = newLvPosPairsF;
    lvPosT = newLvPosT;
    lvPosPairsT = newLvPosPairsT;
    lvActualSize = newLvActualSize;
    Long2LongOpenHashMap oldMap = freqValueCount; // todo: simply move to lv0 pairs.
    freqValueCount = newFreqValueCount;
    if (oldMap != null) {
      Long2LongOpenHashMap toRemoveMap = new Long2LongOpenHashMap();
      long mmp = 0, mmpDistinct = 0;
      // System.out.print("PRE checkN.\t");
      // checkN();
      for (Long2LongMap.Entry entry : oldMap.long2LongEntrySet())
        if (!freqValueCount.containsKey(entry.getLongKey())) {
          N -= entry.getLongValue();
          mmp += entry.getLongValue();
          mmpDistinct++;
          toRemoveMap.put(entry.getLongKey(), entry.getLongValue());
        }
      // System.out.print("MID checkN.\t");
      // checkN();
      // if(mmp>0)System.out.println("MID
      // toRemoveN:"+mmp+"\t\tavgCount:"+mmp/mmpDistinct);
      frozenCheckingHashMapForNonFreqValue = true;
      for (Long2LongMap.Entry entry : toRemoveMap.long2LongEntrySet()) {
        // System.out.println("\t\t!!!WARN\t delete non-freq item:\t" +
        // longToResult(entry.getLongKey()) + "," + entry.getLongValue());
        for (long j = entry.getLongValue(); j > 0; j--) update(entry.getLongKey());
        mmp -= entry.getLongValue();
      }
      frozenCheckingHashMapForNonFreqValue = false;
      // System.out.print("FINAL checkN.\t");
      // checkN();
    }
    // checkN();
  }

  private long getLevelActualSize(int lv) {
    long size = lvPosPairsF[lv] - levelPos[lv] + lvPosPairsT[lv] - lvPosT[lv];
    for (int i = 0; i < getActualLvPairsF(lv); i++)
      size += calcCount(getIndexCountInNum(num, lvPosPairsF[lv], i)) - 1;
    for (int i = 0; i < getActualLvPairsT(lv); i++)
      size += calcCount(getIndexCountInNum(num, lvPosPairsT[lv], i)) - 1;
    return size;
  }
  // @Override
  // public void compact() {
  // long totActualSize=0;
  // double cntW=1,totW=0,cntActualSizeLimit,cntCompressSizeLimit;
  // for(int i=cntLevel-1;i>=0;i--){
  // totActualSize+=lvActualSize[i];
  // totW+=cntW;
  // cntW/=3.0;// exponential actual capacity
  //// if(lvActualSize[i]!=getLevelActualSize(i))
  //// System.out.println("\t\t??? actualSize? \tlv:"+i);
  // }
  // int levelToCompact=cntLevel-1;
  // for(int i=0;i<cntLevel;i++){
  // cntW*=3.0;
  // cntActualSizeLimit=Math.max(MinimumLevelMaxSize,totActualSize*cntW/totW);
  // cntCompressSizeLimit =
  // Math.max(MinimumLevelMaxSize,maxMemoryNumForSketch*cntW/totW);
  // if(COMPACT_ACTUAL_SIZE) {
  // if ((i == 0 && LV0PairsT > bufferPosForLV0) || lvActualSize[i] >=
  // cntActualSizeLimit || i == cntLevel - 1) {
  // levelToCompact = i;
  // break;
  // }
  // }else{
  // if ((i == 0 && LV0PairsT > bufferPosForLV0) || getLevelSize(i) >=
  // cntCompressSizeLimit || i == cntLevel - 1) {
  // levelToCompact = i;
  // break;
  // }
  // }
  // }
  // compactOneLevel(levelToCompact);
  // if(levelToCompact==0)LV0PairsT=getActualLvPairsT(0);
  //
  //// checkN();
  //
  // if(ENABLE_FREQ_HASHMAP&&needToCheckFreqValue())
  // checkFreqValue();
  //// checkOrdered();
  //// showNum();
  //// checkN();
  //
  //// maxPairsTCount=0;
  //// for(int
  // i=0;i<cntLevel;i++)maxPairsTCount=Math.max(maxPairsTCount,2*(levelPos[i+1]-lvPosPairsT[i]));
  //
  //// this.showLevelMaxSize();
  //// this.showNum();
  // }

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
        "\tmaxLV=" + (cntLevel - 1) + "\tmaxERR=" + totERR + "\ttotSIGMA=" + totSIGMA);
  }

  private void addRecordInLevel(long minV, long maxV, int level) {
    int hisNum = compactionNumInLevel.getInt(level);
    hisNum++;
    compactionNumInLevel.set(level, hisNum);
  }

  private void addRecordInLevel(int level) {
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
    // System.out.println("\t\t\t\t..\t\ttmp\t\t"+sig2+"\t"+maxERR+"\t"+Pr+"\t\t\ttmpBound:\t"+tmpBound);
    lastBound = (long) tmpBound;
    lastPr = Pr;
    lastSig2 = sig2;
    lastNormalDis = dis;
    // System.out.println("\tPr:"+Pr+"\t"+"sig2:\t"+sig2+"\t\tbyInvCum:\t"+(long)tmpBound);
    return (long) tmpBound;
  }

  public static long queryRankErrBound(int[] relatedCompactNum, double Pr) {
    double sig2 = 0;
    long maxERR = 0;
    for (int i = 0; i < relatedCompactNum.length; i++)
      sig2 += 0.5 * relatedCompactNum[i] * Math.pow(2, i * 2);
    for (int i = 0; i < relatedCompactNum.length; i++) maxERR += (long) relatedCompactNum[i] << i;
    // System.out.println("\t\t\t\t\t\tqueryRankErrBound\tmaxERR:\t"+maxERR+"\t\tsig2:\t"+
    // sig2+"\t\t\n"+ Arrays.toString(relatedCompactNum));
    return queryRankErrBoundGivenParameter(sig2, maxERR, Pr);
    // NormalDistribution dis =
    // sig2==lastSig2&&lastNormalDis!=null?lastNormalDis:new NormalDistribution(0,
    // Math.sqrt(sig2));
    // if(sig2==lastSig2&&Pr==lastPr)return lastBound;
    // double tmpBound = dis.inverseCumulativeProbability(0.5*(1-Pr));
    // tmpBound=Math.min(-Math.floor(tmpBound),maxERR);
    // lastBound=(int)tmpBound;
    // lastPr=Pr;
    // lastSig2=sig2;
    // lastNormalDis=dis;
    //// System.out.println("\tPr:"+Pr+"\t"+"sig2:\t"+sig2+"\t\tbyInvCum:\t"+(long)tmpBound);
    // return (int)tmpBound;
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
    int pair = 0;
    long cntCount, rank = 0;
    for (int i = levelPos[level]; i < lvPosPairsF[level]; i++) {
      if (checkSomeIndexIsNextPairF(level, pair, i - levelPos[level])) {
        cntCount = calcCount(getIndexCountInNum(num, lvPosPairsF[level], pair));
        pair++;
      } else cntCount = 1;
      if (num[i] <= v) rank += cntCount * (1 << level);
      else break;
    }
    pair = 0;
    for (int i = lvPosT[level]; i < lvPosPairsT[level]; i++) {
      if (checkSomeIndexIsNextPairT(level, pair, i - lvPosT[level])) {
        cntCount = calcCount(getIndexCountInNum(num, lvPosPairsT[level], pair));
        pair++;
      } else cntCount = 1;
      if (num[i] <= v) rank += cntCount * (1 << level);
      else break;
    }

    return (int) (rank);
  }

  protected int findTrueCountInLevel(int level, long v) {
    if (levelPos[level] >= levelPos[level + 1]) return 0;
    if (level == 0) sortLV0();
    int pairID = 0;
    long cntCount, trueFreq = 0;
    for (int i = lvPosT[level]; i < lvPosPairsT[level]; i++) {
      if (checkSomeIndexIsNextPairT(level, pairID, i - lvPosT[level])) {
        cntCount = calcCount(getIndexCountInNum(num, lvPosPairsT[level], pairID));
        pairID++;
      } else cntCount = 1;
      if (num[i] == v) trueFreq += cntCount * (1 << level);
    }
    return (int) (trueFreq);
  }

  private void frozeAndCalcFreqValueCountSum() {
    freqValue = new long[freqValueCount.size()];
    freqCountSum = new long[freqValueCount.size()];
    ObjectArrayList<Long2LongMap.Entry> entries = new ObjectArrayList<>(freqValueCount.size());
    entries.addAll(freqValueCount.long2LongEntrySet());
    entries.sort(Comparator.comparingLong(Long2LongMap.Entry::getLongKey));
    int i = 0;
    long tmpSum = 0;
    for (Long2LongMap.Entry entry : entries) {
      freqValue[i] = entry.getLongKey();
      tmpSum += entry.getLongValue();
      freqCountSum[i] = tmpSum;
      i++;
    }
    freqValueCountFrozen = true;
  }

  public long getTrueCountInSketch(long longV) {
    sortLV0();
    long trueCount = 0;
    if (freqValueCount != null) trueCount = freqValueCount.getOrDefault(longV, 0);
    for (int i = 0; i < cntLevel; i++) trueCount += findTrueCountInLevel(i, longV);
    return trueCount;
  }

  public long getHashMapCountInSketch(long longV) {
    return freqValueCount == null ? 0 : freqValueCount.getOrDefault(longV, 0);
  }

  @Override
  public int getApproxRank(long v) {
    int approxRank = 0;
    for (int i = 0; i < cntLevel; i++)
      if (levelPos[i] < levelPos[i + 1]) {
        approxRank += findRankInLevel(i, v);
      }
    if (ENABLE_FREQ_HASHMAP && freqValueCount != null) {
      if (!freqValueCountFrozen) frozeAndCalcFreqValueCountSum();
      int pos = -1;
      for (int k = Integer.highestOneBit(freqValue.length); k > 0; k /= 2)
        if (pos + k < freqValue.length && freqValue[pos + k] <= v) pos += k;
      if (pos >= 0) approxRank += freqCountSum[pos];
    }
    return approxRank;
  }

  public long findMinValueWithRank(long K) {
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE, mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      // System.out.println("2fen\t\t"+L+"..."+R+"\t\tmid="+mid+"\t\t"+(getApproxRank(mid)>=K));
      if (getApproxRank(mid) > K) R = mid;
      else L = mid + 1;
    }
    // System.out.println("FT K:"+K+"\tN:"+getN()+" rank(L):"+getApproxRank(L));
    return L;
  }

  public LongArrayList findMinValuesWithRanks(LongArrayList Ks) {
    sortLV0();
    LongArrayList Vs = new LongArrayList();
    int[] pairsF = new int[cntLevel],
        indexesF = new int[cntLevel],
        pairsT = new int[cntLevel],
        indexesT = new int[cntLevel];
    int hashMapIndex = 0;
    long cntRank = 0, cntV = -233, lastAns = -233;
    if (ENABLE_FREQ_HASHMAP && freqValueCount != null && !freqValueCountFrozen)
      frozeAndCalcFreqValueCountSum();

    for (long K : Ks) {
      while (cntRank < K) {
        int minVLV = -1;
        boolean minVisF = false, minIsHashMap = false;
        cntV = Long.MAX_VALUE;
        for (int i = 0; i < cntLevel; i++) {
          // if(levelPos[i]+indexes[i]<levelPosForPairs[i])System.out.println("\t\t"+num[levelPos[i]
          // + indexes[i]]+" ???? "+cntV+"\t<?"+(num[levelPos[i] + indexes[i]] < cntV));
          if (levelPos[i] + indexesF[i] < lvPosPairsF[i] && num[levelPos[i] + indexesF[i]] < cntV) {
            minVLV = i;
            cntV = num[levelPos[i] + indexesF[i]];
            minVisF = true;
          }
        }
        for (int i = 0; i < cntLevel; i++) {
          // if(levelPos[i]+indexes[i]<levelPosForPairs[i])System.out.println("\t\t"+num[levelPos[i]
          // + indexes[i]]+" ???? "+cntV+"\t<?"+(num[levelPos[i] + indexes[i]] < cntV));
          if (lvPosT[i] + indexesT[i] < lvPosPairsT[i] && num[lvPosT[i] + indexesT[i]] < cntV) {
            minVLV = i;
            cntV = num[lvPosT[i] + indexesT[i]];
            minVisF = false;
          }
        }
        if (ENABLE_FREQ_HASHMAP
            && freqValueCount != null
            && hashMapIndex < freqValue.length
            && freqValue[hashMapIndex] < cntV) {
          minIsHashMap = true;
          cntV = freqValue[hashMapIndex];
          cntRank +=
              freqCountSum[hashMapIndex] - (hashMapIndex > 0 ? freqCountSum[hashMapIndex - 1] : 0);
          hashMapIndex++;
          continue;
        }
        if (minVLV == -1) break;
        int cntCount = 1;
        if (minVisF) {
          if (checkSomeIndexIsNextPairF(minVLV, pairsF[minVLV], indexesF[minVLV])) {
            cntCount = calcCount(getIndexCountInNum(num, lvPosPairsF[minVLV], pairsF[minVLV]));
            pairsF[minVLV]++;
          }
          indexesF[minVLV]++;
        } else {
          if (checkSomeIndexIsNextPairT(minVLV, pairsT[minVLV], indexesT[minVLV])) {
            cntCount = calcCount(getIndexCountInNum(num, lvPosPairsT[minVLV], pairsT[minVLV]));
            pairsT[minVLV]++;
          }
          indexesT[minVLV]++;
        }
        cntRank += cntCount * (1 << minVLV);
      }
      if (cntV == Long.MAX_VALUE) Vs.add(lastAns);
      else {
        Vs.add(cntV);
        lastAns = cntV;
      }
    }
    // System.out.println("\tKs:"+Ks);
    // System.out.println("\tVs:"+Vs);
    // System.out.println("\tDoubleVs:");for(long
    // v:Vs)System.out.print("\t"+longToResult(v));System.out.println();
    return Vs;
  }

  public void checkN() {
    long numN = levelPos[0] - bufferPosForLV0;
    for (int i = 0; i < cntLevel; i++) {
      // numN+=(long)lvActualSize[i]<<i; //todo lvActualSize不符合实际.
      int pair = 0, lvSize = 0;
      for (int j = levelPos[i]; j < lvPosPairsF[i]; j++) {
        int count = 1;
        if (checkSomeIndexIsNextPairF(i, pair, j - levelPos[i]))
          count = calcCount(getIndexCountInNum(num, lvPosPairsF[i], pair++));
        lvSize += count;
        numN += (long) count << i;
        // System.out.println("\t\t\t\tcount F:"+count+"\tLV:"+i+"\tnumN:"+numN);
      }
      pair = 0;
      for (int j = lvPosT[i]; j < lvPosPairsT[i]; j++) {
        int count = 1;
        if (checkSomeIndexIsNextPairT(i, pair, j - lvPosT[i]))
          count = calcCount(getIndexCountInNum(num, lvPosPairsT[i], pair++));
        lvSize += count;
        numN += (long) count << i;
        // System.out.println("\t\t\t\tcount T:"+count+"\tLV:"+i+"\tnumN:"+numN);
      }
      if (lvSize != lvActualSize[i])
        System.out.println(
            "!![ERR]lvActualSize is wrong. LV:"
                + i
                + "\tcount="
                + lvSize
                + "\tlvActualSize[LV]="
                + lvActualSize[i]);
      // System.out.println("\t\t\t\t\tnumN:"+numN+"\t\t"+(checkSomeIndexIsNextPairT(i,1,1)));
    }
    if (ENABLE_FREQ_HASHMAP && freqValueCount != null)
      for (Long2LongMap.Entry entry : freqValueCount.long2LongEntrySet())
        numN += entry.getLongValue();
    if (numN != N) {
      System.out.println("!![ERR]checkN=" + numN + "\tN=" + N);
      // System.out.print("!![ERR]checkN sketch\t");showNum();
    } else System.out.println("\t[DEBUG]CorrectN=" + numN);
  }

  public void checkTotalN() {
    long numN = levelPos[0] - bufferPosForLV0;
    for (int i = 0; i < cntLevel; i++) {
      numN += (long) lvActualSize[i] << i;
      if (tmpSketch.containsKey(i)) numN += (long) tmpSketch.get(i).actualSize << i;
      // System.out.println("\t\t\t\t\tnumN:"+numN+"\t\t"+(checkSomeIndexIsNextPairT(i,1,1)));
    }
    if (ENABLE_FREQ_HASHMAP && freqValueCount != null)
      for (Long2LongMap.Entry entry : freqValueCount.long2LongEntrySet())
        numN += entry.getLongValue();
    if (numN != N) {
      System.out.println("!![ERR]checkTotalN=" + numN + "\tN=" + N);
      // System.out.print("!![ERR]checkN sketch\t");showNum();
    } else System.out.println("\t[DEBUG]CorrectTotalN=" + numN);
  }

  public void checkOrdered() {
    for (int lv = 0; lv < cntLevel; lv++) {
      for (int i = levelPos[lv] + 1; i < lvPosPairsF[lv]; i++) {
        if (num[i - 1] > num[i]) {
          System.out.println("[ERR] unordered in numF LV:" + lv);
          showNum();
          return;
        }
      }
      for (int i = lvPosT[lv] + 1; i < lvPosPairsT[lv]; i++) {
        if (num[i - 1] > num[i]) {
          System.out.println("[ERR] unordered in numT LV:" + lv);
          showNum();
          return;
        }
      }
    }
  }

  public long getFreqThreshold() {
    return Math.max(getFreqMinimalThreshold(), lastThreshold);
  }
}
