package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Arrays;

public class KLLDupliPair extends KLLSketchForQuantile { // TODO: too large count
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
  int AmortizedRatioForBuffer = 50;

  public KLLDupliPair(int maxMemoryByte) {
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
    return Math.min(1 << 20, maxMemoryByte / 8);
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
    //    System.out.print("\t\t[Process LV0] Distinct values:\t");
    //    for(int i=0;i<distinctInBuffer;i++)System.out.print("\t"+longToResult(tmpNumBuf[i]));
    //    System.out.println();
    //    System.out.print("\t\t\tDupli_Index,Count:\t");
    //    for(int i=0;i<pairInBuffer;i++){
    //      int indexCount=getIndexCountInNum(tmpNumBuf,distinctInBuffer,i);
    //      System.out.print("\t("+(calcIndex(indexCount)-1)+","+calcCount(indexCount)+")");
    //    }
    //    System.out.println();

    mergePairsToLevel(
        tmpNumBuf,
        distinctInBuffer + (cntPair + 1) / 2,
        distinctInBuffer,
        0,
        bufferPosForLV0); // merge two seg
    bufferPosForLV0 = levelPos[0];

    //    showNum();

    return bufferPosForLV0
        <= Math.max(MinimumLevelMaxSize, getLevelSize(0) / AmortizedRatioForBuffer);
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
            [(tmpNumLength - tmpPosForPairs)
                + (levelPos[LV + 1] - levelPosForPairs[LV])
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
    int actualSize = R1 - L1;
    for (int i = levelPosForPairs[level]; i < levelPos[level + 1]; i++) {
      actualSize--;
      actualSize += ((num[i] >>> 32) & maskCountInPair);
      actualSize += ((num[i] & mask32From64_1) & maskCountInPair);
    }
    //    for(int
    // i=0;i<getPairsInLevel(level);i++)actualSize+=calcCount(getIndexCountInNum(num,levelPosForPairs[level],i))-1;
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
        compactOneLevel(i);
        break;
      }
    }

    //    this.showLevelMaxSize();
    //    this.showNum();
  }

  public int getMaxErr() {
    int totERR = 0;
    for (int i = 0; i < cntLevel; i++) totERR += compactionNumInLevel.getInt(i) << i;
    return totERR;
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
}
