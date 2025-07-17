package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KLLSketchLazyExactPriori extends KLLSketchForQuantile {
  IntArrayList compactionNumInLevel;
  static long DEBUG_COUNT_USENORMAL = 0;
  static long DEBUG_COUNT_USEPRERR = 0;
  public XoRoShiRo128PlusRandom randomForReserve = new XoRoShiRo128PlusRandom();
  long MIN_V = Long.MAX_VALUE, MAX_V = Long.MIN_VALUE;

  public KLLSketchLazyExactPriori(int maxMemoryByte) {
    N = 0;
    calcParameters(maxMemoryByte);
    calcLevelMaxSize(1);
  }

  private void calcParameters(int maxMemoryByte) {
    maxMemoryNum = calcMaxMemoryNum(maxMemoryByte);
    num = new long[maxMemoryNum];
    level0Sorted = false;
    cntLevel = 0;

    compactionNumInLevel = new IntArrayList();
  }

  @Override
  protected int calcMaxMemoryNum(int maxMemoryByte) {
    return Math.min(1 << 20, maxMemoryByte / 8);
  }

  @Override
  protected void calcLevelMaxSize(
      int setLevel) { // set cntLevel.  make sure cntLevel won't decrease
    int[] tmpArr = new int[setLevel + 1];
    int maxPos = cntLevel > 0 ? Math.max(maxMemoryNum, levelPos[cntLevel]) : maxMemoryNum;
    for (int i = 0; i < setLevel + 1; i++) tmpArr[i] = i < cntLevel ? levelPos[i] : maxPos;
    levelPos = tmpArr;
    for (int i = cntLevel; i < setLevel; i++) {
      compactionNumInLevel.add(0);
    }
    cntLevel = setLevel;
    levelMaxSize = calcLevelMaxSizeByLevel(maxMemoryNum, cntLevel);
    //    levelMaxSize = new int[cntLevel];
    //    int newK = 0;
    //    for (int addK = 1 << 28; addK > 0; addK >>>= 1) { // find a new K to fit the memory limit.
    //  compact when size>cap. sum of cap<maxMemNum
    //      int need = 0;
    //      for (int i = 0; i < cntLevel; i++)
    //        need += Math.max(3, (int) Math.round(((newK + addK) * Math.pow(2.0 / 3, cntLevel - i -
    // 1))));
    //      if (need <= maxMemoryNum) newK += addK;
    //    }
    //    for (int i = 0; i < cntLevel; i++)
    //      levelMaxSize[i] = Math.max(3, (int) Math.round((newK * Math.pow(2.0 / 3, cntLevel - i -
    // 1))));

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

  @Override
  public void update(long x) { // signed long
    if (levelPos[0] == 0) compact();
    num[--levelPos[0]] = x;
    N++;
    level0Sorted = false;
  }

  private void compactOneLevel(int level) { // compact half of data when numToReduce is small

    if (level == cntLevel - 1) calcLevelMaxSize(cntLevel + 1);
    int L1 = levelPos[level], R1 = levelPos[level + 1]; // [L,R)
    //    System.out.println("T_T\t"+(R1-L1));
    if (level == 0 && !level0Sorted) {
      Arrays.sort(num, L1, R1);
      level0Sorted = true;
    }
    if (R1 - L1 <= 1) return;
    addRecord(false, num[L1], num[R1 - 1], level);
    if (((R1 - L1) & 1) == 1) {
      int reserve_P = randomForReserve.nextBoolean() ? L1 : (R1 - 1);
      long res_V = num[reserve_P];
      for (int i = reserve_P; i > L1; i--) num[i] = num[i - 1];
      num[L1] = res_V;
      L1++;
    }

    randomlyHalveDownToLeft(L1, R1);

    int mid = (L1 + R1) >>> 1;
    mergeSortWithoutSpace(L1, mid, levelPos[level + 1], levelPos[level + 2]);
    levelPos[level + 1] = mid;
    int newP = levelPos[level + 1] - 1, oldP = L1 - 1;
    for (int i = oldP; i >= levelPos[0]; i--) num[newP--] = num[oldP--];

    levelPos[level] = levelPos[level + 1] - (L1 - levelPos[level]);
    int numReduced = (R1 - L1) >>> 1;
    for (int i = level - 1; i >= 0; i--) levelPos[i] += numReduced;
    //    if(levelPos[level+1]-levelPos[level]>levelMaxSize[level+1]){
    //      compactOneLevel(level+1);
    //    }
  }

  @Override
  public void compact() {
    for (int i = 0; i < cntLevel - 1; i++)
      if (getLevelSize(i) > levelMaxSize[i]) {
        compactOneLevel(i);
        return;
      }
    calcLevelMaxSize(cntLevel + 1);
    compactOneLevel(cntLevel - 2);
    //    this.showLevelMaxSize();
    //    this.showNum();
  }

  public void merge(KLLSketchForQuantile another) {
    if (another.cntLevel > cntLevel) calcLevelMaxSize(another.cntLevel);
    for (int i = 0; i < another.cntLevel; i++) {
      int numToMerge = another.levelPos[i + 1] - another.levelPos[i];
      if (numToMerge == 0) continue;
      int mergingL = another.levelPos[i];
      while (numToMerge > 0) {
        if (levelPos[0] == 0) compact();
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
  }

  public void mergeWithTempSpace(KLLSketchForQuantile another, long minV, long maxV) {
    LongArrayList mn = new LongArrayList(1);
    mn.add(minV);
    LongArrayList mx = new LongArrayList(1);
    mx.add(maxV);
    mergeWithTempSpace(Collections.singletonList(another), mn, mx);
  }

  public void mergeWithTempSpace(
      List<KLLSketchForQuantile> otherList, LongArrayList minV, LongArrayList maxV) {
    int[] oldLevelPos = Arrays.copyOf(levelPos, cntLevel + 1);
    int oldCntLevel = cntLevel;
    int otherNumLen = 0;
    long otherN = 0;
    //    System.out.print("\t\t\t\t[mergeWithTempSpace] others:");
    for (int i = 0; i < otherList.size(); i++) {
      KLLSketchForQuantile another = otherList.get(i);
      if (another != null) {
        //      System.out.print("\t"+another.getN());
        if (another.cntLevel > cntLevel) calcLevelMaxSize(another.cntLevel);
        if (another.cntLevel >= 2)
          addRecord(true, minV.getLong(i), maxV.getLong(i), another.cntLevel - 2);
        otherNumLen += another.getNumLen();
        otherN += another.getN();
      }
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
        for (KLLSketchForQuantile another : otherList)
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
        for (KLLSketchForQuantile another : otherList)
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
    Arrays.sort(num, levelPos[0], levelPos[1]);
    level0Sorted = true;
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

  private long getLowerBound(long queryRank, double Pr) {
    long L = getMin() - 1, R = getMax() - 1, mid;
    int approxRankL = getApproxRank(L);
    while (L < R) {
      mid = L + ((R - L + 1) >>> 1);
      assert L <= mid && mid <= R;
      int approxRankMid = getApproxRank(mid);
      if (approxRankMid == approxRankL) {
        L = mid;
        continue;
      }

      long rankErrBound = queryRankErrBound(mid, Pr);
      //
      // System.out.println("\t\t\t\t\t"+longToResult(mid)+"\t\trank:"+approxRankMid+"\t\terr:"+rankErrBound+"\t\t\tL,R,mid:"+L+" "+R+" "+mid);
      if (approxRankMid + rankErrBound < /*<*/ queryRank) {
        L = mid;
        approxRankL = approxRankMid;
      } else R = mid - 1;
    }
    L++;
    //    if(getApproxRank(L)+queryBound(L,Pr)<queryRank)L++;
    //    System.out.println("\t\t[]exactKLL
    // lowerBound.\t\tPr:"+Pr+"\trank:"+queryRank+"\t\t\t\tlowerBoundV:"+L+"(longToResult:"+longToResult(L)+")"+ "\t\tL_rank:"+getApproxRank(L)+"\t\tPrErr:"+queryRankErrBound(L,Pr)+"\t1.0Err:"+queryRankErrBound(L,1.0));
    return L;
  }

  private long getUpperBound(long queryRank, double Pr) {
    long L = getMin(), R = getMax(), mid;
    int approxRankR = getApproxRank(R);
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      int approxRankMid = getApproxRank(mid);
      if (approxRankMid == approxRankR) {
        R = mid;
        continue;
      }
      long rankErrBound = queryRankErrBound(mid, Pr);
      if (approxRankMid - rankErrBound >= queryRank) {
        R = mid;
        approxRankR = approxRankMid;
      } else L = mid + 1;
    }
    //    L--;
    //    System.out.println("\t\t[]exactKLL
    // upperBound.\t\tPr:"+Pr+"\trank:"+queryRank+"\t\t\t\tupperBoundV:"+L+"(longToResult:"+longToResult(L)+")"+"\t\tR_rank:"+getApproxRank(L)+"\t\tePrErr:"+queryRankErrBound(L,Pr)+"\t1.0Err:"+queryRankErrBound(L,1.0));
    //    System.out.println("\t\t\t\trank()");
    return L;
  }

  public double[] findResultRange(long K1, long K2, double Pr) {
    DoubleArrayList result = new DoubleArrayList();
    long valL, valR;
    if (exactResult()) {
      valL = getExactResult((int) K1 - 1);
      valR = getExactResult((int) K2 - 1);
      //      System.out.println("\t\tEXACT
      // RESULT!\tn:"+getN()+"\tK1,2:"+K1+","+K2+"\t\tvalL,R:"+valL+","+valR);
      result.add(longToResult(valL));
      result.add(longToResult(valR));
      result.add(-233);
    } else if (getMin() == getMax()) {
      valL = valR = getMin();
      //      System.out.println("\t\tEXACT
      // RESULT!\tn:"+getN()+"\tK1,2:"+K1+","+K2+"\t\tvalL,R:"+valL+","+valR);
      result.add(longToResult(valL));
      result.add(longToResult(valR));
      result.add(-233);
    } else {
      //      queriedBound.clear();
      valL = getLowerBound(K1, Pr);
      valR = getUpperBound(K2, Pr);
      result.add(longToResult(valL));
      result.add(longToResult(valR));
      result.add(getApproxRank(valL));
      result.add(getApproxRank(valR));
      result.add(queryRankErrBound(valL, Pr));
      result.add(queryRankErrBound(valR, Pr));
    }
    return result.toDoubleArray();
  }

  public double[] getFilterL(
      long CountOfValL, long CountOfValR, double valL, double valR, long K, double Pr) {
    if (K <= CountOfValL) return new double[] {valL, -233.0};
    if (K > CountOfValL + getN()) return new double[] {valR, -233.0};
    K -= CountOfValL;
    if (exactResult()) {
      return new double[] {longToResult(getExactResult((int) K - 1)), -233.0};
    }
    if (getMin() == getMax()) return new double[] {longToResult(getMin()), -233.0};
    return new double[] {longToResult(getLowerBound(K, Pr))};
  }

  public double[] getFilterR(
      long CountOfValL, long CountOfValR, double valL, double valR, long K, double Pr) {
    if (K <= CountOfValL) return new double[] {valL, -233.0};
    if (K > CountOfValL + getN()) return new double[] {valR, -233.0};
    K -= CountOfValL;
    if (exactResult()) {
      return new double[] {longToResult(getExactResult((int) K - 1)), -233.0};
    }
    if (getMin() == getMax()) return new double[] {longToResult(getMin()), -233.0};
    return new double[] {longToResult(getUpperBound(K, Pr))};
  }

  public double[] getFilter(
      long CountOfValL, long CountOfValR, double valL, double valR, long K1, long K2, double Pr) {
    double[] filterL = getFilterL(CountOfValL, CountOfValR, valL, valR, K1, Pr);
    double[] filterR = getFilterR(CountOfValL, CountOfValR, valL, valR, K2, Pr);
    if (filterL.length + filterR.length == 4) return new double[] {filterL[0], filterR[0], -233};
    else return new double[] {filterL[0], filterR[0]};
  }

  public double sumForAvgDupli = 0, countForAvgDupli = 0;

  public double checkDupliElementsInSketch() {
    if (levelPos[0] == levelPos[cntLevel]) return 1.0;
    sortLV0();
    int tmpSize = 0, tmpDistinct = 0;
    for (int lv = 0; lv < cntLevel; lv++) {
      int lvSize = getLevelSize(lv), lvDistinct = 0;
      for (int i = levelPos[lv]; i < levelPos[lv + 1]; i++) {
        if (i == levelPos[lv] || num[i] != num[i - 1]) lvDistinct++;
      }
      tmpSize += lvSize;
      tmpDistinct += lvDistinct;
    }
    //    System.out.println("\t\t\t[DEBUG]\tSize/Distinct:\t"+1.0*tmpSize/tmpDistinct);
    return 1.0 * tmpSize / tmpDistinct;
  }

  public double getAvgDupliInSketch() {
    if (countForAvgDupli > 0) return sumForAvgDupli / countForAvgDupli;
    else return checkDupliElementsInSketch();
  }
}
