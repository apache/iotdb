package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KLLSketchLazyExact extends KLLSketchForQuantile {

  static double[][] PR;
  static final int maxTimesForExact = 4, maxSearchNum = 400, iterLVNum = 1;
  IntArrayList compactionNumInLevel;
  LongArrayList compactionMinVInLevel, compactionMaxVInLevel;
  static long DEBUG_COUNT_USENORMAL = 0;
  static long DEBUG_COUNT_USEPRERR = 0;
  public XoRoShiRo128PlusRandom randomForReserve = new XoRoShiRo128PlusRandom();

  public KLLSketchLazyExact(int maxMemoryByte) { // maxN=7000 for PAGE, 1.6e6 for CHUNK
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
    compactionMinVInLevel = new LongArrayList();
    compactionMaxVInLevel = new LongArrayList();
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
      compactionMinVInLevel.add(Long.MAX_VALUE);
      compactionMaxVInLevel.add(Long.MIN_VALUE);
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

  public static int[] calcLevelMaxSizeByLevel(int maxMemoryNum, int setLevel) {
    int[] levelMaxSize = new int[setLevel];
    int newK = 0;
    for (int addK = 1 << 28; addK > 0; addK >>>= 1) { // find a new K to fit the memory limit.
      int need = 0;
      for (int i = 0; i < setLevel; i++)
        need +=
            Math.max(3, (int) Math.round(((newK + addK) * Math.pow(2.0 / 3, setLevel - i - 1))));
      if (need <= maxMemoryNum) newK += addK;
    }
    for (int i = 0; i < setLevel; i++)
      levelMaxSize[i] = Math.max(3, (int) Math.round((newK * Math.pow(2.0 / 3, setLevel - i - 1))));
    return levelMaxSize;
  }

  public static int[] calcLevelMaxSize(int maxMemoryNum, int maxN) {
    if (maxN <= maxMemoryNum) return new int[] {maxMemoryNum};
    int L = 2, R = (int) Math.ceil(Math.log(maxN / 3.0) / Math.log(2)) + 1;
    while (L < R) {
      int mid = (L + R) / 2;
      int[] capacity = calcLevelMaxSizeByLevel(maxMemoryNum, mid);
      long allCap = 0;
      for (int i = 0; i < mid; i++) allCap += (long) capacity[i] << i;
      if (allCap >= maxN) R = mid;
      else L = mid + 1;
    }
    return calcLevelMaxSizeByLevel(maxMemoryNum, L);
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
    //    if (levelPos[1] - levelPos[0] > levelMaxSize[0]) compact();
    //    boolean flag=false;
    //    for(int i=0;i<cntLevel;i++)if(levelPos[i+1]-levelPos[i]>levelMaxSize[i])flag=true;
    //    if(flag)compact();
    //    System.out.println("\t\t\t"+x);
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
    if (R1 - L1 <= 1) return;
    if (((R1 - L1) & 1) == 1) {
      int reserve_P = randomForReserve.nextBoolean() ? L1 : (R1 - 1);
      long res_V = num[reserve_P];
      for (int i = reserve_P; i > L1; i--) num[i] = num[i - 1];
      num[L1] = res_V;
      L1++;
    }
    addRecord(false, num[L1], num[R1 - 1], level);

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
    boolean compacted = false;
    for (int i = 0; i < cntLevel; i++)
      if (levelPos[i + 1] - levelPos[i] > levelMaxSize[i]) {
        compactOneLevel(i);
        compacted = true;
        break;
      }

    if (!compacted) {
      calcLevelMaxSize(cntLevel + 1);
      compact();
      //      compactOneLevel(cntLevel - 1);
    }
    //    this.showLevelMaxSize();
  }

  public void merge(KLLSketchForQuantile another) {
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

  public void mergeWithTempSpace(KLLSketchForQuantile another, long minV, long maxV) {
    LongArrayList mn = new LongArrayList(1);
    mn.add(minV);
    LongArrayList mx = new LongArrayList(1);
    mx.add(maxV);
    mergeWithTempSpace(Collections.singletonList(another), mn, mx);
  }

  public void mergeWithTempSpace(
      List<KLLSketchForQuantile> otherList, LongArrayList minV, LongArrayList maxV) {
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
    for (int i = 0; i < cntLevel; i++) {
      System.out.print("\t");
      System.out.print("[" + compactionNumInLevel.getInt(i) + "]");
      System.out.print("\t");
      totERR += compactionNumInLevel.getInt(i) << i;
    }
    System.out.println("\tmaxLV=" + (cntLevel - 1) + "\ttotERR=" + totERR);
  }

  private void addRecordInLevel(long minV, long maxV, int level) {
    int hisNum = compactionNumInLevel.getInt(level);
    long hisMinV = compactionMinVInLevel.getLong(level),
        hisMaxV = compactionMaxVInLevel.getLong(level);
    if (maxV <= hisMinV || minV >= hisMaxV) {
      hisNum = Math.max(hisNum, 1);
      //      if(level>=9)
      //      System.out.println("\t\t\t\trecord
      // extreme.\t"+longToResult(minV)+"\t"+longToResult(maxV)+"\t\t\tnewnum:"+hisNum+"\t\tLV:"+level);
    } else {
      hisNum++;
      //      if(level>=9)
      //        System.out.println("\t\t\t\tnormal
      // record.\t"+longToResult(minV)+"\t"+longToResult(maxV)+"\t\t\tnewnum:"+hisNum+"\t\tLV:"+level);
    }
    compactionNumInLevel.set(level, hisNum);
    compactionMinVInLevel.set(level, Math.min(hisMinV, minV));
    compactionMaxVInLevel.set(level, Math.max(hisMaxV, maxV));
  }

  public void addRecord(boolean isSum, long minV, long maxV, int level) {
    if (!isSum) {
      addRecordInLevel(minV, maxV, level);
    } else {
      for (int i = 0; i <= level; i++) addRecordInLevel(minV, maxV, i);
    }
  }

  public double getSumMWeight2() { // sum{ compactNum * weight^2 }
    double sum = 0;
    for (int i = 0; i < cntLevel - 1; i++)
      sum += compactionNumInLevel.getInt(i) * Math.pow((1 << i), 2);
    return sum;
  }

  // return bound of result with possibility >= p
  private static double prOutOfBoundDFS(
      double cntPR,
      int level,
      int topErr,
      int bound,
      int lowLevel,
      NormalDistribution lowDis,
      int lowExtreme,
      int[] relatedCompactNum) {
    //    if(cntPR>0)System.out.println("\t\t\t\tprOutOfBound... cntPR:"+cntPR);
    if (level < lowLevel) {
      DEBUG_COUNT_USENORMAL++;
      int absTopErr = Math.abs(topErr); // lowDistribution对称
      double prOutOfBound =
          lowDis != null
              ? (1.0
                  - (lowDis.cumulativeProbability(bound - absTopErr)
                      - lowDis.cumulativeProbability(-bound - absTopErr)))
              : (absTopErr > bound ? 1 : 0);
      //
      // System.out.println("\t\t\t\t\t\ttopErr="+topErr+"\t\tlimitedBound:"+bound+"\t\tprOfOutOfBound:"+prOutOfBound+"\t\t\tcntPR:"+cntPR);
      return cntPR * prOutOfBound;
    }
    int cntBottomMaxERR = level >= 1 ? maxERRBottom[level - 1] : 0;
    double prSum = 0;
    int cntTime = relatedCompactNum[level];
    for (int absX = 0; absX <= cntTime; absX++)
      for (int x : (absX == 0) ? (new int[] {0}) : (new int[] {-absX, absX})) {
        int cntErr = x * (1 << level), tmpAbsERR = Math.abs(topErr + cntErr);
        if (tmpAbsERR > bound + cntBottomMaxERR) {
          prSum += cntPR * PR[cntTime][x + cntTime];
          continue;
        }
        if (tmpAbsERR + cntBottomMaxERR <= bound) {
          continue;
        }
        //      if(cntPR>0)
        //
        // System.out.println("\t\t\t\t\t\t\t\t\t\tcntErr:"+cntErr+"\t\tlevel="+level+"\t\tx:"+x+"\t\tcntLevelPr="+PR[cntTime][x+cntTime]+"\t\t\tcntLevelCompNum:"+cntTime+"\tcntLevelV:"+x);
        prSum +=
            prOutOfBoundDFS(
                cntPR * PR[cntTime][x + cntTime],
                level - 1,
                topErr + cntErr,
                bound,
                lowLevel,
                lowDis,
                lowExtreme,
                relatedCompactNum);
      }
    return prSum;
  }

  private static boolean checkBound(
      int bound,
      double Pr,
      int lowLevel,
      NormalDistribution lowDis,
      int lowExtreme,
      int[] relatedCompactNum) {
    double prOutOfBound =
        prOutOfBoundDFS(
            1.0,
            relatedCompactNum.length - 1,
            0,
            bound,
            lowLevel,
            lowDis,
            lowExtreme,
            relatedCompactNum);
    //    System.out.println("\t\tcheckBound.\tbound:"+bound+"\t\t\tprOutOfBound:"+prOutOfBound);
    return prOutOfBound <= 1 - Pr;
  }

  static Object2IntOpenHashMap<String> queriedBound = new Object2IntOpenHashMap<>();
  static String lastQuery;
  static int lastBound;
  static double lastSig2;
  static NormalDistribution lastNormalDis;

  private int[] getRelatedCompactNum(long result) {
    int[] relatedCompactNum = new int[cntLevel - 1];
    for (int i = 0; i < cntLevel - 1; i++)
      if (result >= compactionMinVInLevel.getLong(i) && result < compactionMaxVInLevel.getLong(i))
        relatedCompactNum[i] = compactionNumInLevel.getInt(i);
    //    System.out.println(Arrays.toString(relatedCompactNum));
    return relatedCompactNum;
  }

  public long getMin() {
    long mn = Long.MAX_VALUE;
    for (int i = 0; i < cntLevel - 1; i++) mn = Math.min(mn, compactionMinVInLevel.getLong(i));
    for (int i = levelPos[0]; i < levelPos[1]; i++) mn = Math.min(mn, num[i]);
    return mn;
  }

  public long getMax() {
    long mx = Long.MIN_VALUE;
    for (int i = 0; i < cntLevel - 1; i++) mx = Math.max(mx, compactionMaxVInLevel.getLong(i));
    for (int i = levelPos[0]; i < levelPos[1]; i++) mx = Math.max(mx, num[i]);
    return mx;
  }

  static int[] maxERRBottom;

  public static int queryRankErrBound(int[] relatedCompactNum, double Pr) {
    int compLevel = relatedCompactNum.length;
    String cntQuery = Double.toString(Pr);
    cntQuery += Arrays.toString(relatedCompactNum);
    if (cntQuery.equals(lastQuery)) return lastBound;
    //    if (queriedBound.containsKey(cntQuery)) {
    //      return queriedBound.getInt(cntQuery);
    //    }
    DEBUG_COUNT_USEPRERR++;
    if (PR == null) preparePR();
    double sig2 = 0.0;
    int lowLevel = 0, lowExtreme = 0;
    maxERRBottom = new int[compLevel];
    maxERRBottom[0] = relatedCompactNum[0];
    for (int i = 1; i < compLevel; i++)
      maxERRBottom[i] = maxERRBottom[i - 1] + (relatedCompactNum[i] << i);
    for (int i = 0; i < compLevel; i++) {
      double all_case = 1.0;
      for (int j = i; j < compLevel; j++) all_case *= relatedCompactNum[j] * 2 + 1;
      if (relatedCompactNum[i] >= maxTimesForExact
          || all_case > maxSearchNum
          || compLevel - i > iterLVNum) {
        sig2 += relatedCompactNum[i] / 2.0 * Math.pow(2, i * 2);
        lowLevel = i + 1;
        lowExtreme += relatedCompactNum[i] << i;
      }
    }
    NormalDistribution lowDis =
        sig2 > 0
            ? (sig2 == lastSig2 ? lastNormalDis : new NormalDistribution(0, Math.sqrt(sig2)))
            : null;
    lastSig2 = sig2;
    lastNormalDis = lowDis;
    //    System.out.println("\t\t\ts:"+lowDis.cumulativeProbability(-0.5));
    int L = 0, R = maxERRBottom[compLevel - 1], mid;
    if (Pr >= 1.0) L = R;
    //    L=0;R=2;
    while (L < R) {
      mid = (L + R) / 2;
      //      System.out.println("\tcheck: "+L+"..."+R+"\tmid="+mid);
      if (checkBound(mid, Pr, lowLevel, lowDis, lowExtreme, relatedCompactNum)) {
        //        System.out.println("\t\t\tSUCCESS:"+mid);
        R = mid;
      } else L = mid + 1;
    }
    //    queriedBound.put(cntQuery, L);
    lastQuery = cntQuery;
    lastBound = L;
    return L;
  }

  public int queryRankErrBound(double Pr) {
    return queryRankErrBound(compactionNumInLevel.toIntArray(), Pr);
  }

  // 返回误差err，该数值在sketch里估计排名的偏差绝对值有Pr的概率<err
  public int queryRankErrBound(long result, double Pr) {
    int[] relatedCompactNum = getRelatedCompactNum(result);
    return queryRankErrBound(relatedCompactNum, Pr);
    //    String cntQuery = Double.toString(Pr);
    //    cntQuery += Arrays.toString(relatedCompactNum);
    ////    System.out.println("\t\t\tcntQ:"+cntQuery+"\t\t\tallCompact"+compactionNumInLevel);
    //    if (queriedBound.containsKey(cntQuery)) {
    ////      System.out.println("\t\t\tLOL sameConpactionNumber");
    //      return queriedBound.getInt(cntQuery);
    //    }
    ////    System.out.println("\t\t\t"+Arrays.toString(relatedCompactNum));
    ////    System.out.print("\t\t\tcompactionTimesInLevel:\t");
    ////    for(int i=0;i<cntLevel-1;i++)System.out.print("\t"+relatedCompactNum[i]);
    ////    System.out.println();
    //    if (PR == null) preparePR();
    //    double sig2 = 0.0;
    //    int lowLevel = 0, lowExtreme = 0;
    //    maxERRBottom = new int[cntLevel - 1];
    //    maxERRBottom[0] = relatedCompactNum[0];
    //    for (int i = 1; i < cntLevel - 1; i++) maxERRBottom[i] = maxERRBottom[i - 1] +
    // (relatedCompactNum[i] << i);
    //    for (int i = 0; i < cntLevel - 1; i++) {
    //      double all_case = 1.0;
    //      for (int j = i; j < cntLevel - 1; j++) all_case *= relatedCompactNum[j] * 2 + 1;
    //      if (relatedCompactNum[i] >= maxTimesForExact || all_case > maxSearchNum ||
    // cntLevel-1-i>iterLVNum) {
    //        sig2 += relatedCompactNum[i] / 2.0*Math.pow(2,i*2);
    //        lowLevel = i + 1;
    //        lowExtreme += relatedCompactNum[i] << i;
    //      }
    //    }
    //    NormalDistribution lowDis = sig2 > 0 ? new NormalDistribution(0, Math.sqrt(sig2)) : null;
    ////    System.out.println("\t\t\ts:"+lowDis.cumulativeProbability(-0.5));
    //    int L = 0, R = maxERRBottom[cntLevel - 2], mid;
    //    if(Pr>=1.0)L=R;
    ////    L=0;R=2;
    //    while (L < R) {
    //      mid = (L + R) / 2;
    ////      System.out.println("\tcheck: "+L+"..."+R+"\tmid="+mid);
    //      if (checkBound(mid, Pr, lowLevel, lowDis, lowExtreme, relatedCompactNum)) {
    ////        System.out.println("\t\t\tSUCCESS:"+mid);
    //        R = mid;
    //      } else L = mid + 1;
    //    }
    //    queriedBound.put(cntQuery, L);
    //    return L;
  }

  private static void preparePR() {
    int maxTimes = maxTimesForExact;
    PR = new double[maxTimes + 1][maxTimes * 2 + 1];
    PR[1][0] = 0.25;
    PR[1][1] = 0.5;
    PR[1][2] = 0.25;
    for (int m = 2; m <= maxTimes; m++) {
      for (int sum = -m; sum <= m; sum++) {
        if (m - 1 + sum - 1 >= 0) PR[m][m + sum] += 0.25 * PR[m - 1][m - 1 + sum - 1];
        if (m - 1 + sum >= 0) PR[m][m + sum] += 0.5 * PR[m - 1][m - 1 + sum];
        PR[m][m + sum] += 0.25 * PR[m - 1][m - 1 + sum + 1];
      }
      //            for(int
      // sum=-m;sum<=m;sum++)System.out.print("\t"+pr[m][m+sum]*Math.pow(2.0,m*2));System.out.println();
      //      double tmp = 0;
      //      for (int sum = -m; sum <= m; sum++) tmp += PR[m][m + sum];
      //      System.out.print("\t\tsum=" + tmp + "\t\t");
      //      for (int sum = -m; sum <= m; sum++) System.out.print("\t" + PR[m][m + sum]);
      //      System.out.println();
    }
    PR[0][0] = 1.0;
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

      int rankErrBound = queryRankErrBound(mid, Pr);
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
      int rankErrBound = queryRankErrBound(mid, Pr);
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

  public long[] findLongResultRange(long K1, long K2, double Pr) {
    LongArrayList result = new LongArrayList();
    long valL, valR;
    if (exactResult()) {
      valL = getExactResult((int) K1 - 1);
      valR = getExactResult((int) K2 - 1);
      //      System.out.println("\t\tEXACT
      // RESULT!\tn:"+getN()+"\tK1,2:"+K1+","+K2+"\t\tvalL,R:"+valL+","+valR);
      result.add(valL);
      result.add(valR);
      result.add(-233);
    } else if (getMin() == getMax()) {
      valL = valR = getMin();
      //      System.out.println("\t\tEXACT
      // RESULT!\tn:"+getN()+"\tK1,2:"+K1+","+K2+"\t\tvalL,R:"+valL+","+valR);
      result.add(valL);
      result.add(valR);
      result.add(-233);
    } else {
      //      queriedBound.clear();
      valL = getLowerBound(K1, Pr);
      valR = getUpperBound(K2, Pr);
      result.add(valL);
      result.add(valR);
      result.add(getApproxRank(valL));
      result.add(getApproxRank(valR));
      result.add(queryRankErrBound(valL, Pr));
      result.add(queryRankErrBound(valR, Pr));
    }
    return result.toLongArray();
  }

  //  static int[] estimateCompactionNumber(int maxMemoryByte,long n){
  //
  //  }

}
