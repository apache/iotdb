package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

import java.util.Arrays;

// lazy.
public class MRLSketchLazy {
  long N, minV, maxV;
  int maxMemoryNum;
  long[] num;
  int cntLevel;
  int[] levelPos;
  boolean level0Sorted;
  IntArrayList compactionNumInLevel;
  public XoRoShiRo128PlusRandom randomForReserve = new XoRoShiRo128PlusRandom();

  public MRLSketchLazy(int maxMemoryByte) {
    N = 0;
    minV = Long.MAX_VALUE;
    maxV = Long.MIN_VALUE;
    maxMemoryNum = maxMemoryByte / 8;
    num = new long[maxMemoryNum];
    compactionNumInLevel = new IntArrayList();
    setLevel(1);
    level0Sorted = false;
  }

  protected void setLevel(int setLevel) {
    int[] tmpPos = new int[setLevel + 1];
    for (int i = 0; i < cntLevel; i++) tmpPos[i] = levelPos[i];
    for (int i = cntLevel; i <= setLevel; i++) tmpPos[i] = maxMemoryNum;
    for (int i = cntLevel; i < setLevel; i++) compactionNumInLevel.add(0);
    cntLevel = setLevel;
    levelPos = tmpPos;
  }

  public int getCntLevel() {
    return cntLevel;
  }

  public int getLevelSize(int level) {
    return levelPos[level + 1] - levelPos[level];
  }

  public void show() {
    for (int i = 0; i < cntLevel; i++) {
      System.out.print("\t");
      System.out.print("[" + (levelPos[i + 1] - levelPos[i]) + "]");
      System.out.print("\t");
    }
    System.out.println("\tmaxLV=" + (cntLevel - 1));
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

  public void showNum() {
    for (int i = 0; i < cntLevel; i++) {
      System.out.print("\t|");
      for (int j = levelPos[i]; j < levelPos[i + 1]; j++) System.out.print(num[j] + ",");
      //      System.out.print(longToResult(num[j])+", ");
      System.out.print("|\t");
    }
    System.out.println();
  }

  public void update(long x) { // signed long
    if (levelPos[0] == 0) compact();
    num[--levelPos[0]] = x;
    N++;
    level0Sorted = false;
  }

  private void addRecordInLevel(int level) {
    int hisNum = compactionNumInLevel.getInt(level);
    hisNum++;
    compactionNumInLevel.set(level, hisNum);
  }

  private void sortLevel0() {
    int L = levelPos[0], R = levelPos[1];
    Arrays.sort(num, L, R);
    minV = Math.min(minV, num[L]);
    maxV = Math.max(maxV, num[R - 1]);
    level0Sorted = true;
  }

  private void compactOneLevel(int level) { // compact half of data when numToReduce is small
    if (level == cntLevel - 1) setLevel(cntLevel + 1);
    int L1 = levelPos[level], R1 = levelPos[level + 1]; // [L,R)
    if (R1 - L1 <= 1) return;
    //    System.out.println("T_T\t"+(R1-L1));
    if (level == 0 && !level0Sorted) {
      sortLevel0();
    }
    if (((R1 - L1) & 1) == 1) {
      int reserve_P = randomForReserve.nextBoolean() ? L1 : (R1 - 1);
      long res_V = num[reserve_P];
      for (int i = reserve_P; i > L1; i--) num[i] = num[i - 1];
      num[L1] = res_V;
      L1++;
    }
    addRecordInLevel(level);

    halveDownToLeft(L1, R1);

    int mid = (L1 + R1) >>> 1;
    mergeSortWithoutSpace(L1, mid, levelPos[level + 1], levelPos[level + 2]);
    levelPos[level + 1] = mid;
    int newP = levelPos[level + 1] - 1, oldP = L1 - 1;
    for (int i = oldP; i >= levelPos[0]; i--) num[newP--] = num[oldP--];

    levelPos[level] = levelPos[level + 1] - (L1 - levelPos[level]);
    int numReduced = (R1 - L1) >>> 1;
    for (int i = level - 1; i >= 0; i--) levelPos[i] += numReduced;
  }

  protected void compact() {
    int maxLevelSize = maxMemoryNum / cntLevel;
    boolean compacted = false;
    for (int i = 0; i < cntLevel; i++)
      if (getLevelSize(i) > maxLevelSize) {
        compactOneLevel(i);
        compacted = true;
        break;
      }
    if (!compacted) {
      setLevel(cntLevel + 1);
      compact();
    }
  }

  protected void halveDownToLeft(int L, int R) {
    int delta = 0;
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
    if (level == 0 && !level0Sorted) sortLevel0();
    R--;
    if (L > R || num[L] > v) return 0;
    if (v >= num[R]) return (R - L + 1) * (1 << level);
    while (L < R) {
      int mid = (L + R + 1) >> 1;
      if (num[mid] <= v) L = mid;
      else R = mid - 1;
    }
    return (L - levelPos[level] + 1) * (1 << level);
  }

  protected double longToResult(long result) {
    result = (result >>> 63) == 0 ? result : result ^ Long.MAX_VALUE;
    return Double.longBitsToDouble(result);
  }

  public int getApproxRank(long v) { // approxRank >= actualRank
    int approxRank = 0;
    for (int i = 0; i < cntLevel; i++)
      if (levelPos[i] < levelPos[i + 1]) {
        approxRank += findRankInLevel(i, v);
      }
    return approxRank;
  }

  public long getMin() {
    if (!level0Sorted) sortLevel0();
    return minV;
  }

  public long getMax() {
    if (!level0Sorted) sortLevel0();
    return maxV;
  }

  private int queryErrBound(long v) {
    if (v < getMin() || v >= getMax()) return 0;
    int totErr = 0;
    for (int i = 0; i < cntLevel - 1; i++) totErr += compactionNumInLevel.getInt(i) * (1 << i);
    return totErr;
  }

  public int queryErrBound() {
    int totErr = 0;
    for (int i = 0; i < cntLevel - 1; i++) totErr += compactionNumInLevel.getInt(i) * (1 << i);
    return totErr;
  }

  private long getLowerBound(long queryRank) {
    long L = getMin() - 1, R = getMax() - 1, mid;
    while (L < R) {
      mid = L + ((R - L + 1) >>> 1);
      //      assert L <= mid && mid <= R;
      int approxRank = getApproxRank(mid), rankErrBound = 0; // queryBound(mid);
      //
      // System.out.println("\t\t\t\t\t"+longToResult(mid)+"\t\trank:"+approxRank+"\t\terr:"+rankErrBound+"\t\t\tL,R,mid:"+L+" "+R+" "+mid);
      if (approxRank + rankErrBound < /*<*/ queryRank) L = mid;
      else R = mid - 1;
    }
    L++;
    //    if(getApproxRank(L)+queryBound(L,Pr)<queryRank)L++;
    //    System.out.println("\t\t[]exactKLL
    // lowerBound.\t\t\trank:"+queryRank+"\t\t\t\tlowerBoundV:"+L+"(longToResult:"+longToResult(L)+")"+ "\t\tL_rank:"+getApproxRank(L)+"\t\tErr:"+0);
    return L;
  }

  private long getUpperBound(long queryRank) {
    long L = getMin(), R = getMax(), mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      int approxRank = getApproxRank(mid), rankErrBound = queryErrBound(mid);
      if (approxRank - rankErrBound >= queryRank) R = mid;
      else L = mid + 1;
    }
    //    L--;
    //    System.out.println("\t\t[]exactKLL
    // upperBound.\t\tPr:"+Pr+"\trank:"+queryRank+"\t\t\t\tupperBoundV:"+L+"(longToResult:"+longToResult(L)+")"+"\t\tR_rank:"+getApproxRank(L)+"\t\terr:"+queryBound(L,Pr));
    //    System.out.println("\t\t\t\trank()");
    return L;
  }

  public double[] findResultRange(long K1, long K2) {
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
      valL = getLowerBound(K1);
      valR = getUpperBound(K2);
      result.add(longToResult(valL));
      result.add(longToResult(valR));
    }
    return result.toDoubleArray();
  }

  public double[] getFilterL(long CountOfValL, long CountOfValR, double valL, double valR, long K) {
    if (K <= CountOfValL) return new double[] {valL, -233.0};
    if (K > CountOfValL + getN()) return new double[] {valR, -233.0};
    K -= CountOfValL;
    if (exactResult()) {
      return new double[] {longToResult(getExactResult((int) K - 1)), -233.0};
    }
    if (getMin() == getMax()) return new double[] {longToResult(getMin()), -233.0};
    return new double[] {longToResult(getLowerBound(K))};
  }

  public double[] getFilterR(long CountOfValL, long CountOfValR, double valL, double valR, long K) {
    if (K <= CountOfValL) return new double[] {valL, -233.0};
    if (K > CountOfValL + getN()) return new double[] {valR, -233.0};
    K -= CountOfValL;
    if (exactResult()) {
      return new double[] {longToResult(getExactResult((int) K - 1)), -233.0};
    }
    if (getMin() == getMax()) return new double[] {longToResult(getMin()), -233.0};
    return new double[] {longToResult(getUpperBound(K))};
  }

  public double[] getFilter(
      long CountOfValL, long CountOfValR, double valL, double valR, long K1, long K2) {
    double[] filterL = getFilterL(CountOfValL, CountOfValR, valL, valR, K1);
    double[] filterR = getFilterR(CountOfValL, CountOfValR, valL, valR, K2);
    if (filterL.length + filterR.length == 4) return new double[] {filterL[0], filterR[0], -233};
    else return new double[] {filterL[0], filterR[0]};
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
    if (!level0Sorted) sortLevel0();
    return num[levelPos[0] + K];
  }
}
