package org.apache.iotdb.tsfile.utils;

import java.util.Arrays;

public class KLLSketchLazyEmptyForSimuCompact {
  public int cntN, cntLevel, maxMemNum, cntFreeSpace;
  int[] compactNum;
  int[] maxSize, lvSize;

  public KLLSketchLazyEmptyForSimuCompact(int maxMemNum) {
    cntN = 0;
    this.maxMemNum = maxMemNum;
    compactNum = new int[8];
    maxSize = new int[8];
    lvSize = new int[8];

    cntLevel = 1;
    maxSize[0] = maxMemNum;
    cntFreeSpace = maxMemNum;
  }

  public KLLSketchLazyEmptyForSimuCompact(int maxN, int maxMemNum) {
    this.maxMemNum = maxMemNum;
    int lv = KLLSketchLazyExact.calcLevelMaxSize(maxMemNum, maxN).length;
    compactNum = new int[lv];
    maxSize = new int[lv];
    lvSize = new int[lv];

    cntN = 0;
    cntLevel = 1;
    maxSize[0] = maxMemNum;
    cntFreeSpace = maxMemNum;
  }

  private void reset() {
    Arrays.fill(compactNum, 0);
    Arrays.fill(maxSize, 0);
    Arrays.fill(lvSize, 0);
    cntN = 0;
    cntLevel = 1;
    maxSize[0] = maxMemNum;
    cntFreeSpace = maxMemNum;
  }

  private void increaseCntLevel() {
    cntLevel++;
    maxSize = KLLSketchLazyExact.calcLevelMaxSizeByLevel(maxMemNum, cntLevel);
    lvSize = Arrays.copyOf(lvSize, cntLevel);
    compactNum = Arrays.copyOf(compactNum, cntLevel);
  }

  public int[] simulateCompactNumGivenN(int N) {
    if (N <= maxMemNum) return new int[1];
    if (N < cntN) {
      //      System.out.println("\t\t\treset when simulating:\tcntN:"+cntN);
      reset();
    }
    int restN = N - cntN;
    int compLV;
    //    double Sum=0,Count=0;
    while (restN > cntFreeSpace) {
      //      Sum+=cntFreeSpace;Count++;
      restN -= cntFreeSpace;
      lvSize[0] += cntFreeSpace;

      //      compLV=0;// full laziness
      //      while(compLV<cntLevel-1&&lvSize[compLV]<=maxSize[compLV])compLV++;

      compLV = -1; // another laziness
      for (int i = 0; i < cntLevel; i++)
        if (lvSize[i] > maxSize[i]
            && (compLV < 0 || (lvSize[i] << (compLV << 1L)) >= (lvSize[compLV] << (i << 1L))))
          compLV = i;
      //      if(lvSize[0]>maxSize[0]&&compLV>0)
      //      System.out.println("\t\t"+ Arrays.toString(lvSize) +"\t\t"+compLV);
      //
      // if(compLV==8)System.out.println("\t\t???compLV:\t"+compLV+"\t\tlvSize[compLV]:\t"+lvSize[compLV]+"\t\tcntLvSize:"+Arrays.toString(lvSize));

      if (compLV == cntLevel - 1 || compLV < 0) {
        compLV = cntLevel - 1;
        increaseCntLevel();

        //        compLV=-1;// another laziness
        //        for(int
        // i=0;i<cntLevel;i++)if(lvSize[i]>maxSize[i]&&(compLV<0||(lvSize[i]<<(compLV<<1L))>=(lvSize[compLV]<<(i<<1L))))compLV=i;
        //        if(compLV==cntLevel-1){System.out.println("\t\t\tsimulate KLL compact ERROR.");}
      }
      lvSize[compLV + 1] += lvSize[compLV] >>> 1;
      cntFreeSpace = lvSize[compLV] >>> 1;
      lvSize[compLV] &= 1;
      compactNum[compLV]++;
    }
    lvSize[0] += restN;
    cntFreeSpace -= restN;
    cntN = N;
    //    System.out.println("\t\t\tavgCompactPerSimu:\t"+Sum/Count);
    return Arrays.copyOf(compactNum, compactNum.length);
  }

  public long getMaxError() {
    long maxERR = 0;
    for (int i = 0; i < compactNum.length; i++) maxERR += (long) compactNum[i] << i;
    return maxERR;
  }

  public double getSig2() {
    double sig2 = 0;
    for (int i = 0; i < compactNum.length; i++) sig2 += compactNum[i] * 0.5 * Math.pow(2, i * 2);
    return sig2;
  }

  public static long getMaxError(int[] compactNum) {
    long maxERR = 0;
    for (int i = 0; i < compactNum.length; i++) maxERR += (long) compactNum[i] << i;
    return maxERR;
  }

  public static double getSig2(int[] compactNum) {
    double sig2 = 0;
    for (int i = 0; i < compactNum.length; i++) sig2 += compactNum[i] * 0.5 * Math.pow(2, i * 2);
    return sig2;
  }
}
