package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;

public class KLLSketchForSST extends KLLSketchForQuantile {
  ObjectArrayList<KLLSketchForQuantile> subSketchList;

  /** Built with other sketches */
  public KLLSketchForSST(KLLSketchForQuantile subSketch) {
    subSketchList = new ObjectArrayList<>();
    subSketchList.add(subSketch);
  }

  public KLLSketchForSST() {
    subSketchList = new ObjectArrayList<>();
  }

  public void addSubSketch(KLLSketchForQuantile subSketch) {
    this.N += subSketch.getN();
    subSketchList.add(subSketch);
  }

  public void compactSubSketches(int SketchSizeRatio) {
    assert !subSketchList.isEmpty();
    int subLevel = subSketchList.get(0).cntLevel, tmpNumLen = 0, subSizeSum = 0;
    for (KLLSketchForQuantile sketch : subSketchList) {
      subSizeSum += sketch.getNumLen();
      subLevel = Math.max(subLevel, sketch.cntLevel);
    }
    maxMemoryNum = subSizeSum * SketchSizeRatio / subSketchList.size();
    num = new long[subSizeSum];
    levelPos = new int[subLevel + 1];
    cntLevel = subLevel;
    for (int lv = levelPos[0] = 0; lv < cntLevel; lv++) {
      levelPos[lv + 1] = levelPos[lv];
      for (KLLSketchForQuantile sketch : subSketchList)
        if (sketch.cntLevel > lv && sketch.getLevelSize(lv) > 0) {
          System.arraycopy(
              sketch.num, sketch.levelPos[lv], num, levelPos[lv + 1], sketch.getLevelSize(lv));
          levelPos[lv + 1] += sketch.getLevelSize(lv);
        }
      if (getLevelSize(lv) > 0) Arrays.sort(num, levelPos[lv], levelPos[lv + 1]);
    }
    level0Sorted = true;
    //    System.out.println("\t\t before compact");
    //    show();
    for (int lv = 0; getNumLen() > maxMemoryNum; lv++) {
      compactOneLevel(lv);
    }
    //    show();
    //    showNum();
    //    System.out.println("\t\t compact over");
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
    //    if(levelPos[level+1]-levelPos[level]>levelMaxSize[level+1]){
    //      compactOneLevel(level+1);
    //    }
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
            Math.max(1, (int) Math.round(((newK + addK) * Math.pow(2.0 / 3, cntLevel - i - 1))));
      if (need <= maxMemoryNum) newK += addK;
    }
    for (int i = 0; i < cntLevel; i++)
      levelMaxSize[i] = Math.max(1, (int) Math.round((newK * Math.pow(2.0 / 3, cntLevel - i - 1))));
    //    show();
  }
}
