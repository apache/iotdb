package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;

public class KLLSketchForLSMFile extends KLLSketchForQuantile {
  ObjectArrayList<KLLSketchForQuantile> subSketchList;

  /** Built with sorted MemTable(ordered by t). */
  public KLLSketchForLSMFile(KLLSketchForQuantile subSketch) {
    subSketchList = new ObjectArrayList<>();
    subSketchList.add(subSketch);
  }

  public KLLSketchForLSMFile() {
    subSketchList = new ObjectArrayList<>();
  }

  public void addSubSketch(KLLSketchForQuantile subSketch) {
    this.N += subSketch.getN();
    subSketchList.add(subSketch);
  }

  public void compactSubSketches(int additionalLevel) {
    //    int additionalLevel = 1;
    //    while((1<<additionalLevel)<subSketchList.size())additionalLevel++;
    assert !subSketchList.isEmpty();
    int subLevel = subSketchList.get(0).cntLevel,
        targetLevel = subLevel + additionalLevel,
        tmpNumLen = 0;
    levelPos = new int[targetLevel + 1];
    // System.out.println("\t\t[CompactSubSketchesInLSM:] addLV:"+additionalLevel);
    for (KLLSketchForQuantile subSketch : subSketchList) {
      assert subSketch.cntLevel == subLevel;
      assert subSketch.levelPos[0] == subSketch.levelPos[subLevel - 1];
      tmpNumLen += subSketch.levelPos[subLevel] - subSketch.levelPos[subLevel - 1];
    }
    long[] tmpNum = new long[tmpNumLen * 2];
    int cntNumLen = 0;
    for (KLLSketchForQuantile subSketch : subSketchList) {
      System.arraycopy(
          subSketch.num,
          subSketch.levelPos[subLevel - 1],
          tmpNum,
          cntNumLen,
          subSketch.getLevelSize(subLevel - 1));
      cntNumLen += subSketch.getLevelSize(subLevel - 1);
    }
    Arrays.sort(tmpNum, 0, cntNumLen);
    num = tmpNum;
    for (int i = 0; i < subLevel - 1; i++) levelPos[i] = 0;
    cntLevel = subLevel;
    levelPos[cntLevel] = tmpNumLen;
    //    showNum();
    for (int i = 0; i < additionalLevel; i++) compactOneLevel(cntLevel - 1);
    num = Arrays.copyOfRange(num, 0, levelPos[cntLevel]);
    //    showNum();
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

  public void mergeWithTempSpace(List<KLLSketchForQuantile> otherList) {
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
    for (KLLSketchForQuantile another : otherList)
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
}
