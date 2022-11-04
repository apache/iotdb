package org.apache.iotdb.db.utils.quantiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TDigestRadixBetterForMedian {
  static final long deltaForUnsignedCompare = 1L << 63;
  //  long[] bufferValueCount;
  long[] buffer;
  final int bufferSizeLimit, compression;
  int bufferNum;
  //  IntArrayList indexList;
  long[] clusterMinMax, clusterSize, tmpMinMax, tmpSize;
  int clusterNum;
  public long totSize;
  final int radixSHR = 8, radixLoopNum, radixMask = (1 << radixSHR) - 1;
  long[] radixNum;
  long[][] radixCount;

  public TDigestRadixBetterForMedian(int compression, int bufferSizeLimit, int bitsOfDataType) {
    this.compression = compression;
    this.bufferSizeLimit = bufferSizeLimit;
    buffer = new long[this.bufferSizeLimit + 1];
    bufferNum = 0;
    clusterNum = 0;
    totSize = 0;
    int clusterNumUpperBound = (compression * 2 + 5);
    clusterMinMax = new long[clusterNumUpperBound * 2];
    clusterSize = new long[clusterNumUpperBound];
    tmpMinMax = new long[clusterNumUpperBound * 2];
    tmpSize = new long[clusterNumUpperBound];
    radixNum = new long[this.bufferSizeLimit];
    radixLoopNum = (bitsOfDataType + radixSHR - 1) / radixSHR;
    radixCount = new long[radixLoopNum][1 << radixSHR];
  }

  private void sortBuffer() {
    int loop;
    for (loop = 0; loop < radixLoopNum; loop++)
      for (int i = 1; i <= radixMask; i++) radixCount[loop][i] += radixCount[loop][i - 1];

    int cntSHR = 0;
    long[] cntCount;
    for (loop = 0; loop < radixLoopNum; loop += 2) {
      cntCount = radixCount[loop];
      if (cntCount[0] == bufferNum) return;
      for (int i = 1; i <= radixMask && cntCount[i - 1] == 0; i++)
        if (cntCount[i] == bufferNum) return;
      for (int i = bufferNum - 1; i >= 0; i--)
        radixNum[(int) (--cntCount[(int) (buffer[i] >>> cntSHR & radixMask)])] = buffer[i];
      cntSHR += radixSHR;

      cntCount = radixCount[loop + 1];
      if (cntCount[0] == bufferNum) {
        System.arraycopy(radixNum, 0, buffer, 0, bufferNum);
        return;
      }
      for (int i = 1; i <= radixMask && cntCount[i - 1] == 0; i++)
        if (cntCount[i] == bufferNum) {
          System.arraycopy(radixNum, 0, buffer, 0, bufferNum);
          return;
        }
      for (int i = bufferNum - 1; i >= 0; i--)
        buffer[(int) (--cntCount[(int) (radixNum[i] >>> cntSHR & radixMask)])] = radixNum[i];
      cntSHR += radixSHR;
    }
  }

  private void addCluster(long size, long min, long max) {
    clusterSize[clusterNum] = size;
    clusterMinMax[clusterNum << 1] = min;
    clusterMinMax[clusterNum << 1 | 1] = max;
    clusterNum++;
  }

  private long calcMid(int clusterId) {
    return tmpMinMax[clusterId << 1]
        + ((tmpMinMax[clusterId << 1 | 1] - tmpMinMax[clusterId << 1]) >>> 1);
  }

  private void updateFromBuffer() {
    sortBuffer();

    System.arraycopy(clusterMinMax, 0, tmpMinMax, 0, clusterNum * 2);
    System.arraycopy(clusterSize, 0, tmpSize, 0, clusterNum);
    int tmpClusterNum = clusterNum;
    clusterNum = 0;

    long expectedClusterSize = (totSize + compression - 1) / compression;
    //    System.out.println("\t\t expectedClusterSize:"+expectedClusterSize + " totSize:"+totSize+"
    // oldClusterNum:"+tmpClusterNum);
    int p1 = 0, p2 = 0;
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE, size = 0;
    long minValue, maxValue, count, val1 = buffer[0] ^ deltaForUnsignedCompare, mid2 = calcMid(0);
    while (p1 < bufferNum || p2 < tmpClusterNum) {
      if (p2 == tmpClusterNum || (p1 < bufferNum && val1 < mid2)) {
        minValue = maxValue = val1;
        count = 1;
        val1 = buffer[++p1] ^ deltaForUnsignedCompare;
      } else {
        minValue = tmpMinMax[p2 << 1];
        maxValue = tmpMinMax[p2 << 1 | 1];
        count = tmpSize[p2];
        mid2 = calcMid(++p2);
      }
      if (size + count <= expectedClusterSize) {
        size += count;
        min = Math.min(minValue, min);
        max = Math.max(maxValue, max);
      } else {
        if (size > 0) addCluster(size, min, max);
        size = count;
        min = minValue;
        max = maxValue;
      }
    }
    addCluster(size, min, max);

    bufferNum = 0;

    for (int loop = 0; loop < radixLoopNum; loop++) Arrays.fill(radixCount[loop], 0);
    //    System.out.print("\t\t\t");
    //    for(int
    // i=0;i<clusterNum;i++)System.out.print(clusterMinMax[i<<1]+"---"+clusterMinMax[i<<1|1]+":"+clusterSize[i]+"\t");
    //    System.out.println();
  }

  //  public void add(final long value,final long count){
  ////    System.out.println("\t\t add: "+(value)+"  "+count);
  //    totSize += count;
  ////    bufferValueCount[bufferNum<<1]=value;
  ////    bufferValueCount[bufferNum<<1|1]=count;
  //    bufferNum++;
  //    if(bufferNum == bufferSizeLimit)
  //      updateFromBuffer();
  //  }

  public void add(long value) {
    //    System.out.println("\t\t add: "+(value));
    totSize++;
    value ^= deltaForUnsignedCompare;
    buffer[bufferNum++] = value;
    for (int loop = 0; loop < radixLoopNum; loop++) {
      radixCount[loop][(int) (value & radixMask)]++;
      value >>>= radixSHR;
    }
    if (bufferNum == bufferSizeLimit) updateFromBuffer();
  }

  private long possibleSizeLEValue(final long V) {
    long size = 0;
    for (int i = 0; i < clusterNum; i++)
      if (clusterMinMax[i << 1 | 1] <= V) size += clusterSize[i];
      else if (clusterMinMax[i << 1] <= V && V < clusterMinMax[i << 1 | 1])
        size += clusterSize[i] - 1;
    return size;
  }

  private long possibleSizeGEValue(final long V) {
    long size = 0;
    for (int i = 0; i < clusterNum; i++)
      if (V <= clusterMinMax[i << 1]) size += clusterSize[i];
      else if (clusterMinMax[i << 1] < V && V <= clusterMinMax[i << 1 | 1])
        size += clusterSize[i] - 1;
    return size;
  }

  private long minValueWithRank(long K) {
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE, mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      if (possibleSizeLEValue(mid) < K) L = mid + 1;
      else R = mid;
    }
    //    System.out.println("[minValueWithRank] K:"+K+" val:"+L);
    return L;
  }

  private long maxValueWithRank(long K) {
    K = totSize - K + 1;
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE, mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      if (mid == L) mid++;
      //      System.out.println("\t\t\t"+L+"  "+mid+"  "+R);
      if (possibleSizeGEValue(mid) < K) R = mid - 1;
      else L = mid;
    }
    //    System.out.println("[maxValueWithRank] K:"+K+" val:"+L);
    return L;
  }

  public List<Long> findResultRange(final long K1, final long K2) {
    List<Long> result = new ArrayList<>(4);
    if (bufferNum > 0) updateFromBuffer();
    result.add(minValueWithRank(K1));
    result.add(maxValueWithRank(K1));
    result.add(minValueWithRank(K2));
    result.add(maxValueWithRank(K2));
    return result;
  }

  public void reset() {
    clusterNum = bufferNum = 0;
    totSize = 0;
  }
}
