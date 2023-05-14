package org.apache.iotdb.db.utils.quantiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AmortizedForMedian {
  //  long[] bufferValueCount;
  long[] buffer;
  final int bufferSizeLimit, compression;
  int bufferNum;
  //  IntArrayList indexList;
  long[] clusterMinMax, clusterSize, tmpMinMax, tmpSize;
  int clusterNum;
  long totSize;

  public AmortizedForMedian(int compression, int bufferSizeLimit) {
    this.compression = compression;
    //    bufferValueCount = new long[bufferSizeLimit << 1];
    buffer = new long[bufferSizeLimit];
    this.bufferSizeLimit = bufferSizeLimit;
    bufferNum = 0;
    //    indexList = new IntArrayList(bufferSizeLimit);
    //    for (int i = 0; i < bufferSizeLimit; i++) indexList.add(i);
    clusterNum = 0;
    totSize = 0;
    int clusterNumUpperBound = (compression * 2 + 5);
    clusterMinMax = new long[clusterNumUpperBound * 2];
    clusterSize = new long[clusterNumUpperBound];
    tmpMinMax = new long[clusterNumUpperBound * 2];
    tmpSize = new long[clusterNumUpperBound];
  }

  private void sortBuffer() {
    Arrays.sort(buffer, 0, bufferNum);
    //    if(indexList.size()!=bufferNum){
    //      indexList.clear();
    //      for(int i=0;i<bufferNum;i++)indexList.add(i);
    //    }
    //    indexList.sortThis((x, y) -> Long.compare(bufferValueCount[x<<1],
    // bufferValueCount[y<<1]));
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
    int p1 = 0, p2 = 0;
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE, size = 0, minValue, maxValue, count;
    while (p1 < bufferNum || p2 < tmpClusterNum) {
      if (p2 == tmpClusterNum || (p1 < bufferNum && buffer[p1] < calcMid(p2))) {
        minValue = maxValue = buffer[p1];
        count = 1;
        p1++;
      } else {
        minValue = tmpMinMax[p2 << 1];
        maxValue = tmpMinMax[p2 << 1 | 1];
        count = tmpSize[p2];
        p2++;
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

  public void add(final long value) {
    //    System.out.println("\t\t add: "+(value));
    totSize++;
    buffer[bufferNum++] = value;
    if (bufferNum == bufferSizeLimit) updateFromBuffer();
  }

  private long possibleSizeLEValue(long V) {
    long size = 0;
    for (int i = 0; i < clusterNum; i++)
      if (clusterMinMax[i << 1 | 1] <= V) size += clusterSize[i];
      else if (clusterMinMax[i << 1] <= V && V < clusterMinMax[i << 1 | 1])
        size += clusterSize[i] - 1;
    return size;
  }

  private long possibleSizeGEValue(long V) {
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

  public List<Long> findResultRange(long K1, long K2) {
    List<Long> result = new ArrayList<>(4);
    if (bufferNum > 0) updateFromBuffer();

    //    System.out.print("\t\t\t");
    //    for(int
    // i=0;i<clusterNum;i++)System.out.print(clusterMinMax[i<<1]+"~"+clusterMinMax[i<<1|1]+":"+clusterSize[i]+"\t");
    //    System.out.println();
    //    System.out.print("\t\t\t");
    //    for(int
    // i=0;i<clusterNum;i++)System.out.print((clusterMinMax[i<<1])+"~"+(clusterMinMax[i<<1|1])+":"+clusterSize[i]+"\t");
    //    System.out.println();
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
