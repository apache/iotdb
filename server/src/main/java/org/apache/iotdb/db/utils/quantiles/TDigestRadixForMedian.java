package org.apache.iotdb.db.utils.quantiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TDigestRadixForMedian {
  //  long[] bufferValueCount;
  long[] buffer;
  final int bufferSizeLimit, compression;
  int bufferNum;
  //  IntArrayList indexList;
  long[] clusterMinMax, clusterSize, tmpMinMax, tmpSize;
  int clusterNum;
  long totSize;
  final int radixSHR = 8, radixMask = (1 << radixSHR) - 1;
  long[] radixCount, radixNum;

  public TDigestRadixForMedian(int compression, int bufferSizeLimit) {
    this.compression = compression;
    buffer = new long[bufferSizeLimit];
    this.bufferSizeLimit = bufferSizeLimit;
    bufferNum = 0;
    clusterNum = 0;
    totSize = 0;
    int clusterNumUpperBound = (compression * 2 + 5);
    clusterMinMax = new long[clusterNumUpperBound * 2];
    clusterSize = new long[clusterNumUpperBound];
    tmpMinMax = new long[clusterNumUpperBound * 2];
    tmpSize = new long[clusterNumUpperBound];
    radixCount = new long[1 << radixSHR];
    radixNum = new long[bufferSizeLimit];
  }

  private boolean radix(long[] a, long[] b, final int SHR) {
    Arrays.fill(radixCount, 0);
    for (int i = 0; i < bufferNum; i++) radixCount[(int) (a[i] >>> SHR & radixMask)]++;
    //    if(radixCount[0]==bufferNum) {
    //      System.arraycopy(a, 0, b, 0, bufferNum);
    //      return true;
    //    }
    for (int i = 1; i <= radixMask; i++) radixCount[i] += radixCount[i - 1];
    for (int i = bufferNum - 1; i >= 0; i--)
      b[(int) (--radixCount[(int) (a[i] >>> SHR & radixMask)])] = a[i];
    return false;
  }

  private void sortBuffer() {
    //    Arrays.sort(buffer, 0, bufferNum);
    int cntSHR = 0;
    for (int i = 0; i < 64 / radixSHR; i += 2) {
      if (radix(buffer, radixNum, cntSHR)) break;
      cntSHR += radixSHR;
      if (radix(radixNum, buffer, cntSHR)) break;
      cntSHR += radixSHR;
      //      if(buffer[0]>>>cntSHR == buffer[bufferNum-1]>>>cntSHR)
      //        break;
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

  //  public List<Long> findResultRange(long K1,long K2) {
  //
  //    System.out.print("\t\t\t");
  //    for(int
  // i=0;i<clusterNum;i++)System.out.print(clusterMinMax[i<<1]+"~"+clusterMinMax[i<<1|1]+":"+clusterSize[i]+"\t");
  //    System.out.println();
  //    List<Long> result = new ArrayList<>(4);
  //    if (bufferNum > 0)
  //      updateFromBuffer();
  //    indexList.clear();
  //    for (int i = 0; i < clusterNum; i++)
  //      indexList.add(i);
  //    indexList.sortThis((x, y) -> Long.compare(clusterMinMax[x << 1 | 1], clusterMinMax[y << 1 |
  // 1]));
  //    long sum = 0, lowerBound = Long.MIN_VALUE, upperBound = Long.MAX_VALUE;
  //    long L1 = lowerBound, L2 = lowerBound, R1 = upperBound, R2 = upperBound;
  //    int index;
  //    for (int i = 0; i < clusterNum; i++) {
  //      index = indexList.get(i);
  //      if (sum < K1 && sum + clusterSize[index] >= K1)
  //        L1 = lowerBound;
  //      if (sum < K2 && sum + clusterSize[index] >= K2) {
  //        L2 = lowerBound;
  //        break;
  //      }
  //      sum += clusterSize[index];
  //      lowerBound = clusterMinMax[index << 1 | 1];
  //    }
  //    sum = 0;
  //    indexList.sortThis((x, y) -> Long.compare(clusterMinMax[x << 1], clusterMinMax[y << 1]));
  //    for (int i = clusterNum - 1; i >= 0; i--) {
  //      index = indexList.get(i);
  //      if (sum < totSize - K2 + 1 && sum + clusterSize[index] >= totSize - K2 + 1)
  //        R2 = upperBound;
  //      if (sum < totSize - K1 + 1 && sum + clusterSize[index] >= totSize - K1 + 1) {
  //        R1 = upperBound;
  //        break;
  //      }
  //      sum += clusterSize[index];
  //      upperBound = clusterMinMax[index << 1];
  //    }
  //    result.add(L1);
  //    result.add(R1);
  //    result.add(L2);
  //    result.add(R2);
  //    return result;
  //  }
  //
  //  public List<Long> findResultRange(long K1, long K2){
  //    System.out.println("[DEBUG] findResultRange   cluster:");
  ////    System.out.print("\t\t\t");
  ////    for(int
  // i=0;i<clusterNum;i++)System.out.print((clusterMinMax[i<<1]^deltaForUnsignedCompare)+"~"+(clusterMinMax[i<<1|1]^deltaForUnsignedCompare)+":"+clusterSize[i]+"\t");
  ////    System.out.println();
  ////    System.out.print("\t\t\t");
  ////    for(int
  // i=0;i<clusterNum;i++)System.out.print((clusterMinMax[i<<1|1]-clusterMinMax[i<<1])+","+clusterSize[i]+"\t");
  ////    System.out.println();
  //    double totInterval = 0,MMPSIZE=0,FAKEMIN = 1e80;
  //    for(int i=0;i<clusterNum;i++){
  //      double interval=clusterMinMax[i<<1|1]-clusterMinMax[i<<1];
  //      totInterval+=interval;
  //      if(interval<=2333.0)
  //        MMPSIZE+=clusterSize[i];
  //      else
  //        if(Math.abs(clusterMinMax[i<<1]^deltaForUnsignedCompare)<=2333.0)
  //          FAKEMIN = Math.min(FAKEMIN, clusterMinMax[i<<1]^deltaForUnsignedCompare);
  //    }
  //    System.out.println("[avg interval]:"+ totInterval /clusterNum + "\t\t
  // precise_percent:"+MMPSIZE/totSize+"   FAKE_MIN:"+FAKEMIN);
  //    List<Long> result = new ArrayList<>(7);
  //    if(bufferNum>0)
  //      updateFromBuffer();
  //    long sum=0;
  //    for(int i=0;i<clusterNum;i++){
  //      if(sum+clusterSize[i]>=K1&&result.size()==0) {
  //        result.add(sum);
  //        result.add(clusterMinMax[i<<1]^deltaForUnsignedCompare);
  //        result.add(clusterMinMax[i<<1|1]^deltaForUnsignedCompare);
  //      }
  //      if(sum+clusterSize[i]>=K2&&result.size()==3) {
  //        result.add(sum);
  //        result.add(clusterMinMax[i<<1]^deltaForUnsignedCompare);
  //        result.add(clusterMinMax[i<<1|1]^deltaForUnsignedCompare);
  //        return result;
  //      }
  //      sum+=clusterSize[i];
  //    }
  //    return result;
  //  }

}
