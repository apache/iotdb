package org.apache.iotdb.tsfile.utils; // inspired by t-Digest by Ted Dunning. See
// https://github.com/tdunning/t-digest
// This is a simple implementation with radix sort and K0.
// Clusters are NOT strictly in order.

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;

public class TDigestForDupli {
  static final int ElementSeriByte = /*4*8*/ 2 * 8; // use only avg and count in estimation
  static final double EPS = 1e-11;
  public int compression;
  public ObjectArrayList<Cluster> cluster;
  public int clusterNumMemLimit, clusterNumSeriLimit;
  public int maxSeriByte, maxMemByte;
  public long totN;
  boolean sorted = true;
  boolean readyForQuery = false;

  public long getN() {
    return totN;
  }

  class Cluster {
    long count;
    double avg, min, max;

    public Cluster() {
      count = 0;
      avg = 0;
      min = Double.MAX_VALUE;
      max = -Double.MAX_VALUE;
    }

    public Cluster(double v) {
      count = 1;
      avg = min = max = v;
    }

    public Cluster(long count, double avg, double min, double max) {
      this.count = count;
      this.avg = avg;
      this.min = min;
      this.max = max;
    }

    public Cluster(Cluster a, Cluster b) {
      long sizeA = a.count, sizeB = b.count;
      avg = a.avg * (1.0 * sizeA / (sizeA + sizeB)) + b.avg * (1.0 * sizeB / (sizeA + sizeB));
      count = a.count + b.count;
      min = Math.min(a.min, b.min);
      max = Math.max(a.max, b.max);
    }

    public void mergeWith(Cluster b) {
      long sizeA = count, sizeB = b.count;
      avg = avg * (1.0 * sizeA / (sizeA + sizeB)) + b.avg * (1.0 * sizeB / (sizeA + sizeB));
      count = count + b.count;
      min = Math.min(min, b.min);
      max = Math.max(max, b.max);
    }
  }

  //  public TDigestForExact(int maxMemByte, int maxSeriByte) {
  //    maxMemByte+=16;
  //    this.maxMemByte = maxMemByte;
  //    this.maxSeriByte = maxSeriByte;
  //    this.clusterNumMemLimit = maxMemByte / 5 / 8;
  //    this.clusterNumSeriLimit = maxSeriByte / (8+2);
  ////    this.compression = this.clusterNumMemLimit / 11; //  cluster:buffer = 1:10
  //    this.compression = this.clusterNumMemLimit / 8; //  cluster:buffer = 1:7
  //    clusterNum = 0;
  //    totN = 0;
  //    cluster = new DoubleObjectPair<countMinMax>[clusterNumMemLimit];
  //  }
  public TDigestForDupli(int maxMemByte) {
    this.maxMemByte = maxMemByte;
    this.maxSeriByte = maxMemByte;
    this.clusterNumMemLimit = maxMemByte / ElementSeriByte;
    //    this.clusterNumSeriLimit = maxSeriByte / 2 / 8;
    //    this.compression = this.clusterNumMemLimit / 11; //  cluster:buffer = 1:10
    this.compression = this.clusterNumMemLimit / (1 + 1); //  cluster:buffer = 1:1
    totN = 0;
    cluster = new ObjectArrayList<>(clusterNumMemLimit);
  }

  public TDigestForDupli(int maxMemByte, int mergingBuffer) {
    this(maxMemByte);
    this.compression =
        this.clusterNumMemLimit / (1 + mergingBuffer); //  cluster:buffer = 1:mergingBuffer
  }

  public TDigestForDupli(int maxMemByte, int mergingBuffer, int maxSeriByte) {
    this(maxMemByte, mergingBuffer);
    this.maxSeriByte = maxSeriByte;
  }

  private void addCluster(Cluster c) {
    readyForQuery = false;
    if (cluster.size() == clusterNumMemLimit) compaction(this.compression);
    cluster.add(c);
  }

  private void addCluster(double v) {
    addCluster(new Cluster(v));
  }

  public void sortCluster() {
    if (sorted) return;
    cluster.sort(Comparator.comparingDouble(x -> x.avg));
    //    Arrays.parallelSort(cluster,0,clusterNum,
    // Comparator.comparingDouble(DoubleLongPair::getOne));
    sorted = true;
  }

  private void compaction(int compression) {
    sortCluster();
    int oldClusterNum = cluster.size();
    int cntClusterNum = 0;

    long expectedClusterSize = (totN + compression - 1) / compression;
    Cluster cnt = new Cluster();
    //    System.out.println("\t\t expectedClusterSize:"+expectedClusterSize + " totN:"+totN+"
    // oldClusterNum:"+oldClusterNum);
    for (int i = 0; i < oldClusterNum; i++) {
      if (compression != this.compression
          && oldClusterNum - i + cntClusterNum + (cnt.count > 0 ? 1 : 0)
              <= maxSeriByte / ElementSeriByte) { // for serialize.
        //        System.out.println("\t\t\t\tpartial compaction!");
        if (cnt.count > 0) cluster.set(cntClusterNum++, cnt);
        //          addCluster(cnt);
        for (; i < oldClusterNum; i++) cluster.set(cntClusterNum++, cluster.get(i));
        //          addCluster(cluster[i]);
        //        System.out.println("\t\t\t\tpartial compaction!"+clusterNum);
        cluster.size(cntClusterNum);
        return;
      }
      if (cnt.count + cluster.get(i).count <= expectedClusterSize) {
        cnt.mergeWith(cluster.get(i));
      } else {
        if (cnt.count > 0) cluster.set(cntClusterNum++, cnt);
        cnt = cluster.get(i);
      }
    }
    if (cnt.count > 0) cluster.set(cntClusterNum++, cnt);
    cluster.size(cntClusterNum);
    //    System.out.println("\t\t after compaction:"+oldClusterNum+"-->"+clusterNum);
    //    show();
  }

  public void update(double value) {
    totN++;
    sorted = false;
    addCluster(value);
  }

  public void merge(TDigestForDupli another) {
    sorted = false;
    //    System.out.println("\t\t add: "+(value));
    totN += another.totN;
    for (int i = 0; i < another.cluster.size(); i++) addCluster(another.cluster.get(i));
  }

  public void merge(List<TDigestForDupli> anotherList) {
    for (TDigestForDupli another : anotherList) merge(another);
  }

  public double getQuantile(double q) {
    sortCluster();
    double preN = 0;
    for (int i = 0; i < cluster.size(); i++) {
      if (preN + 0.5 * cluster.get(i).count >= q * totN) {
        if (i == 0) return cluster.get(i).avg;
        Cluster c1 = cluster.get(i - 1), c2 = cluster.get(i);
        double wLeft = q * totN - preN + 0.5 * c1.count;
        double wRight = preN - q * totN + 0.5 * c2.count;
        return (c1.avg * wRight + c2.avg * wLeft) / (wLeft + wRight);
      }
      preN += cluster.get(i).count;
    }
    return cluster.get(cluster.size() - 1).avg;
  }

  public DoubleArrayList quantiles(DoubleArrayList qs) {
    sortCluster();
    DoubleArrayList ans = new DoubleArrayList();
    double preN = 0;
    int pos = 0;
    for (double q : qs) {
      while (pos < cluster.size() && preN + 0.5 * cluster.get(pos).count < q * totN) {
        preN += cluster.get(pos).count;
        pos++;
      }
      if (pos == 0) ans.add(cluster.get(0).avg);
      else if (pos >= cluster.size()) ans.add(cluster.get(cluster.size() - 1).avg);
      else {
        Cluster c1 = cluster.get(pos - 1), c2 = cluster.get(pos);
        double wLeft = q * totN - preN + 0.5 * c1.count;
        double wRight = preN - q * totN + 0.5 * c2.count;
        ans.add((c1.avg * wRight + c2.avg * wLeft) / (wLeft + wRight));
      }
    }
    return ans;
  }

  public double getAllMin() {
    prepareQuery();
    return sortedMin[0];
    //    double mn=Double.MAX_VALUE;for(Cluster c:cluster)mn=Math.min(mn,c.min);return mn;
  }

  public double getAllMax() {
    prepareQuery();
    return sortedMax[cluster.size() - 1];
    //    double mx=-Double.MAX_VALUE;for(Cluster c:cluster)mx=Math.max(mx,c.max);return mx;
  }

  private boolean checkPossibleLEQ(
      Cluster c, long k, double V) { // at least k numbers (exclude min&max) in cluster <= V
    // c.min <= V < c.max
    long k2 = c.count - 2 - k;
    //    boolean flag1 = c.min+c.max + c.min*k + V*k2 <= c.avg*c.count;
    boolean flag1 =
        c.min / c.count + c.max / c.count + (1.0 * k / c.count * c.min) + (1.0 * k2 / c.count * V)
            <= c.avg;
    //    boolean flag2 = c.min+c.max + (k*V) + (k2*c.max) >= c.avg*c.count;
    boolean flag2 =
        c.min / c.count + c.max / c.count + (1.0 * k / c.count * V) + (1.0 * k2 / c.count * c.max)
            >= c.avg;
    //    return flag1&&flag2;
    return flag1;
  }

  private boolean checkPossibleGEQ(
      Cluster c, long k, double V) { // at least k numbers (exclude min&max) in cluster >= V
    // c.min < V <= c.max
    long k2 = c.count - 2 - k;
    boolean flag1 =
        c.min / c.count + c.max / c.count + (1.0 * k / c.count * V) + (1.0 * k2 / c.count * c.min)
            <= c.avg;
    boolean flag2 =
        c.min / c.count + c.max / c.count + (1.0 * k / c.count * c.max) + (1.0 * k2 / c.count * V)
            >= c.avg;
    //    return flag1&&flag2;
    return flag2;
  }

  private long possibleSizeLEValue(double v) {
    long size = 0;
    for (Cluster c : cluster)
      if (c.max <= v) size += c.count;
      else if (c.min <= v && c.count > 1) { // v < c.max
        size += c.count - 1;
      }
    return size;
  }

  private long possibleSizeGEValue(double v) {
    long size = 0;
    for (Cluster c : cluster)
      if (c.min >= v) size += c.count;
      else if (c.max > v && c.count > 1) { // c.min<v
        size += c.count - 1;
      }

    return size;
  }

  private long fastPossibleSizeLEValue(double v) {
    prepareQuery();
    int index = -1;
    for (int tmp = Integer.highestOneBit(cluster.size()); tmp > 0; tmp >>>= 1)
      if (index + tmp < cluster.size() && sortedMin[index + tmp] <= v) index += tmp;
    // cluster[index+1].min > v
    if (index == -1) return 0;
    else return preCountSum[index];
  }

  private long fastPossibleSizeGEValue(double v) {
    prepareQuery();
    int index = cluster.size();
    for (int tmp = Integer.highestOneBit(cluster.size()); tmp > 0; tmp >>>= 1)
      if (index - tmp >= 0 && sortedMax[index - tmp] >= v) index -= tmp;
    // cluster[index-1].max < v
    if (index == cluster.size()) return 0;
    else return sufCountSum[index];
  }

  long[] preCountSum, sufCountSum;
  double[] sortedMin, sortedMax;

  private void prepareQuery() {
    if (readyForQuery) return;
    readyForQuery = true;
    cluster.sort(Comparator.comparingDouble(x -> x.min));
    preCountSum = new long[cluster.size()];
    sortedMin = new double[cluster.size()];
    long tmpSum = 0;
    for (int i = 0; i < cluster.size(); i++) {
      sortedMin[i] = cluster.get(i).min;
      tmpSum += cluster.get(i).count;
      preCountSum[i] = tmpSum;
    }
    cluster.sort(Comparator.comparingDouble(x -> x.max));
    sufCountSum = new long[cluster.size()];
    sortedMax = new double[cluster.size()];
    tmpSum = 0;
    for (int i = cluster.size() - 1; i >= 0; i--) {
      sortedMax[i] = cluster.get(i).max;
      tmpSum += cluster.get(i).count;
      sufCountSum[i] = tmpSum;
    }
  }

  private long dataToLong(double data) throws UnSupportedDataTypeException {
    long result;
    result = Double.doubleToLongBits((double) data);
    return (double) data >= 0d ? result : result ^ Long.MAX_VALUE;
  }

  private double longToResult(long result) throws UnSupportedDataTypeException {
    result = (result >>> 63) == 0 ? result : result ^ Long.MAX_VALUE;
    return Double.longBitsToDouble(result);
  }

  private double minValueWithRank(long K) {
    double L = getAllMin(), R = getAllMax(), mid;
    long longL = dataToLong(L), longR = dataToLong(R), longMid;
    //    double cntEPS = Math.max(1,Math.log10(Math.max(Math.abs(L),Math.abs(R))));
    //    System.out.println("\t\t\t"+cntEPS);
    //    cntEPS=EPS*Math.pow(10,cntEPS/2);
    //    System.out.println("minValueWithRank\t\t\t"+L+" "+R+"\t\teps:"+cntEPS);
    while (longL < longR) {
      longMid = longL + ((longR - longL + 1) >>> 1);
      //      System.out.println("\t\t\t\t\t"+L+"\t"+R+"\t\t\tmid:\t"+mid);
      if (fastPossibleSizeLEValue(longToResult(longMid)) <= K) longL = longMid;
      //      if (possibleSizeLEValue(mid) <= K) L = mid;
      else longR = longMid - 1;
    }
    //    System.out.println("minValueWithRank OVER");
    //    System.out.println("[minValueWithRank] K:"+K+" val:"+L+"\t\t"+possibleSizeLEValue(L));
    //    System.out.println("\t\t?!?!\t"+possibleSizeLEValue(-74.00054931640625)+"\t\t");
    //    System.out.println("\t\t?!?!\t"+possibleSizeLEValue(-74.00054931641387)+"\t\t");
    return longToResult(longL);
  }

  private double maxValueWithRank(long K) {
    K = totN - K + 1;
    double L = getAllMin(), R = getAllMax(), mid;
    long longL = dataToLong(L), longR = dataToLong(R), longMid;
    double cntEPS = Math.max(1, Math.log10(Math.max(Math.abs(L), Math.abs(R))));
    cntEPS = EPS * Math.pow(10, cntEPS / 2);
    //    System.out.println("maxValueWithRank\t\t\t"+L+" "+R+"\t\teps:"+cntEPS);
    while (longL < longR) {
      longMid = longL + ((longR - longL + 1) >>> 1);
      if (fastPossibleSizeGEValue(longToResult(longMid)) < K) longR = longMid - 1;
      //      if (possibleSizeGEValue(mid) < K) R=mid-cntEPS;
      else longL = longMid;
    }
    //    System.out.println("maxValueWithRank OVER");
    if (longL < dataToLong(L)) longL++;
    //    System.out.println("[maxValueWithRank] K:"+K+" val:"+L+"\t\t"+possibleSizeGEValue(L));
    return longToResult(longL);
  }

  public double[] findResultRange(long K1, long K2) {
    //    System.out.println("\t\t\tfinding range\t\tK1,2:"+K1+","+K2);
    DoubleArrayList result = new DoubleArrayList(4);
    double valL = minValueWithRank(K1 - 1);
    result.add(valL);
    //    System.out.println("\t\t\t\tFound valL:\t"+valL);
    double valR = maxValueWithRank(K2);
    result.add(valR);
    //    System.out.println("\t\t\t\tFound valR:\t"+valR);
    return result.toDoubleArray();
  }

  public double[] getFilterL(long CountOfValL, long CountOfValR, double valL, double valR, long K) {
    if (K <= CountOfValL) return new double[] {valL, -233.0};
    if (K > CountOfValL + totN) return new double[] {valR, -233.0};
    K -= CountOfValL;
    double lb = minValueWithRank(K - 1);
    return new double[] {lb};
  }

  public double[] getFilterR(long CountOfValL, long CountOfValR, double valL, double valR, long K) {
    if (K <= CountOfValL) return new double[] {valL, -233.0};
    if (K > CountOfValL + totN) return new double[] {valR, -233.0};
    K -= CountOfValL;
    double ub = maxValueWithRank(K);
    return new double[] {ub};
  }

  public double[] getFilter(
      long CountOfValL, long CountOfValR, double valL, double valR, long K1, long K2) {
    double[] filterL = getFilterL(CountOfValL, CountOfValR, valL, valR, K1);
    double[] filterR = getFilterR(CountOfValL, CountOfValR, valL, valR, K2);
    //    System.out.println("\t\t\t\tvalL,R:\t"+filterL[0]+"..."+filterR[0]);
    if (filterL.length + filterR.length == 4) return new double[] {filterL[0], filterR[0], -233};
    else return new double[] {filterL[0], filterR[0]};
  }

  public long findMaxNumberInRange(double L, double R) {
    long num = 0;
    for (Cluster c : cluster) {
      if (c.max < L || c.min > R) continue;
      if (L <= c.min && R >= c.max) num += c.count;
      else if (c.min < L && R < c.max) num += c.count - 2;
      else num += c.count - 1;
    }
    return num;
  }

  public void reset() {
    cluster.clear();
    totN = 0;
  }

  public void compactBeforeSerialization() {
    //    System.out.println("\t\t?compactBeforeSeri\t"+clusterNum);
    int start_compression = this.maxSeriByte / (8 + 2) * 4;
    compaction(start_compression);
    //    System.out.println("\t\t?compactBeforeSeri\t"+clusterNum);
    while (cluster.size() * ElementSeriByte > maxSeriByte) {
      start_compression = start_compression * 4 / 5;
      //      System.out.println("\t\t?compactBeforeSeri\t\t\t\ttry compression"+start_compression);
      compaction(start_compression);
      //      System.out.println("\t\t?compactBeforeSeri\t"+clusterNum);
    }
    //    System.out.println("\t\t?compactBeforeSeri\t"+clusterNum);
    //    System.out.println("\t\tcompactBeforeSeri OVER\t");
  }

  public void show() {
    System.out.print("\t\t[DEBUG TDigest]\t" + cluster.size() + " items\t");
    DecimalFormat df = new DecimalFormat("0.0E0");
    for (int i = 0; i < cluster.size(); i++)
      System.out.print(
          "("
              + df.format(cluster.get(i).avg)
              + ","
              + cluster.get(i).count
              + " "
              + (cluster.get(i).min)
              + "..."
              + (cluster.get(i).max)
              + ")"
              + "\t\t");
    //    for(int i=0;i<cluster.size();i++)System.out.print("("+
    // df.format(cluster.get(i).avg)+","+cluster.get(i).count
    //        +" "+df.format(cluster.get(i).min)+"..."+df.format(cluster.get(i).max)+")"+"\t\t");
    //    for(int i=0;i<cluster.size();i++)System.out.print("("+
    // (long)(cluster.get(i).avg)+","+cluster.get(i).count
    //        +" "+(long)(cluster.get(i).min)+"..."+(long)(cluster.get(i).max)+")"+"\t\t");
    System.out.println();
  }

  //

  public int serialize(OutputStream outputStream) throws IOException { // 15+1*?+8*?
    compactBeforeSerialization(); // if N==maxN
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(totN, outputStream);
    byteLen += ReadWriteIOUtils.write(cluster.size(), outputStream);
    for (Cluster c : cluster) {
      byteLen += ReadWriteIOUtils.write(c.count, outputStream);
      byteLen += ReadWriteIOUtils.write(c.avg, outputStream);
      byteLen += ReadWriteIOUtils.write(c.min, outputStream);
      byteLen += ReadWriteIOUtils.write(c.max, outputStream);
    }
    return byteLen;
  }
  //
  //  public TDigestForExact(InputStream inputStream, int maxMemoryByte, int maxSerializeByte)
  // throws IOException {
  //    this(maxMemoryByte);
  //    this.totN = ReadWriteIOUtils.readLong(inputStream);
  //    int clusterNum = ReadWriteIOUtils.readShort(inputStream);
  //    for(int i=0;i<clusterNum;i++){
  //      double a = ReadWriteIOUtils.readDouble(inputStream);
  //      long b = ReadWriteIOUtils.readShort(inputStream);
  //      addCluster(PrimitiveTuples.pair(a,b));
  //    }
  //    this.sorted = false;
  //  }
  //
  //  public TDigestForExact(ByteBuffer byteBuffer, int maxMemoryByte, int maxSerializeByte) {
  //    this(maxMemoryByte);
  //    this.totN = ReadWriteIOUtils.readLong(byteBuffer);
  //    int clusterNum = ReadWriteIOUtils.readShort(byteBuffer);
  //    for(int i=0;i<clusterNum;i++){
  //      double a = ReadWriteIOUtils.readDouble(byteBuffer);
  //      long b = ReadWriteIOUtils.readShort(byteBuffer);
  //      addCluster(PrimitiveTuples.pair(a,b));
  //    }
  //    this.sorted = false;
  //  }
  //
  public TDigestForDupli(InputStream inputStream) throws IOException {
    this.totN = ReadWriteIOUtils.readLong(inputStream);
    int clusterNum = ReadWriteIOUtils.readInt(inputStream);
    int maxMemByte = (clusterNum + 2) * ElementSeriByte;
    this.maxMemByte = maxMemByte;
    this.maxSeriByte = maxMemByte;
    this.clusterNumMemLimit = maxMemByte / ElementSeriByte;
    this.compression = this.clusterNumMemLimit / (1 + 1); //  cluster:buffer = 1:1
    cluster = new ObjectArrayList<>(clusterNum);
    for (int i = 0; i < clusterNum; i++) {
      long count = ReadWriteIOUtils.readLong(inputStream);
      double avg = ReadWriteIOUtils.readDouble(inputStream);
      double min = ReadWriteIOUtils.readDouble(inputStream);
      double max = ReadWriteIOUtils.readDouble(inputStream);
      cluster.add(new Cluster(count, avg, min, max));
    }
    this.sorted = false;
  }

  public TDigestForDupli(ByteBuffer byteBuffer) {
    this.totN = ReadWriteIOUtils.readLong(byteBuffer);
    int clusterNum = ReadWriteIOUtils.readInt(byteBuffer);
    int maxMemByte = (clusterNum + 2) * ElementSeriByte;
    this.maxMemByte = maxMemByte;
    this.maxSeriByte = maxMemByte;
    this.clusterNumMemLimit = maxMemByte / ElementSeriByte;
    this.compression = this.clusterNumMemLimit / (1 + 1); //  cluster:buffer = 1:1
    cluster = new ObjectArrayList<>(clusterNum);
    for (int i = 0; i < clusterNum; i++) {
      long count = ReadWriteIOUtils.readLong(byteBuffer);
      double avg = ReadWriteIOUtils.readDouble(byteBuffer);
      double min = ReadWriteIOUtils.readDouble(byteBuffer);
      double max = ReadWriteIOUtils.readDouble(byteBuffer);
      cluster.add(new Cluster(count, avg, min, max));
    }
    this.sorted = false;
  }
}
