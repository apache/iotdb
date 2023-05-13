package org.apache.iotdb.tsfile.utils; // inspired by t-Digest by Ted Dunning. See
// https://github.com/tdunning/t-digest
// This is a simple implementation with radix sort and K0.
// Clusters are NOT strictly in order.

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;

public class TDigestForExact {
  static final double EPS = 1e-11;
  public final int compression;
  public ObjectArrayList<Cluster> cluster;
  public int clusterNumMemLimit, clusterNumSeriLimit;
  public int maxSeriByte, maxMemByte;
  public long totN;
  boolean sorted = true;

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
  public TDigestForExact(int maxMemByte, int mergingBuffer) {
    this.maxMemByte = maxMemByte;
    this.maxSeriByte = maxMemByte;
    this.clusterNumMemLimit = maxMemByte / 4 / 8;
    //    this.clusterNumSeriLimit = maxSeriByte / 2 / 8;
    //    this.compression = this.clusterNumMemLimit / 11; //  cluster:buffer = 1:10
    this.compression = this.clusterNumMemLimit / (1 + mergingBuffer); //  cluster:buffer = 1:1
    totN = 0;
    cluster = new ObjectArrayList<>(clusterNumMemLimit);
  }

  public TDigestForExact(int maxMemByte) {
    this.maxMemByte = maxMemByte;
    this.maxSeriByte = maxMemByte;
    this.clusterNumMemLimit = maxMemByte / 4 / 8;
    //    this.clusterNumSeriLimit = maxSeriByte / 2 / 8;
    //    this.compression = this.clusterNumMemLimit / 11; //  cluster:buffer = 1:10
    this.compression = this.clusterNumMemLimit / (1 + 1); //  cluster:buffer = 1:1
    totN = 0;
    cluster = new ObjectArrayList<>(clusterNumMemLimit);
  }

  private void addCluster(Cluster c) {
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
              <= maxSeriByte / 2 / 8) { // for serialize.
        //        System.out.println("\t\t\t\tpartial compaction!");
        if (cnt.count > 0) cluster.set(cntClusterNum++, cnt);
        //          addCluster(cnt);
        for (; i < oldClusterNum; i++) cluster.set(cntClusterNum++, cluster.get(i));
        //          addCluster(cluster[i]);
        //        System.out.println("\t\t\t\tpartial compaction!"+clusterNum);
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

  public void merge(TDigestForExact another) {
    sorted = false;
    //    System.out.println("\t\t add: "+(value));
    totN += another.totN;
    for (int i = 0; i < another.cluster.size(); i++) addCluster(another.cluster.get(i));
  }

  public void merge(List<TDigestForExact> anotherList) {
    for (TDigestForExact another : anotherList) merge(another);
  }

  public double quantile(double q) {
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

  public double getAllMin() {
    double mn = Double.MAX_VALUE;
    for (Cluster c : cluster) mn = Math.min(mn, c.min);
    return mn;
  }

  public double getAllMax() {
    double mx = -Double.MAX_VALUE;
    for (Cluster c : cluster) mx = Math.max(mx, c.max);
    return mx;
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
        //        int num=0;
        //        for(long addNum=Long.highestOneBit(c.count-2);addNum>=1;addNum>>=1)
        //          if(checkPossibleLEQ(c,num+addNum,v))
        //            num+=addNum;
        //        size+=num+1;
      }
    return size;
  }

  private long possibleSizeGEValue(double v) {
    long size = 0;
    for (Cluster c : cluster)
      if (c.min >= v) size += c.count;
      else if (c.max > v && c.count > 1) { // c.min<v
        size += c.count - 1;
        //        int num=0;
        //        for(long addNum=Long.highestOneBit(c.count-2);addNum>=1;addNum>>=1)
        //          if(checkPossibleLEQ(c,num+addNum,v))
        //            num+=addNum;
        //        size+=num+1;
      }
    return size;
  }

  private double minValueWithRank(long K) {
    double L = getAllMin(), R = getAllMax(), mid;
    double cntEPS = Math.max(1, Math.log10(Math.max(Math.abs(L), Math.abs(R))));
    //    System.out.println("\t\t\t"+cntEPS);
    cntEPS = EPS * Math.pow(10, cntEPS / 2);
    while (L + cntEPS < R) {
      mid = 0.5 * (L + R);
      //      System.out.println("\t\t\t\t\t"+L+"\t"+R+"\t\t\tmid:\t"+mid);
      if (possibleSizeLEValue(mid) <= K) L = mid;
      else R = mid - cntEPS;
    }
    //    System.out.println("[minValueWithRank] K:"+K+" val:"+L+"\t\t"+possibleSizeLEValue(L));
    //    System.out.println("\t\t?!?!\t"+possibleSizeLEValue(-74.00054931640625)+"\t\t");
    //    System.out.println("\t\t?!?!\t"+possibleSizeLEValue(-74.00054931641387)+"\t\t");
    return L;
  }

  private double maxValueWithRank(long K) {
    K = totN - K + 1;
    double L = getAllMin(), R = getAllMax(), mid;
    double cntEPS = Math.max(1, Math.log10(Math.max(Math.abs(L), Math.abs(R))));
    cntEPS = EPS * Math.pow(10, cntEPS / 2);
    while (L + cntEPS < R) {
      mid = 0.5 * (L + R);
      if (possibleSizeGEValue(mid) < K) R = mid - cntEPS;
      else L = mid;
    }
    L = Math.min(getAllMax(), L + cntEPS * 5);
    //    System.out.println("[maxValueWithRank] K:"+K+" val:"+L+"\t\t"+possibleSizeGEValue(L));
    return L;
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
    while (cluster.size() * 2 * 8 > maxSeriByte) {
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

  public long getN() {
    return totN;
  }
  //

  //  public int serialize(OutputStream outputStream) throws IOException {// 15+1*?+8*?
  //    compactBeforeSerialization();// if N==maxN
  //    int byteLen = 0;
  //    byteLen+=ReadWriteIOUtils.write(totN, outputStream);
  //    byteLen+=ReadWriteIOUtils.write((short)clusterNum, outputStream);
  //    for(int i=0;i<clusterNum;i++){
  //      byteLen+=ReadWriteIOUtils.write(cluster[i].getOne(),outputStream);
  //      byteLen+=ReadWriteIOUtils.write((short)cluster[i].getTwo(),outputStream);
  //    }
  //    return byteLen;
  //  }
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
  //  public TDigestForExact(InputStream inputStream) throws IOException {
  //    this.totN = ReadWriteIOUtils.readLong(inputStream);
  //    int clusterNum = ReadWriteIOUtils.readShort(inputStream);
  //    int maxMemByte = clusterNum*16+16;
  //    this.maxMemByte = maxMemByte;
  //    this.maxSeriByte = maxMemByte;
  //    this.clusterNumMemLimit = maxMemByte / 2 / 8;
  //    this.clusterNumSeriLimit = maxSeriByte / (2 + 8);
  ////    this.compression = this.clusterNumMemLimit / 6; //  cluster:buffer = 1:5
  //    this.compression = this.clusterNumMemLimit / 8; //  cluster:buffer = 1:7
  //    this.clusterNum = 0;
  //    cluster = new DoubleLongPair[clusterNumMemLimit];
  //    for(int i=0;i<clusterNum;i++){
  //      double a = ReadWriteIOUtils.readDouble(inputStream);
  //      long b = ReadWriteIOUtils.readShort(inputStream);
  //      addCluster(PrimitiveTuples.pair(a,b));
  //    }
  //    this.sorted = false;
  //  }
  //
  //  public TDigestForExact(ByteBuffer byteBuffer) {
  //
  //    this.totN = ReadWriteIOUtils.readLong(byteBuffer);
  //    int clusterNum = ReadWriteIOUtils.readShort(byteBuffer);
  //    int maxMemByte = clusterNum * 16 + 16;
  //    this.maxMemByte = maxMemByte;
  //    this.maxSeriByte = maxMemByte;
  //    this.clusterNumMemLimit = maxMemByte / 2 / 8;
  //    this.clusterNumSeriLimit = maxSeriByte / (2 + 8);
  ////    this.compression = this.clusterNumMemLimit / 6; //  cluster:buffer = 1:5
  //    this.compression = this.clusterNumMemLimit / 8; //  cluster:buffer = 1:7
  //    this.clusterNum = 0;
  //    cluster = new DoubleLongPair[clusterNumMemLimit];
  //    for (int i = 0; i < clusterNum; i++) {
  //      double a = ReadWriteIOUtils.readDouble(byteBuffer);
  //      long b = ReadWriteIOUtils.readShort(byteBuffer);
  //      addCluster(PrimitiveTuples.pair(a, b));
  //    }
  //    this.sorted = false;
  //  }
}
