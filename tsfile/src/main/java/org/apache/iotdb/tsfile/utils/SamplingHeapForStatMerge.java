package org.apache.iotdb.tsfile.utils; // inspired by t-Digest by Ted Dunning. See
// https://github.com/tdunning/t-digest
// This is a simple implementation with radix sort and K0.
// Clusters are NOT strictly in order.

import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.eclipse.collections.api.iterator.MutableDoubleIterator;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

// "Weighted random sampling with a reservoir"
public class SamplingHeapForStatMerge {
  public final int sampleLimit;
  public IntHeapPriorityQueue indexHeap;
  double[] score, weight;
  long[] value;
  public int maxSeriByte, maxMemByte;
  public long totN;
  public XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom();
  boolean sorted = false;
  public IntArrayList indexList;
  public long minV, maxV;

  public SamplingHeapForStatMerge(int maxMemByte, int maxSeriByte) {
    this.maxMemByte = maxMemByte;
    this.maxSeriByte = maxSeriByte;
    this.sampleLimit = maxSeriByte / (8);
    this.indexHeap =
        new IntHeapPriorityQueue(sampleLimit, (x, y) -> (Double.compare(score[x], score[y])));
    this.indexList = new IntArrayList(sampleLimit);
    this.score = new double[sampleLimit];
    this.value = new long[sampleLimit];
    this.weight = new double[sampleLimit];
    this.minV = Long.MAX_VALUE;
    this.maxV = Long.MIN_VALUE;
  }

  public SamplingHeapForStatMerge(int maxMemByte) {
    this.maxMemByte = maxMemByte;
    this.maxSeriByte = maxMemByte;
    this.sampleLimit = maxMemByte / (8 * 3 + 4 + 4);
    this.indexHeap =
        new IntHeapPriorityQueue(sampleLimit, (x, y) -> (Double.compare(score[x], score[y])));
    this.indexList = new IntArrayList(sampleLimit);
    this.score = new double[sampleLimit];
    this.value = new long[sampleLimit];
    this.weight = new double[sampleLimit];
    this.minV = Long.MAX_VALUE;
    this.maxV = Long.MIN_VALUE;
  }

  private void putSample(int index, double score, long value, double weight) {
    this.score[index] = score;
    this.value[index] = value;
    this.weight[index] = weight;
  }

  private void add(long value, double weight) {
    double score =
        weight == 1 ? random.nextDoubleFast() : Math.pow(random.nextDoubleFast(), 1.0 / weight);
    if (indexHeap.size() < sampleLimit) {
      putSample(indexHeap.size(), score, value, weight);
      indexList.add(indexHeap.size());
      indexHeap.enqueue(indexHeap.size());
    } else if (this.score[indexHeap.firstInt()] < score) {
      putSample(indexHeap.firstInt(), score, value, weight);
      indexHeap.changed();
    }
  }

  public void update(long value) {
    totN++;
    //    minV = Math.min(minV,value);
    //    maxV = Math.max(maxV,value);
    minV = value < minV ? value : minV;
    maxV = value > maxV ? value : maxV;
    add(value, 1);
  }

  public void merge(SamplingHeapForStatMerge another) {
    totN += another.totN;
    minV = Math.min(minV, another.minV);
    maxV = Math.max(maxV, another.maxV);
    for (int index = 0; index < another.indexList.size(); index++)
      add(another.value[index], another.weight[index]);
  }

  public void merge(List<SamplingHeapForStatMerge> anotherList) {
    for (SamplingHeapForStatMerge another : anotherList) merge(another);
  }

  public void sortSample() {
    if (sorted) return;
    indexList.sortThis((x, y) -> (Long.compare(value[x], value[y])));
    sorted = true;
  }

  public long quantile(double q) {
    //    System.out.println("\t\t\t\t\tqueryQ:"+q);
    sortSample();
    if (q <= 0) return minV;
    if (q >= 1) return maxV;
    return value[indexList.get((int) (q * indexList.size()))];
  }

  public void reset() {
    totN = 0;
    sorted = false;
    indexHeap.clear();
    indexList.clear();
    this.minV = Long.MAX_VALUE;
    this.maxV = Long.MIN_VALUE;
  }

  public void compactBeforeSerialization() {
    // no-op
  }

  public void show() {
    sortSample();
    System.out.print("\t\t[" + indexList.size() + " samples for N=" + totN + "]");
    for (int i = 0; i < indexList.size(); i++)
      System.out.print("\t(" + value[indexList.get(i)] + "," + weight[indexList.get(i)] + ")");
    System.out.println();
  }

  public void showNum() {
    sortSample();
    System.out.println("\t\t[" + getSampleSize() + " samples for N=" + totN + "]");
  }

  public int getSampleSize() {
    return indexHeap != null ? indexHeap.size() : indexList.size();
  }

  public double getAvgErr() {
    return 1 / 3.0 * Math.pow(getSampleSize(), -0.5);
  }

  private int estimatePass(double bound) {
    return (int) Math.ceil(Math.log(1.0 * getSampleSize() / totN) / Math.log(bound));
  }

  private DoubleArrayList getBoundErr(double q) {
    DoubleArrayList boundErr = new DoubleArrayList();
    boundErr.add(6.0);
    //    int pass = estimatePass(2*getAvgErr()*6);
    //    while(pass>=2) {
    //      pass--;
    //      double nextK = 0.5*Math.exp(Math.log(1.0*getSampleSize()/totN)/pass)/getAvgErr()-0.5;
    //      if(nextK>=2.0)boundErr.add(nextK);
    //      else break;
    //    }
    boundErr.add(2.0);
    //
    // if(estimatePass(2*(getAvgErr()*boundErr.get(1)))==estimatePass(2*(getAvgErr()*boundErr.get(0))))
    //      boundErr.removeAtIndex(1);
    return boundErr;
  }

  public LongArrayList getLowerBound(double q) {
    LongArrayList bound = new LongArrayList();
    DoubleArrayList boundErr = getBoundErr(q);
    bound.add(quantile(0.0));
    for (MutableDoubleIterator it = boundErr.doubleIterator(); it.hasNext(); )
      bound.add(quantile(q - it.next() * getAvgErr()));
    bound.reverseThis();
    return bound;
    //    LongArrayList lowerBound = new LongArrayList();
    //    lowerBound.add(quantile(q-3*getAvgErr()));
    //    lowerBound.add(quantile(q-6*getAvgErr()));
    //    lowerBound.add(quantile(0));
    //    return lowerBound;
  }

  public LongArrayList getUpperBound(double q) {
    LongArrayList bound = new LongArrayList();
    DoubleArrayList boundErr = getBoundErr(q);
    bound.add(quantile(1.0));
    for (MutableDoubleIterator it = boundErr.doubleIterator(); it.hasNext(); )
      bound.add(quantile(q + it.next() * getAvgErr()));
    bound.reverseThis();
    return bound;

    //    upperBound.add(quantile(q+3*getAvgErr()));
    //    upperBound.add(quantile(q+6*getAvgErr()));
    //    upperBound.add(quantile(1.0));
    //    return upperBound;
  }

  public boolean exactResult() {
    return getSampleSize() == totN;
  }

  public long getSampleK(int k) {
    sortSample();
    return value[indexList.get(k)];
  }

  public int serialize(OutputStream outputStream) throws IOException { // 15+1*?+8*?
    compactBeforeSerialization(); // if N==maxN
    double true_weight = 1.0 * totN / sampleLimit;
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(totN, outputStream);
    byteLen += ReadWriteIOUtils.write(sampleLimit, outputStream);
    byteLen += ReadWriteIOUtils.write(minV, outputStream);
    byteLen += ReadWriteIOUtils.write(maxV, outputStream);
    for (int i = 0; i < sampleLimit; i++) {
      byteLen += ReadWriteIOUtils.write(value[i], outputStream);
    }
    return byteLen;
  }

  public SamplingHeapForStatMerge(InputStream inputStream, int maxMemoryByte, int maxSerializeByte)
      throws IOException {
    this(maxMemoryByte);
    this.totN = ReadWriteIOUtils.readLong(inputStream);
    int sampleNum = ReadWriteIOUtils.readInt(inputStream);
    this.minV = ReadWriteIOUtils.readLong(inputStream);
    this.maxV = ReadWriteIOUtils.readLong(inputStream);
    double w = 1.0 * this.totN / sampleNum;
    for (int i = 0; i < sampleNum; i++) {
      long v = ReadWriteIOUtils.readLong(inputStream);
      add(v, w);
    }
  }

  public SamplingHeapForStatMerge(ByteBuffer byteBuffer, int maxMemoryByte, int maxSerializeByte) {
    this(maxMemoryByte);
    this.totN = ReadWriteIOUtils.readLong(byteBuffer);
    int sampleNum = ReadWriteIOUtils.readInt(byteBuffer);
    this.minV = ReadWriteIOUtils.readLong(byteBuffer);
    this.maxV = ReadWriteIOUtils.readLong(byteBuffer);
    double w = 1.0 * this.totN / sampleNum;
    for (int i = 0; i < sampleNum; i++) {
      long v = ReadWriteIOUtils.readLong(byteBuffer);
      add(v, w);
    }
  }

  public SamplingHeapForStatMerge(InputStream inputStream) throws IOException {
    this.totN = ReadWriteIOUtils.readLong(inputStream);
    this.sampleLimit = ReadWriteIOUtils.readInt(inputStream);
    this.maxMemByte = sampleLimit * (8 * 3 + 4 + 4);
    this.indexHeap =
        new IntHeapPriorityQueue(sampleLimit, (x, y) -> (Double.compare(score[x], score[y])));
    this.indexList = new IntArrayList(sampleLimit);
    this.score = new double[sampleLimit];
    this.value = new long[sampleLimit];
    this.weight = new double[sampleLimit];
    this.minV = ReadWriteIOUtils.readLong(inputStream);
    this.maxV = ReadWriteIOUtils.readLong(inputStream);

    double w = 1.0 * this.totN / sampleLimit;
    for (int i = 0; i < sampleLimit; i++) {
      long v = ReadWriteIOUtils.readLong(inputStream);
      add(v, w);
    }
  }

  public SamplingHeapForStatMerge(ByteBuffer byteBuffer) {

    this.totN = ReadWriteIOUtils.readLong(byteBuffer);
    this.sampleLimit = ReadWriteIOUtils.readInt(byteBuffer);
    this.maxMemByte = sampleLimit * (8 * 3 + 4 + 4);
    this.indexHeap =
        new IntHeapPriorityQueue(sampleLimit, (x, y) -> (Double.compare(score[x], score[y])));
    this.indexList = new IntArrayList(sampleLimit);
    this.score = new double[sampleLimit];
    this.value = new long[sampleLimit];
    this.weight = new double[sampleLimit];
    this.minV = ReadWriteIOUtils.readLong(byteBuffer);
    this.maxV = ReadWriteIOUtils.readLong(byteBuffer);

    double w = 1.0 * this.totN / sampleLimit;
    for (int i = 0; i < sampleLimit; i++) {
      long v = ReadWriteIOUtils.readLong(byteBuffer);
      add(v, w);
    }
  }
}
