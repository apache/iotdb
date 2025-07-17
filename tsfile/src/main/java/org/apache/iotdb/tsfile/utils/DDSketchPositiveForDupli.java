package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

public class DDSketchPositiveForDupli implements Serializable {
  public double alpha;
  private double gamma;
  private double multiplier;
  private int bucket_num_limit;
  private int threshold_for_compression;

  private Int2LongOpenHashMap positive_buckets;
  private Int2DoubleOpenHashMap positive_buckets_content;
  private double positive_collapse_bound = -Double.MAX_VALUE;
  private long zero_count;

  private static final double MIN_POSITIVE_VALUE = 1e-30;
  private static final double COEFFICIENT = 1.5;
  boolean valid_buckets = false;
  Bucket[] buckets;
  public static final int bucketNumPerByteInMemoryForExact = 48;
  public static final int bucketNumPerByteInMemoryForApprox = 24;

  public static int getBucketNumLimit(int seriByte) {
    return (int) (seriByte / bucketNumPerByteInMemoryForApprox / COEFFICIENT);
  }
  // B bucket:  at most threshold_for_compression=1.5*B buckets,    OpenHashMap loadFactor0.75
  // a bucket:  key2count&key2lastV(Int2Long,Int2Double)  2*(4+8)=24Byte;
  public DDSketchPositiveForDupli(double alpha, int bucket_num_limit) {
    //        System.out.println("alpha:\t"+alpha+"\t\tbuckets:"+bucket_num_limit);
    this.alpha = alpha;
    this.bucket_num_limit = Math.max(bucket_num_limit, 2);
    this.threshold_for_compression = (int) (bucket_num_limit * COEFFICIENT);
    //
    // System.out.println("\t\t\t\tcompression:"+threshold_for_compression+"\t\tlimit="+bucket_num_limit);

    this.gamma = 2 * alpha / (1 - alpha) + 1;
    this.multiplier = Math.log(Math.E) / (Math.log1p(gamma - 1));
    this.positive_buckets = new Int2LongOpenHashMap((int) (bucket_num_limit));
    this.positive_buckets_content = new Int2DoubleOpenHashMap((int) (bucket_num_limit));
    this.zero_count = 0;
    this.positive_collapse_bound = -Double.MAX_VALUE;
  }

  long maxN;
  int maxSeriByte;

  public DDSketchPositiveForDupli(double alpha, int maxN, int maxSeriByte) {
    this.maxN = maxN;
    this.maxSeriByte = maxSeriByte;
    this.alpha = alpha;
    this.bucket_num_limit = maxSeriByte / (4 + 8 + 8);
    this.threshold_for_compression = maxN;
    this.gamma = 2 * alpha / (1 - alpha) + 1;
    this.multiplier = Math.log(Math.E) / (Math.log1p(gamma - 1));
    this.positive_buckets = new Int2LongOpenHashMap(maxN);
    this.positive_buckets_content = new Int2DoubleOpenHashMap(maxN);
    this.zero_count = 0;
    this.positive_collapse_bound = -Double.MAX_VALUE;
  }

  public void update(double v) {
    valid_buckets = false;
    if (v > MIN_POSITIVE_VALUE) {
      if (v < positive_collapse_bound) {
        v = positive_collapse_bound;
      }
      int i = (int) Math.ceil(Math.log(v) * multiplier);
      positive_buckets.put(i, positive_buckets.getOrDefault(i, 0L) + 1);
      double lastV = positive_buckets_content.getOrDefault(i, v);
      positive_buckets_content.put(i, lastV == v ? v : 0);
    } else {
      zero_count++;
    }
    if (positive_buckets.size() > threshold_for_compression) collapse(bucket_num_limit);
  }

  public void mergeWithDeserialized(DDSketchPositiveForDupli another) {
    if (another.alpha != this.alpha) {
      System.out.println("\t\t[ERROR DDSketchPositive] Merge With Different α.");
      return;
    }
    for (Int2LongMap.Entry b : another.positive_buckets.int2LongEntrySet()) {
      int key = b.getIntKey();
      long anoValue = b.getLongValue();
      double anoContent = another.positive_buckets_content.get(key);
      if (!positive_buckets.containsKey(key)) {
        positive_buckets.put(key, anoValue);
        positive_buckets_content.put(key, anoContent);
      } else {
        positive_buckets.addTo(key, anoValue);
        double lastV = positive_buckets_content.get(key);
        positive_buckets_content.put(key, lastV == anoContent ? anoContent : 0);
        //        if(lastV!=0||anoContent!=0)
        //
        // System.out.println("\t\tindex:\t"+key+"\t\tanoContent:\t"+anoContent+"\t\tthisContent:"+lastV+"\t\tsameOneV:\t"+(lastV==anoContent));
      }
      if (positive_buckets.size() > threshold_for_compression) collapse(bucket_num_limit);
    }
  }

  private void collapse(int limit) {
    //    System.out.println("\t\tcollapse. limit:"+limit);
    int posi_exceed = positive_buckets.size() - limit;

    int[] indices;
    if (posi_exceed > 0) {
      indices = positive_buckets.keySet().toIntArray();
      Arrays.sort(indices);
      long count = 0;
      for (int i = posi_exceed - 1; i >= 0; --i) {
        count += positive_buckets.remove(indices[i]);
        positive_buckets_content.remove(indices[i]);
      }
      positive_buckets.put(
          indices[posi_exceed], positive_buckets.get(indices[posi_exceed]) + count);
      //      if(indices[posi_exceed]>=467)System.out.println("\t[collapse
      // clear].\t"+indices[posi_exceed]+"\t\tR:"+);
      positive_buckets_content.put(indices[posi_exceed], 0);
      positive_collapse_bound = Math.pow(gamma, indices[posi_exceed]);
    }
    //    System.out.println("\t\tDD
    // collapse.\t"+positive_buckets.size()+"\t"+1.0*positive_buckets.size()/threshold_for_compression);
  }

  static final int DIVIDE_DELTA = 1000000000, DIVIDE_HALF = DIVIDE_DELTA / 2;

  private double getL(int index) {
    return index > DIVIDE_HALF
        ? Math.pow(gamma, index - DIVIDE_DELTA - 1)
        : (index == DIVIDE_HALF ? 0 : -Math.pow(gamma, index));
  }

  private double getR(int index) {
    return index > DIVIDE_HALF
        ? Math.pow(gamma, index - DIVIDE_DELTA)
        : (index == DIVIDE_HALF ? 0 : -Math.pow(gamma, index - 1));
  }

  private long getCount(int index) {
    //    System.out.println("\t\t\t\t\t-index="+(-index));
    //    System.out.println("\t\t\t\t\t\t\texist"+negative_buckets.containsKey(-index));
    return index > DIVIDE_HALF
        ? positive_buckets.get(index - DIVIDE_DELTA)
        : (zero_count /*index==DIVIDE_HALF?zero_count:negative_buckets.get(index)*/);
  }

  private double getLastV(int index) {
    //    System.out.println("\t\t\t\t\t-index="+(-index));
    return index > DIVIDE_HALF
        ? positive_buckets_content.get(index - DIVIDE_DELTA)
        : (zero_count /*index==DIVIDE_HALF?0:negative_buckets_content.get(index)*/);
  }

  private void union_buckets() {
    buckets = new Bucket[sketch_size()];
    int i = 0;
    for (Map.Entry<Integer, Long> e : positive_buckets.entrySet()) {
      buckets[i++] = new Bucket(e.getKey() + DIVIDE_DELTA);
    }
    if (zero_count > 0) {
      buckets[i] = new Bucket(DIVIDE_HALF);
    }
    Arrays.sort(buckets, Comparator.comparingDouble(o -> (getL(o.bucketIndex))));
    long sum = 0;
    for (i = 0; i < sketch_size(); i++) {
      sum += getCount(buckets[i].bucketIndex);
      buckets[i].prefixSum = sum;
    }
    valid_buckets = true;
  }

  public long total_count() {
    return positive_buckets.values().longStream().sum() + zero_count;
  }

  private int find_p_index(Bucket[] buckets, long total_count, double q) {
    double rank = q * (total_count - 1);
    int tmp1 = Integer.highestOneBit(buckets.length);
    int p = -1;
    while (tmp1 > 0) {
      if (p + tmp1 < buckets.length && buckets[p + tmp1].prefixSum <= rank) p += tmp1;
      tmp1 /= 2;
    }
    return p + 1;
  }

  private int find_p_index(Bucket[] buckets, long query_rank) {
    int tmp1 = Integer.highestOneBit(buckets.length);
    int p = -1;
    while (tmp1 > 0) {
      if (p + tmp1 < buckets.length && buckets[p + tmp1].prefixSum < query_rank) p += tmp1;
      tmp1 /= 2;
    }
    return p + 1;
  }

  private int find_p_index_LEQ(Bucket[] buckets, long query_rank) {
    int tmp1 = Integer.highestOneBit(buckets.length);
    int p = -1;
    while (tmp1 > 0) {
      if (p + tmp1 < buckets.length && buckets[p + tmp1].prefixSum <= query_rank) p += tmp1;
      tmp1 /= 2;
    }
    return p == -1 ? 0 : p;
  }

  private int find_p_index_GEQ(Bucket[] buckets, long query_rank) {
    int tmp1 = Integer.highestOneBit(buckets.length);
    int p = -1;
    while (tmp1 > 0) {
      if (p + tmp1 < buckets.length && buckets[p + tmp1].prefixSum < query_rank) p += tmp1;
      tmp1 /= 2;
    }
    return p + 1;
  }

  public double getQuantile(double q) {
    if (!valid_buckets) union_buckets();
    long total_count = total_count();
    Bucket p = buckets[find_p_index(buckets, total_count, q)];
    if (getL(p.bucketIndex) < 0) {
      return 2 * getL(p.bucketIndex) / (1 + gamma);
    } else {
      return 2 * getR(p.bucketIndex) / (1 + gamma);
    }
  }

  public DoubleArrayList getQuantiles(DoubleArrayList qs) {
    if (!valid_buckets) union_buckets();
    long total_count = total_count();
    int bucketPos = find_p_index(buckets, total_count, qs.getDouble(0));
    Bucket p;
    DoubleArrayList ans = new DoubleArrayList();
    for (double q : qs) {
      while (bucketPos + 1 < total_count && buckets[bucketPos].prefixSum < q * (total_count - 1))
        bucketPos++;
      p = buckets[bucketPos];
      if (getL(p.bucketIndex) < 0) {
        ans.add(2 * getL(p.bucketIndex) / (1 + gamma));
      } else {
        ans.add(2 * getR(p.bucketIndex) / (1 + gamma));
      }
    }
    return ans;
  }

  public double[] findResultRange(long K1, long K2) {
    if (!valid_buckets) union_buckets();
    DoubleArrayList result = new DoubleArrayList(2);
    int p1 = find_p_index /*_LEQ*/(buckets, K1), p2 = find_p_index /*_GEQ*/(buckets, K2);
    double valL = getL(buckets[p1].bucketIndex), valR = getR(buckets[p2].bucketIndex);
    if (valL > 0 && valL <= positive_collapse_bound) valL = MIN_POSITIVE_VALUE;
    //
    // if(valL==0)valL=p1==0?-Double.MAX_VALUE:(getR(buckets[p1-1].bucketIndex)+Double.MIN_NORMAL);
    //
    // if(valR==0)valL=(p2==buckets.length-1)?Double.MAX_VALUE:(getL(buckets[p2+1].bucketIndex)-Double.MIN_NORMAL);
    if (valL == 0 && valR == 0) {
      result.add(0);
      result.add(0);
      result.add(-233);
    } else {
      if (valL == 0) valL = -MIN_POSITIVE_VALUE;
      if (valR == 0) valR = MIN_POSITIVE_VALUE;
      if (getLastV(buckets[p1].bucketIndex) != 0 && getLastV(buckets[p2].bucketIndex) != 0) {
        //        System.out.println("\t\tWIN!!!");
        result.add(getLastV(buckets[p1].bucketIndex));
        result.add(getLastV(buckets[p2].bucketIndex));
        result.add(-233);
      } else {
        result.add(valL);
        result.add(valR);
      }
    }
    return result.toDoubleArray();
  }

  public double[] getFilterL(long CountOfValL, long CountOfValR, double valL, double valR, long K) {
    if (K <= CountOfValL) return new double[] {valL, CountOfValL, -233.0};
    if (K > CountOfValL + total_count()) return new double[] {valR, CountOfValR, -233.0};
    K -= CountOfValL;
    int p1 = find_p_index(buckets, K);
    valL = getL(buckets[p1].bucketIndex);
    if (valL > 0 && valL <= positive_collapse_bound) valL = MIN_POSITIVE_VALUE;
    if (valL == 0) return new double[] {0, zero_count, -233};
    if (getLastV(buckets[p1].bucketIndex) != 0)
      return new double[] {
        getLastV(buckets[p1].bucketIndex), getCount(buckets[p1].bucketIndex), -233
      };
    return new double[] {valL, p1};
  }

  public double[] getFilterR(long CountOfValL, long CountOfValR, double valL, double valR, long K) {
    if (K <= CountOfValL) return new double[] {valL, CountOfValL, -233.0};
    if (K > CountOfValL + total_count()) return new double[] {valR, CountOfValR, -233.0};
    K -= CountOfValL;
    int p2 = find_p_index(buckets, K);
    valR = getR(buckets[p2].bucketIndex);
    if (valR == 0) return new double[] {0, zero_count, -233};
    if (getLastV(buckets[p2].bucketIndex) != 0)
      return new double[] {
        getLastV(buckets[p2].bucketIndex), getCount(buckets[p2].bucketIndex), -233
      };
    return new double[] {valR, p2};
  }

  public double[] getFilter(
      long CountOfValL, long CountOfValR, double valL, double valR, long K1, long K2) {
    if (!valid_buckets) union_buckets();
    double[] filterL = getFilterL(CountOfValL, CountOfValR, valL, valR, K1);
    double[] filterR = getFilterR(CountOfValL, CountOfValR, valL, valR, K2);
    //    System.out.println("\t\t\t\tvalL,R:\t"+filterL[0]+"..."+filterR[0]);
    long tot_count = 0;
    tot_count += filterL.length == 2 ? getCount(buckets[(int) filterL[1]].bucketIndex) : filterL[1];
    if (!(filterL[0] == filterR[0]
        || (filterL.length == 2 && filterR.length == 2 && filterL[1] == filterR[1]))) {
      tot_count +=
          filterR.length == 2 ? getCount(buckets[(int) filterR[1]].bucketIndex) : filterR[1];
    }
    if (filterL.length + filterR.length == 6) return new double[] {filterL[0], filterR[0], 0, -233};
    else return new double[] {filterL[0], filterR[0], tot_count};
  }

  public int sketch_size() {
    return positive_buckets.size() + +(zero_count == 0 ? 0 : 1);
  }

  private static class Bucket {
    public int bucketIndex;
    public long prefixSum;

    Bucket(int bucketIndex) {
      this.bucketIndex = bucketIndex;
      this.prefixSum = 0;
    }
  }

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(alpha, outputStream);
    byteLen += ReadWriteIOUtils.write(gamma, outputStream);
    byteLen += ReadWriteIOUtils.write(multiplier, outputStream);
    byteLen += ReadWriteIOUtils.write(zero_count, outputStream);
    byteLen += ReadWriteIOUtils.write(positive_buckets.size(), outputStream);
    for (Int2LongMap.Entry b : positive_buckets.int2LongEntrySet()) {
      int key = b.getIntKey();
      long value = b.getLongValue();
      byteLen += ReadWriteIOUtils.write((short) key, outputStream);
      byteLen += ReadWriteIOUtils.write((short) value, outputStream);
      //      byteLen+=ReadWriteIOUtils.write(positive_buckets_content.get(key),outputStream);
    }
    return byteLen;
  }

  public DDSketchPositiveForDupli(InputStream inputStream) throws IOException {
    this.alpha = ReadWriteIOUtils.readDouble(inputStream);
    this.gamma = ReadWriteIOUtils.readDouble(inputStream);
    this.multiplier = ReadWriteIOUtils.readDouble(inputStream);
    this.zero_count = ReadWriteIOUtils.readLong(inputStream);
    int bucketNum = ReadWriteIOUtils.readInt(inputStream);
    this.positive_buckets = new Int2LongOpenHashMap(bucketNum);
    this.positive_buckets_content = new Int2DoubleOpenHashMap(bucketNum);
    for (int i = 0; i < bucketNum; i++) {
      int key = ReadWriteIOUtils.readShort(inputStream);
      long value = ReadWriteIOUtils.readShort(inputStream);
      this.positive_buckets.put(key, value);
      //      double content=ReadWriteIOUtils.readDouble(inputStream);
      this.positive_buckets_content.put(key, 0);
    }
  }

  public DDSketchPositiveForDupli(ByteBuffer byteBuffer) {
    this.alpha = ReadWriteIOUtils.readDouble(byteBuffer);
    this.gamma = ReadWriteIOUtils.readDouble(byteBuffer);
    this.multiplier = ReadWriteIOUtils.readDouble(byteBuffer);
    this.zero_count = ReadWriteIOUtils.readLong(byteBuffer);
    int bucketNum = ReadWriteIOUtils.readInt(byteBuffer);
    this.positive_buckets = new Int2LongOpenHashMap(bucketNum);
    this.positive_buckets_content = new Int2DoubleOpenHashMap(bucketNum);
    for (int i = 0; i < bucketNum; i++) {
      int key = ReadWriteIOUtils.readShort(byteBuffer);
      long value = ReadWriteIOUtils.readShort(byteBuffer);
      this.positive_buckets.put(key, value);
      //      double content=ReadWriteIOUtils.readDouble(byteBuffer);
      this.positive_buckets_content.put(key, 0);
    }
  }
}
