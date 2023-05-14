package org.apache.iotdb.tsfile.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class DDSketchForQuantile implements Serializable {
  private double alpha;
  private double gamma;
  private double multiplier;
  private int bucket_num_limit;
  private int threshold_for_compression;

  private Map<Integer, Long> positive_buckets;
  private Map<Integer, Long> negative_buckets;
  private double collapse_bound;
  private long zero_count;

  private transient double beta;
  private final transient double[] valid_range;

  private static double MIN_POSITIVE_VALUE = 1e-6;
  private static double COEFFICIENT = 1.5;
  boolean valid_buckets = false;
  Bucket[] buckets;

  public DDSketchForQuantile(double alpha, int bucket_num_limit) {
    //        System.out.println(alpha);
    this.alpha = alpha;
    this.bucket_num_limit = Math.max(bucket_num_limit, 2);
    this.threshold_for_compression = (int) (bucket_num_limit * COEFFICIENT);
    //
    // System.out.println("\t\t\t\tcompression:"+threshold_for_compression+"\t\tlimit="+bucket_num_limit);

    this.gamma = 2 * alpha / (1 - alpha) + 1;
    this.multiplier = Math.log(Math.E) / (Math.log1p(gamma - 1));
    this.positive_buckets = new HashMap<>((int) (bucket_num_limit * 0.75));
    this.negative_buckets = new HashMap<>((int) (bucket_num_limit * 0.25));
    this.zero_count = 0;
    this.collapse_bound = -Double.MAX_VALUE;
    this.valid_range = new double[6];
  }

  public void insert(double v) {
    valid_buckets = false;
    if (v < collapse_bound) {
      v = collapse_bound;
    }
    if (v > MIN_POSITIVE_VALUE) {
      int i = (int) Math.ceil(Math.log(v) * multiplier);
      positive_buckets.put(i, positive_buckets.getOrDefault(i, 0L) + 1);
    } else if (v < -MIN_POSITIVE_VALUE) {
      int i = (int) Math.ceil(Math.log(-v) * multiplier);
      negative_buckets.put(i, negative_buckets.getOrDefault(i, 0L) + 1);
    } else {
      zero_count++;
    }
    collapse(threshold_for_compression);
  }

  private void collapse(int limit) {
    if (sketch_size() > limit) {
      int exceed = sketch_size() - bucket_num_limit;
      Integer[] indices = negative_buckets.keySet().toArray(new Integer[0]);
      Arrays.sort(indices);
      long count = 0;
      for (int i = Math.max(0, indices.length - exceed); i < indices.length; ++i) {
        count += negative_buckets.remove(indices[i]);
      }
      if (count > 0) {
        int i = indices.length - exceed - 1;
        if (i >= 0) {
          negative_buckets.put(indices[i], negative_buckets.get(indices[i]) + count);
          collapse_bound = -Math.pow(gamma, indices[i]);
        } else {
          zero_count += count;
          collapse_bound = 0;
        }
      }
      exceed -= (indices.length - Math.max(0, indices.length - exceed));
      if (exceed > 0) {
        count = zero_count;
        if (zero_count > 0) {
          exceed--;
        }
        indices = positive_buckets.keySet().toArray(new Integer[0]);
        Arrays.sort(indices);
        for (int i = exceed - 1; i >= 0; --i) {
          count += positive_buckets.remove(indices[i]);
        }
        positive_buckets.put(indices[exceed], positive_buckets.get(indices[exceed]) + count);
        collapse_bound = Math.pow(gamma, indices[exceed] - 1);
      }
    }
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
        : (index == DIVIDE_HALF ? zero_count : negative_buckets.get(index));
  }

  private void union_buckets() {
    buckets = new Bucket[sketch_size()];
    int i = 0;
    for (Map.Entry<Integer, Long> e : positive_buckets.entrySet()) {
      buckets[i++] = new Bucket(e.getKey() + DIVIDE_DELTA);
    }
    for (Map.Entry<Integer, Long> e : negative_buckets.entrySet()) {
      buckets[i++] = new Bucket(e.getKey());
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

  private long total_count() {
    return positive_buckets.values().stream().mapToLong(l -> l).sum()
        + negative_buckets.values().stream().mapToLong(l -> l).sum()
        + zero_count;
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

  public int sketch_size() {
    return positive_buckets.size() + negative_buckets.size() + (zero_count == 0 ? 0 : 1);
  }

  private static class Bucket {
    public int bucketIndex;
    public long prefixSum;

    Bucket(int bucketIndex) {
      this.bucketIndex = bucketIndex;
      this.prefixSum = 0;
    }
  }
}
