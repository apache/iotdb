package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import static org.apache.commons.math3.util.Precision.EPSILON;
import static org.apache.iotdb.tsfile.utils.Params.*;

public class DDSketch implements Serializable {
  private double alpha;
  private double gamma;
  private double multiplier;
  private int bucket_num_limit;
  private int threshold_for_compression;

  private Int2LongOpenHashMap positive_buckets;
  private Int2LongOpenHashMap negative_buckets;
  private double collapse_bound;
  private long zero_count;

  private transient double beta;
  private final transient double[] valid_range;

  private static double MIN_POSITIVE_VALUE = FS_MIN_POSITIVE_VALUE;
  private static double COEFFICIENT = FS_COMPRESSION_COEFFICIENT;

  public DDSketch() {
    this(EPSILON, BUCKET_NUM_LIMIT);
  }

  public DDSketch(double alpha, int bucket_num_limit) {
    //        System.out.println(alpha);
    this.alpha = alpha;
    this.bucket_num_limit = Math.max(bucket_num_limit, 2);
    this.threshold_for_compression = (int) (bucket_num_limit * COEFFICIENT);

    this.gamma = 2 * alpha / (1 - alpha) + 1;
    this.multiplier = Math.log(Math.E) / (Math.log1p(gamma - 1));
    this.positive_buckets = new Int2LongOpenHashMap((int) (bucket_num_limit * 0.75));
    this.negative_buckets = new Int2LongOpenHashMap((int) (bucket_num_limit * 0.25));
    this.zero_count = 0;
    this.collapse_bound = -Double.MAX_VALUE;
    this.valid_range = new double[6];
  }

  public void insert(double v) {
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
      int[] indices = negative_buckets.keySet().toIntArray();
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
        indices = positive_buckets.keySet().toIntArray();
        Arrays.sort(indices);
        for (int i = exceed - 1; i >= 0; --i) {
          count += positive_buckets.remove(indices[i]);
        }
        positive_buckets.put(indices[exceed], positive_buckets.get(indices[exceed]) + count);
        collapse_bound = Math.pow(gamma, indices[exceed] - 1);
      }
    }
  }

  private Bucket[] union_buckets() {
    Bucket[] buckets = new Bucket[sketch_size()];
    int i = 0;
    for (Map.Entry<Integer, Long> e : positive_buckets.entrySet()) {
      buckets[i++] =
          new Bucket(
              e.getKey(),
              Math.pow(gamma, e.getKey() - 1),
              Math.pow(gamma, e.getKey()),
              e.getValue());
    }
    for (Map.Entry<Integer, Long> e : negative_buckets.entrySet()) {
      buckets[i++] =
          new Bucket(
              e.getKey(),
              -Math.pow(gamma, e.getKey()),
              -Math.pow(gamma, e.getKey() - 1),
              e.getValue());
    }
    if (zero_count > 0) {
      buckets[i] = new Bucket(0, 0, 0, zero_count);
    }
    Arrays.sort(buckets, Comparator.comparingDouble(o -> o.lower_bound));
    return buckets;
  }

  private long total_count() {
    return positive_buckets.values().stream().mapToLong(l -> l).sum()
        + negative_buckets.values().stream().mapToLong(l -> l).sum()
        + zero_count;
  }

  public int find_p_index(Bucket[] buckets, long total_count, double q) {
    long count = 0;
    double rank = q * (total_count - 1);
    for (int i = 0; i < buckets.length; ++i) {
      count += buckets[i].count;
      if (count > rank) {
        return i;
      }
    }
    return -1;
  }

  public double getQuantile(double q) {
    Bucket[] buckets = union_buckets();
    long total_count = total_count();
    Bucket p = buckets[find_p_index(buckets, total_count, q)];
    if (p.lower_bound < 0) {
      return 2 * p.lower_bound / (1 + gamma);
    } else {
      return 2 * p.upper_bound / (1 + gamma);
    }
  }

  public int sketch_size() {
    return positive_buckets.size() + negative_buckets.size() + (zero_count == 0 ? 0 : 1);
  }

  private static class Bucket {
    int index;
    double lower_bound;
    double upper_bound;
    long count;

    Bucket(int index, double lower_bound, double upper_bound, long count) {
      this.index = index;
      this.lower_bound = lower_bound;
      this.upper_bound = upper_bound;
      this.count = count;
    }
  }
}
