/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.library.dprofile.util;

import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;

public class MADSketch {

  private long zero_count;
  private double beta;
  private final double[] valid_range;

  private final double alpha;
  private final double gamma;
  private final double multiplier;

  private final UnifiedMap<Integer, Long> positive_buckets;
  private final UnifiedMap<Integer, Long> negative_buckets;

  private static final double MIN_POSITIVE_VALUE = 1e-6;

  private static final Logger logger = LoggerFactory.getLogger(MADSketch.class);

  public MADSketch(double alpha) {
    this.alpha = alpha;

    this.gamma = 2 * alpha / (1 - alpha) + 1;
    this.multiplier = Math.log(Math.E) / (Math.log1p(gamma - 1));
    this.beta = 1;
    this.positive_buckets = new UnifiedMap<>();
    this.negative_buckets = new UnifiedMap<>();
    this.zero_count = 0;
    this.valid_range = new double[6];
  }

  public void insert(double v) {
    if (Double.isFinite(v)) {
      if (v > MIN_POSITIVE_VALUE) {
        int i = (int) Math.ceil(Math.log(v) * multiplier);
        positive_buckets.put(i, positive_buckets.getOrDefault(i, 0L) + 1);
      } else if (v < -MIN_POSITIVE_VALUE) {
        int i = (int) Math.ceil(Math.log(-v) * multiplier);
        negative_buckets.put(i, negative_buckets.getOrDefault(i, 0L) + 1);
      } else {
        zero_count++;
      }
    }
  }

  public void insert(double v, double[] bounds) {
    if (v < bounds[0]) {
      v = bounds[0];
    } else if (v > bounds[1] && v < bounds[2]) {
      v = bounds[2];
    } else if (v > bounds[3] && v < bounds[4]) {
      v = bounds[4];
    } else if (v > bounds[5]) {
      v = bounds[5];
    }
    insert(v);
  }

  private Bucket[] union_buckets() {
    Bucket[] buckets = new Bucket[sketch_size()];
    int i = 0;
    for (Map.Entry<Integer, Long> e : positive_buckets.entrySet()) {
      buckets[i++] =
          new Bucket(
              e.getKey(),
              Math.pow(gamma, (double) e.getKey() - 1.0),
              Math.pow(gamma, e.getKey()),
              e.getValue());
    }
    for (Map.Entry<Integer, Long> e : negative_buckets.entrySet()) {
      buckets[i++] =
          new Bucket(
              e.getKey(),
              -Math.pow(gamma, e.getKey()),
              -Math.pow(gamma, (double) e.getKey() - 1.0),
              e.getValue());
    }
    if (zero_count > 0) {
      buckets[i] = new Bucket(0, 0, 0, zero_count);
    }
    Arrays.sort(buckets, Comparator.comparingDouble(o -> o.lower_bound));
    return buckets;
  }

  public long total_count() {
    return positive_buckets.values().stream().mapToLong(l -> l).sum()
        + negative_buckets.values().stream().mapToLong(l -> l).sum()
        + zero_count;
  }

  private int find_p_index(Bucket[] buckets, long total_count) {
    long count = 0;
    double rank = 0.5 * (total_count - 1);
    for (int i = 0; i < buckets.length; ++i) {
      count += buckets[i].count;
      if (count > rank) {
        return i;
      }
    }
    return -1;
  }

  private int find_q_index(int p, Bucket[] buckets, long total_count) {
    int q = p;
    long count = buckets[p].count;
    double rank = 0.5 * (total_count - 1);
    int l = p - 1;
    int r = p + 1;
    while (count <= rank && l >= 0 && r < buckets.length) {
      if (buckets[p].lower_bound - buckets[l].upper_bound
          < buckets[r].lower_bound - buckets[p].upper_bound) {
        q = l--;
      } else {
        q = r++;
      }
      count += buckets[q].count;
    }

    while (count <= rank && l >= 0) {
      q = l--;
      count += buckets[q].count;
    }

    while (count <= rank && r < buckets.length) {
      q = r++;
      count += buckets[q].count;
    }

    double m_lower_bound = buckets[p].lower_bound + buckets[p].upper_bound - buckets[q].upper_bound;
    double m_upper_bound = buckets[p].lower_bound + buckets[p].upper_bound - buckets[q].lower_bound;
    if (p > q) {
      r--;
      if (buckets[r].lower_bound <= m_lower_bound && buckets[r].upper_bound >= m_upper_bound) {
        q = r;
      }
    } else if (p < q) {
      l++;
      if (buckets[l].lower_bound <= m_lower_bound && buckets[l].upper_bound >= m_upper_bound) {
        q = l;
      }
    }
    return q;
  }

  private void setValid_range(Bucket p, Bucket q) {
    valid_range[0] = p.lower_bound;
    valid_range[1] = p.upper_bound;
    valid_range[2] = p.lower_bound + q.lower_bound - p.upper_bound;
    valid_range[3] = p.upper_bound + q.upper_bound - p.lower_bound;
    valid_range[4] = 2 * p.lower_bound - q.upper_bound;
    valid_range[5] = 2 * p.upper_bound - q.lower_bound;
    Arrays.sort(valid_range);
  }

  private double minDelta(double delta1, double delta2) {
    double delta;
    if (delta1 < 0 && delta2 < 0) {
      delta = 0;
    } else if (delta1 < 0) {
      delta = delta2;
    } else if (delta2 < 0) {
      delta = delta1;
    } else {
      delta = Math.min(delta1, delta2);
    }
    return delta;
  }

  public Mad getMad() {
    beta = 1;
    Bucket[] buckets = union_buckets();
    long total_count = total_count();
    int p_index = find_p_index(buckets, total_count);

    if (p_index == -1) {
      throw new NoSuchElementException("No values in the time series");
    }

    int q_index = find_q_index(p_index, buckets, total_count);
    Bucket p = buckets[p_index];
    Bucket q = buckets[q_index];

    if (p.lower_bound * q.lower_bound > 0) {
      if (p.lower_bound == q.lower_bound) {
        return new Mad(0, Double.MAX_VALUE);
      } else {
        double mad =
            2
                * (p.upper_bound - q.lower_bound)
                * (p.lower_bound - q.upper_bound)
                / ((gamma + 1) * Math.abs(p.lower_bound - q.lower_bound));
        double gamma_p_q = Math.max(p.upper_bound / q.upper_bound, q.upper_bound / p.upper_bound);
        double delta;
        if (Math.abs(p.lower_bound) < Math.abs(q.lower_bound)) {
          delta =
              minDelta(
                  gamma_p_q / Math.pow(gamma, 2) - 1 / gamma + Math.pow(gamma, -3),
                  1 / (Math.pow(gamma, 3) - gamma_p_q * gamma + Math.pow(gamma, 2)));
        } else {
          delta =
              minDelta(
                  Math.pow(gamma, -2) + Math.pow(gamma, -3) - 1 / (gamma_p_q * gamma),
                  1 / (Math.pow(gamma, 2) / gamma_p_q + Math.pow(gamma, 3) - gamma));
        }
        beta = 1 - 2 / (1 + delta);
        if (need_two_pass()) {
          setValid_range(p, q);
          return new Mad(mad, (1 + 2 / (gamma_p_q - 1)) * alpha);
        } else {
          return new Mad(0, Double.MAX_VALUE);
        }
      }
    } else {
      double mad =
          2
              * Math.max(
                  Math.abs(p.upper_bound - q.lower_bound), Math.abs(q.upper_bound - p.lower_bound))
              / (gamma + 1);
      return new Mad(mad, alpha);
    }
  }

  public int sketch_size() {
    return positive_buckets.size() + negative_buckets.size() + (zero_count == 0 ? 0 : 1);
  }

  public boolean need_two_pass() {
    return beta > 0 && beta < 1;
  }

  public double getBeta() {
    return beta;
  }

  public double[] getValid_range() {
    return valid_range;
  }

  public void show(Bucket[] buckets) {
    for (Bucket bucket : buckets) {
      if (logger.isDebugEnabled()) {
        logger.debug(bucket.index + ": " + bucket.count);
      }
    }
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

  public double getAlpha() {
    return alpha;
  }
}
