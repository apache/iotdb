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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;

public class MADSketch {

  private long zeroCount;
  private double beta;
  private final double[] validRange;

  private final double alpha;
  private final double gamma;
  private final double multiplier;

  private final UnifiedMap<Integer, Long> positiveBuckets;
  private final UnifiedMap<Integer, Long> negativeBuckets;

  private static final double MIN_POSITIVE_VALUE = 1e-6;

  public MADSketch(double alpha) {
    this.alpha = alpha;

    this.gamma = 2 * alpha / (1 - alpha) + 1;
    this.multiplier = Math.log(Math.E) / (Math.log1p(gamma - 1));
    this.beta = 1;
    this.positiveBuckets = new UnifiedMap<>();
    this.negativeBuckets = new UnifiedMap<>();
    this.zeroCount = 0;
    this.validRange = new double[6];
  }

  public void insert(double v) {
    if (Double.isFinite(v)) {
      if (v > MIN_POSITIVE_VALUE) {
        int i = (int) Math.ceil(Math.log(v) * multiplier);
        positiveBuckets.put(i, positiveBuckets.getOrDefault(i, 0L) + 1);
      } else if (v < -MIN_POSITIVE_VALUE) {
        int i = (int) Math.ceil(Math.log(-v) * multiplier);
        negativeBuckets.put(i, negativeBuckets.getOrDefault(i, 0L) + 1);
      } else {
        zeroCount++;
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

  private Bucket[] unionBuckets() {
    Bucket[] buckets = new Bucket[sketchSize()];
    int i = 0;
    for (Map.Entry<Integer, Long> e : positiveBuckets.entrySet()) {
      buckets[i++] =
          new Bucket(
              e.getKey(),
              Math.pow(gamma, (double) e.getKey() - 1.0),
              Math.pow(gamma, e.getKey()),
              e.getValue());
    }
    for (Map.Entry<Integer, Long> e : negativeBuckets.entrySet()) {
      buckets[i++] =
          new Bucket(
              e.getKey(),
              -Math.pow(gamma, e.getKey()),
              -Math.pow(gamma, (double) e.getKey() - 1.0),
              e.getValue());
    }
    if (zeroCount > 0) {
      buckets[i] = new Bucket(0, 0, 0, zeroCount);
    }
    Arrays.sort(buckets, Comparator.comparingDouble(o -> o.lowerBound));
    return buckets;
  }

  public long totalCount() {
    return positiveBuckets.values().stream().mapToLong(l -> l).sum()
        + negativeBuckets.values().stream().mapToLong(l -> l).sum()
        + zeroCount;
  }

  private int findPIndex(Bucket[] buckets, long totalCount) {
    long count = 0;
    double rank = 0.5 * (totalCount - 1);
    for (int i = 0; i < buckets.length; ++i) {
      count += buckets[i].count;
      if (count > rank) {
        return i;
      }
    }
    return -1;
  }

  private int findQIndex(int p, Bucket[] buckets, long totalCount) {
    int q = p;
    long count = buckets[p].count;
    double rank = 0.5 * (totalCount - 1);
    int l = p - 1;
    int r = p + 1;
    while (count <= rank && l >= 0 && r < buckets.length) {
      if (buckets[p].lowerBound - buckets[l].upperBound
          < buckets[r].lowerBound - buckets[p].upperBound) {
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

    double mLowerBound = buckets[p].lowerBound + buckets[p].upperBound - buckets[q].upperBound;
    double mUpperBound = buckets[p].lowerBound + buckets[p].upperBound - buckets[q].lowerBound;
    if (p > q) {
      r--;
      if (buckets[r].lowerBound <= mLowerBound && buckets[r].upperBound >= mUpperBound) {
        q = r;
      }
    } else if (p < q) {
      l++;
      if (buckets[l].lowerBound <= mLowerBound && buckets[l].upperBound >= mUpperBound) {
        q = l;
      }
    }
    return q;
  }

  private void setValidRange(Bucket p, Bucket q) {
    validRange[0] = p.lowerBound;
    validRange[1] = p.upperBound;
    validRange[2] = p.lowerBound + q.lowerBound - p.upperBound;
    validRange[3] = p.upperBound + q.upperBound - p.lowerBound;
    validRange[4] = 2 * p.lowerBound - q.upperBound;
    validRange[5] = 2 * p.upperBound - q.lowerBound;
    Arrays.sort(validRange);
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
    Bucket[] buckets = unionBuckets();
    long totalCount = totalCount();
    int pIndex = findPIndex(buckets, totalCount);

    if (pIndex == -1) {
      throw new NoSuchElementException("No values in the time series");
    }

    int qIndex = findQIndex(pIndex, buckets, totalCount);
    Bucket p = buckets[pIndex];
    Bucket q = buckets[qIndex];

    if (p.lowerBound * q.lowerBound > 0) {
      if (p.lowerBound == q.lowerBound) {
        return new Mad(0, Double.MAX_VALUE);
      } else {
        double mad =
            2
                * (p.upperBound - q.lowerBound)
                * (p.lowerBound - q.upperBound)
                / ((gamma + 1) * Math.abs(p.lowerBound - q.lowerBound));
        double gammaPQ = Math.max(p.upperBound / q.upperBound, q.upperBound / p.upperBound);
        double delta;
        if (Math.abs(p.lowerBound) < Math.abs(q.lowerBound)) {
          delta =
              minDelta(
                  gammaPQ / Math.pow(gamma, 2) - 1 / gamma + Math.pow(gamma, -3),
                  1 / (Math.pow(gamma, 3) - gammaPQ * gamma + Math.pow(gamma, 2)));
        } else {
          delta =
              minDelta(
                  Math.pow(gamma, -2) + Math.pow(gamma, -3) - 1 / (gammaPQ * gamma),
                  1 / (Math.pow(gamma, 2) / gammaPQ + Math.pow(gamma, 3) - gamma));
        }
        beta = 1 - 2 / (1 + delta);
        if (needTwoPass()) {
          setValidRange(p, q);
          return new Mad(mad, (1 + 2 / (gammaPQ - 1)) * alpha);
        } else {
          return new Mad(0, Double.MAX_VALUE);
        }
      }
    } else {
      double mad =
          2
              * Math.max(
                  Math.abs(p.upperBound - q.lowerBound), Math.abs(q.upperBound - p.lowerBound))
              / (gamma + 1);
      return new Mad(mad, alpha);
    }
  }

  public int sketchSize() {
    return positiveBuckets.size() + negativeBuckets.size() + (zeroCount == 0 ? 0 : 1);
  }

  public boolean needTwoPass() {
    return beta > 0 && beta < 1;
  }

  public double getBeta() {
    return beta;
  }

  public double[] getValidRange() {
    return validRange;
  }

  private static class Bucket {

    int index;
    double lowerBound;
    double upperBound;
    long count;

    Bucket(int index, double lowerBound, double upperBound, long count) {
      this.index = index;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.count = count;
    }
  }

  public double getAlpha() {
    return alpha;
  }
}
