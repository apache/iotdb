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
package org.apache.iotdb.library.drepair.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TimestampInterval {

  protected int n;
  protected long[] time;
  protected double[] original;
  protected long[] repaired;
  protected long deltaT;
  protected long start0;

  public TimestampInterval(long[] time, double[] original) throws Exception {
    // keep the time series
    this.time = time;
    this.original = original;
    n = time.length;
    repaired = new long[n];
  }

  // get standard interval
  // -1 median -2 mode -3 cluster
  public long getInterval(int mode) {
    switch (mode) {
      case -1:
        this.deltaT = getIntervalByMedian();
        break;
      case -2:
        this.deltaT = getIntervalByMode();
        break;
      case -3:
        this.deltaT = getIntervalByCluster();
        break;
      default:
        this.deltaT = mode;
    }
    return this.deltaT;
  }

  // median
  private long getIntervalByMedian() {
    ArrayList<Long> arrInterval = new ArrayList<>();
    for (int i = 0; i < n - 2; i++) {
      arrInterval.add(time[i + 1] - time[i]);
    }
    arrInterval.sort(Comparator.naturalOrder());
    int m = n - 1;
    if (m % 2 == 0) {
      return (arrInterval.get(m / 2 - 1) + arrInterval.get(m / 2)) / 2;
    }
    return arrInterval.get(m / 2);
  }

  // mode
  private long getIntervalByMode() {
    repaired = time.clone();
    // get a timestamp interval that appears most times
    HashMap<Object, Integer> map = new LinkedHashMap<>();
    int maxTimes = 0;
    long maxTimesKey = 0;
    for (int i = 0; i < n - 1; i++) {
      map.put(time[i + 1] - time[i], map.getOrDefault(time[i + 1] - time[i], 0) + 1);
    }
    for (Map.Entry<Object, Integer> entry : map.entrySet()) {
      Object key = entry.getKey();
      Integer value = entry.getValue();
      if (value > maxTimes) {
        maxTimes = value;
        maxTimesKey = (long) key;
      }
    }
    return maxTimesKey;
  }

  // cluster
  private long getIntervalByCluster() {
    // get array of timestamp intervals
    HashMap<Object, Integer> map = new LinkedHashMap<>();
    long maxInterval = 0;
    long minInterval = 9999999;
    long[] intervals = new long[n];
    for (int i = 0; i < n - 1; i++) {
      intervals[i] = time[i + 1] - time[i];
      if (intervals[i] > maxInterval) {
        maxInterval = intervals[i];
      }
      if (intervals[i] < minInterval) {
        minInterval = intervals[i];
      }
    }
    int k = 3;
    long[] means = new long[k];
    for (int i = 0; i < k; i++) {
      means[i] = minInterval + (i + 1) * (maxInterval - minInterval) / (k + 1);
    }
    long[][] distance = new long[n - 1][k];
    int[] results = new int[n - 1];
    for (int i = 0; i < n - 1; i++) {
      results[i] = -1;
    }
    boolean changed = true;
    int[] cnts = new int[k];
    int maxClusterId = 0;
    while (changed) {
      changed = false;
      for (int i = 0; i < n - 1; i++) {
        long minDis = 99999999;
        int minDisId = 0;
        for (int j = 0; j < k; j++) {
          distance[i][j] = Math.abs(intervals[i] - means[j]);
          if (distance[i][j] < minDis) {
            minDis = distance[i][j];
            minDisId = j;
          }
        }
        if (minDisId != results[i]) {
          changed = true;
          results[i] = minDisId;
        }
      }
      int maxCluterCnt = 0;
      for (int i = 0; i < k; i++) {
        long sum = 0;
        cnts[i] = 0;
        for (int j = 0; j < n - 1; j++) {
          if (results[j] == i) {
            sum += intervals[j];
            cnts[i] += 1;
          }
        }
        if (cnts[i] != 0) {
          means[i] = sum / cnts[i];
          if (cnts[i] > maxCluterCnt) {
            maxClusterId = i;
            maxCluterCnt = cnts[i];
          }
        }
      }
    }
    return means[maxClusterId];
  }

  // get standard starting point
  public long getStart0(int mode) {
    switch (mode) {
      case 1:
        this.start0 = getStart0ByLinear();
        break;
      case 2:
        this.start0 = getStart0ByMode();
        break;
    }
    return this.start0;
  }

  private long getStart0ByLinear() {
    long sum_ = 0;
    for (int i = 0; i < n; i++) {
      sum_ += time[i];
      sum_ -= this.deltaT * i;
    }
    return sum_ / n;
  }

  private long getStart0ByMode() {
    long[] modn = new long[n];
    // get mode that appears most times
    HashMap<Object, Integer> mapn = new LinkedHashMap<>();
    for (int i = 0; i < n; i++) {
      modn[i] = time[i] % this.deltaT;
      mapn.put(modn[i], mapn.getOrDefault(modn[i], 0) + 1);
    }
    int maxTimesn = 0;
    long maxTimesMode = 0;
    for (Map.Entry<Object, Integer> entry : mapn.entrySet()) {
      Object key = entry.getKey();
      Integer value = entry.getValue();
      if (value > maxTimesn) {
        maxTimesn = value;
        maxTimesMode = (long) key;
      }
    }
    long st = 0;
    for (int i = 0; i < n; i++) {
      if (modn[i] == maxTimesMode) {
        st = time[i];
        while (st > time[0]) {
          st -= deltaT;
        }
      }
    }
    return st;
  }
}
