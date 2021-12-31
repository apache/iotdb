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

import org.apache.iotdb.library.util.Util;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class TimestampInterval {

  protected int n;
  protected long time[];
  protected double original[];
  protected long repaired[];
  protected long deltaT;
  protected long start0;

  public TimestampInterval(long[] time, double[] original) throws Exception {
    // 保存时间序列
    this.time = time;
    this.original = original;
    n = time.length;
    repaired = new long[n];
  }

  public TimestampInterval(String filename) throws Exception {
    Scanner sc = new Scanner(new File(filename));
    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sc.useDelimiter("\\s*(,|\\r|\\n)\\s*"); // 设置分隔符，以逗号或回车分隔，前后可以有若干个空白符
    sc.nextLine();
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (sc.hasNext()) { // 读取数据
      String dt = sc.next();
      // System.out.println(dt);
      System.out.println(dt);
      timeList.add(formatter.parse(dt).getTime());
      // System.out.println(timeList.get(timeList.size()-1));
      Double v = sc.nextDouble();
      if (!Double.isFinite(v)) { // 对空值的处理和特殊值的处理
        originList.add(Double.NaN);
      } else {
        originList.add(v);
      }
    }
    // 保存时间序列
    time = Util.toLongArray(timeList);
    original = Util.toDoubleArray(originList);
    n = time.length;
    repaired = new long[n];
    // NaN处理
    // processNaN();
  }

  // 获得标准间隔
  // mode: -1 中位数 -2 众数 -3 聚类
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

  // 中位数
  private long getIntervalByMedian() {
    ArrayList<Long> arrInterval = new ArrayList<>();
    for (int i = 0; i < n - 2; i++) {
      arrInterval.add(time[i + 1] - time[i]);
      // System.out.println(arrInterval.get(i));
    }
    arrInterval.sort(Comparator.naturalOrder());
    int m = n - 1;
    if (m % 2 == 0) {
      return (arrInterval.get(m / 2 - 1) + arrInterval.get(m / 2)) / 2;
    }
    return arrInterval.get(m / 2);
  }

  // 众数
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

  // 聚类
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
    // 找到一个合适的k
    // int k= (int) ((maxInterval-minInterval)/1000+1);
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
          // 计算点 i 到类 j 的距离
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
        // 1. 找出所有属于自己这一类的所有数据点
        // 2. 把自己的坐标修改为这些数据点的中心点坐标
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
    // 取聚类中心
    return means[maxClusterId];
  }

  // 获得标准起始点
  // 重要： 应该在获得标准间隔后求解
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

  // 直接利用最小二乘求解
  private long getStart0ByLinear() {
    long sum_ = 0;
    for (int i = 0; i < n; i++) {
      sum_ += time[i];
      sum_ -= this.deltaT * i;
    }
    // sum_-=this.deltaT*(n-1)*n/2;
    //        if(sum_/n>time[0]){
    //            System.out.println("err");
    //            return (long) (Math.floor(time[0]/1000)*1000);
    //        }
    return sum_ / n;
  }

  // 众数取模
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

  public static void main(String[] args) throws Exception {
    TimestampInterval tr = new TimestampInterval("test_data_mannual.csv");
    for (int i = 1; i < 4; i++) {
      System.out.println(i + " " + tr.getInterval(i));
    }
    for (int i = 1; i < 3; i++) {
      System.out.println(i + " " + tr.getStart0(i));
    }
  }
}
