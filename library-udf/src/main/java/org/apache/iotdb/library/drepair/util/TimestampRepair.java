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

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.library.util.Util;

import Jama.Matrix;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class TimestampRepair {

  protected int n;
  protected long[] time;
  protected double[] original;
  protected long[] repaired;
  protected double[] repairedValue;
  protected long deltaT;
  protected long start0;

  public TimestampRepair(RowIterator dataIterator, int intervalMode, int startPointMode)
      throws Exception {
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (dataIterator.hasNextRow()) { // 读取数据
      Row row = dataIterator.next();
      double v = Util.getValueAsDouble(row);
      timeList.add(row.getTime());
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
    // NaN处理
    // processNaN();
    // 获得基本参数
    TimestampInterval trParam = new TimestampInterval(time, original);
    this.deltaT = trParam.getInterval(intervalMode);
    this.start0 = trParam.getStart0(startPointMode);
  }

  public TimestampRepair(String filename) throws Exception {
    Scanner sc = new Scanner(new File(filename));
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sc.useDelimiter("\\s*(,|\\r|\\n)\\s*"); // 设置分隔符，以逗号或回车分隔，前后可以有若干个空白符
    sc.nextLine();
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (sc.hasNext()) { // 读取数据
      System.out.println(format.parse(sc.next()).getTime());
      timeList.add(format.parse(sc.next()).getTime());
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
    repairedValue = new double[n];
    // NaN处理
    // processNaN();
    // 获得基本参数
    TimestampInterval trParam = new TimestampInterval(time, original);
    this.deltaT = trParam.getInterval(1);
    this.start0 = trParam.getStart0(2);
  }

  public TimestampRepair(String filename, int intervalMode, int start0Mode) throws Exception {
    Scanner sc = new Scanner(new File(filename));
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sc.useDelimiter("\\s*(,|\\r|\\n)\\s*"); // 设置分隔符，以逗号或回车分隔，前后可以有若干个空白符
    sc.nextLine();
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (sc.hasNext()) { // 读取数据
      String dt = sc.next();
      // System.out.println(format.parse(dt).getTime());
      timeList.add(format.parse(dt).getTime());
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
    repairedValue = new double[n];
    // NaN处理
    // processNaN();
    // 获得基本参数
    TimestampInterval trParam = new TimestampInterval(time, original);
    this.deltaT = trParam.getInterval(intervalMode);
    this.start0 = trParam.getStart0(start0Mode);
  }

  // 方法： 最小二乘法思想
  public void repair() {
    repaired = time.clone();
    // get X matrix
    double[][] xArray = new double[n][2];
    for (int i = 0; i < n; i++) {
      xArray[i][0] = i;
      xArray[i][1] = 1;
    }
    Matrix X = new Matrix(xArray);
    // get Y matrix
    double[][] yArray = new double[n][1];
    for (int i = 0; i < n; i++) {
      yArray[i][0] = time[i];
    }
    Matrix Y = new Matrix(yArray);
    // main logic
    Matrix XT = X.transpose();
    Matrix operator = (XT.times(X)).inverse().times(XT);
    Matrix ans = operator.times(Y);
    for (int i = 0; i < n; i++) {
      repaired[i] = (long) (ans.get(1, 0) + i * ans.get(0, 0));
    }
  }

  //  方法： 众数取模
  public void repair2() {
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
    long[] modn = new long[n];
    // System.out.println(maxTimesKey);
    // get mode that appears most times
    HashMap<Object, Integer> mapn = new LinkedHashMap<>();
    for (int i = 0; i < n; i++) {
      modn[i] = time[i] % maxTimesKey;
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
    // System.out.println(maxTimesMode);
    // for those sparse modes, we adjust its timestamp to a proper one
    for (int i = 1; i < n; i++) {
      if (modn[i] != maxTimesMode) {
        repaired[i] = time[i - 1] + maxTimesKey;
      }
    }
  }

  // 方法： 聚类思想
  public void repair3() {
    repaired = time.clone();
    // get array of timestamp intervals
    HashMap<Object, Integer> map = new LinkedHashMap<>();
    int maxTimes = 0;
    long maxTimesKey = 0;
    long maxInterval = 0;
    long minInterval = 9999999;
    long[] intervals = new long[n];
    for (int i = 0; i < n - 1; i++) {
      intervals[i] = time[i + 1] - time[i];
      if (intervals[i] > maxInterval) {
        // 取秒作为最小粒度
        maxInterval = intervals[i] / 1000 * 1000;
      }
      if (intervals[i] < minInterval) {
        minInterval = intervals[i] / 1000 * 1000;
      }
    }
    // 找到一个合适的k
    int k = (int) ((maxInterval - minInterval) / 1000 + 1);
    long[] means = new long[k];
    for (int i = 0; i < k; i++) {
      means[i] = minInterval + i * 1000;
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
    // 调整小类别的时间戳
    for (int i = 1; i < n - 1; i++) {
      if (results[i] != maxClusterId) {
        repaired[i] = time[i - 1] + means[maxClusterId];
      }
    }
  }

  // 获得标准间隔
  private void getInterval() {}

  // 获取基本参数
  private void getParams() {}

  private void noRepair() {
    for (int i = 0; i < time.length; i++) {
      repaired[i] = time[i];
      repairedValue[i] = original[i];
    }
  }

  // dp修复
  public void dpRepair() {
    if (time.length <= 2) {
      noRepair();
      return;
    }
    int n_ = (int) Math.ceil((time[n - 1] - start0) / deltaT + 1);
    repaired = new long[n_];
    repairedValue = new double[n_];
    int m_ = this.n;
    long[][] f = new long[n_ + 1][m_ + 1];
    int[][] steps = new int[n_ + 1][m_ + 1];
    // dynamic programming
    // 手动定义增/删系数，默认改的系数为1
    int addCostRatio = 100000;
    // 当一个序列为空的时候，则与其可以产生匹配的操作数就是增加/删除对应点。
    for (int i = 0; i < n_ + 1; i++) {
      f[i][0] = addCostRatio * i;
      steps[i][0] = 1;
    }
    for (int i = 0; i < m_ + 1; i++) {
      f[0][i] = addCostRatio * i;
      steps[0][i] = 2;
    }

    for (int i = 1; i < n_ + 1; i++) {
      for (int j = 1; j < m_ + 1; j++) {

        if (time[j - 1] == start0 + (i - 1) * deltaT) {
          // 如果当前时间戳相等，那么当前的最小操作数等于二者前面的匹配操作数
          // 在真实数据上，这种可能性微乎其微
          f[i][j] = f[i - 1][j - 1];
          steps[i][j] = 0;
        } else {
          // 增加或者删除操作
          if (f[i - 1][j] < f[i][j - 1]) {
            f[i][j] = f[i - 1][j] + addCostRatio * 1;
            steps[i][j] = 1;
          } else {
            f[i][j] = f[i][j - 1] + addCostRatio * 1;
            steps[i][j] = 2;
          }
          // 替换操作
          long modifyResult = f[i - 1][j - 1] + Math.abs(time[j - 1] - start0 - (i - 1) * deltaT);
          if (modifyResult < f[i][j]) {
            f[i][j] = modifyResult;
            steps[i][j] = 0;
          }
        }
      }
    }

    int i = n_;
    int j = m_;
    double unionSet = 0;
    double joinSet = 0;
    while (i >= 1 && j >= 1) {
      long ps = start0 + (i - 1) * deltaT;
      if (steps[i][j] == 0) {
        repaired[i - 1] = ps;
        repairedValue[i - 1] = original[j - 1];
        System.out.println(time[j - 1] + "," + ps + "," + original[j - 1]);
        unionSet += 1;
        joinSet += 1;
        i--;
        j--;
      } else if (steps[i][j] == 1) {
        // 增加点
        repaired[i - 1] = ps;
        repairedValue[i - 1] = Double.NaN;
        unionSet += 1;
        System.out.println("add, " + ps + "," + original[j - 1]);
        i--;
      } else {
        // 删除点
        unionSet += 1;
        System.out.println(time[j - 1] + ",delete" + "," + original[j - 1]);
        j--;
      }
    }
    System.out.println(joinSet / unionSet);
    System.out.println(f[n_][m_] / n_);
  }

  public static void main(String[] args) throws Exception {
    String filename = "test_data_sq.csv";
    TimestampRepair tr = new TimestampRepair(filename, 1, 2);
    // System.setOut(new PrintStream(new FileOutputStream("result_mannual.csv")));
    System.out.println("start0: " + tr.start0 + " deltaT: " + tr.deltaT);
    tr.dpRepair();

    //        tr.repair();
    //        System.out.println("repair by method 1");
    //        for (int i = 0; i < tr.n; i++) {
    //            System.out.println(tr.time[i] + " " + tr.repaired[i]);
    //        }
    //        System.setOut(new PrintStream(new FileOutputStream("result2.csv")));
    //        tr.repair2();
    //        System.out.println("repair by method 2");
    //        for (int i = 0; i < tr.n; i++) {
    //            System.out.println(tr.time[i] + "," + tr.repaired[i]);
    //        }
    //        System.setOut(new PrintStream(new FileOutputStream("result.csv")));
    //        tr.repair3();
    //        System.out.println("repair by method 3");
    //        for (int i = 0; i < tr.n; i++) {
    //            System.out.println(tr.time[i] + "," + tr.repaired[i]);
    //        }
  }

  public double[] getRepairedValue() {
    return repairedValue;
  }

  public long[] getRepaired() {
    return repaired;
  }
}
