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

package org.apache.iotdb.library.dquality.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.library.util.Util;

import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * 计算时序数据质量指标的类
 *
 * @author Wang Haoyu
 */
public class TimeSeriesQuality {
  public static final int windowSize = 10;
  private boolean downtime = true; // 是否考虑停机异常
  private int cnt = 0; // 数据点总数
  private int missCnt = 0; // 缺失点个数
  private int specialCnt = 0; // 特殊值点个数
  private int lateCnt = 0; // 延迟点个数
  private int redundancyCnt = 0; // 过密点个数
  private int valueCnt = 0; // 违背取值范围约束的数据点个数
  private int variationCnt = 0; // 违背取值变化约束的数据点个数
  private int speedCnt = 0; // 违背速度约束的数据点个数
  private int speedchangeCnt = 0; // 违背速度变化约束的数据点个数
  private final double[] time; // 除去特殊值的时间序列
  private final double[] origin; // 除去特殊值的时间序列

  public TimeSeriesQuality(RowIterator dataIterator) throws Exception {
    ArrayList<Double> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (dataIterator.hasNextRow()) {
      Row row = dataIterator.next();
      cnt++;
      double v = Util.getValueAsDouble(row);
      double t = Long.valueOf(row.getTime()).doubleValue();
      if (Double.isFinite(v)) {
        timeList.add(t);
        originList.add(v);
      } else { // 对特殊值的处理，包括NAN，INF等
        specialCnt++;
        timeList.add(t);
        originList.add(Double.NaN);
      }
    }
    time = Util.toDoubleArray(timeList);
    origin = Util.toDoubleArray(originList);
    processNaN();
  }

  public TimeSeriesQuality(String filename) throws Exception {
    Scanner sc = new Scanner(new File(filename));
    ArrayList<Double> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sc.useDelimiter("\\s*(,|\\r|\\n)\\s*"); // 设置分隔符，以逗号或回车分隔，前后可以有若干个空白符
    sc.nextLine();
    while (sc.hasNext()) {
      cnt++;
      double t = format.parse(sc.next()).getTime();
      double v = sc.nextDouble();
      if (Double.isFinite(v)) {
        timeList.add(t);
        originList.add(v);
      } else { // 对特殊值的处理，包括NAN，INF等
        specialCnt++;
        timeList.add(t);
        originList.add(Double.NaN);
      }
    }
    time = Util.toDoubleArray(timeList);
    origin = Util.toDoubleArray(originList);
    processNaN();
  }

  /** 对数据序列中的NaN进行处理，采用线性插值方法 */
  private void processNaN() throws Exception {
    int n = origin.length;
    int index1 = 0; // 线性插值的两个基准
    int index2; // 线性插值的两个基准
    // 找到两个非NaN的基准
    while (index1 < n && Double.isNaN(origin[index1])) {
      index1++;
    }
    index2 = index1 + 1;
    while (index2 < n && Double.isNaN(origin[index2])) {
      index2++;
    }
    if (index2 >= n) {
      throw new Exception("At least two non-NaN values are needed");
    }
    // 对序列开头的NaN进行插值
    for (int i = 0; i < index2; i++) {
      origin[i] =
          origin[index1]
              + (origin[index2] - origin[index1])
                  * (time[i] - time[index1])
                  / (time[index2] - time[index1]);
    }
    // 对序列中间的NaN进行插值
    for (int i = index2 + 1; i < n; i++) {
      if (!Double.isNaN(origin[i])) {
        index1 = index2;
        index2 = i;
        for (int j = index1 + 1; j < index2; j++) {
          origin[j] =
              origin[index1]
                  + (origin[index2] - origin[index1])
                      * (time[j] - time[index1])
                      / (time[index2] - time[index1]);
        }
      }
    }
    // 对序列末尾的NaN进行插值
    for (int i = index2 + 1; i < n; i++) {
      origin[i] =
          origin[index1]
              + (origin[index2] - origin[index1])
                  * (time[i] - time[index1])
                  / (time[index2] - time[index1]);
    }
  }

  /** 对时间序列的时间戳进行异常侦测，为计算完整性、一致性、时效性做准备 */
  public void timeDetect() {
    // 计算时间间隔特征
    double[] interval = Util.variation(time);
    Median median = new Median();
    double base = median.evaluate(interval);
    // 寻找时间戳异常
    ArrayList<Double> window = new ArrayList<>();
    int i;
    for (i = 0; i < Math.min(time.length, windowSize); i++) { // 填充初始数据
      window.add(time[i]);
    }
    while (window.size() > 1) {
      double times = (window.get(1) - window.get(0)) / base;
      if (times <= 0.5) { // 处理为过密点并删除
        window.remove(1);
        redundancyCnt++;
      } else if (times >= 2.0 && (!downtime || times <= 9.0)) { // 排除停机
        // 时间间隔过大，可能是数据缺失，也可能是延迟
        int temp = 0; // 在后续窗口中找到的连续过密点个数
        for (int j = 2; j < window.size(); j++) {
          double times2 = (window.get(j) - window.get(j - 1)) / base;
          if (times2 >= 2.0) { // 发现另一个缺失点，停止搜索
            break;
          }
          if (times2 <= 0.5) { // 发现过密点，可能是延迟导致的
            temp++;
            window.remove(j); // 将延迟点移回到前面
            j--;
            if (temp == (int) Math.round(times - 1)) {
              break; // 找到了足够数量的延迟点来填补
            }
          }
        }
        lateCnt += temp;
        missCnt += (Math.round(times - 1) - temp);
      }
      window.remove(0); // 从窗口中移除已经处理的数据点
      while (window.size() < windowSize && i < time.length) {
        // 向窗口中填充数据点直到窗口被填满
        window.add(time[i]);
        i++;
      }
    }
  }

  /** 对时间序列的值进行异常侦测，为计算有效性做准备 */
  public void valueDetect() {
    int k = 3;
    // 原始数据异常检测
    valueCnt = findOutliers(origin, k);
    // 取值变化异常检测
    double[] variation = Util.variation(origin);
    variationCnt = findOutliers(variation, k);
    // 取值变化速度异常检测
    double[] speed = Util.speed(origin, time);
    speedCnt = findOutliers(speed, k);
    // 取值变化加速度异常检测
    double[] speedchange = Util.variation(speed);
    speedchangeCnt = findOutliers(speedchange, k);
  }

  /**
   * 返回序列中偏离中位数超过k倍绝对中位差的点的个数
   *
   * @param value 序列
   * @return 偏离中位数超过k倍绝对中位差的点的个数
   */
  private int findOutliers(double[] value, double k) {
    Median median = new Median();
    double mid = median.evaluate(value);
    double sigma = Util.mad(value);
    int num = 0;
    for (double v : value) {
      if (Math.abs(v - mid) > k * sigma) {
        num++;
      }
    }
    return num;
  }

  /**
   * 返回时间序列的完整性
   *
   * @return 完整性
   */
  public double getCompleteness() {
    return 1 - (missCnt + specialCnt) * 1.0 / (cnt + missCnt);
  }

  /**
   * 返回时间序列的一致性
   *
   * @return 一致性
   */
  public double getConsistency() {
    return 1 - redundancyCnt * 1.0 / cnt;
  }

  /**
   * 返回时间序列的时效性
   *
   * @return 时效性
   */
  public double getTimeliness() {
    return 1 - lateCnt * 1.0 / cnt;
  }

  /**
   * 返回时间序列的有效性
   *
   * @return 有效性
   */
  public double getValidity() {
    return 1 - (valueCnt + variationCnt + speedCnt + speedchangeCnt) * 0.25 / cnt;
  }

  /** @return the downtime */
  public boolean isDowntime() {
    return downtime;
  }

  /** @param downtime the downtime to set */
  public void setDowntime(boolean downtime) {
    this.downtime = downtime;
  }
}
