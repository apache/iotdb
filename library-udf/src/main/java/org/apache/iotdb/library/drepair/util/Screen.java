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

import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.library.util.Util;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * 利用Screen方法进行数值修复的类
 *
 * @author Wang Haoyu
 */
public class Screen extends ValueRepair {

  private double smin, smax;
  private double w;

  public Screen(RowIterator dataIterator) throws Exception {
    super(dataIterator);
    setParameters();
  }

  public Screen(String filename) throws Exception {
    super(filename);
    setParameters();
  }

  private void setParameters() {
    // 设置默认的速度阈值
    double[] speed = Util.speed(original, time);
    Median median = new Median();
    double mid = median.evaluate(speed);
    double sigma = Util.mad(speed);
    smax = mid + 3 * sigma;
    smin = mid - 3 * sigma;
    // 设置默认的窗口大小
    double interval[] = Util.variation(time);
    w = 5 * median.evaluate(interval);
  }

  @Override
  public void repair() {
    // 准备使用固定的window
    ArrayList<Pair<Long, Double>> ans = new ArrayList<>();
    ans.add(Pair.of(time[0], original[0]));
    int startIndex = 0;
    for (int i = 1; i < n; i++) {
      ans.add(Pair.of(time[i], original[i]));
      while (ans.get(startIndex).getLeft() + w < ans.get(i).getLeft()) {
        // 对窗口进行滑动，修复前面的若干个已经移出窗口的数据点
        local(ans, startIndex);
        startIndex++;
      }
    }
    while (startIndex < n) { // 修复窗口中剩余的数据点
      local(ans, startIndex);
      startIndex++;
    }
    int k = 0;
    for (Pair<Long, Double> p : ans) {
      this.repaired[k] = p.getRight();
      k++;
    }
  }

  /**
   * 对给定的数据点，返回所有候选修复的中位数
   *
   * @param list 数据点序列
   * @param index 待修复的数据点索引
   * @return 所有候选修复的中位数
   */
  private double getMedian(ArrayList<Pair<Long, Double>> list, int index) {
    int m = 0;
    while (index + m + 1 < list.size()
        && list.get(index + m + 1).getLeft() <= list.get(index).getLeft() + w) {
      m++;
    }
    double x[] = new double[2 * m + 1];
    x[0] = list.get(index).getRight();
    for (int i = 1; i <= m; i++) {
      x[i] =
          list.get(index + i).getRight()
              + smin * (list.get(index).getLeft() - list.get(index + i).getLeft());
      x[i + m] =
          list.get(index + i).getRight()
              + smax * (list.get(index).getLeft() - list.get(index + i).getLeft());
    }
    Arrays.sort(x);
    return x[m];
  }

  /**
   * 对给定的数据点，返回实际修复后的值
   *
   * @param list 数据点序列
   * @param index 待修复的数据点索引
   * @param mid 所有候选修复的中位数
   * @return 实际修复后的值
   */
  private double getRepairedValue(ArrayList<Pair<Long, Double>> list, int index, double mid) {
    double xmin =
        list.get(index - 1).getRight()
            + smin * (list.get(index).getLeft() - list.get(index - 1).getLeft());
    double xmax =
        list.get(index - 1).getRight()
            + smax * (list.get(index).getLeft() - list.get(index - 1).getLeft());
    double temp = mid;
    temp = xmax < temp ? xmax : temp;
    temp = xmin > temp ? xmin : temp;
    return temp;
  }

  /**
   * 使用论文中的local算法修复一个数据点
   *
   * @param list 数据点序列
   * @param index 待修复的数据点的索引
   */
  private void local(ArrayList<Pair<Long, Double>> list, int index) {
    double mid = getMedian(list, index);
    // 计算x_k'
    if (index == 0) {
      list.set(index, Pair.of(list.get(index).getLeft(), mid));
    } else {
      double temp = getRepairedValue(list, index, mid);
      list.set(index, Pair.of(list.get(index).getLeft(), temp));
    }
  }

  /** @param smin the smin to set */
  public void setSmin(double smin) {
    this.smin = smin;
  }

  /** @param smax the smax to set */
  public void setSmax(double smax) {
    this.smax = smax;
  }

  /** @param w the w to set */
  public void setW(int w) {
    this.w = w;
  }

  public static void main(String[] args) throws Exception {
    Screen screen = new Screen("temp.csv");
    screen.setSmax(0.001);
    screen.setSmin(-0.001);
    screen.repair();
    for (int i = 0; i < screen.n; i++) {
      System.out.println(screen.time[i] + " " + screen.repaired[i]);
    }
  }
}
