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

// Reference to Screen.java
public class ScreenFill extends ValueFill {

  private double smin, smax;
  private double w;

  public ScreenFill(RowIterator dataIterator) throws Exception {
    super(dataIterator);
    setParameters();
  }

  public ScreenFill(String filename) throws Exception {
    super(filename);
    setParameters();
  }

  @Override
  public long[] getTime() {
    return super.getTime();
  }

  @Override
  public double[] getFilled() {
    return super.getFilled();
  }

  @Override
  public void calMeanAndVar() {
    super.calMeanAndVar();
  }

  @Override
  public void fill() {
    ArrayList<Pair<Long, Double>> ans = new ArrayList<>();
    int currentIndex = 0;
    while (currentIndex < n) {
      ans.add(Pair.of(time[currentIndex], original[currentIndex]));
      if (Double.isNaN(original[currentIndex])) {
        // 发现缺失点后，标记为startIndex
        int startIndex = currentIndex;
        long fillTime = time[currentIndex];
        // 构造缺失点之后w长度的时间窗口
        int nextIndex = -1;
        currentIndex++;
        while (currentIndex < n && fillTime + w >= time[currentIndex]) {
          ans.add(Pair.of(time[currentIndex], original[currentIndex]));
          if (Double.isNaN(original[currentIndex]) && nextIndex == -1) {
            nextIndex = currentIndex;
          }
          currentIndex++;
        }
        local(ans, startIndex);
        if (nextIndex > 0) {
          while (currentIndex > nextIndex) {
            ans.remove(currentIndex - 1);
            currentIndex--;
          }
        }
      } else {
        currentIndex++;
      }
    }
    int k = 0;
    for (Pair<Long, Double> p : ans) {
      this.repaired[k] = p.getRight();
      k++;
    }
  }

  private double getMedian(ArrayList<Pair<Long, Double>> list, int index) {
    int m = 0;
    while (index + m + 1 < list.size()
        && list.get(index + m + 1).getLeft() <= list.get(index).getLeft() + w) {
      m++;
    }
    int count = 0;
    for (int i = 1; i <= m; i++) {
      if (!Double.isNaN(list.get(index + i).getRight())) {
        count++;
      }
    }
    double x[] = new double[2 * count];
    int temp_count = 0;
    for (int i = 1; i <= m; i++) {
      if (!Double.isNaN(list.get(index + i).getRight())) {
        x[temp_count] =
            list.get(index + i).getRight()
                + smin * (list.get(index).getLeft() - list.get(index + i).getLeft());
        x[temp_count + count] =
            list.get(index + i).getRight()
                + smax * (list.get(index).getLeft() - list.get(index + i).getLeft());
        temp_count++;
      }
    }
    Arrays.sort(x);
    return x[count];
  }

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

  public void setSmin(double smin) {
    this.smin = smin;
  }

  public void setSmax(double smax) {
    this.smax = smax;
  }

  public void setW(int w) {
    this.w = w;
  }

  public static void main(String[] args) throws Exception {
    ScreenFill screenFill = new ScreenFill("temp.csv");
    screenFill.fill();
    for (int i = 0; i < screenFill.n; i++) {
      System.out.println(screenFill.time[i] + " " + screenFill.repaired[i]);
    }
  }
}
