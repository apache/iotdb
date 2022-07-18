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
import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.exception.UDFException;

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

  @Override
  public long[] getTime() {
    return super.getTime();
  }

  @Override
  public double[] getFilled() {
    return super.getFilled();
  }

  @Override
  public void calMeanAndVar() throws UDFException {
    super.calMeanAndVar();
  }

  @Override
  public void fill() {
    ArrayList<Pair<Long, Double>> ans = new ArrayList<>();
    int currentIndex = 0;
    while (currentIndex < n) {
      ans.add(Pair.of(time[currentIndex], original[currentIndex]));
      if (Double.isNaN(original[currentIndex])) {
        int startIndex = currentIndex;
        long fillTime = time[currentIndex];
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
    temp = Math.min(xmax, temp);
    temp = Math.max(xmin, temp);
    return temp;
  }

  private void local(ArrayList<Pair<Long, Double>> list, int index) {
    double mid = getMedian(list, index);
    if (index == 0) {
      list.set(index, Pair.of(list.get(index).getLeft(), mid));
    } else {
      double temp = getRepairedValue(list, index, mid);
      list.set(index, Pair.of(list.get(index).getLeft(), temp));
    }
  }

  private void setParameters() {
    double[] speed = Util.speed(original, time);
    Median median = new Median();
    double mid = median.evaluate(speed);
    double sigma = Util.mad(speed);
    smax = mid + 3 * sigma;
    smin = mid - 3 * sigma;
    double[] interval = Util.variation(time);
    w = 5 * median.evaluate(interval);
  }
}
