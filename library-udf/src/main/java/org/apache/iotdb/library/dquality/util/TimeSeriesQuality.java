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

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;

import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.util.ArrayList;

/** Class for computing data quality index. */
public class TimeSeriesQuality {
  public static final int windowSize = 10;
  private boolean downtime = true; // count for shutdown period
  private int cnt = 0; // total number of points
  private int missCnt = 0; // number of missing points
  private int specialCnt = 0; // number of special values
  private int lateCnt = 0; // number of latency points
  private int redundancyCnt = 0; // number of redundancy points
  private int valueCnt = 0; // number of out of range points
  private int variationCnt = 0; // number of variation out of range points
  private int speedCnt = 0; // number of speed out of range points
  private int speedchangeCnt = 0; // number of speed change(acceleration) out of range points
  private final double[] time; // series without special values
  private final double[] origin; // series without special values

  public TimeSeriesQuality(RowIterator dataIterator) throws Exception {
    ArrayList<Double> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (dataIterator.hasNextRow()) {
      Row row = dataIterator.next();
      cnt++;
      double v = Util.getValueAsDouble(row);
      double t = (double) row.getTime();
      if (Double.isFinite(v)) {
        timeList.add(t);
        originList.add(v);
      } else { // processing NAN，INF
        specialCnt++;
        timeList.add(t);
        originList.add(Double.NaN);
      }
    }
    time = Util.toDoubleArray(timeList);
    origin = Util.toDoubleArray(originList);
    processNaN();
  }

  /** linear interpolation of NaN */
  private void processNaN() throws Exception {
    int n = origin.length;
    int index1 = 0;
    int index2;
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
    // interpolation at the beginning of the series
    for (int i = 0; i < index2; i++) {
      origin[i] =
          origin[index1]
              + (origin[index2] - origin[index1])
                  * (time[i] - time[index1])
                  / (time[index2] - time[index1]);
    }
    // interpolation at the middle of the series
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
    // interpolation at the end of the series
    for (int i = index2 + 1; i < n; i++) {
      origin[i] =
          origin[index1]
              + (origin[index2] - origin[index1])
                  * (time[i] - time[index1])
                  / (time[index2] - time[index1]);
    }
  }

  /** Detect timestamp errors */
  public void timeDetect() {
    // compute interval properties
    double[] interval = Util.variation(time);
    Median median = new Median();
    double base = median.evaluate(interval);
    // find timestamp anomalies
    ArrayList<Double> window = new ArrayList<>();
    int i;
    for (i = 0; i < Math.min(time.length, windowSize); i++) { // fill initial data
      window.add(time[i]);
    }
    while (window.size() > 1) {
      double times = (window.get(1) - window.get(0)) / base;
      if (times <= 0.5) { // delete over-concentrated points
        window.remove(1);
        redundancyCnt++;
      } else if (times >= 2.0 && (!downtime || times <= 9.0)) { // exclude power-off periods
        // large interval means missing or delaying
        int temp = 0; // find number of over-concentrated points in the following window
        for (int j = 2; j < window.size(); j++) {
          double times2 = (window.get(j) - window.get(j - 1)) / base;
          if (times2 >= 2.0) { // end searching when another missing is found
            break;
          }
          if (times2 <= 0.5) { // over-concentrated points founded, maybe caused by delaying
            temp++;
            window.remove(j); // move delayed points
            j--;
            if (temp == (int) Math.round(times - 1)) {
              break; // enough points to fill have been found
            }
          }
        }
        lateCnt += temp;
        missCnt += (Math.round(times - 1) - temp);
      }
      window.remove(0); // remove processed points
      while (window.size() < windowSize && i < time.length) {
        // fill into the window
        window.add(time[i]);
        i++;
      }
    }
  }

  /** preparation for validity */
  public void valueDetect() {
    int k = 3;
    valueCnt = findOutliers(origin, k);
    // range anomaly
    double[] variation = Util.variation(origin);
    variationCnt = findOutliers(variation, k);
    // speed anomaly
    double[] speed = Util.speed(origin, time);
    speedCnt = findOutliers(speed, k);
    // acceleration anomaly
    double[] speedchange = Util.variation(speed);
    speedchangeCnt = findOutliers(speedchange, k);
  }

  /** return number of points lie out of median +- k * MAD */
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

  public double getCompleteness() {
    return 1 - (missCnt + specialCnt) * 1.0 / (cnt + missCnt);
  }

  public double getConsistency() {
    return 1 - redundancyCnt * 1.0 / cnt;
  }

  public double getTimeliness() {
    return 1 - lateCnt * 1.0 / cnt;
  }

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
