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
public class TimeSeriesSegQuality {
  public static final int WINDOW_SIZE = 10;
  private boolean downtime = false; // count for shutdown period
  private int cnt = 0; // total number of points
  private int missCnt = 0; // number of missing points
  private int specialCnt = 0; // number of special values
  private int lateCnt = 0; // number of latency points
  private int redundancyCnt = 0; // number of redundancy points
  private int valueCnt = 0; // number of out of range points
  private int variationCnt = 0; // number of variation out of range points
  private int speedCnt = 0; // number of speed out of range points
  private int speedchangeCnt = 0; // number of speed change(acceleration) out of range points
  protected double answer;
  private double[] time; // series without special values

  public TimeSeriesSegQuality(RowIterator dataIterator, int mode) throws Exception {
    TimeSeriesSegment tseg = new TimeSeriesSegment(dataIterator);
    tseg.exactRepair();
    long[] starts = tseg.getStart();
    long[] ends = tseg.getEnd();
    answer = 0;
    specialCnt = 0;
    missCnt = 0;
    specialCnt = 0;
    lateCnt = 0;
    redundancyCnt = 0;
    cnt = 0;
    valueCnt = 0;
    variationCnt = 0;
    speedCnt = 0;
    speedchangeCnt = 0;
    for (int i = 0; i < starts.length; i++) {
      RowIterator subIterator = new SubsequenceRowIterator(dataIterator, starts[i], ends[i]);
      ArrayList<Double> timeList = new ArrayList<>();
      while (subIterator.hasNextRow()) {
        Row row = subIterator.next();
        cnt++;
        double v = Util.getValueAsDouble(row);
        double t = row.getTime();
        if (Double.isFinite(v)) {
          timeList.add(t);
        } else { // processing NAN，INF
          specialCnt++;
          timeList.add(t);
        }
      }
      time = Util.toDoubleArray(timeList);
      timeList.clear();
      timeDetect();
      // starttimestamp[i] = starts[i];
    }
    if (mode == 0) {
      answer = getCompleteness();
    } else if (mode == 1) {
      answer = getConsistency();
    } else {
      answer = getTimeliness();
    }
  }

  /** Detect timestamp errors. */
  public void timeDetect() {
    // compute interval properties
    double[] interval = Util.variation(time);
    Median median = new Median();
    double base = median.evaluate(interval);
    // find timestamp anomalies
    ArrayList<Double> window = new ArrayList<>();
    int i;
    for (i = 0; i < Math.min(time.length, WINDOW_SIZE); i++) { // fill initial data
      window.add(time[i]);
    }
    while (window.size() > 1) {
      double times = (window.get(1) - window.get(0)) / base;
      if (times <= 0.5) { // delete over-concentrated points
        window.remove(1);
        redundancyCnt++;
      } else if (times > 1.0 && (!downtime || times <= 9.0)) { // exclude power-off periods
        // large interval means missing or delaying
        int temp = 0; // find number of over-concentrated points in the following window
        for (int j = 2; j < window.size(); j++) {
          double times2 = (window.get(j) - window.get(j - 1)) / base;
          if (times2 >= 2.0) { // end searching when another missing is found
            break;
          }
          if (times2 < 1.0) { // over-concentrated points founded, maybe caused by delaying
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
      while (window.size() < WINDOW_SIZE && i < time.length) {
        // fill into the window
        window.add(time[i]);
        i++;
      }
    }
    window.clear();
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

  /**
   * @return the downtime
   */
  public boolean isDowntime() {
    return downtime;
  }

  /**
   * @param downtime the downtime to set
   */
  public void setDowntime(boolean downtime) {
    this.downtime = downtime;
  }

  public double getAnswer() {
    return answer;
  }
}
