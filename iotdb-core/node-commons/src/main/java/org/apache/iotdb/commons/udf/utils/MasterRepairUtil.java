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
package org.apache.iotdb.commons.udf.utils;

import org.apache.iotdb.udf.api.access.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;

public class MasterRepairUtil {
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> tdCleaned = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Long> tdTime = new ArrayList<>();
  private final int columnCnt;
  private long omega;
  private Double eta;
  private int k;
  private double[] std;
  private KDTreeUtil kdTreeUtil;

  public MasterRepairUtil(int columnCnt, long omega, double eta, int k) {
    this.columnCnt = columnCnt;
    this.omega = omega;
    this.eta = eta;
    this.k = k;
  }

  public boolean isNullRow(Row row) {
    boolean flag = true;
    for (int i = 0; i < row.size(); i++) {
      if (!row.isNull(i)) {
        flag = false;
        break;
      }
    }
    return flag;
  }

  public void addRow(Row row) throws Exception {
    ArrayList<Double> tt = new ArrayList<>(); // time-series tuple
    boolean containsNotNull = false;
    for (int i = 0; i < this.columnCnt; i++) {
      if (!row.isNull(i)) {
        containsNotNull = true;
        BigDecimal bd = BigDecimal.valueOf(getValueAsDouble(row, i));
        tt.add(bd.doubleValue());
      } else {
        tt.add(null);
      }
    }
    if (containsNotNull) {
      td.add(tt);
      tdTime.add(row.getTime());
    }

    ArrayList<Double> mt = new ArrayList<>(); // master tuple
    containsNotNull = false;
    for (int i = this.columnCnt; i < row.size(); i++) {
      if (!row.isNull(i)) {
        containsNotNull = true;
        BigDecimal bd = BigDecimal.valueOf(getValueAsDouble(row, i));
        mt.add(bd.doubleValue());
      } else {
        mt.add(null);
      }
    }
    if (containsNotNull) {
      md.add(mt);
    }
  }

  public static double getValueAsDouble(Row row, int index) throws Exception {
    double ans;
    try {
      switch (row.getDataType(index)) {
        case INT32:
          ans = row.getInt(index);
          break;
        case INT64:
          ans = row.getLong(index);
          break;
        case FLOAT:
          ans = row.getFloat(index);
          break;
        case DOUBLE:
          ans = row.getDouble(index);
          break;
        default:
          throw new Exception("The value of the input time series is not numeric.\n");
      }
    } catch (IOException e) {
      throw new Exception("Fail to get data type in row " + row.getTime(), e);
    }
    return ans;
  }

  public void buildKDTree() {
    this.kdTreeUtil = KDTreeUtil.build(md, this.columnCnt);
  }

  public ArrayList<Double> getCleanResultColumn(int columnPos) {
    ArrayList<Double> column = new ArrayList<>();
    for (ArrayList<Double> tuple : this.tdCleaned) {
      column.add(tuple.get(columnPos - 1));
    }
    return column;
  }

  public ArrayList<Long> getTime() {
    return tdTime;
  }

  public double getTmDistance(ArrayList<Double> tTuple, ArrayList<Double> mTuple) {
    double distance = 0d;
    for (int pos = 0; pos < columnCnt; pos++) {
      double temp = tTuple.get(pos) - mTuple.get(pos);
      temp = temp / std[pos];
      distance += temp * temp;
    }
    distance = Math.sqrt(distance);
    return distance;
  }

  public ArrayList<Integer> calW(int i) {
    ArrayList<Integer> Wi = new ArrayList<>();
    for (int l = i - 1; l >= 0; l--) {
      if (this.tdTime.get(i) <= this.tdTime.get(l) + omega) {
        Wi.add(l);
      }
    }
    return Wi;
  }

  public ArrayList<ArrayList<Double>> calC(int i, ArrayList<Integer> Wi) {
    ArrayList<ArrayList<Double>> Ci = new ArrayList<>();
    if (Wi.isEmpty()) {
      Ci.add(this.kdTreeUtil.query(this.td.get(i), std));
    } else {
      Ci.addAll(this.kdTreeUtil.queryKNN(this.td.get(i), k, std));
      for (Integer integer : Wi) {
        Ci.addAll(this.kdTreeUtil.queryKNN(this.tdCleaned.get(integer), k, std));
      }
    }
    return Ci;
  }

  public void masterRepair() {
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Double> tuple = this.td.get(i);
      ArrayList<Integer> Wi = calW(i);
      ArrayList<ArrayList<Double>> Ci = this.calC(i, Wi);
      double minDis = Double.MAX_VALUE;
      ArrayList<Double> repairTuple = new ArrayList<>();
      for (ArrayList<Double> ci : Ci) {
        boolean smooth = true;
        for (Integer wi : Wi) {
          ArrayList<Double> wis = tdCleaned.get(wi);
          if (getTmDistance(ci, wis) > eta) {
            smooth = false;
            break;
          }
        }
        if (smooth) {
          double dis = getTmDistance(ci, tuple);
          if (dis < minDis) {
            minDis = dis;
            repairTuple = ci;
          }
        }
      }
      this.tdCleaned.add(repairTuple);
    }
  }

  public void setParameters() {
    if (omega == -1) {
      ArrayList<Long> intervals = getIntervals();
      Collections.sort(intervals);
      long interval = intervals.get(intervals.size() / 2);
      omega = interval * 10;
    }
    if (Double.isNaN(eta)) {
      ArrayList<Double> distanceList = new ArrayList<>();
      for (int i = 1; i < this.td.size(); i++) {
        for (int l = i - 1; l >= 0; l--) {
          if (this.tdTime.get(i) <= this.tdTime.get(l) + omega) {
            distanceList.add(getTmDistance(this.td.get(i), this.td.get(l)));
          } else break;
        }
      }
      Collections.sort(distanceList);
      eta = distanceList.get((int) (distanceList.size() * 0.9973));
    }
    if (k == -1) {
      for (int tempK = 2; tempK <= 5; tempK++) {
        ArrayList<Double> distanceList = new ArrayList<>();
        for (ArrayList<Double> tuple : this.td) {
          ArrayList<ArrayList<Double>> neighbors = this.kdTreeUtil.queryKNN(tuple, tempK, std);
          for (ArrayList<Double> neighbor : neighbors) {
            distanceList.add(getTmDistance(tuple, neighbor));
          }
        }
        Collections.sort(distanceList);
        if (distanceList.get((int) (distanceList.size() * 0.9)) > eta) {
          k = tempK;
          break;
        }
      }
      if (k == -1) {
        k = Integer.min(5, this.md.size());
      }
    }
  }

  private double varianceImperative(double[] value) {
    double average = 0.0;
    int cnt = 0;
    for (double p : value) {
      if (!Double.isNaN(p)) {
        cnt += 1;
        average += p;
      }
    }
    if (cnt == 0) {
      return 0d;
    }
    average /= cnt;

    double variance = 0.0;
    for (double p : value) {
      if (!Double.isNaN(p)) {
        variance += (p - average) * (p - average);
      }
    }
    return variance / cnt;
  }

  private double[] getColumn(int pos) {
    double[] column = new double[this.td.size()];
    for (int i = 0; i < this.td.size(); i++) {
      column[i] = this.td.get(i).get(pos);
    }
    return column;
  }

  public void callStd() {
    this.std = new double[this.columnCnt];
    for (int i = 0; i < this.columnCnt; i++) {
      std[i] = Math.sqrt(varianceImperative(getColumn(i)));
    }
  }

  public void repair() {
    fillNullValue();
    buildKDTree();
    callStd();
    setParameters();
    masterRepair();
  }

  public ArrayList<Long> getIntervals() {
    ArrayList<Long> intervals = new ArrayList<>();
    for (int i = 1; i < this.tdTime.size(); i++) {
      intervals.add(this.tdTime.get(i) - this.tdTime.get(i - 1));
    }
    return intervals;
  }

  public void fillNullValue() {
    for (int i = 0; i < columnCnt; i++) {
      double temp = this.td.get(0).get(i);
      for (ArrayList<Double> arrayList : this.td) {
        if (arrayList.get(i) == null) {
          arrayList.set(i, temp);
        } else {
          temp = arrayList.get(i);
        }
      }
    }
  }
}
