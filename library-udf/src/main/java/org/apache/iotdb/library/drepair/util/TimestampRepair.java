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
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;

import java.util.ArrayList;

public class TimestampRepair {

  protected int n;
  protected long[] time;
  protected double[] original;
  protected long[] repaired;
  protected double[] repairedValue;
  protected long deltaT;
  protected long start0;

  public TimestampRepair(RowIterator dataIterator, long intervalMode, int startPointMode)
      throws Exception {
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (dataIterator.hasNextRow()) {
      Row row = dataIterator.next();
      double v = Util.getValueAsDouble(row);
      timeList.add(row.getTime());
      if (!Double.isFinite(v)) {
        originList.add(Double.NaN);
      } else {
        originList.add(v);
      }
    }
    time = Util.toLongArray(timeList);
    original = Util.toDoubleArray(originList);
    n = time.length;
    TimestampInterval trParam = new TimestampInterval(time, original);
    this.deltaT = trParam.getInterval(intervalMode);
    this.start0 = trParam.getStart0(startPointMode);
  }

  private void noRepair() {
    for (int i = 0; i < time.length; i++) {
      repaired[i] = time[i];
      repairedValue[i] = original[i];
    }
  }

  public void dpRepair() {
    if (time.length <= 2) {
      noRepair();
      return;
    }
    int n0 = (int) Math.ceil((time[n - 1] - start0) / (double) deltaT + 1.0);
    repaired = new long[n0];
    repairedValue = new double[n0];
    int m0 = this.n;
    long[][] f = new long[n0 + 1][m0 + 1];
    int[][] steps = new int[n0 + 1][m0 + 1];
    // dynamic programming
    int addCostRatio = 100000;
    for (int i = 0; i < n0 + 1; i++) {
      f[i][0] = (long) addCostRatio * i;
      steps[i][0] = 1;
    }
    for (int i = 0; i < m0 + 1; i++) {
      f[0][i] = (long) addCostRatio * i;
      steps[0][i] = 2;
    }

    for (int i = 1; i < n0 + 1; i++) {
      for (int j = 1; j < m0 + 1; j++) {

        if (time[j - 1] == start0 + (i - 1) * deltaT) {
          // if timestamps are equal, then temporary minimum operation time equals to matched
          // operations before these points
          f[i][j] = f[i - 1][j - 1];
          steps[i][j] = 0;
        } else {
          // addition or deletion
          if (f[i - 1][j] < f[i][j - 1]) {
            f[i][j] = f[i - 1][j] + addCostRatio * 1;
            steps[i][j] = 1;
          } else {
            f[i][j] = f[i][j - 1] + addCostRatio * 1;
            steps[i][j] = 2;
          }
          // replacement
          long modifyResult = f[i - 1][j - 1] + Math.abs(time[j - 1] - start0 - (i - 1) * deltaT);
          if (modifyResult < f[i][j]) {
            f[i][j] = modifyResult;
            steps[i][j] = 0;
          }
        }
      }
    }

    int i = n0;
    int j = m0;

    while (i >= 1 && j >= 1) {
      long ps = start0 + (i - 1) * deltaT;
      if (steps[i][j] == 0) {
        repaired[i - 1] = ps;
        repairedValue[i - 1] = original[j - 1];
        i--;
        j--;
      } else if (steps[i][j] == 1) {
        // add points
        repaired[i - 1] = ps;
        repairedValue[i - 1] = Double.NaN;
        i--;
      } else {
        // delete points
        j--;
      }
    }
  }

  public double[] getRepairedValue() {
    return repairedValue;
  }

  public long[] getRepaired() {
    return repaired;
  }
}
