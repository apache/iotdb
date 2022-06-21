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

package org.apache.iotdb.library.anomaly.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;

import java.util.ArrayList;

public class WindowDetect {

  protected int timeLength;
  protected double len;
  protected double threshold;
  protected long[] time;
  protected double[] original;
  protected double[] repaired;

  public WindowDetect(RowIterator dataIterator, double l, double t) throws Exception {
    len = l;
    threshold = t;
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (dataIterator.hasNextRow()) {
      Row row = dataIterator.next();
      Double v = Util.getValueAsDouble(row);
      timeList.add(row.getTime());
      if (v == null || !Double.isFinite(v)) {
        originList.add(Double.NaN);
      } else {
        originList.add(v);
      }
    }
    time = Util.toLongArray(timeList);
    original = Util.toDoubleArray(originList);
    timeLength = time.length;
    repair();
  }

  private void repair() throws Exception {
    ArrayList<Double> repairedList = new ArrayList<>();
    ArrayList<Long> timeList = new ArrayList<>();
    for (int i = 0; i < timeLength; i++) {
      long curTime = time[i];
      double curValue = original[i];
      double valueBefore = 0.0;
      double valueAfter = 0.0;
      for (int j = (int) Math.max(0, i - len); j <= Math.min(timeLength - 1, i + len); j++) {
        if (j < i && Math.abs(time[j] - curTime) <= len * 1000) {
          valueBefore += original[j];
        }
        if (j > i && Math.abs(time[j] - curTime) <= len * 1000) {
          valueAfter += original[j];
        }
      }
      valueBefore = valueBefore / len;
      valueAfter = valueAfter / len;
      double p1 = Math.abs(valueBefore - curValue) / (Math.max(valueBefore, curValue) + 1);
      double p2 = Math.abs(valueAfter - curValue) / (Math.max(valueAfter, curValue) + 1);
      if (p1 < threshold || p2 < threshold) {
        repairedList.add(curValue);
        timeList.add(curTime);
      }
    }
    time = Util.toLongArray(timeList);
    repaired = Util.toDoubleArray(repairedList);
  }

  /** @return the time */
  public long[] getTime() {
    return time;
  }

  /** @return the repaired */
  public double[] getRepaired() {
    return repaired;
  }
}
