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

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.library.util.Util;

import java.util.ArrayList;

public class WindowDetect {

  protected int n;
  protected double len;
  protected double threshold;
  protected long time[];
  protected double original[];
  protected double repaired[];

  public WindowDetect(RowIterator dataIterator, double l, double t) throws Exception {
    len = l;
    threshold = t;
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (dataIterator.hasNextRow()) { // 读取数据
      Row row = dataIterator.next();
      Double v = Util.getValueAsDouble(row);
      timeList.add(row.getTime());
      if (v == null || !Double.isFinite(v)) { // 对空值的处理和特殊值的处理
        originList.add(Double.NaN);
      } else {
        originList.add(v);
      }
    }
    // 保存时间序列
    time = Util.toLongArray(timeList);
    original = Util.toDoubleArray(originList);
    n = time.length;
    repair();
  }

  private void repair() throws Exception {
    ArrayList<Double> repairedList = new ArrayList<>();
    ArrayList<Long> timeList = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      long cur_time = time[i];
      double cur_value = original[i];
      double value_before = 0.0, value_after = 0.0;
      for (int j = (int) Math.max(0, i - len); j <= Math.min(n - 1, i + len); j++) {
        if (j < i && Math.abs(time[j] - cur_time) <= len * 1000) {
          value_before += original[j];
        }
        if (j > i && Math.abs(time[j] - cur_time) <= len * 1000) {
          value_after += original[j];
        }
      }
      value_before = value_before / len;
      value_after = value_after / len;
      double p1 = Math.abs(value_before - cur_value) / (Math.max(value_before, cur_value) + 1);
      double p2 = Math.abs(value_after - cur_value) / (Math.max(value_after, cur_value) + 1);
      if (p1 < threshold || p2 < threshold) {
        repairedList.add(cur_value);
        timeList.add(cur_time);
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
