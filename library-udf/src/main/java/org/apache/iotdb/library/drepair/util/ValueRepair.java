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

public abstract class ValueRepair {

  protected int n;
  protected long[] time;
  protected double[] original;
  protected double[] repaired;

  public ValueRepair(RowIterator dataIterator) throws Exception {
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (dataIterator.hasNextRow()) {
      Row row = dataIterator.next();
      Double v = Util.getValueAsDouble(row);
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
    repaired = new double[n];
    processNaN();
  }

  public abstract void repair();

  private void processNaN() throws Exception {
    int index1 = 0, index2;
    while (index1 < n && Double.isNaN(original[index1])) {
      index1++;
    }
    index2 = index1 + 1;
    while (index2 < n && Double.isNaN(original[index2])) {
      index2++;
    }
    if (index2 >= n) {
      throw new Exception("At least two non-NaN values are needed");
    }
    for (int i = 0; i < index2; i++) {
      original[i] =
          original[index1]
              + (original[index2] - original[index1])
                  * (time[i] - time[index1])
                  / (time[index2] - time[index1]);
    }
    for (int i = index2 + 1; i < n; i++) {
      if (!Double.isNaN(original[i])) {
        index1 = index2;
        index2 = i;
        for (int j = index1 + 1; j < index2; j++) {
          original[j] =
              original[index1]
                  + (original[index2] - original[index1])
                      * (time[j] - time[index1])
                      / (time[index2] - time[index1]);
        }
      }
    }
    for (int i = index2 + 1; i < n; i++) {
      original[i] =
          original[index1]
              + (original[index2] - original[index1])
                  * (time[i] - time[index1])
                  / (time[index2] - time[index1]);
    }
  }

  public long[] getTime() {
    return time;
  }

  public double[] getRepaired() {
    return repaired;
  }
}
