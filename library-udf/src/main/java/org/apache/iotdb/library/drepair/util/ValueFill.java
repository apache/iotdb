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
import org.apache.iotdb.udf.api.exception.UDFException;

import java.util.ArrayList;

public abstract class ValueFill {
  protected int n;
  protected long[] time;
  protected double[] original;
  protected double[] repaired;
  protected double mean = 0;
  protected double var = 0;
  protected int not_nan_number = 0;

  public ValueFill(RowIterator dataIterator) throws Exception {
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
  }

  public abstract void fill() throws UDFException;

  public long[] getTime() {
    return time;
  }

  public double[] getFilled() {
    return repaired;
  };

  public void calMeanAndVar() throws UDFException {
    for (double v : original) {
      if (!Double.isNaN(v)) {
        mean += v;
        not_nan_number += 1;
      }
    }
    if (not_nan_number == 0) {
      throw new UDFException("All values are NaN");
    }
    mean /= not_nan_number;
    for (double v : original) {
      if (!Double.isNaN(v)) {
        var += (v - mean) * (v - mean);
      }
    }
    var /= not_nan_number;
  }
}
