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

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
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
  protected double variance = 0;
  protected int notNanNumber = 0;
  private int validNumber = 0;

  protected ValueFill(RowIterator dataIterator) throws Exception {
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (dataIterator.hasNextRow()) {
      Row row = dataIterator.next();
      timeList.add(row.getTime());
      if (row.isNull(0)) {
        originList.add(Double.NaN);
        continue;
      }
      double v = Util.getValueAsDouble(row);
      if (!Double.isFinite(v)) {
        originList.add(Double.NaN);
      } else {
        originList.add(v);
        validNumber++;
      }
    }
    time = Util.toLongArray(timeList);
    original = Util.toDoubleArray(originList);
    n = time.length;
    repaired = new double[n];
  }

  public abstract void fill() throws UDFException;

  public boolean hasValidValue() {
    return validNumber > 0;
  }

  public long[] getTime() {
    return time;
  }

  public double[] getFilled() {
    return repaired;
  }

  public void calMeanAndVar() throws UDFException {
    mean = 0;
    variance = 0;
    notNanNumber = 0;
    for (double v : original) {
      if (!Double.isNaN(v)) {
        mean += v;
        notNanNumber += 1;
      }
    }
    if (notNanNumber == 0) {
      throw new UDFException(LibraryUdfMessages.ALL_VALUES_ARE_NAN);
    }
    mean /= notNanNumber;
    for (double v : original) {
      if (!Double.isNaN(v)) {
        variance += (v - mean) * (v - mean);
      }
    }
    variance /= notNanNumber;
  }
}
