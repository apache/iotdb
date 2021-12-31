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

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.library.util.Util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Scanner;

/** @author Zhang Xiaojian */
public abstract class ValueFill {
  protected int n;
  protected long time[];
  protected double original[];
  protected double repaired[];
  protected double mean = 0;
  protected double var = 0;
  protected int not_nan_number = 0;

  public ValueFill(RowIterator dataIterator) throws Exception {
    // 首先用ArrayList存起来，然后再修复
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
    repaired = new double[n];
    //        calMeanAndVar();
  }

  public ValueFill(String filename) throws Exception {
    Scanner sc = new Scanner(new File(filename));
    SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm");
    sc.useDelimiter("\\s*(,|\\r|\\n)\\s*"); // 设置分隔符，以逗号或回车分隔，前后可以有若干个空白符
    sc.nextLine();
    ArrayList<Long> timeList = new ArrayList<>();
    ArrayList<Double> originList = new ArrayList<>();
    while (sc.hasNext()) { // 读取数据
      timeList.add(format.parse(sc.next()).getTime());
      Double v = sc.nextDouble();
      if (!Double.isFinite(v)) { // 对空值的处理和特殊值的处理
        originList.add(Double.NaN);
      } else {
        originList.add(v);
      }
    }
    // 保存时间序列
    time = Util.toLongArray(timeList);
    original = Util.toDoubleArray(originList);
    n = time.length;
    repaired = new double[n];
    //        calMeanAndVar();
  }

  public abstract void fill();

  /** @return the time */
  public long[] getTime() {
    return time;
  }

  /** @return the filled */
  public double[] getFilled() {
    return repaired;
  };

  public void calMeanAndVar() {
    for (double v : original) {
      if (!Double.isNaN(v)) {
        mean += v;
        not_nan_number += 1;
      }
    }
    assert not_nan_number > 0 : "All values are NaN";
    mean /= not_nan_number;
    for (double v : original) {
      if (!Double.isNaN(v)) {
        var += (v - mean) * (v - mean);
      }
    }
    var /= not_nan_number;
  }
}
