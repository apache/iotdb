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

/** @author Wang Haoyu */
public abstract class ValueRepair {

  protected int n;
  protected long time[];
  protected double original[];
  protected double repaired[];

  public ValueRepair(RowIterator dataIterator) throws Exception {
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
    // NaN处理
    processNaN();
  }

  public ValueRepair(String filename) throws Exception {
    Scanner sc = new Scanner(new File(filename));
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
    // NaN处理
    processNaN();
  }

  /** 开始修复 */
  public abstract void repair();

  /**
   * 对数据序列中的NaN进行处理，采用线性插值方法
   *
   * @throws java.lang.Exception
   */
  private void processNaN() throws Exception {
    int index1 = 0, index2; // 线性插值的两个基准
    // 找到两个非NaN的基准
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
    // 对序列开头的NaN进行插值
    for (int i = 0; i < index2; i++) {
      original[i] =
          original[index1]
              + (original[index2] - original[index1])
                  * (time[i] - time[index1])
                  / (time[index2] - time[index1]);
    }
    // 对序列中间的NaN进行插值
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
    // 对序列末尾的NaN进行插值
    for (int i = index2 + 1; i < n; i++) {
      original[i] =
          original[index1]
              + (original[index2] - original[index1])
                  * (time[i] - time[index1])
                  / (time[index2] - time[index1]);
    }
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
