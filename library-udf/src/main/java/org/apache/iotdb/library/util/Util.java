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
package org.apache.iotdb.library.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.eclipse.collections.api.tuple.primitive.LongIntPair;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;

import java.util.ArrayList;

/**
 * @author Wang Haoyu This class offers functions of getting and putting values from iotdb
 *     interface.
 */
public class Util {

  /**
   * 从Row中取出指定位置的值，并转化为double类型。注意，必须保证Row中不存在null。 Get value from specific column from Row, and
   * cast to double. Make sure never get null from Row.
   *
   * @param row 数据行 data row
   * @param index 指定位置的索引 the column index
   * @return Row中的指定位置的值 value of specific column from Row
   * @throws NoNumberException Row中的指定位置的值是非数值类型 when getting a no number datatype
   */
  public static double getValueAsDouble(Row row, int index) throws NoNumberException {
    double ans = 0;
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
        throw new NoNumberException();
    }
    return ans;
  }

  /**
   * 从Row中取出第0列的值，并转化为double类型。注意，必须保证Row中不存在null。 Get value from 0th column from Row, and cast to
   * double. Make sure never get null from Row.
   *
   * @param row 数据行 data row
   * @return Row中的第0列的值 value from 0th column from Row
   * @throws NoNumberException Row中的第0列的值是非数值类型 Row中的指定位置的值是非数值类型 when getting a no number datatype
   */
  public static double getValueAsDouble(Row row) throws NoNumberException {
    return getValueAsDouble(row, 0);
  }

  /**
   * 从Row中取出第一个值，并转化为Object类型 Get value from 0th column from Row, and cast to Object.
   *
   * @param row
   * @return Row中的第一个值 value from 0th column from Row
   */
  public static Object getValueAsObject(Row row) {
    Object ans = 0;
    switch (row.getDataType(0)) {
      case INT32:
        ans = row.getInt(0);
        break;
      case INT64:
        ans = row.getLong(0);
        break;
      case FLOAT:
        ans = row.getFloat(0);
        break;
      case DOUBLE:
        ans = row.getDouble(0);
        break;
      case BOOLEAN:
        ans = row.getBoolean(0);
        break;
      case TEXT:
        ans = row.getString(0);
        break;
    }
    return ans;
  }

  /**
   * 向PointCollector中加入新的数据点 Add new data point to PointCollector
   *
   * @param pc PointCollector
   * @param type 数据类型 datatype
   * @param t 时间戳 timestamp
   * @param o Object类型的值 value in Object type
   * @throws Exception
   */
  public static void putValue(PointCollector pc, TSDataType type, long t, Object o)
      throws Exception {
    switch (type) {
      case INT32:
        pc.putInt(t, (Integer) o);
        break;
      case INT64:
        pc.putLong(t, (Long) o);
        break;
      case FLOAT:
        pc.putFloat(t, (Float) o);
        break;
      case DOUBLE:
        pc.putDouble(t, (Double) o);
        break;
      case TEXT:
        pc.putString(t, (String) o);
        break;
      case BOOLEAN:
        pc.putBoolean(t, (Boolean) o);
    }
  }

  /**
   * cast {@code ArrayList<Double>} to {@code double[]}
   *
   * @param list ArrayList to cast
   * @return cast result
   */
  public static double[] toDoubleArray(ArrayList<Double> list) {
    return list.stream().mapToDouble(Double::valueOf).toArray();
  }

  /**
   * cast {@code ArrayList<Long>} to {@code long[]}
   *
   * @param list ArrayList to cast
   * @return cast result
   */
  public static long[] toLongArray(ArrayList<Long> list) {
    return list.stream().mapToLong(Long::valueOf).toArray();
  }

  /**
   * 计算序列的绝对中位差MAD。为了达到渐进正态性，乘上比例因子1.4826。 <br>
   * 备注: 1.4826 = 1/qnorm(3/4)
   *
   * <p>calculate median absolute deviation of input series. 1.4826 is multiplied in order to
   * achieve asymptotic normality. Note: 1.4826 = 1/qnorm(3/4)
   *
   * @param value 序列 input series
   * @return 绝对中位差MAD median absolute deviation MAD
   */
  public static double mad(double[] value) {
    Median median = new Median();
    double mid = median.evaluate(value);
    double d[] = new double[value.length];
    for (int i = 0; i < value.length; i++) {
      d[i] = Math.abs(value[i] - mid);
    }
    return 1.4826 * median.evaluate(d);
  }

  /**
   * 计算序列的取值变化 calculate 1-order difference of input series
   *
   * @param origin 原始序列 original series
   * @return 取值变化序列 1-order difference
   */
  public static double[] variation(double origin[]) {
    int n = origin.length;
    double var[] = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      var[i] = origin[i + 1] - origin[i];
    }
    return var;
  }

  /**
   * 计算序列的取值变化 calculate 1-order difference of input series
   *
   * @param origin 原始序列 original series
   * @return 取值变化序列 1-order difference
   */
  public static double[] variation(long origin[]) {
    int n = origin.length;
    double var[] = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      var[i] = origin[i + 1] - origin[i];
    }
    return var;
  }

  /**
   * 计算序列的取值变化 calculate 1-order difference of input series
   *
   * @param origin 原始序列 original series
   * @return 取值变化序列 1-order difference
   */
  public static int[] variation(int origin[]) {
    int n = origin.length;
    int var[] = new int[n - 1];
    for (int i = 0; i < n - 1; i++) {
      var[i] = origin[i + 1] - origin[i];
    }
    return var;
  }

  /**
   * 计算时间序列的速度 calculate speed (1-order derivative with backward difference)
   *
   * @param origin 值序列 value series
   * @param time 时间戳序列 timestamp series
   * @return 速度序列 speed series
   */
  public static double[] speed(double origin[], double time[]) {
    int n = origin.length;
    double speed[] = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
    }
    return speed;
  }

  /**
   * 计算时间序列的速度 calculate speed (1-order derivative with backward difference)
   *
   * @param origin 值序列 value series
   * @param time 时间戳序列 timestamp series
   * @return 速度序列 speed series
   */
  public static double[] speed(double origin[], long time[]) {
    int n = origin.length;
    double speed[] = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
    }
    return speed;
  }

  /**
   * 计算序列的众数 computes mode
   *
   * @param values 序列 input series
   * @return 众数 mode
   */
  public static long mode(long[] values) {
    LongIntHashMap map = new LongIntHashMap();
    for (long v : values) {
      map.addToValue(v, 1);
    }
    long key = 0;
    int maxValue = 0;
    for (LongIntPair p : map.keyValuesView()) {
      if (p.getTwo() > maxValue) {
        key = p.getOne();
        maxValue = p.getTwo();
      }
    }
    return key;
  }

  /**
   * cast String to timestamp
   *
   * @param s input string
   * @return timestamp
   */
  public static long parseTime(String s) {
    long unit = 0;
    s = s.toLowerCase();
    s = s.replaceAll(" ", "");
    if (s.endsWith("ms")) {
      unit = 1;
      s = s.substring(0, s.length() - 2);
    } else if (s.endsWith("s")) {
      unit = 1000;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("m")) {
      unit = 60 * 1000;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("h")) {
      unit = 60 * 60 * 1000;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("d")) {
      unit = 24 * 60 * 60 * 1000;
      s = s.substring(0, s.length() - 1);
    }
    double v = Double.parseDouble(s);
    return (long) (unit * v);
  }
}
