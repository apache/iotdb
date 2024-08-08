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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;

import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.eclipse.collections.api.tuple.primitive.LongIntPair;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;

import java.io.IOException;
import java.util.ArrayList;

/** This class offers functions of getting and putting values from iotdb interface. */
public class Util {

  /**
   * Get value from specific column from Row, and cast to double. Make sure never get null from Row.
   *
   * @param row data row
   * @param index the column index
   * @return value of specific column from Row
   * @throws NoNumberException when getting a no number datatype
   */
  public static double getValueAsDouble(Row row, int index) throws Exception {
    double ans = 0;
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
          throw new NoNumberException();
      }
    } catch (IOException e) {
      throw new Exception("Fail to get data type in row " + row.getTime(), e);
    }
    return ans;
  }

  /**
   * Get value from 0th column from Row, and cast to double. Make sure never get null from Row.
   *
   * @param row data row
   * @return value from 0th column from Row
   * @throws NoNumberException when getting a no number datatype
   */
  public static double getValueAsDouble(Row row) throws Exception {
    return getValueAsDouble(row, 0);
  }

  /**
   * Get value from 0th column from Row, and cast to Object.
   *
   * @param row data row
   * @return value from 0th column from Row
   */
  public static Object getValueAsObject(Row row) throws IOException {
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
   * Add new data point to PointCollector
   *
   * @param pc PointCollector
   * @param type datatype
   * @param t timestamp
   * @param o value in Object type
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
   * calculate median absolute deviation of input series. 1.4826 is multiplied in order to achieve
   * asymptotic normality. Note: 1.4826 = 1/qnorm(3/4)
   *
   * @param value input series
   * @return median absolute deviation MAD
   */
  public static double mad(double[] value) {
    Median median = new Median();
    double mid = median.evaluate(value);
    double[] d = new double[value.length];
    for (int i = 0; i < value.length; i++) {
      d[i] = Math.abs(value[i] - mid);
    }
    return 1.4826 * median.evaluate(d);
  }

  /**
   * calculate 1-order difference of input series
   *
   * @param origin original series
   * @return 1-order difference
   */
  public static double[] variation(double[] origin) {
    int n = origin.length;
    double[] var = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      var[i] = origin[i + 1] - origin[i];
    }
    return var;
  }

  /**
   * calculate 1-order difference of input series
   *
   * @param origin original series
   * @return 1-order difference
   */
  public static double[] variation(long[] origin) {
    int n = origin.length;
    double[] var = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      var[i] = (double) (origin[i + 1] - origin[i]);
    }
    return var;
  }

  /**
   * calculate 1-order difference of input series
   *
   * @param origin original series
   * @return 1-order difference
   */
  public static int[] variation(int[] origin) {
    int n = origin.length;
    int[] var = new int[n - 1];
    for (int i = 0; i < n - 1; i++) {
      var[i] = origin[i + 1] - origin[i];
    }
    return var;
  }

  /**
   * calculate speed (1-order derivative with backward difference)
   *
   * @param origin value series
   * @param time timestamp series
   * @return speed series
   */
  public static double[] speed(double[] origin, double[] time) {
    int n = origin.length;
    double[] speed = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
    }
    return speed;
  }

  /**
   * calculate speed (1-order derivative with backward difference)
   *
   * @param origin value series
   * @param time timestamp series
   * @return speed series
   */
  public static double[] speed(double[] origin, long[] time) {
    int n = origin.length;
    double[] speed = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
    }
    return speed;
  }

  /**
   * computes mode
   *
   * @param values input series
   * @return mode
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
      unit = 60 * 1000L;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("h")) {
      unit = 60 * 60 * 1000L;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("d")) {
      unit = 24 * 60 * 60 * 1000L;
      s = s.substring(0, s.length() - 1);
    }
    double v = Double.parseDouble(s);
    return (long) (unit * v);
  }
}
