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
package org.apache.iotdb.library.dprofile.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.FloatArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.util.Arrays;
import java.util.NoSuchElementException;

public class ExactOrderStatistics {

  private TSDataType dataType;
  private FloatArrayList floatArrayList;
  private DoubleArrayList doubleArrayList;
  private IntArrayList intArrayList;
  private LongArrayList longArrayList;

  public ExactOrderStatistics(TSDataType type) throws UDFInputSeriesDataTypeNotValidException {
    this.dataType = type;
    switch (dataType) {
      case INT32:
        intArrayList = new IntArrayList();
        break;
      case INT64:
        longArrayList = new LongArrayList();
        break;
      case FLOAT:
        floatArrayList = new FloatArrayList();
        break;
      case DOUBLE:
        doubleArrayList = new DoubleArrayList();
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  public void insert(Row row) throws UDFInputSeriesDataTypeNotValidException {
    switch (dataType) {
      case INT32:
        intArrayList.add(row.getInt(0));
        break;
      case INT64:
        longArrayList.add(row.getLong(0));
        break;
      case FLOAT:
        float vf = row.getFloat(0);
        if (Float.isFinite(vf)) { // 跳过NaN
          floatArrayList.add(vf);
        }
        break;
      case DOUBLE:
        double vd = row.getDouble(0);
        if (Double.isFinite(vd)) { // 跳过NaN
          doubleArrayList.add(vd);
        }
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  public double getMedian() throws UDFInputSeriesDataTypeNotValidException {
    switch (dataType) {
      case INT32:
        return getMedian(intArrayList);
      case INT64:
        return getMedian(longArrayList);
      case FLOAT:
        return getMedian(floatArrayList);
      case DOUBLE:
        return getMedian(doubleArrayList);
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  public double getMad() throws UDFInputSeriesDataTypeNotValidException {
    switch (dataType) {
      case INT32:
        return getMad(intArrayList);
      case INT64:
        return getMad(longArrayList);
      case FLOAT:
        return getMad(floatArrayList);
      case DOUBLE:
        return getMad(doubleArrayList);
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  public double getPercentile(double phi) throws UDFInputSeriesDataTypeNotValidException {
    switch (dataType) {
      case INT32:
        return getPercentile(intArrayList, phi);
      case INT64:
        return getPercentile(longArrayList, phi);
      case FLOAT:
        return getPercentile(floatArrayList, phi);
      case DOUBLE:
        return getPercentile(doubleArrayList, phi);
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  public static double getMedian(FloatArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return kthSmallest(nums.toArray(), (nums.size() >> 1) + 1, true);
    }
  }

  public static double getMad(FloatArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      float[] arr = nums.toArray();
      float median = kthSmallest(arr, (arr.length >> 1) + 1, true);
      for (int i = 0; i < arr.length; ++i) {
        arr[i] = Math.abs(arr[i] - median);
      }
      return kthSmallest(arr, (arr.length >> 1) + 1, true);
    }
  }

  public static double getPercentile(FloatArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return kthSmallest(nums.toArray(), (int) Math.ceil(nums.size() * phi), false);
    }
  }

  public static double getMedian(DoubleArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return kthSmallest(nums.toArray(), (nums.size() >> 1) + 1, true);
    }
  }

  public static double getMad(DoubleArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double[] arr = nums.toArray();
      final double median = kthSmallest(arr, (arr.length >> 1) + 1, true);
      return kthSmallest(
          Arrays.stream(arr).map(x -> Math.abs(x - median)).toArray(), (arr.length >> 1) + 1, true);
    }
  }

  public static double getPercentile(DoubleArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return kthSmallest(nums.toArray(), (int) Math.ceil(nums.size() * phi), false);
    }
  }

  public static double getMedian(IntArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return kthSmallest(nums.toArray(), (nums.size() >> 1) + 1, true);
    }
  }

  public static double getMad(IntArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      int[] arr = nums.toArray();
      final double median = kthSmallest(arr, (arr.length >> 1) + 1, true);
      return kthSmallest(
          Arrays.stream(arr).mapToDouble(x -> Math.abs(x - median)).toArray(),
          (arr.length >> 1) + 1,
          true);
    }
  }

  public static double getPercentile(IntArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return kthSmallest(nums.toArray(), (int) Math.ceil(nums.size() * phi), false);
    }
  }

  public static double getMedian(LongArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return kthSmallest(nums.toArray(), (nums.size() >> 1) + 1, true);
    }
  }

  public static double getMad(LongArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      long[] arr = nums.toArray();
      final double median = kthSmallest(arr, (arr.length >> 1) + 1, true);
      return kthSmallest(
          Arrays.stream(arr).mapToDouble(x -> Math.abs(x - median)).toArray(),
          (arr.length >> 1) + 1,
          true);
    }
  }

  public static double getPercentile(LongArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return kthSmallest(nums.toArray(), (int) Math.ceil(nums.size() * phi), false);
    }
  }

  private static double kthSmallest(double[] num, int k, boolean even) {
    int partition = kthSmallest(num, 0, num.length - 1, k);
    double median = num[partition];
    if (even && (num.length & 1) == 0) {
      partition = kthSmallest(num, 0, partition - 1, k - 1);
      median = (median + num[partition]) / 2;
    }
    return median;
  }

  private static int kthSmallest(double[] num, int low, int high, int k) {
    int partition = partition(num, low, high);

    while (partition != k - 1) {
      if (partition < k - 1) {
        low = partition + 1;
      } else {
        high = partition - 1;
      }
      partition = partition(num, low, high);
    }

    return partition;
  }

  private static int partition(double[] num, int low, int high) {
    double pivot = num[high];
    int pivotloc = low;
    for (int i = low; i <= high; i++) {
      if (num[i] < pivot) {
        double temp = num[i];
        num[i] = num[pivotloc];
        num[pivotloc] = temp;
        pivotloc++;
      }
    }
    double temp = num[high];
    num[high] = num[pivotloc];
    num[pivotloc] = temp;
    return pivotloc;
  }

  private static float kthSmallest(float[] num, int k, boolean even) {
    int partition = kthSmallest(num, 0, num.length - 1, k);
    float median = num[partition];
    if (even && (num.length & 1) == 0) {
      partition = kthSmallest(num, 0, partition - 1, k - 1);
      median = (median + num[partition]) / 2;
    }
    return median;
  }

  private static int kthSmallest(float[] num, int low, int high, int k) {
    int partition = partition(num, low, high);

    while (partition != k - 1) {
      if (partition < k - 1) {
        low = partition + 1;
      } else {
        high = partition - 1;
      }
      partition = partition(num, low, high);
    }

    return partition;
  }

  private static int partition(float[] num, int low, int high) {
    float pivot = num[high];
    int pivotloc = low;
    for (int i = low; i <= high; i++) {
      if (num[i] < pivot) {
        float temp = num[i];
        num[i] = num[pivotloc];
        num[pivotloc] = temp;
        pivotloc++;
      }
    }
    float temp = num[high];
    num[high] = num[pivotloc];
    num[pivotloc] = temp;
    return pivotloc;
  }

  private static double kthSmallest(int[] num, int k, boolean even) {
    int partition = kthSmallest(num, 0, num.length - 1, k);
    double median = num[partition];
    if (even && (num.length & 1) == 0) {
      partition = kthSmallest(num, 0, partition - 1, k - 1);
      median = (median + num[partition]) / 2;
    }
    return median;
  }

  private static int kthSmallest(int[] num, int low, int high, int k) {
    int partition = partition(num, low, high);

    while (partition != k - 1) {
      if (partition < k - 1) {
        low = partition + 1;
      } else {
        high = partition - 1;
      }
      partition = partition(num, low, high);
    }

    return partition;
  }

  private static int partition(int[] num, int low, int high) {
    int pivot = num[high];
    int pivotloc = low;
    for (int i = low; i <= high; i++) {
      if (num[i] < pivot) {
        int temp = num[i];
        num[i] = num[pivotloc];
        num[pivotloc] = temp;
        pivotloc++;
      }
    }
    int temp = num[high];
    num[high] = num[pivotloc];
    num[pivotloc] = temp;
    return pivotloc;
  }

  private static double kthSmallest(long[] num, int k, boolean even) {
    int partition = kthSmallest(num, 0, num.length - 1, k);
    double median = num[partition];
    if (even && (num.length & 1) == 0) {
      partition = kthSmallest(num, 0, partition - 1, k - 1);
      median = (median + num[partition]) / 2;
    }
    return median;
  }

  private static int kthSmallest(long[] num, int low, int high, int k) {
    int partition = partition(num, low, high);

    while (partition != k - 1) {
      if (partition < k - 1) {
        low = partition + 1;
      } else {
        high = partition - 1;
      }
      partition = partition(num, low, high);
    }

    return partition;
  }

  private static int partition(long[] num, int low, int high) {
    long pivot = num[high];
    int pivotloc = low;
    for (int i = low; i <= high; i++) {
      if (num[i] < pivot) {
        long temp = num[i];
        num[i] = num[pivotloc];
        num[pivotloc] = temp;
        pivotloc++;
      }
    }
    long temp = num[high];
    num[high] = num[pivotloc];
    num[pivotloc] = temp;
    return pivotloc;
  }
}
