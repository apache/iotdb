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

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.FloatArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.io.IOException;
import java.util.NoSuchElementException;

/** Util for computing median, MAD, percentile */
public class ExactOrderStatistics {

  private final TSDataType dataType;
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
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }

  public void insert(Row row) throws UDFInputSeriesDataTypeNotValidException, IOException {
    switch (dataType) {
      case INT32:
        intArrayList.add(row.getInt(0));
        break;
      case INT64:
        longArrayList.add(row.getLong(0));
        break;
      case FLOAT:
        float vf = row.getFloat(0);
        if (Float.isFinite(vf)) {
          floatArrayList.add(vf);
        }
        break;
      case DOUBLE:
        double vd = row.getDouble(0);
        if (Double.isFinite(vd)) {
          doubleArrayList.add(vd);
        }
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
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
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
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
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }

  public String getPercentile(double phi) throws UDFInputSeriesDataTypeNotValidException {
    switch (dataType) {
      case INT32:
        return Integer.toString(getPercentile(intArrayList, phi));
      case INT64:
        return Long.toString(getPercentile(longArrayList, phi));
      case FLOAT:
        return Float.toString(getPercentile(floatArrayList, phi));
      case DOUBLE:
        return Double.toString(getPercentile(doubleArrayList, phi));
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }

  public static double getMedian(FloatArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return ((nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0);
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(FloatArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static float getPercentile(FloatArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(DoubleArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(DoubleArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static double getPercentile(DoubleArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(IntArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(IntArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static int getPercentile(IntArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(LongArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(LongArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static long getPercentile(LongArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }
}
