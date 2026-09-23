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

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.library.util.TypeServices;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.read.common.type.service.TypeService;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.FloatArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Util for computing median, MAD, percentile.
 *
 * <p>Percentile / quantile ({@link #getPercentile}) uses <b>discrete nearest-rank</b>: for sorted
 * size {@code n} and {@code phi} in (0, 1], take 1-based rank {@code k = ceil(n * phi)} and 0-based
 * index {@code k - 1}, clamped to {@code [0, n - 1]}. No interpolation; {@code phi = 0.5} is not
 * required to match {@link #getMedian}.
 */
public class ExactOrderStatistics {

  private final Type dataType;
  private FloatArrayList floatArrayList;
  private DoubleArrayList doubleArrayList;
  private IntArrayList intArrayList;
  private LongArrayList longArrayList;
  private final StatisticsOperations operations;

  public ExactOrderStatistics(Type type) throws UDFInputSeriesDataTypeNotValidException {
    this.dataType = type;
    try {
      operations = OPERATIONS_SERVICE.call(TypeServices.toReadType(type)).apply(this);
    } catch (IllegalArgumentException e) {
      throw new UDFInputSeriesDataTypeNotValidException(
          0, dataType, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
    }
  }

  public void insert(Row row) throws UDFInputSeriesDataTypeNotValidException, IOException {
    operations.insert(row);
  }

  public double getMedian() throws UDFInputSeriesDataTypeNotValidException {
    return operations.median();
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

  public double getMad() throws UDFInputSeriesDataTypeNotValidException {
    return operations.mad();
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

  /** Discrete nearest-rank index into sorted data of length {@code n}; see class Javadoc. */
  private static int discreteNearestRankIndex(int n, double phi) {
    int idx = (int) Math.ceil(n * phi) - 1;
    return Math.max(0, Math.min(n - 1, idx));
  }

  public static float getPercentile(FloatArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get(discreteNearestRankIndex(nums.size(), phi));
    }
  }

  public static double getPercentile(DoubleArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get(discreteNearestRankIndex(nums.size(), phi));
    }
  }

  public String getPercentile(double phi) throws UDFInputSeriesDataTypeNotValidException {
    return operations.percentile(phi);
  }

  private static final TypeService<Function<ExactOrderStatistics, StatisticsOperations>>
      OPERATIONS_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32 ->
                    target -> {
                      target.intArrayList = new IntArrayList();
                      return new StatisticsOperations() {
                        public void insert(Row row) throws IOException {
                          target.intArrayList.add(row.getInt(0));
                        }

                        public double median() {
                          return getMedian(target.intArrayList);
                        }

                        public double mad() {
                          return getMad(target.intArrayList);
                        }

                        public String percentile(double phi) {
                          return Integer.toString(getPercentile(target.intArrayList, phi));
                        }
                      };
                    };
                case INT64 ->
                    target -> {
                      target.longArrayList = new LongArrayList();
                      return new StatisticsOperations() {
                        public void insert(Row row) throws IOException {
                          target.longArrayList.add(row.getLong(0));
                        }

                        public double median() {
                          return getMedian(target.longArrayList);
                        }

                        public double mad() {
                          return getMad(target.longArrayList);
                        }

                        public String percentile(double phi) {
                          return Long.toString(getPercentile(target.longArrayList, phi));
                        }
                      };
                    };
                case FLOAT ->
                    target -> {
                      target.floatArrayList = new FloatArrayList();
                      return new StatisticsOperations() {
                        public void insert(Row row) throws IOException {
                          float value = row.getFloat(0);
                          if (Float.isFinite(value)) {
                            target.floatArrayList.add(value);
                          }
                        }

                        public double median() {
                          return getMedian(target.floatArrayList);
                        }

                        public double mad() {
                          return getMad(target.floatArrayList);
                        }

                        public String percentile(double phi) {
                          return Float.toString(getPercentile(target.floatArrayList, phi));
                        }
                      };
                    };
                case DOUBLE ->
                    target -> {
                      target.doubleArrayList = new DoubleArrayList();
                      return new StatisticsOperations() {
                        public void insert(Row row) throws IOException {
                          double value = row.getDouble(0);
                          if (Double.isFinite(value)) {
                            target.doubleArrayList.add(value);
                          }
                        }

                        public double median() {
                          return getMedian(target.doubleArrayList);
                        }

                        public double mad() {
                          return getMad(target.doubleArrayList);
                        }

                        public String percentile(double phi) {
                          return Double.toString(getPercentile(target.doubleArrayList, phi));
                        }
                      };
                    };
                case BOOLEAN, TEXT, ROW, UNKNOWN, TIMESTAMP, DATE, BLOB, STRING, OBJECT, VECTOR ->
                    target -> {
                      throw new IllegalArgumentException(
                          String.format(
                              LibraryUdfMessages.EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E,
                              type));
                    };
              };

  private interface StatisticsOperations {
    void insert(Row row) throws IOException;

    double median();

    double mad();

    String percentile(double phi);
  }

  public static int getPercentile(IntArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get(discreteNearestRankIndex(nums.size(), phi));
    }
  }

  public static long getPercentile(LongArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get(discreteNearestRankIndex(nums.size(), phi));
    }
  }
}
