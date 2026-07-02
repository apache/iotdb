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

package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.List;

/** This function is used to detect density anomaly of time series. */
public class UDTFLOF implements UDTF {
  private int multipleK;
  private int dim;
  private static final String DEFAULT_METHOD = "default";
  private static final String METHOD_SERIES = "series";
  private String method = DEFAULT_METHOD;
  private int window;

  int partition(Double[][] a, int left, int right) {
    Double key = a[left][1];
    Double key2 = a[left][0];
    while (left < right) {
      while (left < right && a[right][1] >= key) {
        right--;
      }
      if (left < right) {
        a[left][0] = a[right][0];
        a[left][1] = a[right][1];
      }
      while (left < right && a[left][1] <= key) {
        left++;
      }
      if (left < right) {
        a[right][0] = a[left][0];
        a[right][1] = a[left][1];
      }
    }
    a[left][0] = key2;
    a[left][1] = key;
    return left;
  }

  Double findKthNum(Double[][] a, int left, int right, int k) {
    int index = partition(a, left, right);
    if (index + 1 == k) {
      return a[index][0];
    } else if (index + 1 < k) {
      return findKthNum(a, index + 1, right, k);
    } else {
      return findKthNum(a, left, index - 1, k);
    }
  }

  public double getLOF(Double[][] knn, Double[] x, int length) {
    double sum = 0;
    for (int i = 0; i < length; i++) {
      Double[] o = knn[i];
      sum += getLocDens(knn, o, length) / getLocDens(knn, x, length);
    }
    return sum / multipleK;
  }

  public double getLocDens(Double[][] knn, Double[] x, int length) {
    Double[] nnk = findKthPoint(knn, x, length);

    double sum = 0;
    for (int i = 0; i < length; i++) {
      Double[] o = knn[i];
      sum += reachDist(o, x, nnk);
    }
    return sum / multipleK;
  }

  public Double[] findKthPoint(Double[][] knn, Double[] x, int length) {
    int index;
    Double[][] d = new Double[length][2];
    for (int i = 0; i < length; i++) {
      d[i][0] = (double) i;
      d[i][1] = dist(knn[i], x);
    }
    index = (int) (double) (findKthNum(d, 0, length - 1, multipleK + 1));
    return knn[index];
  }

  public double reachDist(Double[] o, Double[] x, Double[] nnk) {
    return Math.max(dist(o, x), dist(nnk, x));
  }

  private double dist(Double[] nnk, Double[] x) {
    double sum = 0;
    for (int i = 0; i < nnk.length; i++) {
      sum += (nnk[i] - x[i]) * (nnk[i] - x[i]);
    }
    return Math.sqrt(sum);
  }

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesNumber(1, Integer.MAX_VALUE);
    for (int i = 0; i < validator.getParameters().getChildExpressionsSize(); i++) {
      validator.validateInputSeriesDataType(i, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
    }
    validator
        .validate(
            k -> (int) k > 0,
            "Parameter k should be a positive integer.",
            validator.getParameters().getIntOrDefault("k", 3))
        .validate(
            window -> (int) window > 0,
            "Parameter window should be a positive integer.",
            validator.getParameters().getIntOrDefault("window", 10000))
        .validate(
            method -> isValidMethod((String) method),
            "Method should be default or series.",
            validator.getParameters().getStringOrDefault("method", DEFAULT_METHOD));
  }

  private static boolean isValidMethod(String method) {
    return DEFAULT_METHOD.equalsIgnoreCase(method) || METHOD_SERIES.equalsIgnoreCase(method);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(
            new SlidingSizeWindowAccessStrategy(parameters.getIntOrDefault("window", 10000)))
        .setOutputDataType(Type.DOUBLE);
    this.multipleK = parameters.getIntOrDefault("k", 3);
    this.dim = parameters.getChildExpressionsSize();
    this.method = parameters.getStringOrDefault("method", DEFAULT_METHOD);
    this.window = parameters.getIntOrDefault("window", 5);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (this.method.equalsIgnoreCase(DEFAULT_METHOD)) {
      int size = 0;
      Double[][] knn = new Double[rowWindow.windowSize()][dim];
      long[] timestamp = new long[rowWindow.windowSize()];
      int row = 0;
      while (row < rowWindow.windowSize()) {
        Double[] values = new Double[dim];
        boolean valid = true;
        for (int j = 0; j < dim; j++) {
          if (rowWindow.getRow(row).isNull(j)) {
            valid = false;
            break;
          }
          double value = Util.getValueAsDouble(rowWindow.getRow(row), j);
          if (!Double.isFinite(value)) {
            valid = false;
            break;
          }
          values[j] = value;
        }
        if (valid) {
          timestamp[size] = rowWindow.getRow(row).getTime();
          knn[size] = values;
          size++;
        }
        row++;
      }
      if (size > multipleK) {
        double[] lof = new double[size];
        for (int m = 0; m < size; m++) {
          try {
            lof[m] = getLOF(knn, knn[m], size);
            collector.putDouble(timestamp[m], lof[m]);
          } catch (Exception e) {
            throw new UDFException(LibraryUdfMessages.FAIL_TO_GET_LOF + m, e);
          }
        }
      }
    } else if (this.method.equalsIgnoreCase(METHOD_SERIES)) {
      int size = rowWindow.windowSize() - window + 1;
      if (size > 0) {
        List<Long> timestamp = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        int row = 0;
        while (row < rowWindow.windowSize()) {
          if (!rowWindow.getRow(row).isNull(0)) {
            double value = Util.getValueAsDouble(rowWindow.getRow(row), 0);
            if (Double.isFinite(value)) {
              timestamp.add(rowWindow.getRow(row).getTime());
              values.add(value);
            }
          }
          row++;
        }
        size = values.size() - window + 1;
        if (size > multipleK) {
          Double[][] knn = new Double[size][window];
          for (int i = 0; i < size; i++) {
            for (int p = 0; p < window; p++) {
              knn[i][p] = values.get(i + p);
            }
          }
          double[] lof = new double[size];
          for (int m = 0; m < size; m++) {
            try {
              lof[m] = getLOF(knn, knn[m], size);
              collector.putDouble(timestamp.get(m), lof[m]);
            } catch (Exception e) {
              throw new UDFException(LibraryUdfMessages.FAIL_TO_GET_LOF + m, e);
            }
          }
        }
      }
    }
  }

  @Override
  public void terminate(PointCollector collector)
      throws Exception { // default implementation ignored
  }
}
