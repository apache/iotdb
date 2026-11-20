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
  private static final int DEFAULT_ACCESS_WINDOW_SIZE = 10000;
  private static final int DEFAULT_SERIES_WINDOW_SIZE = 5;
  private String method = DEFAULT_METHOD;
  private int window;
  private final List<Long> seriesContextTimestamps = new ArrayList<>();
  private final List<Double> seriesContextValues = new ArrayList<>();
  private final List<SeriesVector> seriesNeighbors = new ArrayList<>();
  private final List<SeriesVector> pendingSeriesVectors = new ArrayList<>();
  private boolean hasProcessedSeriesVector;
  private long lastProcessedSeriesTimestamp;

  private double[] calculateLOFs(double[][] points, int length) {
    int neighborCount = Math.min(multipleK, length - 1);
    int[][] neighbors = new int[length][neighborCount];
    double[][] neighborDistances = new double[length][neighborCount];
    int[] heapSizes = new int[length];

    for (int i = 0; i < length; i++) {
      for (int j = i + 1; j < length; j++) {
        double distance = distance(points[i], points[j]);
        heapSizes[i] =
            addNearestNeighbor(neighbors[i], neighborDistances[i], heapSizes[i], j, distance);
        heapSizes[j] =
            addNearestNeighbor(neighbors[j], neighborDistances[j], heapSizes[j], i, distance);
      }
    }

    double[] kDistances = new double[length];
    for (int i = 0; i < length; i++) {
      kDistances[i] = neighborDistances[i][0];
    }

    double[] localDensities = new double[length];
    for (int i = 0; i < length; i++) {
      double reachabilityDistanceSum = 0;
      for (int j = 0; j < neighborCount; j++) {
        int neighbor = neighbors[i][j];
        reachabilityDistanceSum += Math.max(kDistances[neighbor], neighborDistances[i][j]);
      }
      localDensities[i] =
          reachabilityDistanceSum == 0
              ? Double.POSITIVE_INFINITY
              : neighborCount / reachabilityDistanceSum;
    }

    double[] lof = new double[length];
    for (int i = 0; i < length; i++) {
      double densityRatioSum = 0;
      for (int j = 0; j < neighborCount; j++) {
        densityRatioSum += densityRatio(localDensities[neighbors[i][j]], localDensities[i]);
      }
      lof[i] = densityRatioSum / neighborCount;
    }
    return lof;
  }

  private int addNearestNeighbor(
      int[] neighbors, double[] distances, int size, int candidate, double distance) {
    if (size < multipleK) {
      int index = size++;
      neighbors[index] = candidate;
      distances[index] = distance;
      while (index > 0) {
        int parent = (index - 1) / 2;
        if (distances[parent] >= distances[index]) {
          break;
        }
        swap(neighbors, distances, parent, index);
        index = parent;
      }
      return size;
    }

    if (distance >= distances[0]) {
      return size;
    }
    neighbors[0] = candidate;
    distances[0] = distance;
    int index = 0;
    while (true) {
      int left = index * 2 + 1;
      if (left >= size) {
        break;
      }
      int farther = left;
      int right = left + 1;
      if (right < size && distances[right] > distances[left]) {
        farther = right;
      }
      if (distances[index] >= distances[farther]) {
        break;
      }
      swap(neighbors, distances, index, farther);
      index = farther;
    }
    return size;
  }

  private static void swap(int[] neighbors, double[] distances, int left, int right) {
    int neighbor = neighbors[left];
    neighbors[left] = neighbors[right];
    neighbors[right] = neighbor;
    double distance = distances[left];
    distances[left] = distances[right];
    distances[right] = distance;
  }

  private static double densityRatio(double neighborDensity, double pointDensity) {
    if (neighborDensity == pointDensity) {
      return 1;
    }
    if (Double.isInfinite(pointDensity)) {
      return 0;
    }
    if (pointDensity == 0) {
      return Double.POSITIVE_INFINITY;
    }
    return neighborDensity / pointDensity;
  }

  private static double distance(double[] left, double[] right) {
    if (left.length == 1) {
      return Math.abs(left[0] - right[0]);
    }
    double distance = 0;
    for (int i = 0; i < left.length; i++) {
      distance = Math.hypot(distance, left[i] - right[i]);
    }
    return distance;
  }

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    String methodName = validator.getParameters().getStringOrDefault("method", DEFAULT_METHOD);
    int defaultWindowSize =
        METHOD_SERIES.equalsIgnoreCase(methodName)
            ? DEFAULT_SERIES_WINDOW_SIZE
            : DEFAULT_ACCESS_WINDOW_SIZE;
    validator.validateInputSeriesNumber(1, Integer.MAX_VALUE);
    for (int i = 0; i < validator.getParameters().getChildExpressionsSize(); i++) {
      validator.validateInputSeriesDataType(i, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
    }
    validator
        .validate(
            k -> (int) k > 0,
            LibraryUdfMessages.EXCEPTION_PARAMETER_K_SHOULD_BE_A_POSITIVE_INTEGER_62AC8458,
            validator.getParameters().getIntOrDefault("k", 3))
        .validate(
            window -> (int) window > 0,
            LibraryUdfMessages.EXCEPTION_PARAMETER_WINDOW_SHOULD_BE_A_POSITIVE_INTEGER_54FB7DBB,
            validator.getParameters().getIntOrDefault("window", defaultWindowSize))
        .validate(
            method -> isValidMethod((String) method),
            LibraryUdfMessages.EXCEPTION_METHOD_SHOULD_BE_DEFAULT_OR_SERIES_181EEECF,
            validator.getParameters().getStringOrDefault("method", DEFAULT_METHOD));
  }

  private static boolean isValidMethod(String method) {
    return DEFAULT_METHOD.equalsIgnoreCase(method) || METHOD_SERIES.equalsIgnoreCase(method);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    this.method = parameters.getStringOrDefault("method", DEFAULT_METHOD);
    this.multipleK = parameters.getIntOrDefault("k", 3);
    this.dim = parameters.getChildExpressionsSize();
    this.window = parameters.getIntOrDefault("window", DEFAULT_SERIES_WINDOW_SIZE);
    seriesContextTimestamps.clear();
    seriesContextValues.clear();
    seriesNeighbors.clear();
    pendingSeriesVectors.clear();
    hasProcessedSeriesVector = false;
    lastProcessedSeriesTimestamp = 0;
    long seriesAccessWindowSize = (long) this.window + this.multipleK;
    int accessWindowSize =
        this.method.equalsIgnoreCase(METHOD_SERIES)
            ? (int)
                Math.min(
                    Integer.MAX_VALUE, Math.max(DEFAULT_ACCESS_WINDOW_SIZE, seriesAccessWindowSize))
            : parameters.getIntOrDefault("window", DEFAULT_ACCESS_WINDOW_SIZE);
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(accessWindowSize))
        .setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (this.method.equalsIgnoreCase(DEFAULT_METHOD)) {
      int size = 0;
      double[][] knn = new double[rowWindow.windowSize()][dim];
      long[] timestamp = new long[rowWindow.windowSize()];
      int row = 0;
      while (row < rowWindow.windowSize()) {
        double[] values = new double[dim];
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
        double[] lof = calculateLOFs(knn, size);
        for (int m = 0; m < size; m++) {
          try {
            collector.putDouble(timestamp[m], lof[m]);
          } catch (Exception e) {
            throw new UDFException(LibraryUdfMessages.FAIL_TO_GET_LOF + m, e);
          }
        }
      }
    } else if (this.method.equalsIgnoreCase(METHOD_SERIES)) {
      List<Long> timestamps = new ArrayList<>(seriesContextTimestamps);
      List<Double> values = new ArrayList<>(seriesContextValues);
      for (int row = 0; row < rowWindow.windowSize(); row++) {
        if (!rowWindow.getRow(row).isNull(0)) {
          double value = Util.getValueAsDouble(rowWindow.getRow(row), 0);
          if (Double.isFinite(value)) {
            timestamps.add(rowWindow.getRow(row).getTime());
            values.add(value);
          }
        }
      }

      int vectorCount = values.size() - window + 1;
      for (int i = 0; i < vectorCount; i++) {
        long timestamp = timestamps.get(i);
        if (!hasProcessedSeriesVector || timestamp > lastProcessedSeriesTimestamp) {
          double[] vector = new double[window];
          for (int p = 0; p < window; p++) {
            vector[p] = values.get(i + p);
          }
          pendingSeriesVectors.add(new SeriesVector(timestamp, vector));
          hasProcessedSeriesVector = true;
          lastProcessedSeriesTimestamp = timestamp;
        }
      }

      if (seriesNeighbors.size() + pendingSeriesVectors.size() > multipleK) {
        List<SeriesVector> candidates = new ArrayList<>(seriesNeighbors);
        candidates.addAll(pendingSeriesVectors);
        double[][] knn = new double[candidates.size()][window];
        for (int i = 0; i < candidates.size(); i++) {
          knn[i] = candidates.get(i).values;
        }
        double[] lofValues = calculateLOFs(knn, candidates.size());
        for (int i = 0; i < pendingSeriesVectors.size(); i++) {
          SeriesVector vector = pendingSeriesVectors.get(i);
          try {
            double lof = lofValues[seriesNeighbors.size() + i];
            collector.putDouble(vector.timestamp, lof);
          } catch (Exception e) {
            throw new UDFException(LibraryUdfMessages.FAIL_TO_GET_LOF + vector.timestamp, e);
          }
        }

        seriesNeighbors.clear();
        int neighborCount = Math.min(multipleK, candidates.size());
        for (int i = candidates.size() - neighborCount; i < candidates.size(); i++) {
          seriesNeighbors.add(candidates.get(i));
        }
        pendingSeriesVectors.clear();
      }

      seriesContextTimestamps.clear();
      seriesContextValues.clear();
      int contextStart = Math.max(0, values.size() - (window - 1));
      for (int i = contextStart; i < values.size(); i++) {
        seriesContextTimestamps.add(timestamps.get(i));
        seriesContextValues.add(values.get(i));
      }
    }
  }

  private static class SeriesVector {
    private final long timestamp;
    private final double[] values;

    private SeriesVector(long timestamp, double[] values) {
      this.timestamp = timestamp;
      this.values = values;
    }
  }

  @Override
  public void terminate(PointCollector collector)
      throws Exception { // default implementation ignored
  }
}
