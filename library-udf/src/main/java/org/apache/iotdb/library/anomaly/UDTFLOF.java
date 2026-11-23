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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * This function detects density anomalies in time series. In series mode, each new vector is
 * evaluated with at most the preceding 2k vectors and itself.
 */
public class UDTFLOF implements UDTF {
  private static final long MAX_LOF_WORKING_MEMORY_BYTES = 64L * 1024 * 1024;
  private static final long MAX_LOF_DISTANCE_COORDINATE_OPERATIONS_PER_ACCESS = 64L * 1024 * 1024;
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
  private final Deque<SeriesVector> seriesHistory = new ArrayDeque<>();
  private boolean hasProcessedSeriesVector;
  private boolean hasInitializedSeries;
  private long lastProcessedSeriesTimestamp;

  private double[] calculateLOFs(double[][] points, int length) throws UDFException {
    int neighborCount = Math.min(multipleK, length - 1);
    ensureLOFWorkingMemory(length, points[0].length);

    long neighborEntries = (long) length * neighborCount;
    int[] neighbors = new int[(int) neighborEntries];
    double[] neighborDistances = new double[(int) neighborEntries];
    int[] heapSizes = new int[length];

    for (int i = 0; i < length; i++) {
      for (int j = i + 1; j < length; j++) {
        double distance = distance(points[i], points[j]);
        int iOffset = i * neighborCount;
        int jOffset = j * neighborCount;
        heapSizes[i] =
            addNearestNeighbor(
                neighbors, neighborDistances, iOffset, neighborCount, heapSizes[i], j, distance);
        heapSizes[j] =
            addNearestNeighbor(
                neighbors, neighborDistances, jOffset, neighborCount, heapSizes[j], i, distance);
      }
    }

    double[] kDistances = new double[length];
    for (int i = 0; i < length; i++) {
      kDistances[i] = neighborDistances[i * neighborCount];
    }

    double[] localDensities = new double[length];
    for (int i = 0; i < length; i++) {
      double reachabilityDistanceSum = 0;
      for (int j = 0; j < neighborCount; j++) {
        int neighborOffset = i * neighborCount + j;
        int neighbor = neighbors[neighborOffset];
        reachabilityDistanceSum +=
            Math.max(kDistances[neighbor], neighborDistances[neighborOffset]);
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
        densityRatioSum +=
            densityRatio(localDensities[neighbors[i * neighborCount + j]], localDensities[i]);
      }
      lof[i] = densityRatioSum / neighborCount;
    }
    return lof;
  }

  private void ensureLOFWorkingMemory(int length, int dimensions) throws UDFException {
    long bytesPerPoint = (long) dimensions * Double.BYTES + 128L;
    if (bytesPerPoint <= 0 || length > MAX_LOF_WORKING_MEMORY_BYTES / bytesPerPoint) {
      throw new UDFException(
          LibraryUdfMessages.EXCEPTION_LOF_WORKING_MEMORY_EXCEEDS_THE_64_MIB_LIMIT_A21CB57F);
    }

    long workingMemoryBytes = length * bytesPerPoint;
    long neighborEntries = (long) length * Math.min(multipleK, length - 1);
    if (neighborEntries
        > (MAX_LOF_WORKING_MEMORY_BYTES - workingMemoryBytes)
            / (Integer.BYTES + (long) Double.BYTES)) {
      throw new UDFException(
          LibraryUdfMessages.EXCEPTION_LOF_WORKING_MEMORY_EXCEEDS_THE_64_MIB_LIMIT_A21CB57F);
    }
  }

  private void ensureSeriesHistoryMemory(int historySize) throws UDFException {
    long bytesPerVector = (long) window * Double.BYTES + 128L;
    if (historySize > MAX_LOF_WORKING_MEMORY_BYTES / bytesPerVector) {
      throw new UDFException(
          LibraryUdfMessages.EXCEPTION_LOF_WORKING_MEMORY_EXCEEDS_THE_64_MIB_LIMIT_A21CB57F);
    }
  }

  private long estimateDistanceCoordinateOperations(int length, int dimensions) {
    long pairCount = (long) length * (length - 1L) / 2;
    long exceededLimit = MAX_LOF_DISTANCE_COORDINATE_OPERATIONS_PER_ACCESS + 1;
    if (dimensions > 0 && pairCount > exceededLimit / dimensions) {
      return exceededLimit;
    }
    return pairCount * dimensions;
  }

  private void validateSeriesBatchWork(List<Long> timestamps, int vectorCount) throws UDFException {
    long distanceCoordinateOperations = 0;
    int historySize = seriesHistory.size();
    int maximumHistorySize = (int) Math.min(2L * multipleK, Integer.MAX_VALUE);
    boolean initialized = hasInitializedSeries;
    boolean processed = hasProcessedSeriesVector;
    long lastTimestamp = lastProcessedSeriesTimestamp;

    for (int i = 0; i < vectorCount; i++) {
      long timestamp = timestamps.get(i);
      if (processed && timestamp <= lastTimestamp) {
        continue;
      }
      processed = true;
      lastTimestamp = timestamp;

      int candidateCount = 0;
      boolean wasInitialized = initialized;
      if (!initialized) {
        historySize++;
        if (historySize > multipleK) {
          candidateCount = historySize;
          initialized = true;
        } else {
          ensureSeriesHistoryMemory(historySize);
        }
      } else {
        candidateCount = historySize + 1;
      }

      if (candidateCount > 0) {
        ensureLOFWorkingMemory(candidateCount, window);
        long work = estimateDistanceCoordinateOperations(candidateCount, window);
        if (work
            > MAX_LOF_DISTANCE_COORDINATE_OPERATIONS_PER_ACCESS - distanceCoordinateOperations) {
          throw new UDFException(
              LibraryUdfMessages
                  .EXCEPTION_LOF_SERIES_CALCULATION_EXCEEDS_THE_64_MI_DISTANCE_COORDINATE_OPERATION_LIMIT_PER_ACCESS_WINDOW_9A1E82AD);
        }
        distanceCoordinateOperations += work;
      }

      if (initialized && wasInitialized) {
        historySize = Math.min(historySize + 1, maximumHistorySize);
      }
    }
  }

  private int addNearestNeighbor(
      int[] neighbors,
      double[] distances,
      int rowOffset,
      int capacity,
      int size,
      int candidate,
      double distance) {
    if (size < capacity) {
      int index = size++;
      neighbors[rowOffset + index] = candidate;
      distances[rowOffset + index] = distance;
      while (index > 0) {
        int parent = (index - 1) / 2;
        if (distances[rowOffset + parent] >= distances[rowOffset + index]) {
          break;
        }
        swap(neighbors, distances, rowOffset, parent, index);
        index = parent;
      }
      return size;
    }

    if (distance >= distances[rowOffset]) {
      return size;
    }
    neighbors[rowOffset] = candidate;
    distances[rowOffset] = distance;
    int index = 0;
    while (true) {
      int left = index * 2 + 1;
      if (left >= size) {
        break;
      }
      int farther = left;
      int right = left + 1;
      if (right < size && distances[rowOffset + right] > distances[rowOffset + left]) {
        farther = right;
      }
      if (distances[rowOffset + index] >= distances[rowOffset + farther]) {
        break;
      }
      swap(neighbors, distances, rowOffset, index, farther);
      index = farther;
    }
    return size;
  }

  private static void swap(
      int[] neighbors, double[] distances, int rowOffset, int left, int right) {
    int leftOffset = rowOffset + left;
    int rightOffset = rowOffset + right;
    int neighbor = neighbors[leftOffset];
    neighbors[leftOffset] = neighbors[rightOffset];
    neighbors[rightOffset] = neighbor;
    double distance = distances[leftOffset];
    distances[leftOffset] = distances[rightOffset];
    distances[rightOffset] = distance;
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
    seriesHistory.clear();
    hasProcessedSeriesVector = false;
    hasInitializedSeries = false;
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
      double[][] knn = new double[rowWindow.windowSize()][];
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
      validateSeriesBatchWork(timestamps, vectorCount);
      for (int i = 0; i < vectorCount; i++) {
        long timestamp = timestamps.get(i);
        if (!hasProcessedSeriesVector || timestamp > lastProcessedSeriesTimestamp) {
          double[] vector = new double[window];
          for (int p = 0; p < window; p++) {
            vector[p] = values.get(i + p);
          }
          hasProcessedSeriesVector = true;
          lastProcessedSeriesTimestamp = timestamp;
          processSeriesVector(new SeriesVector(timestamp, vector), collector);
        }
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

  private void processSeriesVector(SeriesVector vector, PointCollector collector) throws Exception {
    if (!hasInitializedSeries) {
      seriesHistory.addLast(vector);
      if (seriesHistory.size() > multipleK) {
        List<SeriesVector> initialVectors = new ArrayList<>(seriesHistory);
        double[][] initialPoints = new double[initialVectors.size()][];
        for (int i = 0; i < initialVectors.size(); i++) {
          initialPoints[i] = initialVectors.get(i).values;
        }
        double[] lofValues = calculateLOFs(initialPoints, initialVectors.size());
        for (int i = 0; i < initialVectors.size(); i++) {
          SeriesVector initialVector = initialVectors.get(i);
          try {
            collector.putDouble(initialVector.timestamp, lofValues[i]);
          } catch (Exception e) {
            throw new UDFException(LibraryUdfMessages.FAIL_TO_GET_LOF + initialVector.timestamp, e);
          }
        }
        hasInitializedSeries = true;
      }
      return;
    }

    List<SeriesVector> candidates = new ArrayList<>(seriesHistory);
    candidates.add(vector);
    double[][] points = new double[candidates.size()][];
    for (int i = 0; i < candidates.size(); i++) {
      points[i] = candidates.get(i).values;
    }
    double[] lofValues = calculateLOFs(points, candidates.size());
    try {
      collector.putDouble(vector.timestamp, lofValues[lofValues.length - 1]);
    } catch (Exception e) {
      throw new UDFException(LibraryUdfMessages.FAIL_TO_GET_LOF + vector.timestamp, e);
    }

    seriesHistory.addLast(vector);
    int maximumHistorySize = (int) Math.min(2L * multipleK, Integer.MAX_VALUE);
    while (seriesHistory.size() > maximumHistorySize) {
      seriesHistory.removeFirst();
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
