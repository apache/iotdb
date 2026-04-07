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

package org.apache.iotdb.library.dlearn;

import org.apache.iotdb.library.dlearn.util.cluster.KMeans;
import org.apache.iotdb.library.dlearn.util.cluster.KShape;
import org.apache.iotdb.library.dlearn.util.cluster.MedoidShape;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Clusters a time series by partitioning it into non-overlapping subsequences of length l.
 * Parameters: l, k, method (default kmeans), norm, maxiter, output; medoidshape also uses
 * sample_rate (greedy sampling ratio; use 1 when the window count is small). Requires at least k
 * windows.
 */
public class UDTFCluster implements UDTF {

  private static final String METHOD_KMEANS = "kmeans";
  private static final String METHOD_KSHAPE = "kshape";
  private static final String METHOD_MEDOIDSHAPE = "medoidshape";

  private static final String OUTPUT_LABEL = "label";
  private static final String OUTPUT_CENTROID = "centroid";

  private static final int DEFAULT_MAX_ITER = 200;
  private static final double DEFAULT_SAMPLE_RATE = 0.3;
  private static final String DEFAULT_METHOD = METHOD_KMEANS;

  private int l;
  private int k;
  private String method;
  private boolean norm;
  private int maxIter;
  private String output;
  private double sampleRate;

  private final List<Long> timestamps = new ArrayList<>();
  private final List<Double> values = new ArrayList<>();

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x > 0,
            "Parameter l must be a positive integer.",
            validator.getParameters().getInt("l"))
        .validate(
            x -> (int) x >= 2,
            "Parameter k must be at least 2.",
            validator.getParameters().getInt("k"))
        .validate(
            x -> {
              String m = ((String) x).toLowerCase();
              return METHOD_KMEANS.equals(m)
                  || METHOD_KSHAPE.equals(m)
                  || METHOD_MEDOIDSHAPE.equals(m);
            },
            "Parameter method must be one of: kmeans, kshape, medoidshape.",
            validator.getParameters().getStringOrDefault("method", DEFAULT_METHOD))
        .validate(
            x -> (int) x >= 1,
            "Parameter maxiter must be a positive integer.",
            validator.getParameters().getIntOrDefault("maxiter", DEFAULT_MAX_ITER))
        .validate(
            x -> {
              String o = ((String) x).toLowerCase();
              return OUTPUT_LABEL.equals(o) || OUTPUT_CENTROID.equals(o);
            },
            "Parameter output must be label or centroid.",
            validator.getParameters().getStringOrDefault("output", OUTPUT_LABEL))
        .validate(
            x -> {
              double d = ((Number) x).doubleValue();
              return d > 0 && d <= 1.0;
            },
            "Parameter sample_rate must be in (0, 1].",
            validator.getParameters().getDoubleOrDefault("sample_rate", DEFAULT_SAMPLE_RATE));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    this.output = parameters.getStringOrDefault("output", OUTPUT_LABEL).toLowerCase();
    if (OUTPUT_CENTROID.equals(output)) {
      configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    } else {
      configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.INT32);
    }
    this.l = parameters.getInt("l");
    this.k = parameters.getInt("k");
    this.method = parameters.getStringOrDefault("method", DEFAULT_METHOD).toLowerCase();
    this.norm = parameters.getBooleanOrDefault("norm", true);
    this.maxIter = parameters.getIntOrDefault("maxiter", DEFAULT_MAX_ITER);
    this.sampleRate = parameters.getDoubleOrDefault("sample_rate", DEFAULT_SAMPLE_RATE);
    timestamps.clear();
    values.clear();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!row.isNull(0)) {
      timestamps.add(row.getTime());
      values.add(Util.getValueAsDouble(row));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    int n = values.size();
    if (n < l) {
      throw new UDFException(
          "Time series length must be at least l; got " + n + " points, l=" + l + ".");
    }
    int numWindows = n / l;
    if (numWindows < k) {
      throw new UDFException(
          "Not enough non-overlapping windows: got "
              + numWindows
              + " windows, need at least k="
              + k
              + ".");
    }

    double[][] windows = new double[numWindows][l];
    long[] windowStartTime = new long[numWindows];
    for (int w = 0; w < numWindows; w++) {
      windowStartTime[w] = timestamps.get(w * l);
      for (int j = 0; j < l; j++) {
        windows[w][j] = values.get(w * l + j);
      }
    }

    if (OUTPUT_LABEL.equals(output)) {
      int[] labels;
      if (METHOD_KMEANS.equals(method)) {
        KMeans km = new KMeans();
        km.fit(windows, k, norm, maxIter);
        labels = km.getLabels();
      } else if (METHOD_KSHAPE.equals(method)) {
        KShape ks = new KShape();
        ks.fit(windows, k, norm, maxIter);
        labels = ks.getLabels();
      } else if (METHOD_MEDOIDSHAPE.equals(method)) {
        MedoidShape ms = new MedoidShape();
        ms.setSampleRate(sampleRate);
        ms.fit(windows, k, norm, maxIter);
        labels = ms.getLabels();
      } else {
        throw new UDFException("Unsupported method: " + method);
      }
      for (int w = 0; w < numWindows; w++) {
        collector.putInt(windowStartTime[w], labels[w]);
      }
    } else {
      double[][] centroids;
      if (METHOD_KMEANS.equals(method)) {
        KMeans km = new KMeans();
        km.fit(windows, k, norm, maxIter);
        centroids = km.getCentroids();
      } else if (METHOD_KSHAPE.equals(method)) {
        KShape ks = new KShape();
        ks.fit(windows, k, norm, maxIter);
        centroids = ks.getCentroids();
      } else if (METHOD_MEDOIDSHAPE.equals(method)) {
        MedoidShape ms = new MedoidShape();
        ms.setSampleRate(sampleRate);
        ms.fit(windows, k, norm, maxIter);
        centroids = ms.getCentroids();
      } else {
        throw new UDFException("Unsupported method: " + method);
      }
      emitConcatenatedCentroids(collector, centroids);
    }
  }

  private static void emitConcatenatedCentroids(PointCollector collector, double[][] centroids)
      throws Exception {
    long t = 0L;
    for (double[] row : centroids) {
      for (double v : row) {
        collector.putDouble(t++, v);
      }
    }
  }
}
