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

package org.apache.iotdb.library.dmatch;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

/** This function finds symmetric patterns in a series according to DTW distance. */
public class UDTFPtnSym implements UDTF {

  private int window;
  private double threshold;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x > 0,
            "window has to be a positive integer.",
            validator.getParameters().getIntOrDefault("window", 10))
        .validate(
            x -> (double) x >= 0.0d,
            "threshold has to be non-negative.",
            validator.getParameters().getDoubleOrDefault("threshold", Double.MAX_VALUE));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    window = parameters.getIntOrDefault("window", 10);
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(window, 1))
        .setOutputDataType(Type.DOUBLE);
    threshold = parameters.getDoubleOrDefault("threshold", Double.MAX_VALUE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (rowWindow.windowSize() < window) { // skip too short series
      return;
    }
    DoubleArrayList a = new DoubleArrayList();
    int n = rowWindow.windowSize();
    long time = rowWindow.getRow(0).getTime();
    for (int i = 0; i < n; i++) {
      Row row = rowWindow.getRow(i);
      a.add(Util.getValueAsDouble(row, 0));
    }
    int m = a.size();
    double[][] dp = new double[m + 1][m + 1];
    for (int i = 1; i <= m; i++) {
      dp[i][i] = 0;
      if (i < m) {
        dp[i][i + 1] = Math.pow(Math.abs(a.get(i - 1) - a.get(i)), 2);
      }
    }
    for (int len = 3; len <= m; len++) {
      for (int i = 1, j = len; j <= m; j++) {
        dp[i][j] =
            Math.pow(Math.abs(a.get(0) - a.get(j - 1)), 2)
                + Math.min(Math.min(dp[i + 1][j], dp[i][j - 1]), dp[i + 1][j - 1]);
      }
    }
    if (dp[1][m] <= threshold) {
      collector.putDouble(time, dp[1][m]);
    }
  }
}
