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

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/*
This function is used to detect distance-based anomalies.
*/
public class UDTFOutlier implements UDTF {
  private int k;
  private double r;
  private int w;
  private int s;
  private int i;
  private ArrayList<Long> currentTimeWindow = new ArrayList<>();
  private ArrayList<Double> currentValueWindow = new ArrayList<>();
  private Map<Long, Double> outliers = new HashMap<>();

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x > 0,
            "Parameter k should be a positive integer.",
            validator.getParameters().getIntOrDefault("k", 3))
        .validate(
            x -> Double.isFinite((double) x) && (double) x >= 0,
            "Parameter r should be finite and non-negative.",
            validator.getParameters().getDoubleOrDefault("r", 5))
        .validate(
            x -> (int) x > 0,
            "Parameter w should be a positive integer.",
            validator.getParameters().getIntOrDefault("w", 1000))
        .validate(
            x -> (int) x > 0,
            "Parameter s should be a positive integer.",
            validator.getParameters().getIntOrDefault("s", 500));
  }

  @Override
  public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations)
      throws Exception {
    udtfConfigurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(Type.DOUBLE);
    this.k = udfParameters.getIntOrDefault("k", 3);
    this.r = udfParameters.getDoubleOrDefault("r", 5);
    this.w = udfParameters.getIntOrDefault("w", 1000);
    this.s = udfParameters.getIntOrDefault("s", 500);

    this.i = 0;
    currentTimeWindow.clear();
    currentValueWindow.clear();
    outliers.clear();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      return;
    }
    double v = Util.getValueAsDouble(row);
    if (!Double.isFinite(v)) {
      return;
    }
    if (i >= w && (i - w) % s == 0) {
      detect();
    }

    if (i >= w) {
      currentValueWindow.remove(0);
      currentTimeWindow.remove(0);
    }
    currentTimeWindow.add(row.getTime());
    currentValueWindow.add(v);
    i += 1;
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    for (Long time :
        outliers.keySet().stream().sorted(Comparator.naturalOrder()).collect(Collectors.toList())) {
      collector.putDouble(time, outliers.get(time));
    }
  }

  private void detect() {
    for (int j = 0; j < w; j++) {
      int cnt = 0;
      for (int l = 0; l < w; l++) {
        if (Math.abs(currentValueWindow.get(j) - currentValueWindow.get(l)) <= this.r) {
          cnt++;
        }
      }
      if (cnt < this.k && !outliers.keySet().contains(currentTimeWindow.get(j))) {
        outliers.put(currentTimeWindow.get(j), currentValueWindow.get(j));
      }
    }
  }
}
