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
package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.library.dprofile.util.YuleWalker;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.math3.stat.StatUtils;

import java.util.ArrayList;
import java.util.Arrays;

/** This function solves Yule-Walker equation to calculate partial auto-correlation factor. */
public class UDTFPACF implements UDTF {
  ArrayList<Double> value = new ArrayList<>();
  ArrayList<Long> timestamp = new ArrayList<>();
  int lag;
  int n = 0;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    value.clear();
    timestamp.clear();
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    lag = parameters.getIntOrDefault("lag", -1);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double v = Util.getValueAsDouble(row);
    if (Double.isFinite(v)) {
      value.add(v);
    } else {
      value.add(0d);
    }
    timestamp.add(row.getTime());
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    String method = "adjusted";
    n = value.size();
    if (n > 1) {
      if (lag < 0 || lag > value.size() - 1) {
        lag = Math.min((int) (10 * Math.log10(value.size())), value.size() - 1);
      }
      double[] x =
          Arrays.stream(value.toArray(new Double[0])).mapToDouble(Double::valueOf).toArray();
      double xmean = StatUtils.mean(x);
      for (int i = 0; i < x.length; i++) {
        x[i] -= xmean;
      }
      collector.putDouble(timestamp.get(0), 1.0d);
      for (int k = 1; k <= lag; k++) {
        collector.putDouble(timestamp.get(k), new YuleWalker().yuleWalker(x, k, method, n));
      }
    }
  }
}
