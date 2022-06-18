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

import com.google.common.math.Quantiles;

import java.util.ArrayList;

/*
This function is used to detect anomalies based on IQR.
Stream swap require user to provide Q1 and Q3, while global swap does not.
*/
public class UDTFIQR implements UDTF {
  ArrayList<Double> value = new ArrayList<>();
  ArrayList<Long> timestamp = new ArrayList<>();
  String compute = "batch";
  double q1 = 0.0d;
  double q3 = 0.0d;
  double iqr = 0.0d;

  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> ((String) x).equalsIgnoreCase("batch") || ((String) x).equalsIgnoreCase("stream"),
            "Parameter \"compute\" is illegal. Please use \"batch\" (for default) or \"stream\".",
            validator.getParameters().getStringOrDefault("compute", "batch"))
        .validate(
            params -> (double) params[0] < (double) params[1],
            "parameter $q1$ should be smaller than $q3$",
            validator.getParameters().getDoubleOrDefault("q1", -1),
            validator.getParameters().getDoubleOrDefault("q3", 1));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    value.clear();
    timestamp.clear();
    q1 = 0.0d;
    q3 = 0.0d;
    iqr = 0.0d;
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    compute = parameters.getStringOrDefault("compute", "batch");
    if (compute.equalsIgnoreCase("stream")) {
      q1 = parameters.getDouble("q1");
      q3 = parameters.getDouble("q3");
      iqr = q3 - q1;
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (compute.equalsIgnoreCase("stream") && q3 > q1) {
      double v = Util.getValueAsDouble(row);
      if (v < q1 - 1.5 * iqr || v > q3 + 1.5 * iqr) {
        collector.putDouble(row.getTime(), v);
      }
    } else if (compute.equalsIgnoreCase("batch")) {
      double v = Util.getValueAsDouble(row);
      value.add(v);
      timestamp.add(row.getTime());
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (compute.equalsIgnoreCase("batch")) {
      q1 = Quantiles.quartiles().index(1).compute(value);
      q3 = Quantiles.quartiles().index(3).compute(value);
      iqr = q3 - q1;
    }
    for (int i = 0; i < value.size(); i++) {
      double v = value.get(i);
      if (v < q1 - 1.5 * iqr || v > q3 + 1.5 * iqr) {
        collector.putDouble(timestamp.get(i), v);
      }
    }
  }
}
