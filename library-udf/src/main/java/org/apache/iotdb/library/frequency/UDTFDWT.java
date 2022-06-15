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
package org.apache.iotdb.library.frequency;

import org.apache.iotdb.library.frequency.util.DWTUtil;
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

/**
 * This function calculates discrete wavelet transform of an input series. Input series must be an
 * integer exponent of 2. Input series is treated as sampled in equal distances.
 */
public class UDTFDWT implements UDTF {
  private ArrayList<Long> timestamp = new ArrayList<>();
  private ArrayList<Double> value = new ArrayList<>();
  private String s;
  private String method;
  private int layer;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x ->
                ((String) x).equalsIgnoreCase("Haar")
                    || ((String) x).equalsIgnoreCase("DB2")
                    || ((String) x).equalsIgnoreCase("DB4")
                    || ((String) x).equalsIgnoreCase("DB6")
                    || ((String) x).equalsIgnoreCase("DB8")
                    || ((String) x).equalsIgnoreCase(""),
            "Method not supported, please input coefficient and leave method blank.",
            validator.getParameters().getStringOrDefault("method", ""))
        .validate(
            x -> (int) x > 0,
            "layer has to be a positive integer.",
            validator.getParameters().getIntOrDefault("layer", 1));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    s = parameters.getStringOrDefault("coef", "");
    method = parameters.getStringOrDefault("method", "");
    layer = parameters.getIntOrDefault("layer", 1);
    timestamp.clear();
    value.clear();
  }

  @Override
  public void transform(Row row, PointCollector pointCollector) throws Exception {
    timestamp.add(row.getTime());
    value.add(Util.getValueAsDouble(row));
  }

  @Override
  public void terminate(PointCollector pointCollector) throws Exception {
    if (!s.equals("") || !method.equals("")) { // When user offers at least one parameter
      DWTUtil transformer = new DWTUtil(method, s, layer, value);
      transformer.waveletTransform();
      double[] r = transformer.getData();
      for (int i = 0; i < r.length; i++) {
        pointCollector.putDouble(timestamp.get(i), r[i]);
      }
    }
  }
}
