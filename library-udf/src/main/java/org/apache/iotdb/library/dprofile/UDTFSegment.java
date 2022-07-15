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

import org.apache.iotdb.library.dprofile.util.Segment;
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

/** This function segment input series into linear parts. */
public class UDTFSegment implements UDTF {

  private int windowSize;
  private double maxError;
  private String method;
  private String output;
  private static final ArrayList<Long> timestamp = new ArrayList<>();
  private static final ArrayList<Double> value = new ArrayList<>();

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x > 0,
            "Window size should be a positive integer.",
            validator.getParameters().getIntOrDefault("window", 10))
        .validate(
            x -> (double) x >= 0,
            "Error bound should be no less than 0.",
            validator.getParameters().getDoubleOrDefault("error", 0.1))
        .validate(
            x ->
                ((String) x).equalsIgnoreCase("bottom-up") || ((String) x).equalsIgnoreCase("swab"),
            "Method is illegal.",
            validator.getParameters().getStringOrDefault("method", "bottom-up"))
        .validate(
            x -> ((String) x).equalsIgnoreCase("first") || ((String) x).equalsIgnoreCase("all"),
            "Output type is invalid.",
            validator.getParameters().getStringOrDefault("output", "first"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    timestamp.clear();
    value.clear();
    this.windowSize = parameters.getIntOrDefault("window", 10);
    this.maxError = parameters.getDoubleOrDefault("error", 0.1);
    this.method = parameters.getStringOrDefault("method", "bottom-up");
    this.method = this.method.toLowerCase();
    this.output = parameters.getStringOrDefault("output", "first");
    this.output = this.output.toLowerCase();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double v = Util.getValueAsDouble(row);
    if (Double.isFinite(v)) {
      timestamp.add(row.getTime());
      value.add(Util.getValueAsDouble(row));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    long[] ts = timestamp.stream().mapToLong(Long::valueOf).toArray();
    double[] v = value.stream().mapToDouble(Double::valueOf).toArray();
    ArrayList<double[]> seg = new ArrayList<>();
    if (method.equals("bottom-up")) {
      ArrayList<double[]> temp = Segment.bottom_up(v, maxError);
      seg.addAll(temp);
    } else if (method.equals("swab")) { // haven't tested yet
      seg = Segment.swab_alg(v, ts, maxError, windowSize);
    }
    ArrayList<double[]> res = new ArrayList<>();
    for (double[] doubles : seg) {
      res.add(Segment.approximated_segment(doubles));
    }
    int index = 0;
    if (output.equals("first")) {
      for (double[] doubles : res) {
        collector.putDouble(ts[index], doubles[0]);
        index += doubles.length;
      }
    } else if (output.equals("all")) {
      for (double[] doubles : res) {
        for (double aDouble : doubles) {
          collector.putDouble(ts[index], aDouble);
          index++;
        }
      }
    }
  }
}
